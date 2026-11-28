#!/usr/bin/env python3
"""Build per-system indices using only Libretro GitHub tree listings (no image downloads)."""
import argparse, collections, gzip, hashlib, http.client, json, os, re, sys, time, unicodedata, urllib.error, urllib.request
from functools import lru_cache
from itertools import pairwise
from pathlib import Path

REPOS={'ps1':['Sony_-_PlayStation'],'ps2':['Sony_-_PlayStation_2'],'ps3':['Sony_-_PlayStation_3','Sony_-_PlayStation_3_Downloadable'],'ps4':['Sony_-_PlayStation_4'],'psp':['Sony_-_PlayStation_Portable'],'psvita':['Sony_-_PlayStation_Vita']}
REGIONS={'usa':'us','europe':'eu','japan':'jp','world':'world','asia':'as','australia':'au','korea':'kr','china':'cn','brazil':'br','france':'fr','germany':'de','italy':'it','spain':'es','uk':'eu','canada':'us','russia':'ru','taiwan':'as'}
@lru_cache(maxsize=32768)
def norm(s):
    s=re.sub('[™®©]',' ',s)
    s=''.join(c for c in unicodedata.normalize('NFKD',s) if not unicodedata.combining(c))
    return re.sub('[^a-z0-9]+',' ',s.lower()).strip()
@lru_cache(maxsize=32768)
def title_region(filename, repo=''):
    name=filename[:-4]; region=''
    while True:
        m=re.search(r'\s+\(([^()]*)\)$',name)
        if not m:break
        tag=m.group(1).lower(); parts=[p.strip() for p in tag.split(',')]
        if parts and all(p in REGIONS for p in parts):
            region=REGIONS[parts[0]];name=name[:m.start()];break
        if tag=='psn' or (tag=='psp' and 'Portable' in repo) or (tag=='ps3' and 'PlayStation_3' in repo) or re.fullmatch(r'(?:rev\s+[a-z0-9.]+|disc\s+\d+|v\d+(?:\.\d+)*|[a-z]{2}(?:,[a-z]{2})+)',tag):
            name=name[:m.start()];continue
        return None
    if re.search(r'\((?:beta|proto|demo|sample|test|unl|hack|pirate)',name,re.I):return None
    name=re.sub(r'^(.+), (The|A|An)$',r'\2 \1',name,flags=re.I)
    return norm(name),region

def build_rows(repos,trees):
    result=collections.defaultdict(lambda:collections.defaultdict(list))
    for repo,tree in zip(repos,trees):
        for item in tree:
            if item.get('type')!='blob':continue
            path=item.get('path',''); parts=path.split('/')
            if len(parts)!=2 or parts[0] not in ('Named_Boxarts','Named_Snaps') or not parts[1].lower().endswith('.png'):continue
            parsed=title_region(parts[1],repo);
            if not parsed:continue
            name,region=parsed
            if not name:continue
            kind='front' if parts[0]=='Named_Boxarts' else 'screenshot'
            row=[repo,path,region]; rows=result[name][kind]
            if row not in rows and len(rows)<20:rows.append(row)
    return result

ART_DIRS=('Named_Boxarts','Named_Snaps')
RETRYABLE={408,429,500,502,503,504}
CACHE_DIR=Path('.cache/libretro-index')
MAX_TREE_BYTES=64*1024*1024
MAX_CACHE_BYTES=32*1024*1024


def fetch_tree(repo,ref):
    url=f'https://api.github.com/repos/libretro-thumbnails/{repo}/git/trees/{ref}'
    headers={'User-Agent':'NPS-artwork-index-builder','Accept':'application/vnd.github+json','X-GitHub-Api-Version':'2022-11-28'}
    token=os.environ.get('GITHUB_TOKEN')
    if token:headers['Authorization']='Bearer '+token
    attempt=0
    while True:
        attempt+=1
        try:
            request=urllib.request.Request(url,headers=headers)
            with urllib.request.urlopen(request,timeout=45) as response:
                raw=response.read(MAX_TREE_BYTES+1)
            if len(raw)>MAX_TREE_BYTES: raise RuntimeError(f'Tree response too large: {repo}/{ref}')
            data=json.loads(raw)
            if not isinstance(data,dict) or data.get('truncated') or not isinstance(data.get('tree'),list):
                raise RuntimeError(f'Incomplete tree: {repo}/{ref}')
            if any(not isinstance(item,dict) or not isinstance(item.get('path'),str) for item in data['tree']):
                raise RuntimeError(f'Invalid tree entries: {repo}/{ref}')
            return data['tree']
        except urllib.error.HTTPError as error:
            status=error.code
            rate_limited=status==403 and (error.headers.get('Retry-After') or error.headers.get('X-RateLimit-Remaining')=='0')
            if status in (401,403,404) and not rate_limited and 'Authorization' in headers:
                headers.pop('Authorization');attempt-=1;error.close();continue
            retryable=status in RETRYABLE or rate_limited
            retry_after=error.headers.get('Retry-After','')
            reset=error.headers.get('X-RateLimit-Reset','')
            delay=max(2**attempt,int(retry_after) if retry_after.isdigit() else 0)
            if rate_limited and reset.isdigit():delay=max(delay,int(reset)-int(time.time()))
            error.close()
            if not retryable or attempt>=5 or delay>120:
                raise RuntimeError(f'GitHub HTTP {status}: {repo}/{ref}') from error
            reason=f'HTTP {status}'
        except (urllib.error.URLError,TimeoutError,http.client.HTTPException,OSError,json.JSONDecodeError) as error:
            if attempt>=5:raise RuntimeError(f'GitHub request failed: {repo}/{ref}') from error
            delay=2**attempt;reason=type(error).__name__
        print(f'{repo}/{ref}: {reason}; retry {attempt+1}/5 in {delay}s',file=sys.stderr,flush=True)
        time.sleep(delay)

def valid_png(path):
    return (isinstance(path,str) and 0 < len(path) <= 4096 and '/' not in path and '\\' not in path
            and not any(ord(c)<32 for c in path) and path.lower().endswith('.png'))


def folder_files(repo, folder, sha, cache_dir, force=False):
    path=cache_dir/repo/(folder+'.json.gz')
    if not force:
        try:
            if path.stat().st_size > MAX_CACHE_BYTES: raise ValueError('cache size')
            with gzip.open(path,'rb') as handle: raw=handle.read(MAX_TREE_BYTES+1)
            if len(raw)>MAX_TREE_BYTES: raise ValueError('cache size')
            data=json.loads(raw)
            files=data.get('files')
            if (data.get('version')==1 and data.get('sha')==sha and isinstance(files,list)
                    and (folder != 'Named_Boxarts' or files)
                    and all(valid_png(name) for name in files) and all(a < b for a,b in pairwise(files))):
                print(f'{repo}/{folder}: unchanged SHA; {len(files)} cached filenames',flush=True)
                return files
        except (OSError, EOFError, ValueError, AttributeError, TypeError): pass
    tree=fetch_tree(repo,sha)
    files=sorted({item['path'] for item in tree if item.get('type')=='blob' and valid_png(item.get('path'))})
    if folder=='Named_Boxarts' and not files: raise RuntimeError(f'Empty Named_Boxarts: {repo}')
    try:
        path.parent.mkdir(parents=True,exist_ok=True)
        tmp=path.with_suffix('.tmp')
        try:
            raw=json.dumps({'version':1,'sha':sha,'files':files},ensure_ascii=False,separators=(',',':')).encode('utf-8')
            if len(raw)<=MAX_TREE_BYTES:
                with gzip.open(tmp,'wb',compresslevel=3) as handle: handle.write(raw)
                if tmp.stat().st_size<=MAX_CACHE_BYTES: tmp.replace(path)
        finally: tmp.unlink(missing_ok=True)
    except OSError as error:
        print(f'::warning::Could not save {repo}/{folder} cache: {error}',flush=True)
    return files


def folder_snapshot(repo):
    print(f'Fetching {repo}',flush=True)
    root=fetch_tree(repo,'master')
    folders={item['path']:item['sha'] for item in root if item.get('type')=='tree' and item.get('path') in ART_DIRS and re.fullmatch(r'[0-9a-f]{40}',str(item.get('sha','')))}
    if 'Named_Boxarts' not in folders:raise RuntimeError(f'Missing Named_Boxarts: {repo}')
    return folders


def download(repo, cache_dir=CACHE_DIR, force=False, folders=None):
    if folders is None: folders=folder_snapshot(repo)
    result=[]
    for folder in ART_DIRS:
        if folder not in folders:continue
        files=folder_files(repo,folder,folders[folder],cache_dir,force)
        result.extend({'type':'blob','path':folder+'/'+name} for name in files)
    if not any(item['path'].startswith('Named_Boxarts/') for item in result):
        raise RuntimeError(f'Empty Named_Boxarts: {repo}')
    return result


def coverage(titles):
    front = screenshots = front_rows = screenshot_rows = 0
    for record in titles.values():
        n = len(record.get('front', [])); m = len(record.get('screenshot', []))
        front += bool(n); screenshots += bool(m)
        front_rows += n; screenshot_rows += m
    return {'titles':len(titles), 'front':front, 'screenshot':screenshots,
            'front rows':front_rows, 'screenshot rows':screenshot_rows}


def digest(path):
    h=hashlib.sha256()
    with path.open('rb') as f:
        for chunk in iter(lambda:f.read(1024*1024),b''): h.update(chunk)
    return h.hexdigest()


def load_manifest(path):
    try:
        if path.stat().st_size>65536: return {}
        data=json.loads(path.read_text(encoding='utf-8'))
        return data if isinstance(data,dict) and data.get('version')==1 else {}
    except (OSError,ValueError,TypeError): return {}


def save_manifest(path,snapshots,script_hash,output_hash):
    path.parent.mkdir(parents=True,exist_ok=True)
    tmp=path.with_suffix('.tmp')
    try:
        tmp.write_text(json.dumps({'version':1,'folders':snapshots,'script':script_hash,'output':output_hash},sort_keys=True,separators=(',',':')),encoding='utf-8')
        tmp.replace(path)
    finally: tmp.unlink(missing_ok=True)


def write_index(out,platform,rows):
    if not rows or not any(v.get('front') for v in rows.values()):
        raise ValueError('No usable boxart titles; previous index preserved')
    data={'version':1,'platform':platform,'titles':rows}
    encoded=json.dumps(data,ensure_ascii=False,sort_keys=True,separators=(',',':')).encode('utf-8')
    if out.exists():
        previous=out.read_bytes()
        if previous==encoded: return False
        old=json.loads(previous.decode('utf-8-sig'))
        if old.get('version')!=1 or old.get('platform')!=platform or not isinstance(old.get('titles'),dict):
            raise ValueError('Unexpected previous index format')
        before,after=coverage(old['titles']),coverage(rows)
        for kind,count in before.items():
            if count and after[kind]<count*.85:
                raise ValueError(f'{kind} coverage dropped ({count} -> {after[kind]}); previous index preserved')
    tmp=out.with_suffix('.tmp')
    try:
        tmp.write_bytes(encoded);tmp.replace(out)
    finally: tmp.unlink(missing_ok=True)
    return True


def build(output,platforms=None,cache_dir=CACHE_DIR,force=False):
    output.mkdir(parents=True,exist_ok=True)
    failures=[]
    script_hash=digest(Path(__file__))
    for platform in dict.fromkeys(platforms or REPOS):
        try:
            repos=REPOS[platform]
            out=output/(platform+'.json'); manifest=cache_dir/(platform+'.manifest.json')
            snapshots={repo:folder_snapshot(repo) for repo in repos}
            state=load_manifest(manifest) if not force else {}
            if (state.get('folders')==snapshots and state.get('script')==script_hash and out.is_file()
                    and state.get('output')==digest(out)):
                print(f'{platform}: unchanged folder SHAs and output; index reconstruction skipped',flush=True)
                continue
            rows=build_rows(repos,(download(repo,cache_dir,force,snapshots[repo]) for repo in repos))
            changed=write_index(out,platform,rows)
            try: save_manifest(manifest,snapshots,script_hash,digest(out))
            except OSError as error: print(f'::warning::Could not save {platform} manifest: {error}',flush=True)
            print(platform,len(rows),out.stat().st_size,'updated' if changed else 'unchanged',flush=True)
        except (OSError,ValueError,RuntimeError,TypeError,AttributeError) as error:
            failures.append(platform)
            print(f'::warning::{platform}: {error}; previous index preserved',flush=True)
    if failures: raise SystemExit('Incomplete Libretro refresh: '+', '.join(failures))


def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('output_dir',type=Path)
    p.add_argument('--platform',choices=REPOS,action='append');p.add_argument('--cache',type=Path,default=CACHE_DIR)
    p.add_argument('--force',action='store_true');a=p.parse_args()
    build(a.output_dir,a.platform,a.cache,a.force)
if __name__=='__main__':main()
