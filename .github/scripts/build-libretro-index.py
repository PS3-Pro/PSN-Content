#!/usr/bin/env python3
"""Build per-system indices using only Libretro GitHub tree listings (no image downloads)."""
import argparse, collections, http.client, json, os, re, sys, time, unicodedata, urllib.error, urllib.request
from pathlib import Path

REPOS={'ps1':['Sony_-_PlayStation'],'ps2':['Sony_-_PlayStation_2'],'ps3':['Sony_-_PlayStation_3','Sony_-_PlayStation_3_Downloadable'],'ps4':['Sony_-_PlayStation_4'],'psp':['Sony_-_PlayStation_Portable'],'psvita':['Sony_-_PlayStation_Vita']}
REGIONS={'usa':'us','europe':'eu','japan':'jp','world':'world','asia':'as','australia':'au','korea':'kr','china':'cn','brazil':'br','france':'fr','germany':'de','italy':'it','spain':'es','uk':'eu','canada':'us','russia':'ru','taiwan':'as'}
def norm(s):
    s=re.sub('[™®©]',' ',s)
    s=''.join(c for c in unicodedata.normalize('NFKD',s) if not unicodedata.combining(c))
    return re.sub('[^a-z0-9]+',' ',s.lower()).strip()
def title_region(filename, repo=''):
    name=filename[:-4]; region=''; tags=[]
    while True:
        m=re.search(r'\s+\(([^()]*)\)$',name)
        if not m:break
        tag=m.group(1).lower(); parts=[p.strip() for p in tag.split(',')]
        if parts and all(p in REGIONS for p in parts):
            region=REGIONS[parts[0]];name=name[:m.start()];break
        if tag=='psn' or (tag=='psp' and 'Portable' in repo) or (tag=='ps3' and 'PlayStation_3' in repo) or re.fullmatch(r'(?:rev\s+[a-z0-9.]+|disc\s+\d+|v\d+(?:\.\d+)*|[a-z]{2}(?:,[a-z]{2})+)',tag):
            tags.append(tag);name=name[:m.start()];continue
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
            with urllib.request.urlopen(request,timeout=45) as response:data=json.load(response)
            if data.get('truncated') or not isinstance(data.get('tree'),list):
                raise RuntimeError(f'Incomplete tree: {repo}/{ref}')
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

def download(repo):
    print(f'Fetching {repo}',flush=True)
    root=fetch_tree(repo,'master')
    folders={item['path']:item['sha'] for item in root if item.get('type')=='tree' and item.get('path') in ART_DIRS and re.fullmatch(r'[0-9a-f]{40}',str(item.get('sha','')))}
    if 'Named_Boxarts' not in folders:raise RuntimeError(f'Missing Named_Boxarts: {repo}')
    result=[]
    for folder in ART_DIRS:
        if folder not in folders:continue
        tree=fetch_tree(repo,folders[folder])
        for item in tree:
            path=item.get('path','')
            if item.get('type')=='blob' and '/' not in path and path.lower().endswith('.png'):
                result.append({'type':'blob','path':folder+'/'+path})
    if not any(item['path'].startswith('Named_Boxarts/') for item in result):
        raise RuntimeError(f'Empty Named_Boxarts: {repo}')
    return sorted(result,key=lambda item:item['path'])

def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('output_dir',type=Path);p.add_argument('--platform',choices=REPOS,action='append');a=p.parse_args();a.output_dir.mkdir(parents=True,exist_ok=True)
    for platform in a.platform or REPOS:
        repos=REPOS[platform]; rows=build_rows(repos,[download(repo) for repo in repos]);out=a.output_dir/(platform+'.json');tmp=out.with_suffix('.tmp');tmp.write_text(json.dumps({'version':1,'platform':platform,'titles':rows},ensure_ascii=False,sort_keys=True,separators=(',',':')),encoding='utf-8');tmp.replace(out);print(platform,len(rows),out.stat().st_size)
if __name__=='__main__':main()
