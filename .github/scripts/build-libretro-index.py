#!/usr/bin/env python3
"""Build per-system indices using only Libretro GitHub tree listings (no image downloads)."""
import argparse, collections, json, os, re, unicodedata, urllib.request
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

def download(repo):
    headers={'User-Agent':'NPS-artwork-index-builder','Accept':'application/vnd.github+json'}
    if os.environ.get('GITHUB_TOKEN'):headers['Authorization']='Bearer '+os.environ['GITHUB_TOKEN']
    request=urllib.request.Request('https://api.github.com/repos/libretro-thumbnails/'+repo+'/git/trees/master?recursive=1',headers=headers)
    with urllib.request.urlopen(request,timeout=45) as response:data=json.load(response)
    if data.get('truncated') or not isinstance(data.get('tree'),list):raise RuntimeError('Incomplete tree: '+repo)
    return data['tree']

def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('output_dir',type=Path);p.add_argument('--platform',choices=REPOS,action='append');a=p.parse_args();a.output_dir.mkdir(parents=True,exist_ok=True)
    for platform in a.platform or REPOS:
        repos=REPOS[platform]; rows=build_rows(repos,[download(repo) for repo in repos]);out=a.output_dir/(platform+'.json');tmp=out.with_suffix('.tmp');tmp.write_text(json.dumps({'version':1,'platform':platform,'titles':rows},ensure_ascii=False,sort_keys=True,separators=(',',':')),encoding='utf-8');tmp.replace(out);print(platform,len(rows),out.stat().st_size)
if __name__=='__main__':main()
