#!/usr/bin/env python3
"""Index public original game-manual PDFs; no PDF downloads or redistribution."""
import argparse
import collections
import json
import re
import time
import unicodedata
from pathlib import Path
from urllib.parse import urlencode, quote
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError

MAX_PDF = 16 * 1024 * 1024
MAX_JSON = 32 * 1024 * 1024
COLLECTIONS = {'ps1': 'SonyPlaystationManuals', 'ps2': 'SonyPlaystation2Manuals_201812', 'psp': 'SonyPSPManuals'}
PLATFORMS = {'ps3': r'PS3|(?:Sony\s+)?PlayStation\s*3', 'ps4': r'PS4|(?:Sony\s+)?PlayStation\s*4', 'psvita': r'PS\s*Vita|(?:Sony\s+)?PlayStation\s*Vita'}
REGIONS = {'usa':'us','us':'us','north america':'us','europe':'eu','eur':'eu','eu':'eu','uk':'eu','japan':'jp','jp':'jp','jpn':'jp','australia':'au','world':'world','asia':'asi','korea':'kr','brazil':'br'}
ID = re.compile(r'[A-Za-z0-9][A-Za-z0-9_.-]{0,179}\Z')
REJECT = re.compile(r'\b(?:strategy guide|walkthrough|quick start|safety|instruction sheet|artbook|card book|soundtrack|demo|sample|preview|insert|leaflet|service manual|console manual|controller manual)\b', re.I)


def norm(s):
    s = re.sub('[™®©]', ' ', s)
    s = ''.join(c for c in unicodedata.normalize('NFKD',s) if not unicodedata.combining(c))
    return re.sub('[^a-z0-9]+',' ',s.lower()).strip()


def split_name(name):
    name = name.strip()
    area = ''
    m = re.search(r'\s*[\[(]([^()\[\]]+)[\])]\s*$',name)
    if m and norm(m.group(1)) in REGIONS:
        area = REGIONS[norm(m.group(1))]; name = name[:m.start()].strip()
    if not name or REJECT.search(name) or re.search(r'[\[\]()]',name):
        return None
    article=re.search(r',\s*(The|A|An)$',name,re.I)
    if article: name=article.group(1)+' '+name[:article.start()]
    key = norm(name)
    return (key,area) if len(key)>=2 else None


def get_json(path, query=None):
    url='https://archive.org/'+path+(('?'+urlencode(query,doseq=True)) if query else '')
    for attempt in range(3):
        try:
            with urlopen(Request(url,headers={'User-Agent':'PS3-Pro manual index (metadata only)','Accept':'application/json'}),timeout=25) as response:
                raw=response.read(MAX_JSON+1)
            if len(raw)>MAX_JSON: raise ValueError('metadata limit exceeded')
            return json.loads(raw)
        except (HTTPError,URLError,TimeoutError):
            if attempt==2: raise
            time.sleep(2**(attempt+1))


def original_pdfs(meta):
    if not isinstance(meta,dict) or not isinstance(meta.get('files'),list): raise ValueError('invalid IA metadata')
    if str(meta.get('metadata',{}).get('access-restricted-item','')).lower() in ('true','1','yes'): return []
    for f in meta['files']:
        name=f.get('name',''); size=str(f.get('size',''))
        if (not isinstance(name,str) or not name.lower().endswith('.pdf') or len(name)>240 or '/' in name or '\\' in name
            or any(ord(c)<32 for c in name) or f.get('source')!='original' or str(f.get('private','')).lower() in ('true','1','yes')
            or not size.isdigit() or not 0<int(size)<=MAX_PDF or REJECT.search(name) or re.search(r'_(?:text|bw)\.pdf$',name,re.I)): continue
        yield name,int(size)


def add(rows,ident,name,size,key,region):
    if ID.fullmatch(ident): rows[key].add((ident,name,region,size))


def collection_rows(ident, meta):
    rows=collections.defaultdict(set)
    for name,size in original_pdfs(meta):
        result=split_name(name[:-4])
        if result: add(rows,ident,name,size,*result)
    return rows


def individual_title(title,platform):
    # Only accept items whose title explicitly identifies a game manual AND platform.
    if not isinstance(title,str) or REJECT.search(title): return None
    region=''
    m=re.search(r'\s*[\[(]([^()\[\]]+)[\])]\s*$',title)
    if m and norm(m.group(1)) in REGIONS:
        region=REGIONS[norm(m.group(1))]; title=title[:m.start()].strip()
    p=PLATFORMS[platform]
    patterns=[rf'^(.+?)\s*[-–: ]*\s*\(\s*(?:{p})\s*\)\s*[-–: ]*\s*(?:game\s+)?manual$',
              rf'^(.+?)\s*[-–:]\s*(?:{p})\s+(?:game\s+)?manual$',
              rf'^(.+?)\s*[-–: ]+\s*(?:game\s+)?manual\s*\(\s*(?:{p})\s*\)$',
              rf'^(?:{p})\s+(?:game\s+)?manual\s*[-–:]\s*(.+)$',
              rf'^(.+?)\s*[-–:]\s*(?:game\s+)?manual\s*[-–:]\s*(?:{p})$']
    for pattern in patterns:
        m=re.fullmatch(pattern,title.strip(),re.I)
        if m:
            result=split_name(m.group(1).strip(' -–:'))
            if result and (not region or not result[1] or region==result[1]): return result[0],region or result[1]
    return None


def discovery_rows(platform):
    platform_words={'ps3':['PS3','PlayStation 3'],'ps4':['PS4','PlayStation 4'],'psvita':['PS Vita','PlayStation Vita']}[platform]
    query='mediatype:texts AND (collection:consolemanuals OR subject:"game manuals" OR subject:"video game manuals") AND title:manual AND ('+' OR '.join('title:"'+s+'"' for s in platform_words)+')'
    response=get_json('advancedsearch.php',{'q':query,'fl[]':['identifier','title'],'rows':500,'page':1,'output':'json'})['response']
    docs=response['docs']
    if response['numFound']>len(docs): raise ValueError('discovery results truncated; preserve previous index')
    rows=collections.defaultdict(set)
    eligible=[(d,individual_title(d.get('title'),platform)) for d in docs if ID.fullmatch(str(d.get('identifier','')))]
    eligible=[(d,m) for d,m in eligible if m]
    if len(eligible)>150: raise ValueError('discovery request budget exceeded')
    for d,(key,region) in eligible:
        ident=d['identifier']; files=list(original_pdfs(get_json('metadata/'+quote(ident,safe=''))))
        # Multiple PDFs can be different editions or supplements. Do not guess.
        if len(files)==1: add(rows,ident,*files[0],key,region)
        time.sleep(.15)
    return rows


def payload(platform,rows):
    # Avoid ambiguous matches for the same normalized title and territory.
    clean={}
    for key,values in sorted(rows.items()):
        by_region=collections.defaultdict(list)
        for value in sorted(values): by_region[value[2]].append(value)
        unique=[v[0] for v in by_region.values() if len(v)==1]
        if unique: clean[key]=sorted(unique)
    return {'version':1,'platform':platform,'titles':clean}


def build(output):
    output.mkdir(parents=True,exist_ok=True); updated=0
    for platform in [*COLLECTIONS,*PLATFORMS]:
        path=output/(platform+'.json')
        try:
            ident=COLLECTIONS.get(platform)
            rows=collection_rows(ident,get_json('metadata/'+ident)) if ident else discovery_rows(platform)
            data=payload(platform,rows); count=len(data['titles'])
            old=len(json.loads(path.read_text())['titles']) if path.exists() else 0
            if not count or count<old*.85: raise ValueError(f'empty or reduced index ({count}, previous {old})')
            encoded=json.dumps(data,ensure_ascii=False,sort_keys=True,separators=(',',':'))+'\n'
            if len(encoded.encode('utf-8'))>4*1024*1024: raise ValueError('index size limit exceeded')
            tmp=path.with_suffix('.tmp'); tmp.write_text(encoded,encoding='utf-8');tmp.replace(path)
            updated+=1; print(f'{platform}: {count} titles / {path.stat().st_size} bytes',flush=True)
        except Exception as exc:
            print(f'::warning::{platform}: {type(exc).__name__}: {exc}; previous index preserved',flush=True)
    if not updated: raise SystemExit('No manual indexes updated')

if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('output_dir',type=Path);build(parser.parse_args().output_dir)
