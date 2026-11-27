#!/usr/bin/env python3
"""Build compact per-platform art indexes from the official LaunchBox Metadata.zip."""
import argparse
import collections
import datetime
import json
import re
import unicodedata
import xml.etree.ElementTree as ET
import zipfile
from pathlib import Path

PLATFORMS = {'Sony PlayStation':'ps1','Sony PlayStation 2':'ps2','Sony PlayStation 3':'ps3','Sony PlayStation 4':'ps4','Sony PSP':'psp','Sony PlayStation Portable':'psp','Sony PlayStation Vita':'psvita','NEC TurboGrafx-16':'pce','NEC PC Engine':'pce','SNK Neo Geo AES':'neogeo'}
TYPES = {'Box - Front':'front','Box - Back':'back','Box - Spine':'spine','Box - Full':'texture','Cart - Front':'cart','Box - 3D':'box3d','Disc':'disc','Disc - Front':'disc','Fanart - Background':'background'}
FILE = re.compile(r'[A-Za-z0-9_-]{1,100}\.(?:png|jpe?g|webp)', re.I)

def norm(value):
    value = re.sub('[™®©]', ' ', value)
    value = ''.join(c for c in unicodedata.normalize('NFKD', value) if not unicodedata.combining(c))
    return re.sub('[^a-z0-9]+', ' ', value.lower()).strip()

PLATFORMS = {norm(name): platform for name, platform in PLATFORMS.items()}
TYPES = {norm(name): kind for name, kind in TYPES.items()}

def records(z, members):
    for member in members:
        with z.open(member) as stream:
            stack=[]
            for event, elem in ET.iterparse(stream, events=('start','end')):
                if event=='start':
                    stack.append(elem)
                    continue
                if len(stack)==2:
                    yield elem
                    stack[-2].remove(elem)
                    elem.clear()
                stack.pop()

def metadata(e):
    result={}
    for tag,key in [('Developer','developers'),('Publisher','publishers'),('Genre','genres')]:
        values=list(dict.fromkeys(v.strip()[:120] for v in e.findtext(tag,'').split(';') if v.strip() and v.strip().lower() not in ('unknown','n/a','none')))
        if values: result[key]=values[:6]
    raw_date=(e.findtext('ReleaseDate','') or '').strip()
    year=re.match(r'(?:19|20)\d{2}',raw_date or e.findtext('ReleaseYear',''))
    if year: result['year']=year.group()
    full=re.match(r'^((?:19|20)\d{2}-\d{2}-\d{2})(?:$|[T ])',raw_date)
    if full:
        try: result['releaseDate']=datetime.date.fromisoformat(full.group(1)).isoformat()
        except ValueError: pass
    modes=[]
    for mode in re.split(r'[;,/]',e.findtext('PlayMode','')):
        value=re.sub(r'[^a-z]','',mode.lower())
        label={'singleplayer':'Single Player','multiplayer':'Multiplayer','coop':'Co-op','cooperative':'Co-op','splitscreen':'Split Screen','pvp':'PvP'}.get(value)
        if label and label not in modes: modes.append(label)
    if modes: result['modes']=modes
    return result

def build(source, target):
    games={}; meta={}; aliases=collections.defaultdict(set); art=collections.defaultdict(lambda:collections.defaultdict(list))
    platform_counts=collections.Counter(); matched_counts=collections.Counter(); rejected_images=collections.Counter()
    with zipfile.ZipFile(source) as z:
        members=[n for n in z.namelist() if Path(n).name.lower()=='metadata.xml']
        if not members: raise ValueError('Metadata.xml not found in archive')
        for e in records(z,members):
            if e.tag=='Game':
                raw_platform=e.findtext('Platform','').strip()
                platform_counts[raw_platform]+=1
                platform=PLATFORMS.get(norm(raw_platform)); ident=e.findtext('DatabaseID','').strip(); name=norm(e.findtext('Name',''))
                if platform and ident.isdigit() and name:
                    games[ident]=(platform,name); aliases[ident].add(name); meta[ident]=metadata(e); matched_counts[platform]+=1
        for e in records(z,members):
            ident=e.findtext('DatabaseID','').strip()
            if ident not in games: continue
            if e.tag=='GameAlternateName':
                name=norm(e.findtext('AlternateName',''))
                if name: aliases[ident].add(name)
            elif e.tag=='GameImage':
                kind=TYPES.get(norm(e.findtext('Type',''))); file=e.findtext('FileName','').strip(); region=e.findtext('Region','').strip()
                if kind and not FILE.fullmatch(file): rejected_images[games[ident][0]]+=1
                if kind and FILE.fullmatch(file):
                    row=[file,region]
                    if row not in art[ident][kind] and len(art[ident][kind])<24: art[ident][kind].append(row)
    print('LaunchBox platform names:', ', '.join(f'{name!r}: {count}' for name,count in sorted(platform_counts.items()) if 'sony' in name.casefold() or norm(name) in PLATFORMS), flush=True)
    if not games: raise ValueError('No supported games found; check Metadata.xml schema')
    indexes=collections.defaultdict(dict)
    for ident,(platform,name) in games.items():
        for alias in aliases[ident]:
            index=indexes[platform]
            if alias in index and index[alias]!=ident: index[alias]=None
            else: index[alias]=ident
    target.mkdir(parents=True,exist_ok=True)
    for platform in sorted(set(PLATFORMS.values())):
        keys={k:v for k,v in indexes[platform].items() if v is not None and (art.get(v) or meta.get(v))}
        ids=set(keys.values()); images={v:art[v] for v in ids if art.get(v)}
        payload={'version':1,'platform':platform,'titles':keys,'art':images,'meta':{v:meta[v] for v in ids if meta.get(v)}}
        path=target/(platform+'.json'); tmp=path.with_suffix('.tmp')
        tmp.write_text(json.dumps(payload,ensure_ascii=False,sort_keys=True,separators=(',',':')),encoding='utf-8'); tmp.replace(path)
        print(f'{platform}: {matched_counts[platform]} matched games, {len(keys)} titles, {sum(bool(v.get("front")) for v in images.values())} covers, {sum(bool(v.get("background")) for v in images.values())} fanarts, {rejected_images[platform]} rejected image filenames / {path.stat().st_size:,} bytes', flush=True)

if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__); p.add_argument('metadata_zip',type=Path); p.add_argument('output_dir',type=Path); a=p.parse_args(); build(a.metadata_zip,a.output_dir)
