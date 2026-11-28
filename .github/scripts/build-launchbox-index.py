#!/usr/bin/env python3
"""Build compact per-platform art indexes from the official LaunchBox Metadata.zip."""
import argparse
import collections
import datetime
import hashlib
import tempfile
import json
import re
import unicodedata
import xml.etree.ElementTree as ET
import zipfile
from functools import lru_cache
from pathlib import Path

PLATFORMS = {'Sony PlayStation':'ps1','Sony PlayStation 2':'ps2','Sony PlayStation 3':'ps3','Sony PlayStation 4':'ps4','Sony PSP':'psp','Sony PlayStation Portable':'psp','Sony PlayStation Vita':'psvita','NEC TurboGrafx-16':'pce','NEC PC Engine':'pce','SNK Neo Geo AES':'neogeo'}
TYPES = {'Box - Front':'front','Box - Back':'back','Box - Spine':'spine','Box - Full':'texture','Cart - Front':'cart','Box - 3D':'box3d','Disc':'disc','Disc - Front':'disc','Fanart - Background':'background'}
FILE = re.compile(r'[A-Za-z0-9_-]{1,100}\.(?:png|jpe?g|webp)', re.I)

@lru_cache(maxsize=32768)
def norm(value):
    value = re.sub('[™®©]', ' ', value)
    value = ''.join(c for c in unicodedata.normalize('NFKD', value) if not unicodedata.combining(c))
    return re.sub('[^a-z0-9]+', ' ', value.lower()).strip()

PLATFORMS = {norm(name): platform for name, platform in PLATFORMS.items()}
OUTPUT_PLATFORMS = tuple(sorted(set(PLATFORMS.values())))
CACHE_PATH = Path(".cache/launchbox-index/manifest.json")
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

def digest(path):
    h = hashlib.sha256()
    with path.open('rb') as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b''):
            h.update(chunk)
    return h.hexdigest()


def unchanged(source_hash, target, cache_path):
    try:
        if cache_path.stat().st_size > 65536:
            return False
        state = json.loads(cache_path.read_text(encoding='utf-8'))
        return (state.get('version') == 1 and state.get('source') == source_hash
                and state.get('script') == digest(Path(__file__))
                and all(state['outputs'].get(platform) == digest(target / (platform + '.json'))
                        for platform in OUTPUT_PLATFORMS))
    except (OSError, ValueError, TypeError, AttributeError, KeyError):
        return False


def save_manifest(source_hash, output_hashes, cache_path):
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    state = {'version': 1, 'source': source_hash, 'script': digest(Path(__file__)),
             'outputs': output_hashes}
    temp = cache_path.with_suffix('.tmp')
    try:
        temp.write_text(json.dumps(state, separators=(',', ':')), encoding='utf-8')
        temp.replace(cache_path)
    finally:
        temp.unlink(missing_ok=True)


def coverage(data):
    result = {'titles': len(data['titles']), 'metadata': len(data['meta']), 'artwork': len(data['art'])}
    kinds = set(TYPES.values())
    fields = {field: 'meta:'+field for field in ('developers','publishers','genres','year','releaseDate','modes')}
    result.update(dict.fromkeys(kinds, 0))
    result.update(dict.fromkeys(fields.values(), 0))
    for record in data['art'].values():
        for kind, value in record.items():
            if value and kind in kinds: result[kind] += 1
    for record in data['meta'].values():
        for field, value in record.items():
            if value and field in fields: result[fields[field]] += 1
    return result


def verify_coverage(path, data):
    if not path.exists():
        return None
    previous = path.read_bytes()
    old = json.loads(previous.decode('utf-8-sig'))
    if old.get('version') != 1 or old.get('platform') != data['platform']:
        raise ValueError(f'Unexpected previous index format: {path.name}')
    before, after = coverage(old), coverage(data)
    for kind, count in before.items():
        if count and after[kind] < count * .85:
            raise ValueError(f'{path.name}: {kind} coverage dropped ({count} -> {after[kind]}); previous indexes preserved')
    return hashlib.sha256(previous).hexdigest()


def build(source, target, cache_path=CACHE_PATH, force=False):
    source_hash = digest(source)
    if not force and unchanged(source_hash, target, cache_path):
        print('LaunchBox source and all output hashes unchanged; XML parsing skipped', flush=True)
        return
    games={}; meta={}; aliases=collections.defaultdict(set); art=collections.defaultdict(lambda:collections.defaultdict(list))
    platform_counts=collections.Counter(); matched_counts=collections.Counter(); rejected_images=collections.Counter()
    with zipfile.ZipFile(source) as z:
        members=[n for n in z.namelist() if Path(n).name.lower()=='metadata.xml']
        if not members: raise ValueError('Metadata.xml not found in archive')
        for e in records(z,members):
            if e.tag=='Game':
                raw_platform=e.findtext('Platform','').strip()
                platform_counts[raw_platform]+=1
                platform=PLATFORMS.get(norm(raw_platform))
                if not platform: continue
                ident=e.findtext('DatabaseID','').strip()
                if not ident.isdigit(): continue
                name=norm(e.findtext('Name',''))
                if name:
                    if ident in games and games[ident][0] != platform:
                        raise ValueError(f'Conflicting platform for DatabaseID {ident}')
                    games[ident]=(platform,name); aliases[ident].add(name); meta[ident]=metadata(e); matched_counts[platform]+=1
        for e in records(z,members):
            if e.tag not in ('GameAlternateName', 'GameImage'): continue
            ident=e.findtext('DatabaseID','').strip()
            if ident not in games: continue
            if e.tag=='GameAlternateName':
                name=norm(e.findtext('AlternateName',''))
                if name: aliases[ident].add(name)
            elif e.tag=='GameImage':
                kind=TYPES.get(norm(e.findtext('Type',''))); file=e.findtext('FileName','').strip(); region=e.findtext('Region','').strip()
                if not kind: continue
                if not FILE.fullmatch(file):
                    rejected_images[games[ident][0]]+=1
                    continue
                rows=art[ident][kind]
                if len(rows)<24:
                    row=[file,region]
                    if row not in rows: rows.append(row)
    print('LaunchBox platform names:', ', '.join(f'{name!r}: {count}' for name,count in sorted(platform_counts.items()) if 'sony' in name.casefold() or norm(name) in PLATFORMS), flush=True)
    if not games: raise ValueError('No supported games found; check Metadata.xml schema')
    indexes=collections.defaultdict(dict)
    for ident,(platform,name) in games.items():
        index=indexes[platform]
        for alias in aliases[ident]:
            if alias in index and index[alias]!=ident: index[alias]=None
            else: index[alias]=ident
    target.mkdir(parents=True,exist_ok=True)
    # Validate every platform before replacing any output. Stage on the same filesystem.
    output_hashes = {}
    with tempfile.TemporaryDirectory(prefix='.launchbox-', dir=target) as stage:
        staged = []
        for platform in OUTPUT_PLATFORMS:
            keys={k:v for k,v in indexes[platform].items() if v is not None and (art.get(v) or meta.get(v))}
            ids=set(keys.values()); images={v:art[v] for v in ids if art.get(v)}
            payload={'version':1,'platform':platform,'titles':keys,'art':images,'meta':{v:meta[v] for v in ids if meta.get(v)}}
            path=target/(platform+'.json'); temp=Path(stage)/path.name
            previous_hash = verify_coverage(path,payload)
            with temp.open('w',encoding='utf-8',newline='\n') as handle:
                json.dump(payload,handle,ensure_ascii=False,sort_keys=True,separators=(',',':'))
            current_hash = digest(temp)
            output_hashes[platform] = current_hash
            staged.append((temp,path,previous_hash != current_hash))
            print(f'{platform}: {matched_counts[platform]} matched games, {len(keys)} titles, {sum(bool(v.get("front")) for v in images.values())} covers, {sum(bool(v.get("background")) for v in images.values())} fanarts, {rejected_images[platform]} rejected image filenames / {temp.stat().st_size:,} bytes', flush=True)
        if digest(source) != source_hash:
            raise ValueError('Metadata archive changed during parsing; previous indexes preserved')
        for temp,path,changed in staged:
            if changed:
                temp.replace(path)
    try:
        save_manifest(source_hash,output_hashes,cache_path)
    except OSError as error:
        print(f'::warning::LaunchBox manifest not saved: {error}',flush=True)

if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('metadata_zip',type=Path); p.add_argument('output_dir',type=Path)
    p.add_argument('--cache',type=Path,default=CACHE_PATH); p.add_argument('--force',action='store_true')
    a=p.parse_args(); build(a.metadata_zip,a.output_dir,a.cache,a.force)
