#!/usr/bin/env python3
import csv
import datetime as dt
import gzip
import json
import os
import re
import time
import unicodedata
from collections import defaultdict, Counter
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError

ROOT = Path('.')
INDEX = Path('resources/database/helpers/game-metadata.json')
CACHE = Path('.cache/game-metadata/igdb.json.gz')
PLATFORMS = {'ps3': 9, 'ps2': 8, 'psx': 7, 'psp': 38, 'minis': 38, 'pce': 86, 'neogeo': 80}
FIELDS = 'name,alternative_names.name,platforms,category,genres.name,themes.name,involved_companies.company.name,involved_companies.developer,involved_companies.publisher,release_dates.platform,release_dates.date,release_dates.category,release_dates.region,release_dates.y,summary,aggregated_rating,aggregated_rating_count,screenshots,artworks,cover,game_modes.name'
TTL = 30 * 86400


def key(value):
    s = unicodedata.normalize('NFKC', str(value or '')).casefold()
    return ''.join(c for c in s if c.isalnum())


def title_key(value):
    s = str(value or '').replace('™', '').replace('®', '').replace('©', '')
    s = re.sub(r'[\[(]\s*(?:PS[123X]|PSP|Minis?|PS[12]\s*Classics?|PC\s*Engine|Turbo\s*Grafx(?:\s*-\s*16)?|Neo\s*Geo(?:\s*Station)?|English|French|German|Japanese|US|EU|JP)\s*[\])]', ' ', s, flags=re.I)
    s = re.sub(r'\s*[\[(]?\b(?:digital version|downloadable version|full game)\b[\])]?$','',s,flags=re.I)
    s = re.sub(r'^Neo\s*Geo\s*Station\s*[-:]?\s*','',s,flags=re.I)
    return key(s)


def platform(tid, name, cid):
    if 'NEOGEOSTATION' in cid.upper(): return 'neogeo'
    for pattern, tag in [(r'\(\s*PS2[^)]*\)|\bPS2\s*CLASSICS?\b','ps2'),(r'\(\s*PS[1X]\s*\)|\bPSONE\b','psx'),(r'\(\s*MINIS?\s*\)','minis'),(r'\(\s*PSP\s*\)','psp'),(r'\(\s*(?:PC\s*ENGINE|TURBO\s*GRAFX[^)]*)\)','pce'),(r'\bNEO\s*GEO(?:\s*STATION)?\b','neogeo')]:
        if re.search(pattern,name,re.I): return tag
    if tid[:2] in ('UC','UL'): return 'psp'
    if tid[:2] in ('SC','SL'): return 'ps2'
    if tid.startswith('NP'):
        r,t = tid[2:4]
        if t in 'ZX': return 'minis'
        if tid.startswith('NPUF') or tid.startswith('NPJJ3'): return 'pce'
        if t in 'GH': return 'psp'
        if t in 'DC': return 'ps2'
        if t in {'U':'JFI','J':'JI','E':'FE'}.get(r,''): return 'psx'
    return 'ps3'


def catalog():
    html = (ROOT/'files/website/modern.html').read_text(encoding='utf-8-sig')
    block = re.search(r'\bconst DICT_APPS_FILTER\s*=\s*\{(.*?)\};',html,re.S)
    if not block: raise ValueError('Browse exclusions not found')
    apps = set(re.findall(r'[\'\"]([A-Z0-9]{9})[\'\"]\s*:\s*1\b',block[1]))
    if not apps: raise ValueError('Empty Browse exclusions')
    result = defaultdict(set)
    base = ROOT/'resources/database/content/official'
    for path in [base/'games.tsv',base/'pending/games.tsv']:
        if not path.exists() and path.parent.name == 'pending': continue
        with path.open(encoding='utf-8-sig',newline='') as f:
            for row in csv.reader(f,delimiter='\t'):
                if len(row)<4: continue
                tid,region,name,pkg = (x.strip() for x in row[:4]); tid=tid.upper()
                if not re.fullmatch(r'[A-Z]{4}\d{5}',tid) or tid in apps or not name or name.upper()=='MISSING' or not pkg or pkg.upper()=='MISSING': continue
                if re.search(r'\b(?:demo|trial|beta|network test|soundtrack|strategy guide|firstplay|qore)\b',name,re.I): continue
                cid=row[5] if len(row)>5 else ''
                result[tid].add((platform(tid,name,cid),region.upper(),name))
    if not result: raise ValueError('Empty catalog')
    return result


def request(url, body, headers):
    for attempt in range(4):
        try:
            with urlopen(Request(url,data=body,headers=headers),timeout=45) as r:
                return json.loads(r.read(24*1024*1024+1))
        except HTTPError as e:
            if e.code not in (429,500,502,503,504) or attempt==3: raise RuntimeError(f'Provider HTTP {e.code}') from None
        except (URLError,TimeoutError):
            if attempt==3: raise RuntimeError('Provider unavailable') from None
        time.sleep(2**attempt)


def fetch_games():
    if CACHE.exists() and time.time()-CACHE.stat().st_mtime<TTL:
        with gzip.open(CACHE,'rt',encoding='utf-8') as f: data=json.load(f)
        if isinstance(data,dict) and data.get('version')==2 and data.get('games') and time.time()-data['at']<TTL: return data['games']
    cid=os.environ.get('IGDB_CLIENT_ID',''); secret=os.environ.get('IGDB_CLIENT_SECRET','')
    if not cid or not secret: raise RuntimeError('Configure IGDB_CLIENT_ID and IGDB_CLIENT_SECRET as GitHub Actions secrets')
    token=request('https://id.twitch.tv/oauth2/token',urlencode({'client_id':cid,'client_secret':secret,'grant_type':'client_credentials'}).encode(),{'Content-Type':'application/x-www-form-urlencoded'})['access_token']
    headers={'Client-ID':cid,'Authorization':'Bearer '+token,'Accept':'application/json','Content-Type':'text/plain'}
    games={}
    for pid in sorted(set(PLATFORMS.values())):
        last=0; count=0
        while True:
            rows=request('https://api.igdb.com/v4/games',f'fields {FIELDS}; where platforms = ({pid}) & id > {last}; sort id asc; limit 500;'.encode(),headers)
            if not isinstance(rows,list): raise ValueError('Invalid IGDB page')
            if not rows: break
            for g in rows: games[g['id']]=g
            nxt=rows[-1]['id']; count+=len(rows)
            if nxt<=last or count>100000: raise ValueError('Invalid pagination')
            last=nxt; time.sleep(.28)
            if len(rows)<500: break
        if not count: raise ValueError(f'No games returned for platform {pid}')
        print(f'Platform {pid}: {count} records')
    if not games: raise ValueError('Empty IGDB response')
    CACHE.parent.mkdir(parents=True,exist_ok=True); tmp=CACHE.with_suffix('.tmp')
    with gzip.open(tmp,'wt',encoding='utf-8',compresslevel=5) as f: json.dump({'version':2,'at':time.time(),'games':list(games.values())},f,ensure_ascii=False,separators=(',',':'))
    tmp.replace(CACHE)
    return list(games.values())


def names(rows):
    return sorted({str(x.get('name','')).strip() for x in rows or [] if isinstance(x,dict) and x.get('name')},key=str.casefold)


def release(g,pid,region):
    regions={'US':2,'EU':1,'JP':5,'ASIA':7,'ASI':7,'KR':9,'BR':10}
    rows=[d for d in g.get('release_dates',[]) if d.get('platform')==pid and d.get('region') in (regions.get(region,-1),8) and d.get('date') and d.get('category') in (0,1,2)]
    exact=[d for d in rows if d.get('region')==regions.get(region,-1)]
    rows=exact or rows
    if not rows: return ''
    d=min(rows,key=lambda x:(x['date'],x['category']))
    try:
        date=dt.datetime.fromtimestamp(d['date'],dt.timezone.utc).date().isoformat()
        return date if d['category']==0 else date[:4]
    except (ValueError,OverflowError,OSError): return ''


def build(cat,games):
    buckets=defaultdict(set); byid={}
    for g in games:
        if g.get('category',0) not in (0,2,3,4,8,9,10,11,12,13): continue
        gid=g['id']; byid[gid]=g
        keys={title_key(g.get('name')),*(title_key(a.get('name')) for a in g.get('alternative_names',[]) if isinstance(a,dict))}-{''}
        for pid in g.get('platforms',[]):
            for k in keys: buckets[(pid,k)].add(gid)
    rows={}; audit=Counter(); examples=defaultdict(list)
    for tid, variants in sorted(cat.items()):
        matches=set(); unresolved=False
        for tag,region,name in sorted(variants):
            ids=buckets.get((PLATFORMS[tag],title_key(name)),set())
            if len(ids)!=1: unresolved=True
            matches.update((i,tag) for i in ids)
        if unresolved or len(matches)!=1:
            reason='ambiguous' if len(matches)>1 else 'unmatched'
            audit[reason]+=1
            if len(examples[reason])<60: examples[reason].append(f'{tid} — {sorted(variants)[0][2]}')
            continue
        gid,tag=next(iter(matches)); g=byid[gid]; companies=g.get('involved_companies',[])
        dev=names([x.get('company',{}) for x in companies if x.get('developer')]); pub=names([x.get('company',{}) for x in companies if x.get('publisher')]); genre=names(g.get('genres',[]))
        dates={release(g,PLATFORMS[tag],r) for _,r,_ in variants}; date=next(iter(dates)) if len(dates)==1 else ''
        rows[tid]=[gid,tag,dev,pub,genre,date,names(g.get('themes',[]))]
        audit['matched']+=1
        for field,value in [('developer',dev),('publisher',pub),('genre',genre),('platform release date',date),('summary',g.get('summary')),('critic rating',g.get('aggregated_rating_count')),('screenshots',g.get('screenshots')),('artwork',g.get('artworks')),('cover',g.get('cover')),('game modes',g.get('game_modes'))]:
            if not value: audit['missing '+field]+=1
    return {'version':1,'games':rows},audit,examples


def main():
    cat=catalog(); payload,audit,examples=build(cat,fetch_games())
    if not payload['games']: raise ValueError('No trusted matches; refusing to replace index')
    report=['## IGDB metadata coverage','',f'Eligible Title IDs: **{len(cat)}**','', '| Metric | Title IDs |','|---|---:|']+[f'| {k} | {v} |' for k,v in sorted(audit.items())]
    report+=['','Missing fields describe IGDB only, not combined live site metadata.']
    for reason,items in examples.items(): report+=['',f'### {reason} (sample)','',*['- '+x.replace('\n',' ') for x in items]]
    if os.environ.get('GITHUB_STEP_SUMMARY'):
        with open(os.environ['GITHUB_STEP_SUMMARY'],'a',encoding='utf-8') as f:f.write('\n'.join(report)+'\n')
    print('\n'.join(report[:len(audit)+7]))
    INDEX.parent.mkdir(parents=True,exist_ok=True); text=json.dumps(payload,ensure_ascii=False,separators=(',',':'))+'\n'
    if len(text.encode())>8*1024*1024: raise ValueError('Index exceeds browser limit')
    if INDEX.exists():
        old=json.loads(INDEX.read_text(encoding='utf-8'))
        if len(payload['games'])<.8*len(old.get('games',{})): raise ValueError('Coverage unexpectedly dropped; refusing to replace index')
        if INDEX.read_text(encoding='utf-8')==text:return
    tmp=INDEX.with_suffix('.tmp');tmp.write_text(text,encoding='utf-8');tmp.replace(INDEX)


if __name__=='__main__': main()
