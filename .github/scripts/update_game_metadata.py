#!/usr/bin/env python3
import csv
import datetime as dt
import gzip
import hashlib
import heapq
import json
import os
import re
import time
import unicodedata
import generate_franchises as franchise
from collections import defaultdict, Counter
from functools import lru_cache
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError

ROOT = Path('.')
INDEX = Path('resources/database/helpers/game-metadata.json')
CACHE = Path('.cache/game-metadata/igdb.json.gz')
PLATFORMS = {tag:set(ids) for tag,ids in franchise.PLATFORMS.items()}
PSN_SOURCES = {36}
FIELDS = 'name,alternative_names.name,platforms,category,game_type.type,game_localizations.name,external_games.uid,external_games.external_game_source,external_games.platform,external_games.url,genres.name,themes.name,involved_companies.company.name,involved_companies.developer,involved_companies.publisher,release_dates.platform,release_dates.date,release_dates.category,release_dates.region,release_dates.date_format.format,release_dates.release_region.region,release_dates.human,release_dates.y,summary,aggregated_rating_count,screenshots,artworks,cover,game_modes'
TTL = 30 * 86400
IGDB_CACHE_VERSION = 5
IGDB_FULL_REFRESH = 30 * 86400
IGDB_UPDATE_OVERLAP = 3600
REGION_ALIASES = {
    "US": 2, "USA": 2, "NA": 2, "NORTHAMERICA": 2, "EU": 1, "EUR": 1, "EUROPE": 1,
    "JP": 5, "JPN": 5, "JAPAN": 5, "ASIA": 7, "ASI": 7, "ASN": 7, "AS": 7,
    "KR": 9, "KOR": 9, "KOREA": 9, "SOUTHKOREA": 9, "BR": 10, "BRA": 10, "BRAZIL": 10,
    "AU": 3, "AUS": 3, "AUSTRALIA": 3, "NZ": 4, "NEWZEALAND": 4,
    "CN": 6, "CHN": 6, "CHINA": 6, "WORLDWIDE": 8, "WORLD": 8, "WW": 8,
}



def load_provider_secrets():
    path = ROOT / 'resources/server/server-secrets.json'
    try:
        data = json.loads(path.read_text(encoding='utf-8-sig'))
    except FileNotFoundError:
        raise RuntimeError('Missing resources/server/server-secrets.json') from None
    except (OSError, ValueError):
        raise RuntimeError('Cannot read resources/server/server-secrets.json') from None
    if not isinstance(data, dict):
        raise RuntimeError('Invalid server-secrets.json: expected a JSON object')
    aliases = {
        'IGDB_CLIENT_ID': ('IGDB_CLIENT_ID',),
        'IGDB_CLIENT_SECRET': ('IGDB_CLIENT_SECRET',),
        'RAWG_KEY': ('RAWG_KEYS', 'RAWG_KEY', 'RAWG_API_KEY', 'RAWG_API_KEYS'),
        'SCREENSCRAPER_DEV_ID': ('SCREENSCRAPER_DEV_ID',),
        'SCREENSCRAPER_DEV_PASSWORD': ('SCREENSCRAPER_DEV_PASSWORD',),
        'SCREENSCRAPER_USER': ('SCREENSCRAPER_USER',),
        'SCREENSCRAPER_PASSWORD': ('SCREENSCRAPER_PASSWORD',),
    }
    values = {}
    for target, keys in aliases.items():
        for name in keys:
            value = data.get(name)
            if target == 'RAWG_KEY' and isinstance(value, list):
                value = ' '.join(x.strip() for x in value if isinstance(x, str) and x.strip())
            if isinstance(value, str) and value.strip():
                values[target] = value.strip()
                break
    missing = [name for name in ('IGDB_CLIENT_ID', 'IGDB_CLIENT_SECRET') if not values.get(name)]
    if missing:
        raise RuntimeError('Missing provider keys in server-secrets.json: ' + ', '.join(missing))
    # Only the metadata providers are exposed to this process; never log values.
    for target in aliases:
        os.environ.pop(target, None)
    os.environ.update(values)


def key(value):
    s = unicodedata.normalize('NFKC', str(value or '')).casefold()
    return ''.join(c for c in s if c.isalnum())


@lru_cache(maxsize=32768)
def title_key(value):
    s = str(value or '').replace('™', '').replace('®', '').replace('©', '')
    s = re.sub(r'[\[(]\s*(?:PS[123X]|PSP|Minis?|PS[12]\s*Classics?|PC\s*Engine|Turbo\s*Grafx(?:\s*-\s*16)?|Neo\s*Geo(?:\s*Station)?|English|French|German|Japanese|US|EU|JP)\s*[\])]', ' ', s, flags=re.I)
    s = re.sub(r'\s*[\[(]?\b(?:digital version|downloadable version|full game)\b[\])]?$','',s,flags=re.I)
    s = re.sub(r'^Neo\s*Geo\s*Station\s*[-:]?\s*','',s,flags=re.I)
    return key(s)


def platform(tid, name, cid):
    return franchise.platform(name, tid, cid)


def catalog(content_ids=None):
    browse = franchise.BrowseContentFilter(ROOT/'files/website/modern.html')
    result = defaultdict(set)
    base = ROOT/'resources/database/content'
    for file in franchise.FILES:
        path = base/file
        if not path.is_file() and file != franchise.FILES[0]: continue
        with path.open(encoding='utf-8-sig', newline='') as f:
            for row in csv.reader(f, delimiter='\t', quoting=csv.QUOTE_NONE):
                if len(row)<6: continue
                tid, region, name, pkg, _, cid = (x.strip() for x in row[:6]); tid=tid.upper()
                if not franchise.ID.fullmatch(tid) or len(name)<2 or name.upper()=='MISSING' or not pkg.lower().startswith(('http://','https://')): continue
                if browse.category(name, tid, cid, file.endswith('demos.tsv'))!='eligible_rows': continue
                result[tid].add((platform(tid,name,cid),region.upper(),name))
                product=cid.upper()
                if content_ids is not None and franchise.PSN_CONTENT_ID.fullmatch(product) and franchise.psn_title_id(product)==tid:
                    content_ids[tid].add(product)
    if not result: raise ValueError('Empty catalog')
    return result


def request(url, body, headers):
    for attempt in range(4):
        try:
            with urlopen(Request(url,data=body,headers=headers),timeout=45) as r:
                raw=r.read(24*1024*1024+1)
                if len(raw)>24*1024*1024: raise ValueError("IGDB response exceeds limit")
                return json.loads(raw)
        except HTTPError as e:
            if e.code not in (429,500,502,503,504) or attempt==3: raise RuntimeError(f'Provider HTTP {e.code}') from None
        except (URLError,TimeoutError):
            if attempt==3: raise RuntimeError('Provider unavailable') from None
        time.sleep(2**attempt)


AUDIT_ONLY_FIELDS = ('summary', 'screenshots', 'artworks', 'cover', 'game_modes', 'aggregated_rating_count')


def compact_igdb_game(game):
    for field in AUDIT_ONLY_FIELDS:
        if field in game:
            game[field] = bool(game[field])
    return game


class InvalidIGDBCache(Exception):
    pass


def cached_igdb_games():
    try:
        with gzip.open(CACHE,'rt',encoding='utf-8') as f:
            f.readline()
            last=0
            for line in f:
                game=json.loads(line)
                if not isinstance(game,dict) or type(game.get('id')) is not int or game['id']<=last:
                    raise ValueError('invalid game order')
                last=game['id']
                yield game
            if not last: raise ValueError('empty cache')
    except (OSError,ValueError,EOFError) as e:
        raise InvalidIGDBCache('Invalid IGDB snapshot') from e


def write_igdb_snapshot(header,games,required_platforms=()):
    CACHE.parent.mkdir(parents=True,exist_ok=True)
    tmp=CACHE.with_suffix('.tmp'); count=0; last=0; seen=set()
    try:
        with gzip.open(tmp,'wt',encoding='utf-8',compresslevel=3,newline='\n') as f:
            f.write(json.dumps(header,separators=(',',':'))+'\n')
            for game in games:
                if type(game.get('id')) is not int or game['id']<=last: raise ValueError('Invalid IGDB snapshot order')
                last=game['id']; count+=1
                if required_platforms: seen.update(game.get('platforms') or ())
                f.write(json.dumps(game,ensure_ascii=False,separators=(',',':'))+'\n')
        if not count or not set(required_platforms)<=seen:
            raise ValueError('Incomplete IGDB snapshot; previous cache preserved')
        tmp.replace(CACHE)
    finally:
        tmp.unlink(missing_ok=True)
    return count


def snapshot_digest(path):
    digest=hashlib.sha256()
    with path.open('rb') as handle:
        for block in iter(lambda: handle.read(1024*1024), b''):
            digest.update(block)
    return digest.hexdigest()


def checkpoint_path(path):
    return path.with_name(path.name.removesuffix('.json.gz')+'.checkpoint.json.gz')


def validated_snapshot_checkpoint(path, header, now):
    watermark=header['watermark']
    try:
        with gzip.open(checkpoint_path(path),'rt',encoding='utf-8') as handle:
            entry=json.load(handle)
        if (isinstance(entry,dict) and type(entry.get('watermark')) is int
            and watermark < entry['watermark'] <= now
            and entry.get('digest')==snapshot_digest(path)):
            return entry
    except (OSError,ValueError,EOFError):
        pass
    return {}


def snapshot_watermark(path, header, now):
    return validated_snapshot_checkpoint(path, header, now).get('watermark',header['watermark'])


def save_snapshot_checkpoint(path, watermark, count=None, digest=None):
    target=checkpoint_path(path); temp=target.with_suffix('.tmp')
    try:
        entry={'watermark':watermark,'digest':digest or snapshot_digest(path)}
        if count is not None:
            entry['validated_count']=count
        with gzip.open(temp,'wt',encoding='utf-8',compresslevel=1) as handle:
            json.dump(entry,handle,separators=(',',':'))
        temp.replace(target)
    except OSError:
        print('IGDB checkpoint unavailable; using snapshot watermark next run')
    finally:
        temp.unlink(missing_ok=True)


def fetch_games():
    started=int(time.time())
    cid=os.environ.get('IGDB_CLIENT_ID',''); secret=os.environ.get('IGDB_CLIENT_SECRET','')
    if not cid or not secret: raise RuntimeError('Missing IGDB credentials in server-secrets.json')
    def authenticate():
        return request('https://id.twitch.tv/oauth2/token',urlencode({'client_id':cid,'client_secret':secret,'grant_type':'client_credentials'}).encode(),{'Content-Type':'application/x-www-form-urlencoded'})['access_token']
    headers={'Client-ID':cid,'Authorization':'Bearer '+authenticate(),'Accept':'application/json','Content-Type':'text/plain'}
    def reference_pages(endpoint):
        last=0
        while True:
            rows=request('https://api.igdb.com/v4/'+endpoint,f'fields id,name; where id > {last}; sort id asc; limit 500;'.encode(),headers)
            if not isinstance(rows,list) or len(rows)>500: raise ValueError('Invalid IGDB reference page')
            for row in rows:
                if not isinstance(row,dict) or type(row.get('id')) is not int or row['id']<=last: raise ValueError('Invalid IGDB reference order')
                last=row['id']; yield row
            time.sleep(.35)
            if len(rows)<500: return
    platforms=list(reference_pages('platforms'))
    resolved={tag:{r['id'] for r in platforms if franchise.norm(r.get('name','')) in variants} for tag,variants in franchise.PLATFORM_NAMES.items()}
    if any(not ids for ids in resolved.values()): raise ValueError('IGDB platform mapping incomplete')
    PLATFORMS.update(resolved)
    PSN_SOURCES.clear()
    PSN_SOURCES.update(r['id'] for r in reference_pages('external_game_sources') if 'playstation' in franchise.norm(r.get('name','')) or franchise.norm(r.get('name','')).startswith('psn'))
    pids=sorted(set().union(*PLATFORMS.values()))
    signature={'version':IGDB_CACHE_VERSION,'fields':FIELDS,'platforms':pids}
    previous=None
    try:
        with gzip.open(CACHE,'rt',encoding='utf-8') as f:
            header=json.loads(f.readline())
        if isinstance(header,dict) and all(header.get(k)==v for k,v in signature.items()):
            full,watermark=header.get('full_at'),header.get('watermark')
            if type(full) is int and type(watermark) is int and 0<full<=watermark<=started and started-full<IGDB_FULL_REFRESH:
                previous=header
    except (OSError,ValueError,EOFError): pass
    where='platforms = ('+','.join(map(str,pids))+')'
    next_request=0
    def pages(extra=''):
        nonlocal next_request
        last=0; count=0
        while True:
            body=f'fields {FIELDS}; where {where} & id > {last}{extra}; sort id asc; limit 500;'.encode()
            time.sleep(max(0,next_request-time.monotonic()))
            next_request=time.monotonic()+.35
            try:
                rows=request('https://api.igdb.com/v4/games',body,headers)
            except RuntimeError as e:
                if str(e)!='Provider HTTP 401': raise
                headers['Authorization']='Bearer '+authenticate()
                rows=request('https://api.igdb.com/v4/games',body,headers)
            if not isinstance(rows,list) or len(rows)>500: raise ValueError('Invalid IGDB page')
            for g in rows:
                if not isinstance(g,dict) or type(g.get('id')) is not int or g['id']<=last:
                    raise ValueError('Invalid IGDB pagination order')
                last=g['id']; count+=1
                if count>100000*len(pids): raise ValueError('IGDB pagination limit exceeded')
                yield compact_igdb_game(g)
            if len(rows)<500: return
    if previous is not None:
        checkpoint=validated_snapshot_checkpoint(CACHE,previous,started)
        since=max(0,checkpoint.get('watermark',previous['watermark'])-IGDB_UPDATE_OVERLAP)
        changes={g['id']:g for g in pages(f' & updated_at >= {since}')}
        try:
            if not changes:
                count=checkpoint.get('validated_count')
                if type(count) is not int or count<=0:
                    count=0; seen=set()
                    for game in cached_igdb_games():
                        count+=1; seen.update(game.get('platforms') or ())
                    if not set(pids)<=seen: raise InvalidIGDBCache('Incomplete platform snapshot')
                save_snapshot_checkpoint(CACHE,started,count,checkpoint.get('digest'))
                print(f'IGDB cache unchanged: {count} records')
            else:
                old=(g for g in cached_igdb_games() if g['id'] not in changes)
                merged=heapq.merge(old,(changes[k] for k in sorted(changes)),key=lambda g:g['id'])
                count=write_igdb_snapshot({**signature,'full_at':previous['full_at'],'watermark':started},merged,pids)
                print(f'IGDB incremental refresh: {len(changes)} updated records; {count} cached records')
            return cached_igdb_games()
        except InvalidIGDBCache:
            print('IGDB cache damaged; full refresh')
    count=write_igdb_snapshot({**signature,'full_at':started,'watermark':started},pages(),pids)
    print(f'IGDB full refresh: {count} records')
    return cached_igdb_games()


def names(rows):
    return sorted({str(x.get('name','')).strip() for x in rows or [] if isinstance(x,dict) and x.get('name')},key=str.casefold)


def release_region(d):
    current=d.get('release_region')
    if isinstance(current,dict):
        value=REGION_ALIASES.get(re.sub(r'[^A-Z]','',str(current.get('region','')).upper()))
        if value is not None: return value
    return d.get('region')


def release_value(d):
    current=d.get('date_format')
    fmt=re.sub(r'[^a-z0-9]','',str(current.get('format','')).lower()) if isinstance(current,dict) else ''
    category=d.get('category')
    human=str(d.get('human','')).strip()
    if fmt in ('tbd','unknown') or human.casefold()=='tbd': return ''
    if fmt in ('yyyymmmmdd','yyyymmdd','ddmmmmyyyy','ddmmmyyyy'): precision=0
    elif fmt in ('yyyymmmm','yyyymm','yyyy','yyyyq1','yyyyq2','yyyyq3','yyyyq4'): precision=1
    elif category in (0,1,2,3,4,5,6): precision=0 if category==0 else 1
    elif category==7: return ''
    elif re.fullmatch(r'[A-Za-z]{3,9} \d{1,2},? \d{4}|\d{1,2} [A-Za-z]{3,9} \d{4}|\d{4}-\d{2}-\d{2}',human): precision=0
    elif re.search(r'\b(?:19|20)\d{2}\b',human): precision=1
    else: return ''
    stamp=d.get('date')
    if precision==0 and isinstance(stamp,(int,float)):
        try: return valid_date(dt.datetime.fromtimestamp(stamp,dt.timezone.utc).date().isoformat())
        except (ValueError,OverflowError,OSError): return ''
    year=valid_date(str(d.get('y','')))
    if year: return year
    if isinstance(stamp,(int,float)):
        try: return valid_date(str(dt.datetime.fromtimestamp(stamp,dt.timezone.utc).year))
        except (ValueError,OverflowError,OSError): pass
    return ''


def release(g,pid,region):
    region_id=REGION_ALIASES.get(re.sub(r'[^A-Z]','',region.upper()))
    exact=[]; worldwide=[]
    for d in g.get('release_dates') or []:
        if not isinstance(d,dict) or d.get('platform')!=pid: continue
        rid=release_region(d)
        if rid not in (region_id,8) or rid is None: continue
        value=release_value(d)
        if value: (exact if rid==region_id else worldwide).append(value)
    values=exact or worldwide
    if not values: return ''
    # Never manufacture a day from a month/year placeholder.
    earliest_year=min(v[:4] for v in values)
    candidates=[v for v in values if v.startswith(earliest_year)]
    return earliest_year if any(len(v)==4 for v in candidates) else min(candidates)


def identity_keys(name, tid=None):
    if not isinstance(name,str) or not name: return ()
    clean=franchise.catalog_match_name(name,tid) if tid else name
    # Full title only; no franchise, parent, edition or fuzzy inference.
    words=franchise.extended_title_words(clean,bool(tid)) if not franchise.JAPANESE_SCRIPT.search(clean) and not re.search(r"\b(?:part|disc)\s*\d+\b",clean,re.I) else ()
    return tuple(dict.fromkeys(k for k in (title_key(name), title_key(clean), franchise.extended_name_key(words)) if k))


def build(cat,games):
    needed=defaultdict(set); prepared={}; byid={}; exact=defaultdict(set); expanded=defaultdict(set); direct=defaultdict(set)
    for tid,variants in cat.items():
        prepared[tid]=tuple(dict.fromkeys((tag,identity_keys(name,tid)) for tag,name in sorted({(tag,name) for tag,_,name in variants})))
        for tag,keys in prepared[tid]: needed[tag].update(keys)
    tags_by_pid=defaultdict(set)
    for tag in needed:
        for pid in PLATFORMS[tag]: tags_by_pid[pid].add(tag)
    for g in games:
        if g.get('game_type'):
            if not isinstance(g['game_type'],dict): continue
            if not franchise.is_game_record(g) and str((g.get('game_type') or {}).get('type','')).lower().replace('_',' ') not in ('expansion','fork','pack'): continue
        elif g.get('category',0) not in (0,2,3,4,8,9,10,11,12,13): continue
        gid=g['id']; tags=set().union(*(tags_by_pid.get(pid,()) for pid in g.get('platforms') or ()))
        if not tags: continue
        titles=franchise.game_names(g)
        original={title_key(n) for n in titles}-{''}
        more={k for n in titles+franchise.localized_game_names(g) for k in identity_keys(n)}
        for tag in tags:
            for k in original & needed[tag]: exact[tag,k].add(gid); byid[gid]=g
            for k in more & needed[tag]: expanded[tag,k].add(gid); byid[gid]=g
        for item in g.get('external_games') or ():
            if not isinstance(item,dict): continue
            # Accept only documented PlayStation sources or official product URLs.
            uid=franchise.psn_title_id(item.get('uid')) if item.get('external_game_source') in PSN_SOURCES else ''
            url_uid=franchise.psn_product_url_id(item.get('url'))
            if uid and url_uid and uid!=url_uid: continue
            tid=uid or url_uid
            if tid not in cat: continue
            for tag in tags & tags_by_pid.get(item.get('platform'),set()):
                if any(t==tag for t,_,_ in cat[tid]): direct[tag,tid].add(gid); byid[gid]=g
    rows={}; audit=Counter(); examples=defaultdict(list); metadata={}; dates_cache={}
    for tid,variants in sorted(cat.items()):
        tags={tag for tag,_,_ in variants}; matches=None; union=set(); missing=False; method='exact title'; multiple=False
        if len(tags)==1:
            tag=next(iter(tags))
            evidence=direct.get((tag,tid),set())
            if evidence: method='verified PS Store ID'
            for _,keys in prepared[tid]:
                primary=exact.get((tag,keys[0]),set()) if keys else set()
                secondary=set().union(*(expanded.get((tag,k),()) for k in keys)) if not primary else set()
                # External IDs may identify a localized title absent from IGDB names,
                # but must never override a contradictory exact title.
                candidates=primary or secondary
                if not primary and secondary and not evidence: method='localized or normalized title'
                if evidence: candidates=(evidence & candidates) if candidates else evidence
                if not candidates: missing=True
                multiple=multiple or len(candidates)>1
                union.update(candidates)
                matches=candidates if matches is None else matches & candidates
        if len(tags)!=1 or missing or not matches or len(matches)!=1:
            reason='ambiguous' if len(union)>1 or len(tags)!=1 else 'unmatched'
            audit[reason]+=1
            if len(examples[reason])<60: examples[reason].append(f'{tid} — {min(variants)[2]}')
            continue
        gid=next(iter(matches)); g=byid[gid]
        audit['matched by '+method]+=1
        if multiple and method=='exact title': audit['resolved by exact alias intersection']+=1
        if gid not in metadata:
            companies=[x for x in g.get('involved_companies') or [] if isinstance(x,dict)]
            metadata[gid]=(names([x.get('company',{}) for x in companies if x.get('developer')]),
                           names([x.get('company',{}) for x in companies if x.get('publisher')]),
                           names(g.get('genres')),names(g.get('themes')))
        dev,pub,genre,themes=metadata[gid]
        dates=set()
        for r in {r for _,r,_ in variants}:
            k=(gid,tag,r)
            if k not in dates_cache:
                pids=set(g.get('platforms') or ()) & PLATFORMS[tag]
                values={release(g,pid,r) for pid in pids}
                # Multiple distinct platforms (e.g. Minis) must not lend dates to one another.
                dates_cache[k]=next(iter(values)) if len(values)==1 else ''
            dates.add(dates_cache[k])
        date=next(iter(dates)) if len(dates)==1 else ''
        if not date and dates and all(dates) and len({v[:4] for v in dates})==1: date=next(iter(dates))[:4]
        rows[tid]=[gid,tag,dev,pub,genre,date,themes]
        audit['matched']+=1
        for field,value in [('developer',dev),('publisher',pub),('genre',genre),('platform release date',date),('summary',g.get('summary')),('critic rating',g.get('aggregated_rating_count')),('screenshots',g.get('screenshots')),('artwork',g.get('artworks')),('cover',g.get('cover')),('game modes',g.get('game_modes'))]:
            if not value: audit['missing '+field]+=1
    return {'version':1,'games':rows},audit,examples


# Fallbacks run only in Actions, never through the application's server.
CACHE_DIR = Path('.cache/game-metadata')
FALLBACK_BUDGET = {'psn': 100, 'rawg': 100, 'screenscraper': 60}
FALLBACK_TIME_BUDGET = 600  # Per run; the workflow has its own outer timeout.
RAWG_PLATFORMS = {'ps3':'playstation 3','ps2':'playstation 2','psx':'playstation','psp':'psp','minis':'psp','neogeo':'neo geo','pce':'pc engine'}
SS_PLATFORMS = {'ps3':59,'ps2':58,'psx':57,'psp':61,'minis':61,'pce':31,'neogeo':142}
LB_PLATFORMS = {'ps3':'ps3','ps2':'ps2','psx':'ps1','psp':'psp','pce':'pce','neogeo':'neogeo'}
GENRE_ALIASES = {'rpg':'Role-playing (RPG)','role-playing':'Role-playing (RPG)','role-playing (rpg)':'Role-playing (RPG)','platformer':'Platform','platformers':'Platform','fighting games':'Fighting'}


def strings(values, genre=False):
    if not isinstance(values,list): return []
    result={}
    for v in values:
        if not isinstance(v,str): continue
        v=' '.join(unicodedata.normalize('NFKC',v).split())
        if not v or len(v)>250 or v.casefold() in ('unknown','n/a','none','not available'): continue
        if genre: v=GENRE_ALIASES.get(v.casefold(),v)
        result.setdefault(v.casefold(),v)
    return sorted(result.values(),key=str.casefold)[:20]


def valid_date(value):
    if not isinstance(value,str): return ''
    if re.fullmatch(r'(?:19|20)\d{2}',value): return value
    m=re.match(r'^((?:19|20)\d{2}-\d{2}-\d{2})(?:$|T| )',value)
    if m:
        try: return dt.date.fromisoformat(m[1]).isoformat()
        except ValueError: pass
    return ''


def compact(dev=None,pub=None,genre=None,date='',themes=None):
    return [strings(dev),strings(pub),strings(genre,True),valid_date(date),strings(themes,True)]


def merge(row, values, source, audit):
    for pos,value in zip((2,3,4,5,6),values):
        if not row[pos] and value:
            row[pos]=value; audit[f'{source}: filled { {2:"developer",3:"publisher",4:"genre",5:"date",6:"theme"}[pos]}']+=1


@lru_cache(maxsize=16384)
def lb_key(name):
    s=unicodedata.normalize('NFKD',str(name).replace('™',' ').replace('®',' ').replace('©',' '))
    return re.sub(r'[^a-z0-9]+',' ',''.join(c for c in s if not unicodedata.combining(c)).lower()).strip()


def launchbox_lookup(variants, indexes):
    matches=set()
    for tag,name in {(tag,name) for tag,_,name in variants}:
        platform=LB_PLATFORMS.get(tag)
        if not platform: return None
        if platform not in indexes:
            path=ROOT/'resources/database/launchbox'/f'{platform}.json'
            try:
                data=json.loads(path.read_text(encoding='utf-8-sig'))
                indexes[platform]=data if data.get('version')==1 and data.get('platform')==platform else {}
            except (OSError,ValueError): indexes[platform]={}
        data=indexes[platform]; titles=data.get('titles',{}); meta=data.get('meta',{})
        if not isinstance(titles,dict) or not isinstance(meta,dict): return None
        candidates={str(titles[k]) for k in (lb_key(name),lb_key(re.sub(r'\s*\((?:PS[123X]|PS2 Classics?|PSP)\)\s*$','',name,flags=re.I))) if k in titles}
        if len(candidates)!=1: return None
        gid=candidates.pop(); matches.add((platform,gid))
        if len(matches)>1: return None
    if len(matches)!=1: return None
    platform,gid=next(iter(matches)); m=indexes[platform].get('meta',{}).get(gid)
    if not isinstance(m,dict): return None
    return compact(m.get('developers'),m.get('publishers'),m.get('genres'))


class ProviderError(Exception): pass


def get_json(url,params=None,missing_ok=False):
    # No logging authenticated URLs. Bounded body, time and request count.
    try:
        with urlopen(Request(url+('?' + urlencode(params) if params else ''),headers={'Accept':'application/json','User-Agent':'PSN-Content-Metadata/1.0'}),timeout=8) as r:
            raw=r.read(8*1024*1024+1)
            if len(raw)>8*1024*1024: raise ProviderError('response too large')
            return json.loads(raw)
    except HTTPError as e:
        if e.code==404 and missing_ok: return None
        raise ProviderError(f'HTTP {e.code}') from None
    except (URLError,TimeoutError,ValueError,OSError): raise ProviderError('unavailable') from None


def psn_locale(region,cid):
    prefix=cid[:2] if cid else ''
    regional=REGION_ALIASES.get(re.sub(r'[^A-Z]','',region.upper()))
    countries={1:('GB','en'),2:('US','en'),3:('AU','en'),4:('NZ','en'),5:('JP','ja'),7:('HK','en'),9:('KR','ko'),10:('BR','pt')}
    locale={'JP':('JP','ja'),'KP':('KR','ko'),'HP':('HK','en'),'AP':('HK','en'),'EP':('GB','en'),'UP':('US','en')}.get(prefix) or countries.get(regional)
    return locale


def psn_lookup(tid,region,cid):
    locale=psn_locale(region,cid)
    if not locale: return compact()
    country,lang=locale
    endpoint='container' if cid else 'titlecontainer'
    data=get_json(f'https://store.playstation.com/store/api/chihiro/00_09_000/{endpoint}/{country}/{lang}/19/{cid or tid}',missing_ok=True)
    if not isinstance(data,dict): return compact()
    # Reject a conflicting product identifier when the store supplies one.
    for field in ('id','content_id','contentId','title_id','titleId'):
        value=data.get(field)
        if not isinstance(value,str): continue
        value=value.strip().upper()
        product=franchise.PSN_CONTENT_ID.fullmatch(value)
        if product and (product[1]!=tid or (cid and value!=cid)): return compact()
        if re.fullmatch(r'[A-Z]{4}\d{5}',value) and value!=tid: return compact()
    # Only root product fields; nested related products may describe another game.
    provider=next((data[k] for k in ('provider_name','providerName','publisher_name','publisherName') if isinstance(data.get(k),str) and data[k].strip()),'')
    return compact(pub=[provider],date=data.get('release_date') or data.get('releaseDate',''))


def rawg_lookup(tag,name,api_key):
    data=get_json('https://api.rawg.io/api/games',{'key':api_key,'search':name,'page_size':20})
    if not isinstance(data,dict) or not isinstance(data.get('results'),list): raise ProviderError('invalid search response')
    def match(g):
        return title_key(g.get('name'))==title_key(name) and RAWG_PLATFORMS[tag] in {str(p.get('platform',{}).get('name','')).casefold() for p in g.get('platforms') or [] if isinstance(p,dict)}
    hits=[g for g in data['results'] if isinstance(g,dict) and match(g)]
    if len(hits)!=1 or data.get('next'): return compact()
    gid=hits[0].get('id')
    if not isinstance(gid,int) or gid<=0: return compact()
    time.sleep(.25)
    g=get_json(f'https://api.rawg.io/api/games/{gid}',{'key':api_key})
    if not isinstance(g,dict) or g.get('id')!=gid or not match(g): return compact()
    # RAWG's global release date is not a platform-specific release date.
    return compact(names(g.get('developers')),names(g.get('publishers')),names(g.get('genres')))


def ss_lookup(tid,tag,name,credentials):
    g=get_json('https://api.screenscraper.fr/api2/jeuInfos.php',{**credentials,'softname':'PSN-Content','output':'json','systemeid':SS_PLATFORMS[tag],'serialnum':tid},missing_ok=True)
    if g is None: return compact()
    if not isinstance(g,dict): raise ProviderError('invalid response')
    game=g.get('response',{}).get('jeu')
    if not isinstance(game,dict): return compact()
    if str(game.get('systemeid') or (game.get('systeme') or {}).get('id'))!=str(SS_PLATFORMS[tag]) or str(game.get('notgame','')).lower() in ('1','true'): return compact()
    # Serial-only endpoint must actually confirm the serial; name must match too.
    def collect(obj,pattern):
        if isinstance(obj,list): return [s for v in obj for s in collect(v,pattern)]
        if not isinstance(obj,dict): return []
        return [s for k,v in obj.items() for s in ([v] if isinstance(v,str) and re.fullmatch(pattern,k,re.I) else collect(v,pattern))]
    serials=collect(game.get('roms',[]),r'romserial|serialnum|serial')+collect(game.get('rom',{}),r'romserial|serialnum|serial')
    confirmed={re.sub('[^A-Z0-9]','',x.upper()) for s in serials for x in re.split('[,;|/]',s)}
    aliases=collect(game.get('noms',{}),r'nom(?:_.*)?|name(?:_.*)?|text|texte|title|value')+[game.get('nom'),game.get('name')]
    if tid not in confirmed or title_key(name) not in {title_key(v) for v in aliases if isinstance(v,str)}: return compact()
    genres=game.get('genres',{}); genre=[]
    if isinstance(genres,dict): genre=[genres.get('genre_en') or genres.get('en') or genres.get('genre_us') or '']
    # The serial match supplies company/genre only; region dates stay with PS Store/IGDB.
    return compact([game.get('developpeur')],[game.get('editeur')],genre)


class ProviderCache:
    def __init__(self,name):
        self.path=CACHE_DIR/f'{name}.json.gz';self.rows={};self.used=0;self.limit=FALLBACK_BUDGET[name];self.disabled=False;self.errors=0;self.name=name;self.attempted=set();self.dirty=False
        try:
            if self.path.stat().st_size > 4*1024*1024: raise ValueError('Provider cache exceeds size limit')
            with gzip.open(self.path,'rb') as f: raw=f.read(64*1024*1024+1)
            if len(raw)>64*1024*1024: raise ValueError('Provider cache exceeds expanded size limit')
            data=json.loads(raw)
            if data.get('version')==1 and isinstance(data.get('rows'),dict):
                self.rows={k:v for k,v in data['rows'].items() if isinstance(k,str) and isinstance(v,list) and len(v)==2 and isinstance(v[0],(int,float)) and isinstance(v[1],list) and len(v[1])==5 and isinstance(v[1][3],str) and all(isinstance(v[1][i],list) and all(isinstance(x,str) for x in v[1][i]) for i in (0,1,2,4))}
        except (OSError,ValueError,AttributeError,EOFError): pass
    def lookup(self,k,fn,audit):
        entry=self.rows.get(k); now=time.time(); old=None
        if isinstance(entry,list) and len(entry)==2 and isinstance(entry[0],(int,float)) and isinstance(entry[1],list) and len(entry[1])==5:
            age=now-entry[0]
            if age>=0:
                old=entry[1]
                if age<(TTL if any(old) else 7*86400): audit[self.name+': cache hit']+=1;return old
        if self.disabled or self.used>=self.limit or k in self.attempted: return old
        self.attempted.add(k)
        self.used+=1
        try:
            result=fn()
            # Preserve verified fields when a refresh returns only partial data.
            if old: result=[new or previous for new,previous in zip(result,old)]
            self.rows[k]=[now,result];self.dirty=True;self.errors=0;return result
        except ProviderError as e:
            self.errors+=1;audit[self.name+': errors']+=1
            if self.errors>=3 or str(e) in ('HTTP 400','HTTP 401','HTTP 403','HTTP 429','HTTP 430','HTTP 431'): self.disabled=True
            return old
        finally: time.sleep(.25)
    def save(self):
        if not self.dirty: return
        self.path.parent.mkdir(parents=True,exist_ok=True)
        # Bound cache by records and compressed bytes; keep newest first.
        rows=dict(sorted(self.rows.items(),key=lambda x:x[1][0],reverse=True)[:12000]);tmp=self.path.with_suffix('.tmp')
        while True:
            with gzip.open(tmp,'wt',encoding='utf-8',compresslevel=5) as f: json.dump({'version':1,'rows':rows},f,ensure_ascii=False,separators=(',',':'))
            if tmp.stat().st_size<=4*1024*1024: break
            rows=dict(list(rows.items())[:len(rows)//2])
        tmp.replace(self.path)


def supplement(cat,payload,content_ids):
    rows=payload['games']; audit=Counter(); indexes={}; caches={s:ProviderCache(s) for s in FALLBACK_BUDGET}
    rawg_keys=re.split(r'[\s,;]+',os.environ.get('RAWG_KEY','').strip());rawg=rawg_keys[0]; ss={'devid':os.environ.get('SCREENSCRAPER_DEV_ID',''),'devpassword':os.environ.get('SCREENSCRAPER_DEV_PASSWORD','')}
    if os.environ.get('SCREENSCRAPER_USER') and os.environ.get('SCREENSCRAPER_PASSWORD'): ss.update(ssid=os.environ['SCREENSCRAPER_USER'],sspassword=os.environ['SCREENSCRAPER_PASSWORD'])
    if not rawg: audit['RAWG not configured']=1
    if not ss['devid'] or not ss['devpassword']: audit['ScreenScraper not configured']=1
    jobs=[];title_safe=set();deadline=time.monotonic()+FALLBACK_TIME_BUDGET
    for tid,variants in sorted(cat.items()):
        tags={v[0] for v in variants}; regions={v[1] for v in variants}; keys={title_key(v[2]) for v in variants}
        if len(tags)!=1: continue
        tag=next(iter(tags));row=rows.get(tid,[0,tag,[],[],[],'',[]])
        # LaunchBox must independently resolve every variant to the same game.
        if not all(row[i] for i in (2,3,4)):
            lb=launchbox_lookup(variants,indexes)
            if lb: merge(row,lb,'LaunchBox',audit)
        # Prefer full regional date only from PS Store or IGDB, not other platform dates.
        safe=len(keys)==1 and '' not in keys
        if safe: title_safe.add(tid)
        region=next(iter(regions)) if len(regions)==1 else ''
        cids=content_ids.get(tid,set())
        cid=(next(iter(cids)) if len(cids)==1 else '' if not cids and safe else None) if region else None
        if safe or cid:
            name=min(v[2] for v in variants)
            jobs.append((tid,tag,region,name,cid,row))
        if any(row[2:]): rows[tid]=row
    # Unqueried entries first; refresh expired entries after new work. Cache hits still apply to every title.
    remaining_sources=1+bool(rawg)+bool(ss['devid'] and ss['devpassword'])
    def run(source,key_fn,eligible,fetch):
        nonlocal remaining_sources
        cache=caches[source]
        active=not cache.disabled
        source_deadline=time.monotonic()+max(0,deadline-time.monotonic())/max(1,remaining_sources)
        if active: remaining_sources-=1
        grouped=defaultdict(list)
        for j in jobs:
            if eligible(j): grouped[key_fn(j)].append(j)
        positions=(3,5) if source=='psn' else (2,3,4)
        def priority(pair):
            k, members=pair
            return (cache.rows.get(k,[0])[0],-sum(not j[-1][i] for j in members for i in positions),k)
        candidates=grouped.items() if cache.disabled else sorted(grouped.items(),key=priority)
        for k,members in candidates:
            if time.monotonic()>=source_deadline: cache.disabled=True
            value=cache.lookup(k,lambda:fetch(max(members,key=lambda j:sum(not j[-1][i] for i in positions))),audit)
            if value:
                for j in members: merge(j[-1],value,source,audit)
        audit[source+': requests']=cache.used
    try:
        # PS Store identifies by product rather than fuzzy name, but is used only to fill missing fields.
        run('psn',lambda j:'|'.join((j[0],j[2],j[4])),lambda j:j[4] is not None and re.fullmatch(r'[A-Z]{4}\d{5}',j[0]) and (not j[-1][3] or not j[-1][5]) and psn_locale(j[2],j[4]) is not None,lambda j:psn_lookup(j[0],j[2],j[4]))
        if rawg or caches['rawg'].rows:
            if not rawg: caches['rawg'].disabled=True
            run('rawg',lambda j:j[1]+'|'+title_key(j[3]),lambda j:j[0] in title_safe and not all(j[-1][i] for i in (2,3,4)),lambda j:rawg_lookup(j[1],j[3],rawg))
        if (ss['devid'] and ss['devpassword']) or caches['screenscraper'].rows:
            if not ss['devid'] or not ss['devpassword']: caches['screenscraper'].disabled=True
            run('screenscraper',lambda j:'|'.join((j[0],j[1],title_key(j[3]))),lambda j:j[0] in title_safe and bool(re.fullmatch(r'(?:B[CL]|S[CL]|U[CL])[A-Z]{2}\d{5}',j[0])) and not all(j[-1][i] for i in (2,3,4)),lambda j:ss_lookup(j[0],j[1],j[3],ss))
    finally:
        for c in caches.values(): c.save()
    for tid,_,_,_,_,row in jobs:
        if any(row[2:]): rows[tid]=row
    for row in rows.values():
        for i in (2,3,4,6): row[i]=strings(row[i],i in (4,6))
    audit['indexed after fallbacks']=len(rows)
    for i,label in [(2,'developer'),(3,'publisher'),(4,'genre'),(5,'date')]: audit['final missing '+label]=sum(not r[i] for r in rows.values())+len(cat)-len(rows)
    return audit

def main():
    load_provider_secrets()
    content_ids=defaultdict(set);cat=catalog(content_ids); payload,audit,examples=build(cat,fetch_games())
    if not audit['matched']: raise ValueError('No trusted IGDB matches; refusing to replace index')
    combined=supplement(cat,payload,content_ids)
    old_text=INDEX.read_text(encoding='utf-8') if INDEX.exists() else None
    old=json.loads(old_text) if old_text is not None else None
    if isinstance(old,dict) and old.get('version')==1:
        for tid,row in payload['games'].items():
            previous=old.get('games',{}).get(tid)
            if not isinstance(previous,list) or len(previous)!=7: continue
            if previous[0] and not row[0]: combined['previous IGDB identity no longer resolved']+=1
            if previous[0] and row[0] and previous[:2]!=row[:2]: combined['previous IGDB identity changed']+=1
            if not row[0] or row[:2]!=previous[:2]: continue
            # Keep previously verified non-date metadata for the same identity only.
            for pos,label in ((2,'developer'),(3,'publisher'),(4,'genre'),(6,'theme')):
                if not row[pos]:
                    value=strings(previous[pos],pos in (4,6))
                    if value: row[pos]=value; combined['retained previous '+label]+=1
        for i,label in [(2,'developer'),(3,'publisher'),(4,'genre'),(5,'date')]:
            combined['final missing '+label]=sum(not r[i] for r in payload['games'].values())+len(cat)-len(payload['games'])
    if not payload['games']: raise ValueError('No trusted matches; refusing to replace index')
    report=['## IGDB metadata coverage','',f'Eligible Title IDs: **{len(cat)}**','', '| Metric | Title IDs |','|---|---:|']+[f'| {k} | {v} |' for k,v in sorted(audit.items())]
    report+=['','Missing fields above describe IGDB only.','', '## Combined discovery index', '', '| Metric | Title IDs / lookups |','|---|---:|']+[f'| {k} | {v} |' for k,v in sorted(combined.items())]
    report+=['','Live manual overrides are applied in the browser; not included in these counts.']
    for reason,items in examples.items(): report+=['',f'### {reason} (sample)','',*['- '+x.replace('\n',' ') for x in items]]
    if os.environ.get('GITHUB_STEP_SUMMARY'):
        with open(os.environ['GITHUB_STEP_SUMMARY'],'a',encoding='utf-8') as f:f.write('\n'.join(report)+'\n')
    print('\n'.join(report[:len(audit)+7]))
    payload['games']=dict(sorted(payload['games'].items()))
    INDEX.parent.mkdir(parents=True,exist_ok=True); text=json.dumps(payload,ensure_ascii=False,separators=(',',':'))+'\n'
    if len(text.encode())>8*1024*1024: raise ValueError('Index exceeds browser limit')
    if old is not None:
        previous_ids=set(old.get('games',{})) & cat.keys()
        retained=len(previous_ids & payload['games'].keys())
        print(f'Coverage of previously indexed catalog IDs: {retained}/{len(previous_ids)}')
        if retained<.8*len(previous_ids): raise ValueError('Coverage unexpectedly dropped; refusing to replace index')
        if old_text==text:return
    tmp=INDEX.with_suffix('.tmp');tmp.write_text(text,encoding='utf-8');tmp.replace(INDEX)


if __name__=='__main__': main()
