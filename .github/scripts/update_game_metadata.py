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


def catalog(content_ids=None):
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
                if content_ids is not None and re.fullmatch(r'[A-Z]{2}\d{4}-[A-Z0-9]{9}_\d{2}-[A-Z0-9]{16}',cid.strip().upper()) and tid in cid.upper(): content_ids[tid].add(cid.strip().upper())
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
    if not cid or not secret: raise RuntimeError('Missing IGDB credentials in server-secrets.json')
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


def lb_key(name):
    s=unicodedata.normalize('NFKD',str(name).replace('™',' ').replace('®',' ').replace('©',' '))
    return re.sub(r'[^a-z0-9]+',' ',''.join(c for c in s if not unicodedata.combining(c)).lower()).strip()


def launchbox_lookup(variants, indexes):
    matches=[]
    for tag,region,name in variants:
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
        gid=candidates.pop(); matches.append((platform,gid))
    if len(set(matches))!=1 or not matches: return None
    platform,gid=matches[0]; m=indexes[platform].get('meta',{}).get(gid)
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


def psn_lookup(tid,region,cid):
    prefix=cid[:2] if cid else ''
    country,lang=('JP','ja') if prefix=='JP' or region=='JP' else ('KR','ko') if prefix=='KP' or region=='KR' else ('HK','en') if prefix in ('HP','AP') or region=='ASIA' else ('GB','en') if prefix=='EP' or region=='EU' else ('US','en')
    endpoint='container' if cid else 'titlecontainer'
    data=get_json(f'https://store.playstation.com/store/api/chihiro/00_09_000/{endpoint}/{country}/{lang}/19/{cid or tid}',missing_ok=True)
    if not isinstance(data,dict): return compact()
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
        self.path=CACHE_DIR/f'{name}.json.gz';self.rows={};self.used=0;self.limit=FALLBACK_BUDGET[name];self.disabled=False;self.errors=0;self.name=name
        try:
            with gzip.open(self.path,'rt',encoding='utf-8') as f: data=json.load(f)
            if data.get('version')==1 and isinstance(data.get('rows'),dict):
                self.rows={k:v for k,v in data['rows'].items() if isinstance(k,str) and isinstance(v,list) and len(v)==2 and isinstance(v[0],(int,float)) and isinstance(v[1],list) and len(v[1])==5}
        except (OSError,ValueError,AttributeError): pass
    def lookup(self,k,fn,audit):
        entry=self.rows.get(k); now=time.time(); old=None
        if isinstance(entry,list) and len(entry)==2 and isinstance(entry[0],(int,float)) and isinstance(entry[1],list) and len(entry[1])==5:
            old=entry[1]
            if now-entry[0]<(TTL if any(old) else 7*86400): audit[self.name+': cache hit']+=1;return old
        if self.disabled or self.used>=self.limit: return old
        self.used+=1
        try:
            result=fn(); self.rows[k]=[now,result];self.errors=0;return result
        except ProviderError as e:
            self.errors+=1;audit[self.name+': errors']+=1
            if self.errors>=3 or str(e) in ('HTTP 400','HTTP 401','HTTP 403','HTTP 429','HTTP 430','HTTP 431'): self.disabled=True
            return old
        finally: time.sleep(.25)
    def save(self):
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
    jobs=[];deadline=time.monotonic()+FALLBACK_TIME_BUDGET
    for tid,variants in sorted(cat.items()):
        tags={v[0] for v in variants}; regions={v[1] for v in variants}; keys={title_key(v[2]) for v in variants}
        if len(tags)!=1: continue
        tag=next(iter(tags));row=rows.get(tid,[0,tag,[],[],[],'',[]])
        # Avoid ambiguous title associations. Only a single catalog identity can use remote title lookup.
        if len(keys)==1 and not all(row[i] for i in (2,3,4)):
            lb=launchbox_lookup(variants,indexes)
            if lb: merge(row,lb,'LaunchBox',audit)
        # Prefer full regional date only from PS Store or IGDB, not other platform dates.
        if len(keys)==1 and len(regions)==1:
            region=next(iter(regions)); name=sorted(variants)[0][2]; cids=content_ids.get(tid,set()); cid=next(iter(cids)) if len(cids)==1 else '' if not cids else None
            jobs.append((tid,tag,region,name,cid,row))
        if any(row[2:]): rows[tid]=row
    # Unqueried entries first; refresh expired entries after new work. Cache hits still apply to every title.
    def run(source,key_fn,eligible,fetch):
        cache=caches[source]
        candidates=[j for j in jobs if eligible(j)]
        candidates.sort(key=lambda j:cache.rows.get(key_fn(j),[0])[0])
        for j in candidates:
            k=key_fn(j)
            if time.monotonic()>=deadline: cache.disabled=True
            value=cache.lookup(k,lambda j=j:fetch(j),audit)
            if value: merge(j[-1],value,source,audit)
        audit[source+': requests']=cache.used
    try:
        # PS Store identifies by product rather than fuzzy name, but is used only to fill missing fields.
        run('psn',lambda j:'|'.join((j[0],j[2],j[4])),lambda j:j[4] is not None and (not j[-1][3] or not j[-1][5]),lambda j:psn_lookup(j[0],j[2],j[4]))
        if rawg or caches['rawg'].rows:
            if not rawg: caches['rawg'].disabled=True
            run('rawg',lambda j:j[1]+'|'+title_key(j[3]),lambda j:not all(j[-1][i] for i in (2,3,4)),lambda j:rawg_lookup(j[1],j[3],rawg))
        if (ss['devid'] and ss['devpassword']) or caches['screenscraper'].rows:
            if not ss['devid'] or not ss['devpassword']: caches['screenscraper'].disabled=True
            run('screenscraper',lambda j:'|'.join((j[0],j[1],title_key(j[3]))),lambda j:bool(re.fullmatch(r'(?:B[CL]|S[CL]|U[CL])[A-Z]{2}\d{5}',j[0])) and not all(j[-1][i] for i in (2,3,4)),lambda j:ss_lookup(j[0],j[1],j[3],ss))
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
    combined=supplement(cat,payload,content_ids)
    if not audit['matched']: raise ValueError('No trusted IGDB matches; refusing to replace index')
    if not payload['games']: raise ValueError('No trusted matches; refusing to replace index')
    report=['## IGDB metadata coverage','',f'Eligible Title IDs: **{len(cat)}**','', '| Metric | Title IDs |','|---|---:|']+[f'| {k} | {v} |' for k,v in sorted(audit.items())]
    report+=['','Missing fields above describe IGDB only.','', '## Combined discovery index', '', '| Metric | Title IDs / lookups |','|---|---:|']+[f'| {k} | {v} |' for k,v in sorted(combined.items())]
    report+=['','Live manual overrides are applied in the browser; not included in these counts.']
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
