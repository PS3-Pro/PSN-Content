#!/usr/bin/env python3
"""Index public original game-manual PDFs; no PDF downloads or redistribution."""
import argparse
import collections
import csv
from collections import defaultdict
import gzip
import json
import re
import time
import unicodedata
from functools import lru_cache
from pathlib import Path
from urllib.parse import urlencode, quote, urlsplit
from urllib.request import Request, HTTPRedirectHandler, build_opener
from urllib.error import HTTPError, URLError

API = "https://archive.org/"
MAX_PDF = 16 * 1024 * 1024
MAX_JSON = 32 * 1024 * 1024
CACHE_TTL = 30 * 86400
NEGATIVE_TTL = 7 * 86400
COLLECTION_TTL = 86400
SEARCH_TTL = 86400
CACHE_MAX_BYTES = 8 * 1024 * 1024
CACHE_MAX_ITEMS = 12000
DISCOVERY_BUDGET = 150
SEARCH_PAGE_SIZE = 500
MAX_SEARCH_PAGES = 20
MAX_BYTES = MAX_PDF
INDEX = Path("resources/database/manuals")
CATALOG = Path("resources/database/content/official")
LEGACY_STATE = Path(".cache/ps3-manual-index/state.json")
CACHE_PATH = Path('.cache/manual-index/metadata.json.gz')
COLLECTIONS = {'ps1': 'SonyPlaystationManuals', 'ps2': 'SonyPlaystation2Manuals_201812', 'psp': 'SonyPSPManuals'}
PLATFORMS = {'ps3': r'PS3|(?:Sony\s+)?PlayStation\s*3', 'ps4': r'PS4|(?:Sony\s+)?PlayStation\s*4', 'psvita': r'PS\s*Vita|(?:Sony\s+)?PlayStation\s*Vita'}
REGIONS = {'usa':'us','us':'us','north america':'us','europe':'eu','eur':'eu','eu':'eu','uk':'eu','japan':'jp','jp':'jp','jpn':'jp','australia':'au','world':'world','asia':'asi','korea':'kr','brazil':'br'}
ID = re.compile(r'[A-Za-z0-9][A-Za-z0-9_.-]{0,179}\Z')
REJECT = re.compile(r'\b(?:strategy guide|walkthrough|quick start|safety|instruction sheet|artbook|card book|soundtrack|demo|sample|preview|insert|leaflet|service manual|console manual|controller manual)\b', re.I)

SERIAL = re.compile(r'\b(?:B[CL][UEJAK]S|NP[UEJHIK][AB])[-_ ]?\d{5}\b', re.I)
PS3 = re.compile(r'\b(?:playstation\s*3|ps3)\b', re.I)
MANUAL = re.compile(r'\b(?:manuals?|instruction\s+(?:manual|booklet|book)|game\s+instructions)\b', re.I)
EXCLUDED = re.compile(r'\b(?:strategy\s+guide|walkthrough|prima|bradygames|guidebook|play\s*view|sound\s*track|sountrack|ost|banda\s+sonora|colonna\s+sonora|trilha\s+sonora|original\s+score|trailer|teaser|featurette|making\s+(?:of|the)|behind\s+the\s+scenes|short\s+film|quick\s+start|safety\s+(?:and|&)\s+support|service\s+manual|console\s+manual|accessory\s+manual|inserts?|art\s*book|first\s*play|qore|demo|beta|network\s+test|ps[12]\s+classics?|minis)\b', re.I)
OTHER_PLATFORM = re.compile(r'\b(?:xbox(?:\s*360)?|playstation\s*[1245]|ps[1245]|psp|ps\s*vita|nintendo|wii(?:\s*u)?|switch|pc|windows|steam)\b', re.I)
REGION_RE = {'us': r'\b(?:usa|united states|north america|ntsc[ -]?u|us)\b', 'eu': r'\b(?:europe|eur|eu|pal|united kingdom|uk)\b', 'jp': r'\b(?:japan|jpn|ntsc[ -]?j)\b', 'au': r'\b(?:australia|aus)\b', 'asi': r'\b(?:asia|asian)\b', 'br': r'\b(?:brazil|brasil)\b', 'kr': r'\b(?:korea|kor)\b', 'world': r'\b(?:worldwide|world)\b'}
SEEDS = ('grand-theft-auto-v_bles-01807-manual',)
QUERY = 'mediatype:texts AND (title:manual* OR subject:manual* OR title:"instruction booklet") AND (title:"PlayStation 3" OR title:PS3 OR subject:"PlayStation 3" OR subject:PS3 OR identifier:ps3*)'


def text(value):
    if isinstance(value, list):
        return ' '.join(text(v) for v in value)
    return value if isinstance(value, str) else ''


@lru_cache(maxsize=8192)
def normalized(value):
    value = re.sub(r'[™®©]|\b(?:tm|registered)\b', ' ', value, flags=re.I)
    value = re.sub(r'\s*[\[(](?:usa|eur|europe|japan|ps3|ps2|psp|ps1|ps4)[)\]]', '', value, flags=re.I)
    value = re.sub(r'\bdigital version\b', '', value, flags=re.I)
    value = unicodedata.normalize('NFKD', value)
    value = ''.join(c for c in value if not '\u0300' <= c <= '\u036f')
    return re.sub(r'[^a-z0-9]+', ' ', value.lower()).strip()


@lru_cache(maxsize=8192)
def title_key(value):
    value = value.replace('_', ' ')
    value = re.sub(r'[\[(]\s*(?:usa|us|eur|eu|europe|uk|japan|jpn|asia|australia|world|english|french|german|spanish|italian|japanese|(?:en|fr|de|es|it|ja|pt|ru)(?:\s*[,/]\s*(?:en|fr|de|es|it|ja|pt|ru))*)\s*[)\]]', ' ', value, flags=re.I)
    value = SERIAL.sub(' ', value)
    value = re.sub(r'\b(?:sony\s+)?(?:playstation\s*3|ps3)\b', ' ', value, flags=re.I)
    value = re.sub(r'\b(?:instruction\s+manual|instruction\s+booklet|game\s+manual|manual|game\s+instructions)\b', ' ', value, flags=re.I)
    value = re.sub(r'(?:[_ -]+(?:text|searchable|ocr))$', '', value, flags=re.I)
    return normalized(value)


@lru_cache(maxsize=8192)
def territory(value):
    regions = set()
    for serial in SERIAL.findall(value):
        s = re.sub(r'[^A-Z0-9]', '', serial.upper())
        code = s[2]
        region = {'U':'us','E':'eu','J':'jp','A':'asi','H':'asi','K':'kr'}.get(code)
        if region:
            regions.add(region)
    # Only interpret region annotations, not words inside a game's title.
    tags = ' '.join(re.findall(r'[\[(]([^\])]+)[\])]', value))
    regions.update(r for r, pattern in REGION_RE.items() if re.search(pattern, tags, re.I))
    return next(iter(regions)) if len(regions) == 1 else '' if not regions else None


def catalog(root):
    modern = root / 'files/website/modern.html'
    if not modern.is_file():
        raise ValueError('Expected files/website/modern.html; cannot load Browse app exclusions')
    html = modern.read_text(encoding='utf-8-sig')
    app_block = re.search(r'\bconst DICT_APPS_FILTER\s*=\s*\{(.*?)\};', html, re.S)
    if not app_block:
        raise ValueError('Browse app filter not found')
    apps = set(re.findall(r'[\'"]([A-Z0-9]{9})[\'"]\s*:\s*1\b', app_block.group(1)))
    if not apps:
        raise ValueError('Empty Browse app filter')
    games = defaultdict(set)
    for path in (root / CATALOG / 'games.tsv', root / CATALOG / 'pending/games.tsv'):
        if not path.exists():
            if path.parent.name != 'pending':
                raise ValueError(f'Missing {path}')
            continue
        with path.open(encoding='utf-8-sig', newline='') as f:
            for row in csv.reader(f, delimiter='\t'):
                if len(row) < 4:
                    continue
                tid, region, name, pkg = (v.strip() for v in row[:4])
                tid = tid.upper()
                if not re.fullmatch(r'(?:NP[UEJHIK][AB]|B[CL][UEJAK]S)\d{5}', tid) or tid in apps or not pkg or pkg.upper() == 'MISSING' or not name or name.upper() == 'MISSING':
                    continue
                if EXCLUDED.search(name) or re.search(r'\b(?:trial|test\s*version)\b', name, re.I) or re.search(r'\b(?:movie|video)\s+service\b', name, re.I):
                    continue
                cid = row[5].upper() if len(row)>5 else ''
                if re.search(r'VIDEO|MOVIES?|TRAILER|FEATURETTE|MAKINGOF|BEHINDTHESCENES', cid) and not re.search(r'MOVIEGAME|LEGOMOVIE|GVIDEOPOKE', cid):
                    continue
                key = normalized(name)
                if key:
                    games[key].add(tid)
                # Known distribution suffixes only; do not remove editions or subtitles.
                cleaned = re.sub(r'\s*[\[(]?(?:digital version|full game|downloadable version)[)\]]?\s*$', '', name, flags=re.I).strip()
                key = normalized(cleaned)
                if key:
                    games[key].add(tid)
    if not games:
        raise ValueError('Empty eligible PS3 catalog')
    return games



@lru_cache(maxsize=8192)
def norm(s):
    s = re.sub('[™®©]', ' ', s)
    s = ''.join(c for c in unicodedata.normalize('NFKD',s) if not unicodedata.combining(c))
    return re.sub('[^a-z0-9]+',' ',s.lower()).strip()


@lru_cache(maxsize=8192)
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


class ArchiveRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        target = urlsplit(newurl)
        host = target.hostname or ''
        if target.scheme != 'https' or not (host == 'archive.org' or host.endswith('.archive.org')):
            raise HTTPError(req.full_url, 403, 'Untrusted redirect', headers, fp)
        redirected = super().redirect_request(req, fp, code, msg, headers, newurl)
        if redirected is not None and req.get_method() == 'HEAD':
            redirected.method = 'HEAD'
        return redirected


class Archive:
    def __init__(self, cache_path, legacy_state=None):
        self.path = cache_path
        self.entries = {}
        self.searches = {}
        self.metadata_rows = {}
        self.cursor = 0
        migrated = False
        self.head_results = {}
        self.deadline = time.monotonic() + 18 * 60
        self.blocked = False
        self.dirty = False
        self.calls = self.hits = 0
        self.next_request = 0.0
        self.opener = build_opener(ArchiveRedirect())
        try:
            if cache_path.stat().st_size > CACHE_MAX_BYTES:
                raise ValueError('cache size')
            with gzip.open(cache_path, 'rb') as f:
                raw = f.read(32 * 1024 * 1024 + 1)
            if len(raw) > 32 * 1024 * 1024:
                raise ValueError('cache size')
            data = json.loads(raw)
            if data.get('version') not in (1, 2) or not isinstance(data.get('entries'), dict):
                raise ValueError('cache version')
            now = time.time()
            for ident, entry in data['entries'].items():
                if ID.fullmatch(ident) and isinstance(entry, list) and len(entry) == 2 and type(entry[0]) in (int, float) and 0 <= now-entry[0] and isinstance(entry[1], list) and all(valid_pdf_pair(p) for p in entry[1]):
                    self.entries[ident] = entry
            for platform, entry in data.get('searches', {}).items():
                if (platform in PLATFORMS and isinstance(entry,list) and len(entry)==2
                        and type(entry[0]) in (int,float) and 0<=now-entry[0]<SEARCH_TTL
                        and valid_search_docs(entry[1])):
                    self.searches[platform]=entry
            if data.get('version') == 2:
                self.cursor = max(0, int(data.get('cursor', 0)))
                self.metadata_rows = load_metadata_rows(data.get('metadata', {}), now)
                migrated = True
        except (OSError, EOFError, ValueError, AttributeError, TypeError, OverflowError):
            self.entries = {}
            self.searches = {}
            self.metadata_rows = {}
            self.cursor = 0
        if not migrated and legacy_state:
            try:
                if legacy_state.stat().st_size > CACHE_MAX_BYTES:
                    raise ValueError('legacy cache size')
                old = json.loads(legacy_state.read_text(encoding='utf-8'))
                if old.get('version') == 1:
                    self.metadata_rows = load_metadata_rows(old.get('metadata', {}), time.time())
                    self.cursor = max(0, int(old.get('cursor', 0)))
                    self.dirty = True
            except (OSError, ValueError, TypeError, AttributeError, OverflowError):
                pass

    def request(self, path, query=None, method="GET"):
        if self.blocked: raise RuntimeError('Archive requests paused for this run after rate limit')
        url = 'https://archive.org/' + path + (('?' + urlencode(query, doseq=True)) if query else '')
        for attempt in range(3):
            if max(time.monotonic(),self.next_request) + 25 > self.deadline:
                raise TimeoutError('Archive network time budget exhausted')
            time.sleep(max(0, self.next_request - time.monotonic()))
            self.next_request = time.monotonic() + 1.1
            self.calls += 1
            try:
                with self.opener.open(Request(url, method=method, headers={'User-Agent':'PS3-Pro manual index (metadata only)', 'Accept':'application/json' if method=='GET' else '*/*'}), timeout=25) as response:
                    if method == 'HEAD':
                        length = response.headers.get('Content-Length', '')
                        return (length.isascii() and length.isdigit() and len(length)<=10
                                and 0 < int(length) <= MAX_PDF and response.headers.get_content_type() in ('application/pdf', 'application/octet-stream'))
                    raw = response.read(MAX_JSON + 1)
                if len(raw) > MAX_JSON:
                    raise ValueError('metadata limit exceeded')
                return json.loads(raw)
            except HTTPError as exc:
                retry = exc.headers.get('Retry-After', '') if exc.headers else ''
                if method == 'HEAD' and exc.code in (401,403,404,405,410) and not retry:
                    return False
                if (retry.isdigit() and int(retry) > 60) or (exc.code == 429 and attempt == 2):
                    self.blocked = True
                    raise RuntimeError('Archive rate limited; remaining network requests paused') from None
                if exc.code not in (429, 500, 502, 503, 504) or attempt == 2:
                    raise
                delay = max(2 ** (attempt+1), int(retry) if retry.isdigit() else 0)
            except (URLError, TimeoutError, OSError):
                if attempt == 2:
                    raise
                delay = 2 ** (attempt+1)
            self.next_request = max(self.next_request, time.monotonic() + delay)

    def cached(self, ident, collection=False):
        if not collection and (data := self.fresh_metadata(ident)) is not None:
            return sorted(set(original_pdfs(data)))
        entry = self.entries.get(ident)
        if entry:
            ttl = COLLECTION_TTL if collection else CACHE_TTL if entry[1] else NEGATIVE_TTL
            if 0 <= time.time() - entry[0] < ttl:
                return entry[1]
        return None

    def pdfs(self, ident, collection=False):
        files = self.cached(ident, collection)
        if files is not None:
            self.hits += 1
            return files
        files = sorted(set(original_pdfs(self.request('metadata/' + quote(ident, safe='')) if collection else self.metadata(ident))))
        if collection or self.fresh_metadata(ident) is None:
            self.entries[ident] = [time.time(), files]
            self.dirty = True
        return files

    def fresh_metadata(self, ident):
        entry = self.metadata_rows.get(ident)
        if entry and 0 <= time.time()-entry[0] < (CACHE_TTL if entry[1].get('files') and not restricted(entry[1]) else NEGATIVE_TTL):
            return entry[1]
        return None

    def metadata(self, ident):
        now = time.time()
        if (data := self.fresh_metadata(ident)) is not None:
            self.hits += 1
            return data
        data = compact_metadata(self.request('metadata/' + quote(ident, safe=''), {'extended_err':1}))
        if len(json.dumps(data, ensure_ascii=True)) <= 128 * 1024:
            self.metadata_rows[ident] = [now, data]
            self.entries.pop(ident,None)
            self.dirty = True
        return data

    def available(self, row):
        pair = tuple(row[:2])
        if pair not in self.head_results:
            self.head_results[pair] = self.request('download/'+quote(pair[0],safe='')+'/'+quote(pair[1],safe=''), method='HEAD')
        return self.head_results[pair]

    def search(self, limit):
        found = set(SEEDS[:limit])
        offset = self.cursor
        visited = 0
        total = None
        while len(found) < limit:
            page, skip = divmod(offset, 100)
            response = self.request('advancedsearch.php', {'q':QUERY,'fl[]':'identifier','sort[]':'identifier asc','rows':100,'page':page+1,'output':'json'})['response']
            count = response.get('numFound')
            docs = response.get('docs')
            if type(count) is not int or count < 0 or not isinstance(docs,list):
                raise ValueError('invalid PS3 search response')
            if total is not None and count != total:
                raise ValueError('PS3 search changed during pagination')
            total = count
            if total == 0 or visited >= total:
                if not total: offset = 0
                break
            if offset >= total:
                offset = 0
                continue
            if len(docs) != min(100,total-page*100):
                raise ValueError('truncated PS3 search page')
            for doc in docs[skip:]:
                ident = doc.get('identifier') if isinstance(doc,dict) else None
                if not isinstance(ident,str) or not ID.fullmatch(ident):
                    raise ValueError('invalid PS3 search identifier')
                offset += 1
                visited += 1
                found.add(ident)
                if len(found) >= limit or visited >= total: break
            if offset >= total: offset = 0
            if visited >= total: break
        self.cursor = offset
        self.dirty = True
        return sorted(found)

    def save(self):
        if not self.dirty:
            return
        self.path.parent.mkdir(parents=True, exist_ok=True)
        temp = self.path.with_suffix('.tmp')
        rows = dict(sorted(self.entries.items(), key=lambda item: item[1][0], reverse=True)[:CACHE_MAX_ITEMS])
        try:
            searches = {p:e for p,e in self.searches.items() if 0 <= time.time()-e[0] < SEARCH_TTL}
            metadata = dict(sorted(self.metadata_rows.items(), key=lambda item:item[1][0], reverse=True)[:CACHE_MAX_ITEMS])
            while True:
                raw = json.dumps({'version':2, 'entries':rows, 'searches':searches, 'metadata':metadata, 'cursor':self.cursor}, ensure_ascii=False, separators=(',', ':')).encode('utf-8')
                if len(raw) <= 32 * 1024 * 1024:
                    with gzip.open(temp, 'wb', compresslevel=3) as f:
                        f.write(raw)
                    if temp.stat().st_size <= CACHE_MAX_BYTES:
                        temp.replace(self.path)
                        self.dirty = False
                        return
                if metadata and len(metadata) >= len(rows):
                    metadata = dict(list(metadata.items())[:len(metadata)//2])
                elif rows:
                    rows = dict(list(rows.items())[:len(rows)//2])
                elif searches:
                    searches = {}
                else:
                    raise ValueError('Manual cache exceeds limits')
        finally:
            temp.unlink(missing_ok=True)


def restricted(data):
    return any(str(v).lower() in ('true','1','yes') for v in (data.get('is_dark'), data.get('metadata',{}).get('access-restricted-item')))


def compact_metadata(data):
    if not isinstance(data,dict) or data.get('error') or not isinstance(data.get('metadata'),dict) or not isinstance(data.get('files'),list):
        raise ValueError('invalid IA item metadata')
    meta = data['metadata']
    return {'is_dark':data.get('is_dark'), 'metadata':{k:meta[k] for k in ('title','subject','language','mediatype','access-restricted-item') if k in meta},
            'files':[{k:f[k] for k in ('name','size','source','private') if k in f} for f in data['files']
                     if isinstance(f,dict) and isinstance(f.get('name'),str) and f['name'].lower().endswith('.pdf')]}


def load_metadata_rows(rows, now):
    result = {}
    if not isinstance(rows,dict): return result
    for ident,entry in rows.items():
        if not ID.fullmatch(ident) or not isinstance(entry,list) or len(entry)!=2 or type(entry[0]) not in (int,float) or not 0<=now-entry[0]<CACHE_TTL: continue
        try:
            result[ident] = [entry[0],compact_metadata(entry[1])]
        except (ValueError,TypeError): pass
    return result


def valid_pdf_pair(pair):
    return (isinstance(pair, (tuple, list)) and len(pair) == 2 and isinstance(pair[0], str)
            and pair[0].lower().endswith('.pdf') and len(pair[0]) <= 240
            and not any(c in pair[0] for c in ('/', '\\')) and not any(ord(c)<32 for c in pair[0])
            and type(pair[1]) is int and 0 < pair[1] <= MAX_PDF
            and not REJECT.search(pair[0]) and not re.search(r'_(?:text|bw)\.pdf$',pair[0],re.I))


def original_pdfs(meta):
    if not isinstance(meta,dict) or not isinstance(meta.get('files'),list): raise ValueError('invalid IA metadata')
    if meta.get('error') or not isinstance(meta.get('metadata'),dict): raise ValueError('invalid IA item')
    if any(str(value).lower() in ('true','1','yes') for value in (meta.get('is_dark'),meta['metadata'].get('access-restricted-item'))): return
    for f in meta['files']:
        if not isinstance(f,dict) or f.get('source')!='original' or str(f.get('private','')).lower() in ('true','1','yes'): continue
        name=f.get('name',''); size=str(f.get('size',''))
        # Reject malformed or enormous integer fields before conversion.
        if len(size)>len(str(MAX_PDF)) or not size.isascii() or not size.isdigit(): continue
        pair=(name,int(size))
        if valid_pdf_pair(pair): yield pair


def add(rows,ident,name,size,key,region):
    if ID.fullmatch(ident): rows[key].add((ident,name,region,size))


def collection_rows(ident, files):
    rows=collections.defaultdict(set)
    for name,size in files:
        result=split_name(name[:-4])
        if result: add(rows,ident,name,size,*result)
    return rows


def individual_title(title,platform):
    # Only accept items whose title explicitly identifies a game manual AND platform.
    if not isinstance(title,str) or len(title)>4096 or REJECT.search(title): return None
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


def valid_search_docs(docs):
    return (isinstance(docs,dict) and len(docs)<=MAX_SEARCH_PAGES*SEARCH_PAGE_SIZE
            and all(ID.fullmatch(ident) and (title is None or isinstance(title,str) and len(title)<=4096)
                    for ident,title in docs.items()))


def discover_docs(platform, archive):
    cached = archive.searches.get(platform)
    if cached and 0 <= time.time()-cached[0] < SEARCH_TTL:
        return cached[1]
    platform_words={'ps3':['PS3','PlayStation 3'],'ps4':['PS4','PlayStation 4'],'psvita':['PS Vita','PlayStation Vita']}[platform]
    query='mediatype:texts AND (collection:consolemanuals OR subject:"game manuals" OR subject:"video game manuals") AND title:manual AND ('+' OR '.join('title:"'+s+'"' for s in platform_words)+')'
    docs = {}
    total = None
    for page in range(1, MAX_SEARCH_PAGES+1):
        response=archive.request('advancedsearch.php',{'q':query,'fl[]':['identifier','title'],'sort[]':'identifier asc','rows':SEARCH_PAGE_SIZE,'page':page,'output':'json'})['response']
        if type(response.get('numFound')) is not int or not isinstance(response.get('docs'),list):
            raise ValueError('invalid discovery response')
        if total is None:
            total = response['numFound']
        if total != response['numFound'] or total < 0 or len(response['docs']) != min(SEARCH_PAGE_SIZE, max(0,total-(page-1)*SEARCH_PAGE_SIZE)):
            raise ValueError('discovery results changed or truncated')
        for doc in response['docs']:
            if not isinstance(doc,dict) or not isinstance(doc.get('identifier'),str) or not ID.fullmatch(doc['identifier']) or doc['identifier'] in docs:
                raise ValueError('invalid or duplicate discovery identifier')
            title=doc.get('title')
            docs[doc['identifier']] = title if isinstance(title,str) and len(title)<=4096 else None
        if len(docs) == total:
            break
    if len(docs) != total:
        raise ValueError('discovery pagination budget exceeded')
    archive.searches[platform]=[time.time(),docs]
    archive.dirty=True
    return docs


def discovery_rows(platform, archive):
    docs=discover_docs(platform,archive)
    rows=collections.defaultdict(set)
    eligible=[(ident, match) for ident,title in docs.items() if (match:=individual_title(title,platform))]
    # New identifiers first, then oldest stale entries. A large existing cache
    # cannot consume the whole budget before the unseen candidates are reached.
    eligible.sort(key=lambda item: (item[0] in archive.entries, archive.entries.get(item[0], [0])[0], item[0]))
    # Cached candidates cost no HTTP calls; new work accumulates across runs.
    missing = 0
    requests = 0
    for ident,(key,region) in eligible:
        if archive.cached(ident) is None:
            if requests >= DISCOVERY_BUDGET:
                missing += 1
                continue
            requests += 1
        files = archive.pdfs(ident)
        # Multiple PDFs can be different editions or supplements. Do not guess.
        if len(files)==1:
            add(rows,ident,*files[0],key,region)
    if missing:
        raise ValueError(f'{missing} candidates pending; cached progress saved, previous index preserved')
    return rows


def payload(platform,rows):
    # Avoid ambiguous matches for the same normalized title and territory.
    clean={}
    for key,values in sorted(rows.items()):
        by_region=collections.defaultdict(list)
        for value in sorted(values): by_region[value[2]].append(value)
        unique=[v[0] for v in by_region.values() if len(v)==1]
        if unique: clean[key]=[list(row) for row in sorted(unique)]
    return {'version':1,'platform':platform,'titles':clean}


def candidates(identifier, data, games):
    if not isinstance(data, dict) or restricted(data) or data.get('error'):
        return []
    meta = data.get('metadata', {})
    if meta.get('access-restricted-item') in (True,'true','1',1) or meta.get('mediatype') != 'texts':
        return []
    title = text(meta.get('title'))
    evidence = ' '.join((title,text(meta.get('subject')),identifier.replace('_',' ').replace('-',' ')))
    if not (PS3.search(evidence) or SERIAL.search(evidence)) or EXCLUDED.search(title) or not MANUAL.search(evidence):
        return []
    if OTHER_PLATFORM.search(title):
        return []
    item_key = title_key(title)
    language = text(meta.get('language')).lower()
    english = bool(re.search(r'\b(?:en|eng|english)\b',language))
    result = []
    for f in data.get('files', []):
        if not isinstance(f,dict): continue
        name = f.get('name','')
        if not isinstance(name,str) or len(name)>240 or not name.lower().endswith('.pdf') or re.search(r'[\\/\x00-\x1f]',name) or str(f.get('private','')).lower() in ('true','1','yes'):
            continue
        if EXCLUDED.search(name) or OTHER_PLATFORM.search(name):
            continue
        try:
            raw_size = str(f.get('size',0))
            if len(raw_size)>len(str(MAX_BYTES)) or not raw_size.isascii() or not raw_size.isdigit(): continue
            size = int(raw_size)
        except (ValueError, TypeError):
            continue
        if not 0 < size <= MAX_BYTES:
            continue
        file_key = title_key(name[:-4])
        matches = {key for key in (item_key,file_key) if key in games}
        if len(matches)!=1:
            continue
        key = next(iter(matches))
        # A generic filename can inherit the item's exact game title. A conflicting
        # title from the same catalog is never substituted for the actual file.
        if item_key in games and file_key != item_key and file_key not in ('', 'scan', 'document', 'booklet'):
            continue
        region = territory(title+' '+name+' '+identifier.replace('_',' '))
        if region is None:
            continue
        file_english = bool(re.search(r'\b(?:en|eng|english)\b',name,re.I))
        if language and not english and not file_english and region not in ('jp','kr','asi','br','eu'):
            continue
        row = [identifier,name,region,size]
        score = (int(file_english or english or region in ('us','au')),int(f.get('source')=='original'),-size)
        result.append((key,score,row))
    return result


def valid_row(row):
    return isinstance(row,list) and len(row)==4 and isinstance(row[0],str) and ID.fullmatch(row[0]) and isinstance(row[1],str) and len(row[1])<=240 and row[1].lower().endswith('.pdf') and not re.search(r'[\\/\x00-\x1f]',row[1]) and isinstance(row[2],str) and type(row[3]) is int and 0<row[3]<=MAX_BYTES


def merge(index, found, archive):
    grouped = defaultdict(list)
    for key,score,row in found:
        grouped[key].append((score,row))
    additions = 0
    for key,items in sorted(grouped.items()):
        old = index['titles'].get(key,[])
        if not isinstance(old,list):
            continue
        existing = [r for r in old if valid_row(r)]
        # Adding another region would make the old single unknown-region fallback
        # disappear in the current reader, so preserve that existing behavior.
        if any(not r[2] for r in existing):
            continue
        known = {r[2] for r in existing}
        by_region = defaultdict(list)
        for score,row in items:
            by_region[row[2]].append((score,row))
        if not known and set(by_region)=={''}:
            # Multiple independent scans with no region evidence are ambiguous.
            if len({r[0] for _,r in by_region['']})!=1:
                continue
        else:
            by_region.pop('',None)
        new = []
        for region, rows in sorted(by_region.items()):
            if region in known:
                continue
            seen = set()
            for _,row in sorted(rows,key=lambda t:(t[0],t[1][0],t[1][1]),reverse=True):
                pair = tuple(row[:2])
                if pair in seen:
                    continue
                seen.add(pair)
                if archive.available(row):
                    new.append(row)
                    break
        if new:
            index['titles'][key] = old+new
            additions += len(new)
            print(f'{key}: +{len(new)} manual(s)')
    return additions


def read_index(path, platform):
    data = json.loads(path.read_text(encoding='utf-8-sig')) if path.exists() else {'version':1,'platform':platform,'titles':{}}
    if data.get('version') != 1 or data.get('platform') != platform or not isinstance(data.get('titles'),dict):
        raise ValueError('unexpected previous index format')
    if any(not isinstance(key,str) or not isinstance(rows,list) or any(not valid_row(row) for row in rows) for key,rows in data['titles'].items()):
        raise ValueError('invalid previous index rows')
    return data


def write_index(path, data, previous):
    if data == previous and path.exists(): return False
    raw = (json.dumps(data,ensure_ascii=False,sort_keys=True,separators=(',',':'))+'\n').encode('utf-8')
    if len(raw)>4*1024*1024: raise ValueError('index size limit exceeded')
    tmp=path.with_suffix('.tmp')
    try:
        tmp.write_bytes(raw);tmp.replace(path)
    finally: tmp.unlink(missing_ok=True)
    return True


def update_ps3(root, path, archive, limit, dry_run=False):
    games = catalog(root)
    index = read_index(path,'ps3')
    previous = {'version':1,'platform':'ps3','titles':dict(index['titles'])}
    cursor = archive.cursor
    try:
        identifiers = archive.search(limit)
        found = []
        # The broad PS3 scan is authoritative; the narrow general discovery
        # is a second source of identifiers, with the same catalog and HEAD checks.
        for ident in identifiers:
            found.extend(candidates(ident,archive.metadata(ident),games))
        additions = merge(index,found,archive)
        if not dry_run and additions:
            write_index(path,index,previous)
        # Once the primary window is committed, do not roll it back if the
        # secondary search fails. Existing manual rows remain untouched.
    except Exception:
        archive.cursor = cursor
        raise
    if dry_run: return additions
    if not archive.blocked:
        try:
            docs = discover_docs('ps3',archive)
            pending = [(ident, match) for ident,title in docs.items() if (match:=individual_title(title,'ps3')) and match[0] in games]
            pending.sort(key=lambda item:(item[0] in archive.metadata_rows,archive.metadata_rows.get(item[0],[0])[0],item[0]))
            primary = set(identifiers)
            secondary = []
            used = 0
            for ident,(key,region) in pending:
                if ident in primary: continue
                existing = index['titles'].get(key,[])
                if any(not r[2] or r[2]==region for r in existing): continue
                if archive.fresh_metadata(ident) is None:
                    if used>=DISCOVERY_BUDGET: continue
                    used+=1
                data=archive.metadata(ident)
                secondary.extend(candidates(ident,data,games))
            before={'version':1,'platform':'ps3','titles':dict(index['titles'])}
            extra=merge(index,secondary,archive)
            if extra: write_index(path,index,before)
            additions+=extra
        except (OSError,ValueError,RuntimeError,TypeError,KeyError,AttributeError) as exc:
            print(f'::warning::PS3 secondary discovery: {exc}; primary scan preserved',flush=True)
    print(f'PS3: {len(games)} eligible catalog keys; {additions} added; {len(index["titles"])} indexed titles; next offset {archive.cursor}',flush=True)
    return additions


def build(output, cache_path=CACHE_PATH, platforms=None, root=Path('.'), max_items=400, dry_run=False):
    if not dry_run: output.mkdir(parents=True,exist_ok=True)
    archive = Archive(cache_path,root/LEGACY_STATE)
    failures=[]
    selected = list(dict.fromkeys(platforms or [*COLLECTIONS,'ps4','psvita','ps3']))
    for platform in selected:
        archive.deadline = time.monotonic() + (18*60 if platform=='ps3' else 5*60)
        path=output/(platform+'.json')
        try:
            if platform=='ps3':
                update_ps3(root,path,archive,max_items,dry_run)
                continue
            ident=COLLECTIONS.get(platform)
            rows=collection_rows(ident,archive.pdfs(ident,collection=True)) if ident else discovery_rows(platform,archive)
            data=payload(platform,rows); previous=read_index(path,platform)
            count=len(data['titles']); old=len(previous['titles'])
            if not count or count<old*.85: raise ValueError(f'empty or reduced index ({count}, previous {old})')
            if sum(map(len,data['titles'].values()))<.85*sum(map(len,previous['titles'].values())):
                raise ValueError('reduced manual coverage')
            changed=not dry_run and write_index(path,data,previous)
            print(f'{platform}: {count} titles; {"updated" if changed else "unchanged"}',flush=True)
        except (OSError,ValueError,RuntimeError,TypeError,KeyError,AttributeError) as exc:
            failures.append(platform)
            print(f'::warning::{platform}: {type(exc).__name__}: {exc}; previous index preserved',flush=True)
        finally:
            if not dry_run:
                try: archive.save()
                except (OSError,ValueError) as exc: print(f'::warning::Could not save manual cache: {exc}',flush=True)
    print(f'Metadata cache: {archive.hits} hits; {archive.calls} HTTP requests',flush=True)
    if failures: raise SystemExit('Incomplete manual refresh: '+', '.join(failures))


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('output_dir',type=Path,nargs='?')
    parser.add_argument('--root',type=Path,default=Path('.'))
    parser.add_argument('--cache',type=Path)
    parser.add_argument('--platform',choices=[*COLLECTIONS,*PLATFORMS],action='append')
    parser.add_argument('--max-items',type=int,default=400)
    parser.add_argument('--dry-run',action='store_true')
    args=parser.parse_args()
    if not 1<=args.max_items<=1000: parser.error('--max-items must be between 1 and 1000')
    build(args.output_dir or args.root/INDEX,args.cache or args.root/CACHE_PATH,args.platform,args.root,args.max_items,args.dry_run)

if __name__=='__main__': main()
