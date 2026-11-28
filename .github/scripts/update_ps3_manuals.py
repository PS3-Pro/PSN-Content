#!/usr/bin/env python3
import argparse
import csv
import json
import re
import time
import unicodedata
from collections import defaultdict
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlencode, urlsplit
from urllib.request import Request, HTTPRedirectHandler, build_opener

API = 'https://archive.org/'
USER_AGENT = 'PSN-Content-ManualIndex/1.0 (+https://github.com/PS3-Pro/PSN-Content)'
MAX_BYTES = 16 * 1024 * 1024
CACHE_TTL = 30 * 86400
CACHE_MAX_ITEMS = 2000
CACHE_MAX_BYTES = 8 * 1024 * 1024
STATE = Path('.cache/ps3-manual-index/state.json')
INDEX = Path('resources/database/manuals/ps3.json')
CATALOG = Path('resources/database/content/official')
ID = re.compile(r'^[A-Za-z0-9][A-Za-z0-9_.-]{0,179}$')
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


def normalized(value):
    value = re.sub(r'[™®©]|\b(?:tm|registered)\b', ' ', value, flags=re.I)
    value = re.sub(r'\s*[\[(](?:usa|eur|europe|japan|ps3|ps2|psp|ps1|ps4)[)\]]', '', value, flags=re.I)
    value = re.sub(r'\bdigital version\b', '', value, flags=re.I)
    value = unicodedata.normalize('NFKD', value)
    value = ''.join(c for c in value if not '\u0300' <= c <= '\u036f')
    return re.sub(r'[^a-z0-9]+', ' ', value.lower()).strip()


def title_key(value):
    value = value.replace('_', ' ')
    value = re.sub(r'[\[(]\s*(?:usa|us|eur|eu|europe|uk|japan|jpn|asia|australia|world|english|french|german|spanish|italian|japanese|(?:en|fr|de|es|it|ja|pt|ru)(?:\s*[,/]\s*(?:en|fr|de|es|it|ja|pt|ru))*)\s*[)\]]', ' ', value, flags=re.I)
    value = SERIAL.sub(' ', value)
    value = re.sub(r'\b(?:sony\s+)?(?:playstation\s*3|ps3)\b', ' ', value, flags=re.I)
    value = re.sub(r'\b(?:instruction\s+manual|instruction\s+booklet|game\s+manual|manual|game\s+instructions)\b', ' ', value, flags=re.I)
    value = re.sub(r'(?:[_ -]+(?:text|searchable|ocr))$', '', value, flags=re.I)
    return normalized(value)


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
    def __init__(self, state_path=None):
        self.last = 0
        self.deadline = time.monotonic() + 18 * 60
        self.opener = build_opener(ArchiveRedirect())
        self.state_path = state_path
        self.cursor = 0
        self.cache = {}
        if state_path and state_path.is_file() and state_path.stat().st_size <= CACHE_MAX_BYTES:
            try:
                state = json.loads(state_path.read_text(encoding='utf-8'))
                if state.get('version') == 1:
                    self.cursor = max(0, int(state.get('cursor', 0)))
                    self.cache = {key: entry for key, entry in state.get('metadata', {}).items()
                                  if ID.fullmatch(key) and isinstance(entry, list) and len(entry) == 2
                                  and isinstance(entry[0], (int, float)) and 0 <= time.time() - entry[0] < CACHE_TTL
                                  and isinstance(entry[1], dict)}
            except (ValueError, TypeError, AttributeError, OSError):
                self.cursor, self.cache = 0, {}
        self.hits = 0
        self.calls = 0

    def save(self):
        if not self.state_path:
            return
        entries = sorted(self.cache.items(), key=lambda item: item[1][0], reverse=True)[:CACHE_MAX_ITEMS]
        state = {'version': 1, 'cursor': self.cursor, 'metadata': {}}
        # Bound the serialized state without retaining full Archive responses.
        remaining = CACHE_MAX_BYTES - 1024
        for key, entry in entries:
            size = len(json.dumps({key: entry}, ensure_ascii=True, separators=(',', ':')).encode()) + 1
            if size > remaining:
                continue
            state['metadata'][key] = entry
            remaining -= size
        self.state_path.parent.mkdir(parents=True, exist_ok=True)
        temp = self.state_path.with_suffix('.tmp')
        temp.write_text(json.dumps(state, separators=(',', ':')), encoding='utf-8')
        temp.replace(self.state_path)
        print(f'Metadata cache: {self.hits} hits; {self.calls} HTTP requests; next offset {self.cursor}')

    def request(self, url, method='GET'):
        if urlsplit(url).hostname != 'archive.org':
            raise ValueError('Invalid Archive URL')
        for attempt in range(3):
            time.sleep(max(0, 1.1 - (time.monotonic() - self.last)))
            self.last = time.monotonic()
            if self.deadline - self.last < 15:
                raise TimeoutError('Indexer network time budget exhausted; index not overwritten')
            self.calls += 1
            try:
                req = Request(url, method=method, headers={'User-Agent': USER_AGENT, 'Accept': 'application/json' if method == 'GET' else '*/*'})
                with self.opener.open(req, timeout=15) as res:
                    if method == 'HEAD':
                        host = urlsplit(res.geturl()).hostname or ''
                        if host != 'archive.org' and not host.endswith('.archive.org'):
                            return False
                        length = int(res.headers.get('Content-Length', '0') or 0)
                        return 0 < length <= MAX_BYTES and res.headers.get_content_type() in ('application/pdf', 'application/octet-stream')
                    raw = res.read(8 * 1024 * 1024 + 1)
                    if len(raw) > 8 * 1024 * 1024:
                        raise ValueError('Oversized metadata response')
                    return json.loads(raw)
            except HTTPError as exc:
                if exc.code in (401, 403, 404, 405, 410) and method == 'HEAD':
                    return False
                if exc.code not in (429, 500, 502, 503, 504):
                    raise
                delay = exc.headers.get('Retry-After', '')
                if delay.isdigit() and int(delay) > 60:
                    raise RuntimeError('Archive requested a long retry delay; stopping') from exc
                if attempt == 2:
                    raise
                time.sleep(max(2 ** (attempt+1), int(delay) if delay.isdigit() else 0))
            except (URLError, TimeoutError):
                if attempt == 2:
                    raise
                time.sleep(2 ** (attempt+1))

    def search(self, limit):
        # Advance through a bounded window of results, wrapping after the last page.
        # A stable order and persisted offset avoid repeatedly scanning the first 400.
        found = set(SEEDS[:limit])
        offset = self.cursor
        visited = 0
        total = None
        while len(found) < limit:
            page, skip = divmod(offset, 100)
            params = [('q',QUERY),('fl[]','identifier'),('sort[]','identifier asc'),('rows',100),('page',page+1),('output','json')]
            response = self.request(API + 'advancedsearch.php?' + urlencode(params))['response']
            total = int(response['numFound'])
            if total <= 0:
                offset = 0
                break
            if visited >= total:
                break
            if offset >= total:
                offset = 0
                continue
            docs = response['docs'][skip:]
            if not docs:
                offset = 0
                break
            for doc in docs:
                identifier = doc.get('identifier','')
                offset += 1
                visited += 1
                if isinstance(identifier, str) and ID.fullmatch(identifier):
                    found.add(identifier)
                if len(found) >= limit or visited >= total:
                    break
            if offset >= total:
                offset = 0
            if visited >= total:
                break
        self.cursor = offset
        print(f'Archive search: {len(found)} items (limit {limit}; next offset {offset})')
        return sorted(found)

    def metadata(self, identifier):
        now = time.time()
        entry = self.cache.get(identifier)
        if entry and 0 <= now - entry[0] < CACHE_TTL:
            self.hits += 1
            return entry[1]
        data = self.request(API + 'metadata/' + quote(identifier,safe='') + '?extended_err=1')
        if not isinstance(data, dict) or data.get('error'):
            return data
        # Only persist fields used by candidate filtering, not descriptions or other files.
        meta = data.get('metadata') or {}
        compact = {'is_dark': data.get('is_dark'),
                   'metadata': {key: meta[key] for key in ('title','subject','language','mediatype','access-restricted-item') if key in meta},
                   'files': [{key: f[key] for key in ('name','size','source','private') if key in f}
                             for f in data.get('files', []) if isinstance(f, dict)
                             and isinstance(f.get('name'), str) and f['name'].lower().endswith('.pdf')]}
        if len(json.dumps(compact, ensure_ascii=True)) <= 128 * 1024:
            self.cache[identifier] = [now, compact]
        return compact

    def available(self, row):
        return self.request(API+'download/'+quote(row[0],safe='')+'/'+quote(row[1],safe=''), 'HEAD')


def candidates(identifier, data, games):
    if not isinstance(data, dict) or data.get('is_dark') or data.get('error'):
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
        name = f.get('name','')
        if not isinstance(name,str) or len(name)>240 or not name.lower().endswith('.pdf') or re.search(r'[\\/\x00-\x1f]',name) or f.get('private') in (True,'true','1',1):
            continue
        if EXCLUDED.search(name) or OTHER_PLATFORM.search(name):
            continue
        try:
            size = int(f.get('size',0))
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


def run(root, limit, dry_run=False, archive=None):
    games = catalog(root)
    path = root / INDEX
    index = json.loads(path.read_text(encoding='utf-8-sig')) if path.is_file() else {'version':1,'platform':'ps3','titles':{}}
    if index.get('version')!=1 or index.get('platform')!='ps3' or not isinstance(index.get('titles'),dict):
        raise ValueError('Unexpected existing manual index; refusing to overwrite')
    archive = archive or Archive(root / STATE)
    persistent = isinstance(archive, Archive) and not dry_run
    previous_cursor = archive.cursor if persistent else 0
    try:
        found = []
        for identifier in archive.search(limit):
            found.extend(candidates(identifier,archive.metadata(identifier),games))
        print(f'Eligible catalog keys: {len(games)}; candidate PDFs: {len(found)}')
        additions = merge(index, found, archive)
        print(f'Added: {additions}; indexed titles: {len(index["titles"])}')
        if additions and not dry_run:
            path.parent.mkdir(parents=True,exist_ok=True)
            temporary = path.with_suffix('.json.tmp')
            temporary.write_text(json.dumps(index,ensure_ascii=False,indent=2,sort_keys=True)+'\n',encoding='utf-8',newline='\n')
            if temporary.stat().st_size > 4 * 1024 * 1024:
                temporary.unlink()
                raise ValueError('Index exceeds frontend size limit')
            temporary.replace(path)
    except Exception:
        if persistent:
            archive.cursor = previous_cursor
        raise
    finally:
        if persistent:
            archive.save()
    return additions


def main():
    parser = argparse.ArgumentParser(description='Extend PS3 manual index from public Archive metadata')
    parser.add_argument('--root',type=Path,default=Path('.'))
    parser.add_argument('--max-items',type=int,default=400)
    parser.add_argument('--dry-run',action='store_true')
    args = parser.parse_args()
    if not 1 <= args.max_items <= 1000:
        parser.error('--max-items must be between 1 and 1000')
    run(args.root,args.max_items,args.dry_run)


if __name__ == '__main__':
    main()
