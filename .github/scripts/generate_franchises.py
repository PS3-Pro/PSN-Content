#!/usr/bin/env python3
import argparse
import csv
import gzip
import heapq
import json
import os
import re
import time
import unicodedata
from collections import Counter, defaultdict
from functools import lru_cache
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

ROOT = Path(__file__).resolve().parents[2]
OUTPUT = 'resources/database/helpers/franchises.json'
CACHE_FILE = '.cache/franchises-igdb-v1/games.json.gz'
CACHE_VERSION = 1
FULL_REFRESH_SECONDS = 7 * 86400
UPDATE_OVERLAP_SECONDS = 3600
FILES = ('official/games.tsv', 'official/pending/games.tsv', 'external/nostalgia_vault.tsv', 'official/demos.tsv', 'official/pending/demos.tsv')
PLATFORMS = {
    'ps3': {9}, 'ps2': {8}, 'psx': {7}, 'psp': {38}, 'minis': {9, 38},
    'pce': {86, 150, 128}, 'neogeo': {79, 80, 136},
}
PLATFORM_NAMES = {'pce': {'turbografx16pcengine', 'turbografx16pcenginecd', 'pcenginesupergrafx'}, 'neogeo': {'neogeoaes', 'neogeomvs', 'neogeocd'}}
ALLOWED_TYPES = {'main game', 'remake', 'remaster', 'port', 'bundle', 'standalone expansion', 'expanded game'}
ID = re.compile(r'^[A-Z0-9]{9}$')
TRIAL = re.compile(r'\btrial\s+to\s+full\b', re.I)
EXCLUDED = re.compile(r'\b(?:demo|trial|beta|prototype|network\s+test|test\s+version|soundtrack|guidebook|playview|avatar|dlc)\b|体験版|체험판', re.I)
TECH = re.compile(r'\s*[\[(]\s*(?:PS[123XP]|PS2\s*classics?|minis?|PC\s*Engine|TurboGrafx(?:-16)?|Neo\s*Geo(?:\s*Station)?|Nostalgia Vault|(?:English|French|German|Spanish|Italian|Japanese)(?:\s+Version)?|digital version|trial to full|part\s*\d+|disc\s*\d+)\s*[\])]\s*', re.I)


@lru_cache(maxsize=4096)
def norm(value):
    value = unicodedata.normalize('NFKD', str(value).replace('™', '').replace('®', '').replace('©', '')).casefold()
    return ''.join(c for c in value if c.isalnum())


@lru_cache(maxsize=4096)
def names(value):
    raw = str(value).strip()
    cleaned = TECH.sub(' ', raw)
    cleaned = TRIAL.sub(' ', cleaned)
    cleaned = re.sub(r'\s+(?:digital version|(?:French|English|German|Italian|Spanish|Japanese) version)\s*$', '', cleaned, flags=re.I)
    return tuple(dict.fromkeys(x for x in (norm(raw), norm(cleaned)) if x))


# Secondary keys are deliberately separate from the original exact index.
ROMAN = {'i':'1','ii':'2','iii':'3','iv':'4','v':'5','vi':'6','vii':'7','viii':'8','ix':'9','x':'10','xi':'11','xii':'12','xiii':'13','xiv':'14','xv':'15','xvi':'16'}
VAULT = re.compile(r'\s*\(Nostalgia Vault\)\s*$', re.I)
VAULT_PART = re.compile(r'\s+(?:part|disc)\s*\d+\s*$', re.I)
PLATFORM_PREFIX = re.compile(r'^\s*(?:PSP|PS[123X]|NEO\s*GEO\s+STATION|PC\s*ENGINE)\s*(?:[-:–—]\s*|\s+)', re.I)
EDITION_SUFFIX = re.compile(r'(?:\s*[-:–—]\s*|\s+)(?:game of the year(?: edition)?|goty(?: edition)?|(?:deluxe|complete|collector[’\']?s|special|templar|ultimate|digital) edition|classics? hd|hd)\s*$', re.I)

@lru_cache(maxsize=4096)
def recovery_keys(value, catalog=False):
    raw = unicodedata.normalize('NFKC', str(value).replace('™', '').replace('®', '').replace('©', '')).strip()
    if catalog and VAULT.search(raw):
        raw = VAULT_PART.sub('', VAULT.sub('', raw))
    raw = TECH.sub(' ', raw)
    raw = TRIAL.sub(' ', raw).strip()
    if catalog:
        raw = re.sub(r'\s*[\[(](?:ENG|FRA|FRE|GER|DEU|ITA|SPA|ESP|JPN|KOR|POR)[\])]\s*$', '', raw, flags=re.I).strip()
    if catalog:
        raw = PLATFORM_PREFIX.sub('', raw).strip()
    def key(title):
        words = franchise_words(title.replace('&', ' and '))
        return ''.join(ROMAN.get(w, w) if i and (w != 'i' or i == len(words)-1) else w for i,w in enumerate(words))
    strong = {norm(raw), key(raw)} - {''}
    article = re.fullmatch(r'(.+),\s*(the|an|a)', raw, re.I)
    if article:
        reordered = article[2] + ' ' + article[1]
        strong.update((norm(reordered), key(reordered)))
    # A numeric brand (e.g. 007) may be preceded by a publisher's IP name
    # in the official title. Keep the entire branded suffix, never just words.
    words = franchise_words(raw)
    for i,w in enumerate(words[:4]):
        if re.fullmatch(r'0\d{2,3}', w) and len(words)-i >= 2:
            strong.add(key(' '.join(words[i:])))
    weak = set()
    if catalog:
        stripped = EDITION_SUFFIX.sub('', raw).strip()
        if stripped != raw and len(franchise_words(stripped)) >= 2:
            weak.update((norm(stripped), key(stripped)))
    return frozenset(strong), frozenset(weak - strong)


def platform(title, tid, cid):
    if 'NEOGEOSTATION' in cid.upper(): return 'neogeo'
    for regex, tag in [(r'\(\s*PS2[^)]*\)|\bPS2\s*CLASSICS?\b','ps2'), (r'\(\s*PS[1X]\s*\)|\bPSONE\b','psx'), (r'\(\s*MINIS?\s*\)','minis'), (r'\(\s*PSP\s*\)','psp'), (r'\(\s*(?:PC\s*ENGINE|TURBO\s*GRAFX(?:\s*-\s*16)?)\s*\)','pce'), (r'\bNEO\s*GEO(?:\s*STATION)?\b','neogeo')]:
        if re.search(regex,title,re.I): return tag
    if tid[:2] in ('UC','UL'): return 'psp'
    if tid[:2] in ('SL','SC'): return 'ps2'
    if tid[:2] == 'NP':
        if tid[3] in 'ZX': return 'minis'
        if tid[:4] == 'NPUF' or tid.startswith('NPJJ3'): return 'pce'
        if tid[3] in 'GH': return 'psp'
        if tid[3] in 'DC': return 'ps2'
        if tid[3] in {'U':'JFI','J':'JI','E':'FE'}.get(tid[2],''): return 'psx'
    return 'ps3'


def read_catalog(root, diagnostic=None):
    base = root / 'resources/database/content'
    if not (base / FILES[0]).is_file(): raise RuntimeError('Missing official games.tsv')
    modern = root / 'files/website/modern.html'
    apps = set()
    if modern.is_file():
        block = re.search(r'const DICT_APPS_FILTER\s*=\s*\{(.*?)\};', modern.read_text(encoding='utf-8'), re.S)
        if block: apps = set(re.findall(r'"([A-Z0-9]{9})"\s*:\s*1', block[1]))
    result = defaultdict(set)
    sources = {}
    for file in FILES:
        path = base / file
        if not path.is_file():
            sources[file] = {'missing': True}
            continue
        counts = Counter()
        sources[file] = counts
        with path.open(encoding='utf-8-sig', newline='') as handle:
            for row in csv.reader(handle, delimiter='\t', quoting=csv.QUOTE_NONE):
                counts['rows'] += 1
                if len(row) < 6:
                    counts['invalid_row'] += 1
                    continue
                tid, _, title, url, _, cid = (x.strip() for x in row[:6])
                tid = tid.upper()
                if not ID.fullmatch(tid): reason = 'invalid_id_or_header'
                elif tid in apps: reason = 'app'
                elif not title or title.upper() == 'MISSING': reason = 'missing_title'
                elif not url.startswith(('http://','https://')): reason = 'missing_download_url'
                elif file.endswith('demos.tsv') and not TRIAL.search(title): reason = 'demo'
                elif EXCLUDED.search(TRIAL.sub('',title)): reason = 'excluded_content'
                else:
                    reason = 'eligible_rows'
                    result[tid].add((title, platform(title,tid,cid)))
                counts[reason] += 1
    if diagnostic is not None:
        diagnostic['catalog_sources'] = sources
        diagnostic['apps_filter_available'] = bool(apps)
    if not result: raise RuntimeError('Catalog is empty')
    return result


def igdb_credentials(root, config_path=None):
    client = os.environ.get('IGDB_CLIENT_ID', '').strip()
    secret = os.environ.get('IGDB_CLIENT_SECRET', '').strip()
    if client and secret:
        return client, secret
    path = Path(config_path) if config_path else Path('resources/server/server-secrets.json')
    if not path.is_absolute():
        path = root / path
    try:
        config = json.loads(path.read_text(encoding='utf-8-sig'))
        if not isinstance(config, dict):
            raise ValueError()
        pair = tuple(config.get(key) for key in ('IGDB_CLIENT_ID', 'IGDB_CLIENT_SECRET'))
        if not all(isinstance(value, str) and value.strip() for value in pair):
            raise ValueError()
        return tuple(value.strip() for value in pair)
    except (OSError, ValueError, UnicodeError):
        raise RuntimeError(f'IGDB credentials unavailable at {path}: check file location, JSON format and IGDB keys, or configure Actions secrets') from None


class IGDB:
    def __init__(self, root=ROOT, config_path=None):
        self.client, self.secret = igdb_credentials(root, config_path)
        self.token = self.authenticate()
        self.next_request = 0
        self.requests = 0

    def authenticate(self):
        req = Request('https://id.twitch.tv/oauth2/token', data=urlencode({'client_id':self.client,'client_secret':self.secret,'grant_type':'client_credentials'}).encode(), headers={'Content-Type':'application/x-www-form-urlencoded'})
        try:
            with urlopen(req,timeout=30) as response: return json.load(response)['access_token']
        except (HTTPError, URLError, KeyError, ValueError): raise RuntimeError('IGDB authentication failed') from None

    def query(self, endpoint, body):
        for attempt in range(5):
            time.sleep(max(0,self.next_request-time.monotonic()))
            self.next_request=time.monotonic()+0.35
            self.requests+=1
            req=Request('https://api.igdb.com/v4/'+endpoint, data=body.encode(), headers={'Client-ID':self.client,'Authorization':'Bearer '+self.token,'Accept':'application/json','Content-Type':'text/plain'})
            try:
                with urlopen(req,timeout=60) as response: data=json.load(response)
                if not isinstance(data,list): raise RuntimeError('Invalid IGDB response')
                return data
            except HTTPError as err:
                if err.code==401 and attempt==0: self.token=self.authenticate();continue
                if err.code not in (429,500,502,503,504): raise RuntimeError(f'IGDB {endpoint}: HTTP {err.code}') from None
            except (URLError, TimeoutError): pass
            time.sleep(min(30,2**(attempt+1)))
        raise RuntimeError(f'IGDB {endpoint}: retries exhausted')

    def pages(self, endpoint, fields, where=''):
        last=0
        while True:
            rows=self.query(endpoint,f'fields {fields}; where id > {last}{" & " + where if where else ""}; sort id asc; limit 500;')
            if not rows: return
            new=rows[-1]['id']
            if new<=last: raise RuntimeError('Invalid IGDB pagination')
            yield from rows
            last=new
            if len(rows)<500:return


class InvalidMetadataCache(Exception):
    pass


def cache_header(path):
    try:
        with gzip.open(path, 'rt', encoding='utf-8') as handle:
            header = json.loads(handle.readline())
        if not isinstance(header, dict):
            raise ValueError()
        return header
    except (OSError, EOFError, ValueError, UnicodeError) as error:
        raise InvalidMetadataCache('missing or invalid cache header') from error


def cached_games(path):
    try:
        with gzip.open(path, 'rt', encoding='utf-8') as handle:
            handle.readline()
            previous = 0
            for line in handle:
                game = json.loads(line)
                if not isinstance(game, dict) or type(game.get('id')) is not int or game['id'] <= previous:
                    raise ValueError('invalid game ID or order')
                previous = game['id']
                yield game
            if not previous:
                raise ValueError('empty game cache')
    except (OSError, EOFError, ValueError, UnicodeError) as error:
        raise InvalidMetadataCache('invalid or truncated game cache') from error


def write_metadata_cache(path, header, games):
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_suffix('.tmp')
    count = 0
    previous = 0
    try:
        with gzip.open(temp, 'wt', encoding='utf-8', compresslevel=3, newline='\n') as handle:
            handle.write(json.dumps(header, separators=(',', ':')) + '\n')
            for game in games:
                if not isinstance(game, dict) or type(game.get('id')) is not int or game['id'] <= previous:
                    raise RuntimeError('Invalid IGDB game ID or cache order')
                previous = game['id']
                handle.write(json.dumps(game, ensure_ascii=False, separators=(',', ':')) + '\n')
                count += 1
        if not count:
            raise RuntimeError('Empty IGDB metadata; previous cache preserved')
        temp.replace(path)
    finally:
        temp.unlink(missing_ok=True)
    return count


def metadata_snapshot(api, path, fields, platform_ids, force_full=False, now=None):
    started = int(time.time() if now is None else now)
    where = 'platforms = (' + ','.join(map(str, platform_ids)) + ')'
    signature = {'version': CACHE_VERSION, 'platforms': platform_ids, 'fields': fields}
    previous = None
    if not force_full and path.is_file():
        try:
            header = cache_header(path)
            full, watermark = header.get('full_at'), header.get('watermark')
            if (all(header.get(key) == value for key, value in signature.items())
                and type(full) is int and type(watermark) is int
                and 0 < full <= watermark <= started
                and started - full < FULL_REFRESH_SECONDS):
                previous = header
            else:
                print('IGDB cache expired or metadata scope changed; full refresh')
        except InvalidMetadataCache:
            print('IGDB cache header invalid; full refresh')
    if previous is not None:
        since = max(0, previous['watermark'] - UPDATE_OVERLAP_SECONDS)
        changes = {game['id']: game for game in api.pages('games', fields, f'{where} & updated_at >= {since}')}
        try:
            old = (game for game in cached_games(path) if game['id'] not in changes)
            merged = heapq.merge(old, (changes[key] for key in sorted(changes)), key=lambda game: game['id'])
            count = write_metadata_cache(path, {**signature, 'full_at': previous['full_at'], 'watermark': started}, merged)
            print(f'IGDB incremental refresh: {len(changes)} updated records; {count} cached records')
            return cached_games(path)
        except InvalidMetadataCache:
            print('IGDB cache damaged; recovering with full refresh')
    count = write_metadata_cache(path, {**signature, 'full_at': started, 'watermark': started}, api.pages('games', fields, where))
    print(f'IGDB full refresh: {count} records cached')
    return cached_games(path)


def metadata(api, cache_path, force_full=False):
    platform_map={k:set(v) for k,v in PLATFORMS.items()}
    platforms=list(api.pages('platforms','id,name'))
    for tag, variants in PLATFORM_NAMES.items():
        platform_map[tag]={r['id'] for r in platforms if norm(r.get('name','')) in variants}
    if any(not ids for ids in platform_map.values()):raise RuntimeError('IGDB platform mapping incomplete')
    sources={r['id'] for r in api.pages('external_game_sources','id,name') if 'playstation' in norm(r.get('name','')) or norm(r.get('name','')).startswith('psn')}
    all_platforms=sorted(set().union(*platform_map.values()))
    fields='id,name,platforms,game_type.type,alternative_names.name,collections.id,collections.name,franchises.id,franchises.name,franchise.id,franchise.name,external_games.uid,external_games.external_game_source,external_games.platform'
    games = metadata_snapshot(api, cache_path, fields, all_platforms, force_full)
    relations = list(api.pages('collection_relations', 'id,child_collection.id,child_collection.name,parent_collection.id,parent_collection.name,type.name'))
    return games,platform_map,sources,relations


@lru_cache(maxsize=4096)
def franchise_words(value):
    value = unicodedata.normalize('NFKD', str(value).replace('™', '').replace('®', '').replace('©', '')).casefold()
    value = ''.join(c for c in value if not unicodedata.combining(c))
    return tuple(re.findall(r'[^\W_]+', value, re.UNICODE))


@lru_cache(maxsize=2048)
def franchise_name_key(name):
    plain = re.sub(r'\s+(?:series|franchise)\s*$', '', str(name).strip(), flags=re.I)
    return norm(plain) or norm(name)


@lru_cache(maxsize=8192)
def label_in_name(label, title):
    # Whole words, with punctuation/spacing variants (e.g. apostrophes).
    label_key = franchise_name_key(label)
    words = franchise_words(title)
    if not label_key:
        return False
    for i in range(len(words)):
        candidate = ''
        for word in words[i:]:
            candidate += word
            if candidate == label_key:
                return True
            if len(candidate) >= len(label_key) or not label_key.startswith(candidate):
                break
    return False


def game_names(game):
    return tuple(dict.fromkeys(str(name) for name in [game.get('name', '')] +
        [item.get('name', '') for item in game.get('alternative_names') or [] if isinstance(item, dict)] if name))


def source_entries(game, field, prefix):
    items = list(game.get(field) or [])
    if field == 'franchises' and isinstance(game.get('franchise'), dict):
        items.append(game['franchise'])
    return set((prefix + ':' + str(item['id']), str(item['name']).strip())
               for item in items if isinstance(item, dict) and isinstance(item.get('id'), int) and str(item.get('name') or '').strip())


def select_game_groups(game):
    collections = source_entries(game, 'collections', 'c')
    franchises = source_entries(game, 'franchises', 'f')
    if len({franchise_name_key(name) for _, name in franchises}) <= 1:
        return collections | franchises, set()
    titles = game_names(game)
    # Several IPs on one game are often character/crossover associations.
    # Prefer the game's series and labels supported by its own names.
    # A collection's name may also establish a parent franchise.
    anchors = titles + tuple(name for _, name in collections)
    supported = {entry for entry in franchises if any(label_in_name(entry[1], title) for title in anchors)}
    return collections | supported, franchises - supported


def franchise_label_order(entry):
    key, name = entry
    qualified = bool(re.search(r'\s+(?:series|franchise)\s*$', name.strip(), re.I))
    return qualified, name.islower(), key.startswith('f:'), name.casefold(), name, key


def acronym(name):
    words = franchise_words(name)
    return ''.join(word if word.isdigit() else word[0] for word in words) if len(words) > 1 else ''


def collection_parent_links(relations):
    links = defaultdict(set)
    for relation in relations:
        kind = relation.get('type') or {}
        child = relation.get('child_collection') or {}
        parent = relation.get('parent_collection') or {}
        if not all(isinstance(value, dict) for value in (kind, child, parent)):
            continue
        label = norm(kind.get('name', ''))
        if not any(token in label for token in ('subseries', 'subcollection', 'subfranchise', 'partof', 'parentchild', 'spinoff')) and label not in {'parent', 'child', 'contains', 'haspart'}:
            continue
        if not all(isinstance(item.get('id'), int) and str(item.get('name') or '').strip() for item in (child, parent)):
            continue
        if child['id'] != parent['id']:
            links['c:' + str(child['id'])].add(('c:' + str(parent['id']), str(parent['name']).strip()))
    return links


def flatten_collection_parents(groups, relations):
    links = collection_parent_links(relations)
    if not links:
        return groups
    result = defaultdict(dict)
    def root(entry):
        seen = {entry[0]}
        current = entry
        while len(links.get(current[0], ())) == 1:
            parent = next(iter(links[current[0]]))
            if parent[0] in seen:
                return entry
            seen.add(parent[0])
            current = parent
        return current
    roots = {entry: root(entry) for entry in groups}
    promoted = defaultdict(set)
    for entry, target in roots.items():
        if target != entry:
            promoted[franchise_name_key(entry[1])].add(target)
    for entry, members in sorted(groups.items(), key=lambda item: franchise_label_order(item[0])):
        target = roots[entry]
        alternatives = promoted.get(franchise_name_key(entry[1]), ())
        if target == entry and len(alternatives) == 1:
            target = next(iter(alternatives))
        if target != entry:
            print(f'Merge IGDB hierarchy: {entry[1]} -> {target[1]}')
        result[target].update(members)
    return result


def flatten_named_subseries(components, records=None):
    records = records or {}
    words = [franchise_words(entry[1]) for entry, _ in components]
    signatures = [set(members.values()) for _, members in components]
    by_game = defaultdict(list)
    for i, signature in enumerate(signatures):
        for gid in signature:
            by_game[gid].append(i)
    support = {}
    def name_support(i, gid):
        key = (i, gid)
        if key not in support:
            support[key] = any(label_in_name(components[i][0][1], title) for title in game_names(records.get(gid, {})))
        return support[key]
    parents = {}
    for i, parts in enumerate(words):
        a = signatures[i]
        if not a or not parts:
            continue
        overlaps = defaultdict(int)
        for gid in a:
            for j in by_game[gid]:
                if j != i:
                    overlaps[j] += 1
        candidates = []
        for j, count in overlaps.items():
            b = signatures[j]
            other = words[j]
            if not other:
                continue
            coverage = count / len(a)
            prefix = len(other) < len(parts) and parts[:len(other)] == other
            suffix = len(other) < len(parts) and parts[-len(other):] == other
            shared_start = len(parts[0]) >= 4 and parts[0] == other[0]
            # Directional ranks prevent cycles, including equal-size aliases.
            if len(b) <= len(a) and not (prefix or suffix):
                continue
            if prefix and coverage >= .5 or suffix and coverage >= .75:
                strength = 4
            elif shared_start and len(b) > len(a) and coverage >= .5 and (count >= 2 or len(a) == 2):
                strength = 3
            elif count >= 2 and coverage == 1 and len(b) <= 2 * len(a) and len(b) > len(a) and components[j][0][0].startswith('c:'):
                # Exact canonical subset, not a coincidental single shared game.
                strength = 2
            elif records and count >= 2 and coverage >= 2/3 and len(b) > len(a) and components[i][0][0].startswith('c:') and all(not name_support(i,gid) for gid in a) and all(name_support(j,gid) for gid in a & b):
                # An umbrella collection with a different label, backed by game titles.
                strength = 1
            else:
                continue
            candidates.append((strength, coverage, count, j))
        if candidates:
            candidates.sort(key=lambda item: (-item[0], -item[1], -item[2], len(words[item[3]]), -len(signatures[item[3]]), franchise_label_order(components[item[3]][0])))
            best = candidates[0]
            # Do not guess between unrelated, equally plausible parents.
            if len(candidates) > 1 and best[:3] == candidates[1][:3] and best[0] <= 2:
                continue
            parents[i] = best[3]
    result = {}
    for i, (entry, members) in enumerate(components):
        target = i
        seen = set()
        while target in parents and target not in seen:
            seen.add(target)
            target = parents[target]
        if target in seen:
            target = i
        if target != i:
            print(f'Merge subseries/alias: {entry[1]} -> {components[target][0][1]}')
        result.setdefault(target, {}).update(members)
    print(f'Subseries consolidation: {len(parents)} candidate groups absorbed')
    return [(components[i][0], members) for i, members in sorted(result.items())]


def prune_incidental_views(components, records):
    if not records:
        return components
    by_game = defaultdict(list)
    signatures = [set(members.values()) for _, members in components]
    for i, signature in enumerate(signatures):
        for gid in signature:
            by_game[gid].append(i)
    labels = {}
    def supported(i, gid):
        key = (i, gid)
        if key not in labels:
            labels[key] = any(label_in_name(components[i][0][1], title) for title in game_names(records.get(gid, {})))
        return labels[key]
    rejected = defaultdict(set)
    # A game with a clear collection name should not be listed in a different
    # series whose own name is absent. Keep valid multi-name crossovers.
    for gid, indexes in by_game.items():
        if len(indexes) < 2:
            continue
        anchors = [i for i in indexes if components[i][0][0].startswith('c:') and supported(i,gid)]
        if not anchors:
            continue
        for i in indexes:
            if i in anchors or supported(i,gid):
                continue
            if any(supported(i, other) for other in signatures[i] - {gid}):
                rejected[i].add(gid)
    # Brand/franchise-only views that merely repeat several independent named
    # collections are redundant. Require strong coverage, not just a crossover.
    for i, (entry, _) in enumerate(components):
        if not entry[0].startswith('f:'):
            continue
        a = signatures[i]
        coverage = defaultdict(set)
        for gid in a:
            for j in by_game[gid]:
                if j != i and components[j][0][0].startswith('c:') and supported(j,gid):
                    coverage[j].add(gid)
        distinct = [ids for j, ids in coverage.items() if len(ids) >= 2 and len(ids) / len(signatures[j]) >= .75]
        if len(distinct) >= 2:
            combined = set().union(*distinct)
            if len(combined) >= .85 * len(a) and sum(map(len, distinct)) <= 1.2 * len(combined):
                rejected[i].update(combined)
    cleaned = []
    for i, (entry, members) in enumerate(components):
        if rejected[i]:
            print(f'Remove incidental/redundant game memberships: {entry[1]} ({len(rejected[i])})')
            members = {tid:gid for tid,gid in members.items() if gid not in rejected[i]}
        cleaned.append((entry,members))
    # Pruning must not erase a title simply because its remaining group fell
    # below the two-game threshold. Restore only affected groups if needed.
    before = {tid for _, members in components if len(set(members.values())) >= 2 for tid in members}
    covered = {tid for _, members in cleaned if len(set(members.values())) >= 2 for tid in members}
    missing = before - covered
    if missing:
        for i, (entry, members) in enumerate(components):
            if len(set(members.values())) >= 2 and missing.intersection(members):
                cleaned[i] = (entry, members)
                missing.difference_update(members)
                print(f'Preserve unique catalog coverage: {entry[1]}')
                if not missing:
                    break
    return cleaned


def consolidate_groups(groups, records=None, relations=(), diagnostic=None):
    groups = flatten_collection_parents(groups, relations)
    records = records or {}
    entries = sorted(groups, key=franchise_label_order)
    parents = list(range(len(entries)))
    by_source, by_name = {}, {}

    def find(index):
        while parents[index] != index:
            parents[index] = parents[parents[index]]
            index = parents[index]
        return index

    def union(a, b):
        a, b = find(a), find(b)
        if a != b:
            parents[max(a,b)] = min(a,b)

    for i, (key, name) in enumerate(entries):
        if key in by_source:
            union(i, by_source[key])
        else:
            by_source[key] = i
        normalized = franchise_name_key(name)
        if normalized in by_name:
            union(i, by_name[normalized])
        else:
            by_name[normalized] = i

    merged = defaultdict(list)
    for i, entry in enumerate(entries):
        merged[find(i)].append(entry)

    components = []
    name_merges = 0
    for component in merged.values():
        chosen = min(component, key=franchise_label_order)
        members = {}
        for entry in component:
            members.update(groups[entry])
        if len(component) > 1:
            name_merges += len(component)-1
            print(f'Merge by name/source: {component!r} -> {chosen!r}')
        if members:
            components.append((chosen, members))

    # Merge abbreviations only with strong canonical-game overlap. Other
    # differently named, partly overlapping series stay separate.
    keys = defaultdict(list)
    for i, ((_, name), _) in enumerate(components):
        keys[franchise_name_key(name)].append(i)
    parents = list(range(len(components)))
    signatures = [set(members.values()) for _, members in components]
    abbreviation_merges = 0
    for i, ((_, name), members) in enumerate(components):
        short = acronym(name)
        if len(short) < 2:
            continue
        a = signatures[i]
        for j in keys.get(short, []):
            if i == j or find(i) == find(j):
                continue
            b = signatures[j]
            overlap = len(a & b)
            if overlap >= 3 and overlap / (len(a) + len(b) - overlap) >= .75:
                union(i,j)
                abbreviation_merges += 1
                print(f'Merge abbreviation: {components[j][0][1]} -> {name}')
    combined = defaultdict(list)
    for i, item in enumerate(components):
        combined[find(i)].append(item)

    # Resolve duplicate views by actual game identity. A collection or a
    # label supported by the games is preferable to an incidental IP label.
    def rank(entry, signature):
        support = sum(any(label_in_name(entry[1], title) for title in game_names(records.get(gid, {}))) for gid in signature)
        return -support, franchise_label_order(entry)

    flattened = []
    for component in combined.values():
        members = {}
        for _, part in component:
            members.update(part)
        signature = frozenset(members.values())
        chosen = component[0][0] if len(component) == 1 else min((entry for entry, _ in component), key=lambda entry: rank(entry, signature))
        flattened.append((chosen, members))
    by_games = {}
    content_merges = 0
    for chosen, members in prune_incidental_views(flatten_named_subseries(flattened, records), records):
        signature = frozenset(members.values())
        if len(signature) < 2:
            if diagnostic is not None:
                diagnostic.setdefault('discarded_groups', []).append({
                    'id': chosen[0], 'name': chosen[1], 'distinct_igdb_games': len(signature),
                    'titles': sorted(members), 'igdb_ids': sorted(signature)})
            continue
        previous = by_games.get(signature)
        if previous is None:
            by_games[signature] = (chosen, members)
        else:
            previous_entry, previous_members = previous
            winner = min(previous_entry, chosen, key=lambda entry: rank(entry, signature))
            previous_members.update(members)
            by_games[signature] = (winner, previous_members)
            content_merges += 1
            print(f'Consolidate identical game view ({len(signature)}): {previous_entry!r}, {chosen!r} -> {winner!r}')

    result = [{'id': key, 'name': name[:1].upper() + name[1:] if name.islower() else name, 'titles': sorted(members)}
              for (key, name), members in by_games.values()]
    result.sort(key=lambda g: (g['name'].casefold(), g['id']))
    print(f'Consolidation: {name_merges} name/source duplicates; {abbreviation_merges} abbreviations; {content_merges} identical-game views')
    return result


def validate_coverage(previous, result, catalog):
    # A successful consolidation reduces the number of groups, not the coverage
    # of existing catalog entries. Do not reject a valid deduplication as shrinkage.
    old_titles = {tid for group in previous.get('franchises', []) for tid in group.get('titles', [])}
    old_titles.intersection_update(catalog)
    new_titles = {tid for group in result['franchises'] for tid in group['titles']}
    retained = len(old_titles & new_titles)
    print(f'Coverage of previously indexed catalog IDs: {retained}/{len(old_titles)}')
    if len(old_titles) >= 20 and retained < len(old_titles)*0.75:
        raise RuntimeError('Unexpected index coverage loss; previous index preserved')


def is_game_record(game):
    kind = game.get('game_type') or {}
    return isinstance(kind, dict) and str(kind.get('type', '')).lower().replace('_', ' ') in ALLOWED_TYPES


def resolve_catalog_game(tid, variants, primary, aliases, ext):
    direct, exact, alternate = set(), set(), set()
    for tag, keys in variants:
        direct.update(ext.get((tag, tid), ()))
        for key in keys:
            found = primary.get((tag, key), ())
            if found:
                exact.update(found)
                break
        for key in keys:
            found = aliases.get((tag, key), ())
            if found:
                alternate.update(found)
                break
    if direct:
        # A platform-specific external ID is stronger than an alternate name.
        # Explicitly conflicting evidence is not guessed away.
        for evidence in (exact, alternate):
            if evidence:
                common = direct & evidence
                if common:
                    direct = common
                elif len(direct) > 1 or evidence is exact:
                    return direct | evidence
        return direct
    return exact or alternate


def attach_exact_series_roots(groups, pending, records):
    # An unclassified, matched game can be the original of an established
    # series. Only exact official names and established series qualify.
    by_name = defaultdict(list)
    for entry, members in groups.items():
        if members:
            by_name[franchise_name_key(entry[1])].append(entry)
    recovered = 0
    for gid, tids in sorted(pending.items()):
        name = records[gid].get('name', '')
        entries = [entry for entry in by_name.get(franchise_name_key(name), ()) if any(other != gid for other in groups[entry].values())]
        if not entries:
            continue
        # All candidates share the same normalized label and will be merged.
        entry = min(entries, key=franchise_label_order)
        groups[entry].update((tid, gid) for tid in tids)
        recovered += len(tids)
        print(f'Recover exact series root: {name} -> {entry[1]} ({len(tids)} catalog IDs)')
    return recovered


def attach_numbered_series(groups, pending, records):
    # Require an existing IGDB group, an explicit sequel marker immediately
    # after its label and at least one already named member with another ID.
    labels=defaultdict(list)
    for entry,members in groups.items():
        words=franchise_words(entry[1])
        if not words:
            continue
        ids=set(members.values())
        if len(ids)<2:
            continue
        anchors={gid for gid in ids if label_in_name(entry[1],records.get(gid,{}).get('name',''))}
        if len(words)>=2 and anchors:
            labels[words].append(entry)
        elif len(words)==1 and len(words[0])>=4:
            # Single-word series need two independently named members, at
            # least one with an explicit number, before inferring a sequel.
            named=[franchise_words(records.get(gid,{}).get('name','')) for gid in anchors]
            rooted=[title for title in named if title[:1]==words and (len(title)==1 or (len(title)>1 and ROMAN.get(title[1],title[1]).isdigit()))]
            if len(rooted)>=2 and any(len(title)>1 for title in rooted):
                labels[words].append(entry)
    recovered=0
    already={tid for members in groups.values() for tid in members}
    for gid,tids in sorted(pending.items()):
        tids=[tid for tid in tids if tid not in already]
        if not tids: continue
        words=tuple(franchise_words(records[gid].get('name','')))
        matches=[]
        for i in range(1,len(words)):
            number=ROMAN.get(words[i],words[i])
            if not number.isdigit() or not 2<=int(number)<=99: continue
            for entry in labels.get(words[:i],()):
                matches.append((i,entry))
        if not matches: continue
        best=max(i for i,_ in matches)
        entries=[entry for i,entry in matches if i==best]
        if len({franchise_name_key(entry[1]) for entry in entries})!=1: continue
        entry=min(entries,key=franchise_label_order)
        groups[entry].update((tid,gid) for tid in tids)
        recovered+=len(tids)
        print(f'Recover numbered series member: {records[gid].get("name")} -> {entry[1]} ({len(tids)} catalog IDs)')
    return recovered


def build(catalog, games, platform_map, psn_sources=frozenset({36}), relations=(), diagnostic=None):
    primary=defaultdict(set); aliases=defaultdict(set); ext=defaultdict(set); records={}
    requested_names=defaultdict(set); requested_ids=defaultdict(set)
    catalog_keys={}; recovery_catalog={}; recovery_requested=defaultdict(set); recovery_index=defaultdict(set)
    for tid, variants in catalog.items():
        prepared=[]
        for name, tag in variants:
            keys=names(name)
            prepared.append((tag,keys))
            requested_names[tag].update(keys)
            requested_ids[tag].add(tid)
        catalog_keys[tid]=prepared
        recovery_catalog[tid]=[(tag, *recovery_keys(name, True)) for name,tag in sorted(variants)]
        for tag,strong,weak in recovery_catalog[tid]:
            recovery_requested[tag].update(strong | weak)
    tags_by_platform=defaultdict(set)
    for tag, platforms in platform_map.items():
        if requested_ids.get(tag):
            for platform_id in platforms:
                tags_by_platform[platform_id].add(tag)
    metadata_count=0
    for game in games:
        metadata_count+=1
        if not is_game_record(game): continue
        gid=game['id']
        tags=set()
        for platform_id in game.get('platforms') or []:
            tags.update(tags_by_platform.get(platform_id, ()))
        if not tags: continue
        official = game.get('name', '')
        titles = game_names(game)
        official_keys = set(names(official)) if official else set()
        keys = official_keys.copy()
        secondary = set()
        for title in titles:
            if title != official:
                keys.update(names(title))
            secondary.update(recovery_keys(title)[0])
        relevant=False
        for tag in tags:
            for key in secondary & recovery_requested[tag]:
                recovery_index[tag,key].add(gid)
                relevant=True
            for key in official_keys & requested_names[tag]:
                primary[tag,key].add(gid)
            for key in keys & requested_names[tag]:
                aliases[tag,key].add(gid)
                relevant=True
        for item in game.get('external_games') or []:
            if not isinstance(item,dict) or item.get('external_game_source') not in psn_sources: continue
            uid=str(item.get('uid','')).upper()
            if not ID.fullmatch(uid): continue
            external_platform = item.get('platform')
            compatible = tags if external_platform is None else tags.intersection(tags_by_platform.get(external_platform, ()))
            for tag in compatible:
                if uid in requested_ids[tag]:
                    ext[tag,uid].add(gid)
                    relevant=True
        if relevant:
            records[gid]=game
    print(f'IGDB metadata rows: {metadata_count}; relevant records indexed: {len(records)}')
    groups=defaultdict(dict); matched=0; ambiguous=0; canonical={}; memberships={}; rejected_games=set(); rejected_links=0
    pending=defaultdict(list); unresolved=[]
    rejected_memberships = {}; recovered_matches = Counter()
    for tid, variants in sorted(catalog_keys.items()):
        candidates=resolve_catalog_game(tid, variants, primary, aliases, ext)
        if not candidates:
            possible=set()
            used_edition=False
            for tag,strong,weak in recovery_catalog[tid]:
                found=set()
                for key in strong:
                    found.update(recovery_index.get((tag,key), ()))
                if not found:
                    for key in weak:
                        found.update(recovery_index.get((tag,key), ()))
                    used_edition = used_edition or bool(found)
                possible.update(found)
            candidates=possible
            if len(possible)==1:
                recovered_matches['edition' if used_edition else 'normalized'] += 1
        if len(candidates)!=1:
            ambiguous+=bool(candidates)
            unresolved.append((tid, variants, candidates))
            continue
        gid=next(iter(candidates));game=records[gid]
        matched+=1
        canonical[tid]=gid
        if gid not in memberships:
            accepted, rejected = select_game_groups(game)
            memberships[gid] = accepted
            if rejected:
                if diagnostic is not None: rejected_memberships[gid] = rejected
                rejected_games.add(gid)
                rejected_links += len(rejected)
                print(f'Ignore incidental IP associations: {game.get("name", gid)} ({gid}): {", ".join(sorted(name for _,name in rejected))}')
        if not memberships[gid]:
            pending[gid].append(tid)
        for entry in memberships[gid]:
            groups[entry][tid]=gid
    recovered=attach_exact_series_roots(groups, pending, records)
    numbered_recovered=attach_numbered_series(groups, pending, records)
    if diagnostic is not None: diagnostic['numbered_series_recoveries']=numbered_recovered
    series_keys={franchise_name_key(entry[1]) for entry in groups}
    for tid, variants, candidates in unresolved:
        if any(key in series_keys for _, keys in variants for key in keys):
            print(f'Unresolved exact series-title candidate: {tid}; IGDB candidates: {sorted(candidates)}')
    print(f'Exact series-root recovery: {recovered} catalog IDs')
    print(f'Secondary name recovery: {dict(recovered_matches)}')
    if diagnostic is not None: diagnostic['secondary_match_recoveries'] = dict(recovered_matches)
    print(f'Incidental IP associations: {rejected_links} removed across {len(rejected_games)} games')
    output = consolidate_groups(groups, records, relations, diagnostic)
    print(f'Catalog: {len(catalog)} IDs | matched: {matched} | ambiguous: {ambiguous} | franchises: {len(output)}')
    used={tid for group in output for tid in group['titles']}
    if diagnostic is not None:
        collect_diagnostics(diagnostic, catalog, records, canonical, unresolved, groups,
                            memberships, rejected_memberships, output, metadata_count, recovered)
    result = {'version':1,'franchises':output,'games':{tid:canonical[tid] for tid in sorted(used)}}
    for cached in (norm, names, recovery_keys, franchise_words, franchise_name_key, label_in_name):
        cached.cache_clear()
    return result


STATUS_LABELS = {
    'indexed': 'Indexed in a franchise',
    'no_match': 'No eligible IGDB match',
    'ambiguous': 'Multiple IGDB candidates',
    'no_franchise_metadata': 'Matched; no franchise or collection metadata',
    'incidental_only': 'Matched; only incidental IP associations',
    'single_game_franchise': 'Matched; final group has fewer than two distinct games',
    'excluded_during_consolidation': 'Matched; not retained during consolidation',
}


def collect_diagnostics(report, catalog, records, canonical, unresolved, groups,
                        memberships, rejected, output, metadata_count, recovered):
    final = defaultdict(list)
    assigned = defaultdict(list)
    discarded = defaultdict(list)
    for group in output:
        for tid in group['titles']:
            final[tid].append(group['name'])
    for (_, name), members in groups.items():
        for tid in members:
            assigned[tid].append(name)
    for group in report.get('discarded_groups', []):
        for tid in group['titles']:
            discarded[tid].append(group['name'])
    candidates_by_tid = {tid: candidates for tid, _, candidates in unresolved}
    counts = Counter()
    distinct = defaultdict(set)
    by_platform = defaultdict(Counter)
    rows = []
    for tid, variants in sorted(catalog.items()):
        gid = canonical.get(tid)
        candidates = candidates_by_tid.get(tid, ())
        if tid in final: status = 'indexed'
        elif gid is None: status = 'ambiguous' if candidates else 'no_match'
        elif tid in discarded: status = 'single_game_franchise'
        elif tid in assigned: status = 'excluded_during_consolidation'
        elif rejected.get(gid): status = 'incidental_only'
        else: status = 'no_franchise_metadata'
        counts[status] += 1
        if gid is not None: distinct[status].add(gid)
        tags = sorted({tag for _, tag in variants})
        for tag in tags: by_platform[tag][status] += 1
        rows.append({
            'title_id': tid, 'catalog_names': sorted({name for name, _ in variants}),
            'platforms': tags, 'status': status, 'igdb_id': gid,
            'igdb_name': records[gid].get('name', '') if gid is not None else '',
            'candidates': [{'id': candidate, 'name': records.get(candidate, {}).get('name', '')}
                           for candidate in sorted(candidates)],
            'accepted_groups': sorted({name for _, name in memberships.get(gid, ())}),
            'rejected_groups': sorted({name for _, name in rejected.get(gid, ())}),
            'assigned_groups': sorted(set(assigned.get(tid, ()))),
            'discarded_groups': sorted(set(discarded.get(tid, ()))),
            'final_groups': sorted(set(final.get(tid, ()))),
        })
    report.update({
        'schema_version': 1, 'catalog_title_ids': len(catalog),
        'matched_title_ids': len(canonical), 'distinct_matched_igdb_games': len(set(canonical.values())),
        'indexed_title_ids': len(final),
        'distinct_indexed_igdb_games': len({canonical[tid] for tid in final}),
        'unindexed_title_ids': len(catalog) - len(final),
        'franchises': len(output), 'metadata_rows': metadata_count,
        'recovered_series_root_title_ids': recovered,
        'status_counts': {key: counts[key] for key in STATUS_LABELS},
        'distinct_matched_games_by_status': {key: len(distinct[key]) for key in STATUS_LABELS},
        'platform_status_counts': {tag: dict(counter) for tag, counter in sorted(by_platform.items())},
        'titles': rows,
    })
    if sum(counts.values()) != len(catalog) or counts['indexed'] != len(final):
        raise RuntimeError('Diagnostic counts inconsistent')


def write_diagnostics(report, directory):
    directory.mkdir(parents=True, exist_ok=True)
    (directory / 'coverage.json').write_text(json.dumps(report, ensure_ascii=False, indent=2)+'\n', encoding='utf-8')
    fields = ('title_id', 'catalog_names', 'platforms', 'status', 'igdb_id', 'igdb_name',
              'candidates', 'accepted_groups', 'rejected_groups', 'assigned_groups', 'discarded_groups', 'final_groups')
    with (directory / 'titles.tsv').open('w', encoding='utf-8', newline='') as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, delimiter='\t')
        writer.writeheader()
        for row in report['titles']:
            writer.writerow({key: json.dumps(value, ensure_ascii=False, separators=(',', ':')) if isinstance(value, list) else value
                             for key, value in row.items()})
    lines = ['## Franchise coverage', '',
             f"Eligible catalog Title IDs: **{report['catalog_title_ids']}** | Indexed: **{report['indexed_title_ids']}** | Not indexed: **{report['unindexed_title_ids']}**",
             '', f"Distinct IGDB games: **{report['distinct_matched_igdb_games']} matched / {report['distinct_indexed_igdb_games']} indexed** | Franchise cards: **{report['franchises']}**",
             '', '| Result | Title IDs | Distinct matched IGDB games |', '|---|---:|---:|']
    for key, label in STATUS_LABELS.items():
        lines.append(f"| {label} | {report['status_counts'][key]} | {report['distinct_matched_games_by_status'][key]} |")
    lines += ['', f"Groups discarded below minimum: **{len(report.get('discarded_groups', []))}**. Exact series-root recoveries: **{report['recovered_series_root_title_ids']}**.",
              '', 'Title ID statuses are mutually exclusive. Distinct IGDB counts by status may overlap; unmatched records have no confirmed IGDB ID. The website may count regional groups differently.',
              '', '### By platform', '', '| Platform | Eligible IDs | Indexed IDs | No match | Ambiguous | Other unindexed |', '|---|---:|---:|---:|---:|---:|']
    for tag, counter in report['platform_status_counts'].items():
        total = sum(counter.values()); indexed = counter.get('indexed', 0); missing = counter.get('no_match', 0); ambiguous = counter.get('ambiguous', 0)
        lines.append(f'| {tag} | {total} | {indexed} | {missing} | {ambiguous} | {total-indexed-missing-ambiguous} |')
    lines += ['', 'Platform counts may overlap when a Title ID has multiple catalog classifications.', '',
              '### TSV input', '', '| Source | Rows | Eligible rows | Excluded rows |', '|---|---:|---:|---:|']
    for file, counts in report.get('catalog_sources', {}).items():
        if counts.get('missing'): lines.append(f'| {file} | missing | — | — |')
        else: lines.append(f"| {file} | {counts.get('rows',0)} | {counts.get('eligible_rows',0)} | {counts.get('rows',0)-counts.get('eligible_rows',0)} |")
    lines += ['', 'Rows include headers, duplicates and regional releases. See coverage.json for exclusions by reason.',
              '', 'Download the franchise-diagnostics artifact for all Title IDs, candidate names, accepted/rejected memberships and discarded groups. No credentials or download URLs are included.', '']
    recoveries = report.get('secondary_match_recoveries', {})
    lines += ['### Secondary recovery', '', f"Normalized names: **{recoveries.get('normalized', 0)}** | Edition fallback: **{recoveries.get('edition', 0)}** | Numbered series memberships: **{report.get('numbered_series_recoveries', 0)}**.", '']
    summary = '\n'.join(lines)
    (directory / 'summary.md').write_text(summary, encoding='utf-8')
    print(summary)


def main():
    p=argparse.ArgumentParser();p.add_argument('--root',type=Path,default=ROOT);p.add_argument('--metadata',type=Path,help='Offline JSON fixture for testing');p.add_argument('--secrets-file', type=Path, default=os.environ.get('PSN_SECRETS_FILE') or None, help='Path to private JSON credentials');p.add_argument('--full-refresh', action='store_true', help='Ignore previous IGDB metadata cache');p.add_argument('--diagnostics', type=Path, help='Write coverage reports to this directory');args=p.parse_args()
    diagnostic = {} if args.diagnostics else None
    catalog=read_catalog(args.root, diagnostic)
    if args.metadata:
        fixture=json.loads(args.metadata.read_text()); platforms=PLATFORMS; sources={36}
        games,relations=(fixture['games'],fixture.get('relations',[])) if isinstance(fixture,dict) else (fixture,[])
    else:
        api=IGDB(args.root,args.secrets_file);games,platforms,sources,relations=metadata(api, args.root/CACHE_FILE, args.full_refresh)
    result=build(catalog,games,platforms,sources,relations,diagnostic)
    if diagnostic is not None:
        write_diagnostics(diagnostic, args.diagnostics)
    if not args.metadata: print(f'IGDB requests: {api.requests}')
    if not result['franchises']:raise RuntimeError('No franchises matched; previous index preserved')
    path=args.root/OUTPUT
    if path.exists():
        validate_coverage(json.loads(path.read_text(encoding='utf-8')), result, catalog)
    path.parent.mkdir(parents=True,exist_ok=True)
    content=json.dumps(result,ensure_ascii=False,separators=(',',':'))+'\n'
    if not path.exists() or path.read_text(encoding='utf-8')!=content:
        temp=path.with_suffix('.tmp');temp.write_text(content,encoding='utf-8',newline='\n');temp.replace(path)


if __name__=='__main__':main()
