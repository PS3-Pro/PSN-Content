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
from collections import defaultdict
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


def norm(value):
    value = unicodedata.normalize('NFKD', str(value).replace('™', '').replace('®', '').replace('©', '')).casefold()
    return ''.join(c for c in value if c.isalnum())


def names(value):
    raw = str(value).strip()
    cleaned = TECH.sub(' ', raw)
    cleaned = TRIAL.sub(' ', cleaned)
    cleaned = re.sub(r'\s+(?:digital version|(?:French|English|German|Italian|Spanish|Japanese) version)\s*$', '', cleaned, flags=re.I)
    return list(dict.fromkeys(x for x in (norm(raw), norm(cleaned)) if x))


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


def read_catalog(root):
    base = root / 'resources/database/content'
    if not (base / FILES[0]).is_file(): raise RuntimeError('Missing official games.tsv')
    modern = root / 'files/website/modern.html'
    apps = set()
    if modern.is_file():
        block = re.search(r'const DICT_APPS_FILTER\s*=\s*\{(.*?)\};', modern.read_text(encoding='utf-8'), re.S)
        if block: apps = set(re.findall(r'"([A-Z0-9]{9})"\s*:\s*1', block[1]))
    result = defaultdict(set)
    for file in FILES:
        path = base / file
        if not path.is_file(): continue
        with path.open(encoding='utf-8-sig', newline='') as handle:
            for row in csv.reader(handle, delimiter='\t', quoting=csv.QUOTE_NONE):
                if len(row) < 6: continue
                tid, _, title, url, _, cid = (x.strip() for x in row[:6])
                tid = tid.upper()
                if not ID.fullmatch(tid) or tid in apps or not title or title.upper() == 'MISSING' or not url.startswith(('http://','https://')): continue
                if file.endswith('demos.tsv') and not TRIAL.search(title): continue
                if EXCLUDED.search(TRIAL.sub('',title)): continue
                result[tid].add((title, platform(title,tid,cid)))
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


def franchise_words(value):
    value = unicodedata.normalize('NFKD', str(value).replace('™', '').replace('®', '').replace('©', '')).casefold()
    value = ''.join(c for c in value if not unicodedata.combining(c))
    return re.findall(r'[^\W_]+', value, re.UNICODE)


def franchise_name_key(name):
    plain = re.sub(r'\s+(?:series|franchise)\s*$', '', str(name).strip(), flags=re.I)
    return norm(plain) or norm(name)


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
    return qualified, key.startswith('f:'), name.casefold(), name, key


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


def flatten_named_subseries(components):
    # Directional, whole-word names plus overlap of canonical games. Never
    # connect unrelated series just because a crossover appears in both.
    words = [franchise_words(entry[1]) for entry, _ in components]
    signatures = [set(members.values()) for _, members in components]
    prefixes = defaultdict(list)
    for i, parts in enumerate(words):
        prefixes[tuple(parts)].append(i)
    parents = {}
    for i, parts in enumerate(words):
        candidates = set()
        for n in range(1, len(parts)):
            candidates.update(prefixes.get(tuple(parts[:n]), ()))
        for n in range(2, len(parts)):
            candidates.update(j for j in prefixes.get(tuple(parts[-n:]), ())
                              if len(signatures[i] & signatures[j]) / len(signatures[i]) >= .75)
        valid = [j for j in candidates if signatures[i] and
                 len(signatures[i] & signatures[j]) / len(signatures[i]) >= .5]
        if valid:
            parents[i] = min(valid, key=lambda j: (len(words[j]), -len(signatures[j]), franchise_label_order(components[j][0])))
    result = {}
    for i, (entry, members) in enumerate(components):
        target = i
        while target in parents:
            target = parents[target]
        if target != i:
            print(f'Merge named subseries: {entry[1]} -> {components[target][0][1]}')
        result.setdefault(target, {}).update(members)
    print(f'Subseries consolidation: {len(parents)} named groups absorbed')
    return [(components[i][0], members) for i, members in sorted(result.items())]


def consolidate_groups(groups, records=None, relations=()):
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
    for chosen, members in flatten_named_subseries(flattened):
        signature = frozenset(members.values())
        if len(signature) < 2:
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

    result = [{'id': key, 'name': name, 'titles': sorted(members)}
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


def build(catalog, games, platform_map, psn_sources=frozenset({36}), relations=()):
    aliases=defaultdict(set); ext=defaultdict(set); records={}
    requested_names=defaultdict(set); requested_ids=defaultdict(set)
    catalog_keys={}
    for tid, variants in catalog.items():
        prepared=[]
        for name, tag in variants:
            keys=names(name)
            prepared.append((tag,keys))
            requested_names[tag].update(keys)
            requested_ids[tag].add(tid)
        catalog_keys[tid]=prepared
    tags_by_platform=defaultdict(set)
    for tag, platforms in platform_map.items():
        if requested_ids.get(tag):
            for platform_id in platforms:
                tags_by_platform[platform_id].add(tag)
    metadata_count=0
    for game in games:
        metadata_count+=1
        gid=game['id']
        tags=set()
        for platform_id in game.get('platforms') or []:
            tags.update(tags_by_platform.get(platform_id, ()))
        if not tags: continue
        keys={key for name in game_names(game) for key in names(name)}
        relevant=False
        for tag in tags:
            for key in keys & requested_names[tag]:
                aliases[tag,key].add(gid)
                relevant=True
        for item in game.get('external_games') or []:
            if not isinstance(item,dict) or item.get('external_game_source') not in psn_sources: continue
            uid=str(item.get('uid','')).upper()
            if not ID.fullmatch(uid): continue
            for tag in tags.intersection(tags_by_platform.get(item.get('platform'), ())):
                if uid in requested_ids[tag]:
                    ext[tag,uid].add(gid)
                    relevant=True
        if relevant:
            records[gid]=game
    print(f'IGDB metadata rows: {metadata_count}; relevant records indexed: {len(records)}')
    groups=defaultdict(dict); matched=0; ambiguous=0; canonical={}; memberships={}; rejected_games=set(); rejected_links=0
    for tid, variants in sorted(catalog_keys.items()):
        candidates=set()
        for tag,keys in variants:
            direct=ext.get((tag,tid),set())
            found=set()
            for key in keys:
                found=aliases.get((tag,key),set())
                if found: break
            if direct and found:
                candidates.update(direct & found or direct | found)
            else:
                candidates.update(direct or found)
        if len(candidates)!=1:
            ambiguous+=bool(candidates);continue
        gid=next(iter(candidates));game=records[gid]
        kind=game.get('game_type') or {}
        if not isinstance(kind,dict) or str(kind.get('type','')).lower().replace('_',' ') not in ALLOWED_TYPES: continue
        matched+=1
        canonical[tid]=gid
        if gid not in memberships:
            accepted, rejected = select_game_groups(game)
            memberships[gid] = accepted
            if rejected:
                rejected_games.add(gid)
                rejected_links += len(rejected)
                print(f'Ignore incidental IP associations: {game.get("name", gid)} ({gid}): {", ".join(sorted(name for _,name in rejected))}')
        for entry in memberships[gid]:
            groups[entry][tid]=gid
    print(f'Incidental IP associations: {rejected_links} removed across {len(rejected_games)} games')
    output = consolidate_groups(groups, records, relations)
    print(f'Catalog: {len(catalog)} IDs | matched: {matched} | ambiguous: {ambiguous} | franchises: {len(output)}')
    used={tid for group in output for tid in group['titles']}
    return {'version':1,'franchises':output,'games':{tid:canonical[tid] for tid in sorted(used)}}


def main():
    p=argparse.ArgumentParser();p.add_argument('--root',type=Path,default=ROOT);p.add_argument('--metadata',type=Path,help='Offline JSON fixture for testing');p.add_argument('--secrets-file', type=Path, default=os.environ.get('PSN_SECRETS_FILE') or None, help='Path to private JSON credentials');p.add_argument('--full-refresh', action='store_true', help='Ignore previous IGDB metadata cache');args=p.parse_args()
    catalog=read_catalog(args.root)
    if args.metadata:
        fixture=json.loads(args.metadata.read_text()); platforms=PLATFORMS; sources={36}
        games,relations=(fixture['games'],fixture.get('relations',[])) if isinstance(fixture,dict) else (fixture,[])
    else:
        api=IGDB(args.root,args.secrets_file);games,platforms,sources,relations=metadata(api, args.root/CACHE_FILE, args.full_refresh)
    result=build(catalog,games,platforms,sources,relations)
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
