#!/usr/bin/env python3
import argparse
import csv
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


def metadata(api):
    platform_map={k:set(v) for k,v in PLATFORMS.items()}
    platforms=list(api.pages('platforms','id,name'))
    for tag, variants in PLATFORM_NAMES.items():
        platform_map[tag]={r['id'] for r in platforms if norm(r.get('name','')) in variants}
    if any(not ids for ids in platform_map.values()):raise RuntimeError('IGDB platform mapping incomplete')
    sources={r['id'] for r in api.pages('external_game_sources','id,name') if 'playstation' in norm(r.get('name','')) or norm(r.get('name','')).startswith('psn')}
    all_platforms=sorted(set().union(*platform_map.values()))
    where='platforms = ('+','.join(map(str,all_platforms))+')'
    fields='id,name,platforms,game_type.type,alternative_names.name,collections.id,collections.name,franchises.id,franchises.name,franchise.id,franchise.name,external_games.uid,external_games.external_game_source,external_games.platform'
    return list(api.pages('games',fields,where)),platform_map,sources


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


def consolidate_groups(groups, records=None):
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
        if len(set(members.values())) >= 2:
            components.append((chosen, members))

    # Merge abbreviations only with strong canonical-game overlap. Other
    # differently named, partly overlapping series stay separate.
    keys = defaultdict(list)
    for i, ((_, name), _) in enumerate(components):
        keys[franchise_name_key(name)].append(i)
    parents = list(range(len(components)))
    abbreviation_merges = 0
    for i, ((_, name), members) in enumerate(components):
        short = acronym(name)
        if len(short) < 2:
            continue
        a = set(members.values())
        for j in keys.get(short, []):
            if i == j or find(i) == find(j):
                continue
            b = set(components[j][1].values())
            if len(a & b) >= 3 and len(a & b) / len(a | b) >= .75:
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

    by_games = {}
    content_merges = 0
    for component in combined.values():
        members = {}
        for _, part in component:
            members.update(part)
        signature = frozenset(members.values())
        chosen = min((entry for entry, _ in component), key=lambda entry: rank(entry, signature))
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


def build(catalog, games, platform_map, psn_sources=frozenset({36})):
    aliases=defaultdict(set); ext=defaultdict(set); records={}
    for game in games:
        gid=game['id']; records[gid]=game
        for tag, platforms in platform_map.items():
            if not platforms.intersection(game.get('platforms') or []): continue
            for name in [game.get('name','')]+[x.get('name','') for x in game.get('alternative_names') or [] if isinstance(x,dict)]:
                for key in names(name): aliases[tag,key].add(gid)
            for item in game.get('external_games') or []:
                uid=str(item.get('uid','')).upper() if isinstance(item,dict) else ''
                if ID.fullmatch(uid) and item.get('external_game_source') in psn_sources and item.get('platform') in platforms: ext[tag,uid].add(gid)
    groups=defaultdict(dict); matched=0; ambiguous=0; canonical={}; memberships={}; rejected_games=set(); rejected_links=0
    for tid, variants in sorted(catalog.items()):
        candidates=set()
        for name,tag in variants:
            direct=ext.get((tag,tid),set())
            found=set()
            for key in names(name):
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
    output = consolidate_groups(groups, records)
    print(f'Catalog: {len(catalog)} IDs | matched: {matched} | ambiguous: {ambiguous} | franchises: {len(output)}')
    used={tid for group in output for tid in group['titles']}
    return {'version':1,'franchises':output,'games':{tid:canonical[tid] for tid in sorted(used)}}


def main():
    p=argparse.ArgumentParser();p.add_argument('--root',type=Path,default=ROOT);p.add_argument('--metadata',type=Path,help='Offline JSON fixture for testing');p.add_argument('--secrets-file', type=Path, default=os.environ.get('PSN_SECRETS_FILE') or None, help='Path to private JSON credentials');args=p.parse_args()
    catalog=read_catalog(args.root)
    if args.metadata:
        games=json.loads(args.metadata.read_text()); platforms=PLATFORMS; sources={36}
    else:
        api=IGDB(args.root,args.secrets_file);games,platforms,sources=metadata(api);print(f'IGDB requests: {api.requests}; metadata rows: {len(games)}')
    result=build(catalog,games,platforms,sources)
    if not result['franchises']:raise RuntimeError('No franchises matched; previous index preserved')
    path=args.root/OUTPUT
    if path.exists():
        validate_coverage(json.loads(path.read_text(encoding='utf-8')), result, catalog)
    path.parent.mkdir(parents=True,exist_ok=True)
    content=json.dumps(result,ensure_ascii=False,separators=(',',':'))+'\n'
    if not path.exists() or path.read_text(encoding='utf-8')!=content:
        temp=path.with_suffix('.tmp');temp.write_text(content,encoding='utf-8',newline='\n');temp.replace(path)


if __name__=='__main__':main()
