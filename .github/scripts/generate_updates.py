#!/usr/bin/env python3
"""Build update TSV from immutable Git blobs; cache parsed XML by blob SHA."""
import gzip
import hashlib
import json
import os
import re
import time
from pathlib import Path
from urllib.parse import quote

import requests

REPO = 'PS3-Pro/Game-Updates'
API = f'https://api.github.com/repos/{REPO}'
OUTPUT = Path('resources/database/content/official/game_updates.tsv')
CACHE = Path('.cache/game-updates/rows.json.gz')
SHA = re.compile(r'[0-9a-f]{40}')
REGIONS = {'U':'US','E':'EU','J':'JP','A':'ASIA','H':'ASIA','L':'ASIA'}
MAX_BODY = 32 * 1024 * 1024
NETWORK_BUDGET = 30 * 60
PARSER_VERSION = 1
CHECKPOINT_VERSION = 2


def request(session, url, limit=MAX_BODY):
    for attempt in range(4):
        try:
            with session.get(url, timeout=(10, 30), stream=True) as response:
                retry_after = response.headers.get('Retry-After', '')
                rate_limited = response.status_code == 403 and (retry_after or response.headers.get('X-RateLimit-Remaining') == '0')
                if response.status_code in (408, 429, 500, 502, 503, 504) or rate_limited:
                    delay = max(2**attempt, int(retry_after) if retry_after.isdigit() else 0)
                    reset = response.headers.get('X-RateLimit-Reset', '')
                    if rate_limited and reset.isdigit():
                        delay = max(delay, int(reset) - int(time.time()))
                    if attempt == 3 or delay > 60:
                        response.raise_for_status()
                    time.sleep(delay)
                    continue
                response.raise_for_status()
                body = bytearray()
                for chunk in response.iter_content(65536):
                    body.extend(chunk)
                    if len(body) > limit:
                        raise ValueError('Response exceeds size limit')
                return bytes(body)
        except (requests.ConnectionError, requests.Timeout, requests.exceptions.ChunkedEncodingError):
            if attempt == 3:
                raise
            time.sleep(2**attempt)
    raise RuntimeError('Request failed')


def parse_xml(body):
    # Preserve existing TSV extraction conventions and fallback values for valid XML.
    import xml.etree.ElementTree as ET
    ET.fromstring(body)
    c = body.decode('utf-8-sig')
    name_m = re.search(r'<TITLE>(.*?)</TITLE>', c, re.I)
    name = name_m.group(1).strip() if name_m else ''
    pkgs = re.findall(r'url="(http[^"]+\.pkg)"', c, re.I)
    cid_m = re.search(r'/([^/]+)\.pkg', pkgs[-1]) if pkgs else None
    size = sum(map(int, re.findall(r'size="(\d+)"', c)))
    versions = re.findall(r'version="([\d.]+)"', c)
    # Numeric comparison handles version components of different lengths.
    version = max(versions, key=lambda v: tuple(int(p or 0) for p in v.split('.'))) if versions else '01.00'
    return [name, '|'.join(pkgs), cid_m.group(1) if cid_m else '', str(size), 'v'+version]


def load_cache(path):
    try:
        with gzip.open(path, 'rb') as f:
            raw = f.read(MAX_BODY+1)
        if len(raw)>MAX_BODY: return {}
        data = json.loads(raw)
        if data.get('version') != PARSER_VERSION: return {}
        return {k:v for k,v in data['rows'].items() if SHA.fullmatch(k) and isinstance(v,list) and len(v)==5 and all(isinstance(x,str) and not any(c in x for c in '\t\r\n') for x in v)}
    except (OSError, EOFError, ValueError, TypeError, AttributeError, KeyError):
        return {}


def save_cache(path, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix('.tmp')
    try:
        with gzip.open(tmp, 'wt', encoding='utf-8', compresslevel=3) as f:
            json.dump({'version':PARSER_VERSION,'rows':rows},f,ensure_ascii=False,separators=(',',':'))
        tmp.replace(path)
    finally:
        tmp.unlink(missing_ok=True)


def tree(session, tree_sha):
    data = json.loads(request(session, f'{API}/git/trees/{tree_sha}?recursive=1'))
    if data.get('truncated') or not isinstance(data.get('tree'),list):
        raise ValueError('Incomplete GitHub tree; previous TSV preserved')
    return data['tree']


def checkpoint_matches(path, output, tree_sha):
    try:
        state = json.loads(path.read_text(encoding='utf-8'))
        return (state.get('version') == CHECKPOINT_VERSION and state.get('tree') == tree_sha
                and state.get('script') == hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
                and state.get('output') == hashlib.sha256(output.read_bytes()).hexdigest())
    except (OSError, ValueError, AttributeError):
        return False


def save_checkpoint(path, tree_sha, encoded):
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_suffix('.tmp')
    try:
        temp.write_text(json.dumps({'version':CHECKPOINT_VERSION,'tree':tree_sha,'script':hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),'output':hashlib.sha256(encoded).hexdigest()}, separators=(',',':')),encoding='utf-8')
        temp.replace(path)
    finally:
        temp.unlink(missing_ok=True)


def run(output=OUTPUT, cache=CACHE):
    deadline = time.monotonic() + NETWORK_BUDGET
    checkpoint = cache.with_name(cache.name + '.checkpoint.json')
    rows = {}
    lines = []
    with requests.Session() as api, requests.Session() as raw:
        api.headers.update({'Accept':'application/vnd.github+json','User-Agent':'PSN-Content-Updates'})
        token = os.environ.get('GITHUB_TOKEN')
        if token: api.headers['Authorization'] = 'Bearer '+token
        snapshot = json.loads(request(api, f'{API}/commits/main'))
        commit = snapshot['sha']
        tree_sha = snapshot['commit']['tree']['sha']
        if not SHA.fullmatch(commit) or not SHA.fullmatch(tree_sha): raise ValueError('Invalid GitHub snapshot')
        if checkpoint_matches(checkpoint, output, tree_sha):
            print('Game Updates tree and output unchanged; XML cache and tree listing skipped')
            return
        old_cache = load_cache(cache)
        items = []
        for item in tree(api, tree_sha):
            path = item.get('path','')
            if item.get('type') != 'blob' or not path.startswith('np/np/') or not path.endswith('-ver.xml'): continue
            parts = path.split('/')
            if len(parts)<4 or not re.fullmatch(r'[A-Z]{4}\d{5}',parts[2]) or not SHA.fullmatch(item.get('sha','')):
                raise ValueError('Invalid update tree entry')
            items.append((path,parts[2],item['sha']))
        if not items: raise ValueError('Empty update tree; previous TSV preserved')
        hits = 0
        try:
            for path,tid,sha in items:
                value = rows.get(sha) or old_cache.get(sha)
                if value is None:
                    if time.monotonic() + 180 > deadline:
                        raise TimeoutError('XML scan time budget reached; cached progress saved for next run')
                    url = f'https://raw.githubusercontent.com/{REPO}/{commit}/{quote(path,safe="/")}'
                    body = request(raw,url,2*1024*1024)
                    blob_hash = hashlib.sha1(f'blob {len(body)}\0'.encode())
                    blob_hash.update(body)
                    if blob_hash.hexdigest()!=sha:
                        raise ValueError('XML blob SHA mismatch')
                    value = parse_xml(body)
                    if any(any(c in x for c in '\t\r\n') for x in value):
                        raise ValueError('Invalid TSV field')
                else: hits += 1
                rows[sha] = value
                name, pkg, cid, size, version = value
                # Use the branch URL for the published fallback, not a changing commit SHA.
                if not pkg: pkg = f'https://raw.githubusercontent.com/{REPO}/main/{quote(path,safe="/")}'
                if not name: name = 'Update: '+tid
                lines.append('\t'.join((tid,REGIONS.get(tid[2],'ASIA'),name,pkg,'NOT REQUIRED',cid,'','',size,'',version)))
        except Exception:
            # Keep successful work without publishing an incomplete TSV.
            if any(sha not in old_cache for sha in rows):
                try:
                    save_cache(cache,{**old_cache,**rows})
                except OSError as error:
                    print(f'::warning::Could not preserve XML cache: {error}',flush=True)
            raise
    if rows != old_cache: save_cache(cache,rows)
    encoded = '\n'.join(sorted(lines,key=lambda line:line.split('\t',3)[2])).encode('utf-8')
    if output.exists():
        previous = output.read_bytes()
        if len(lines)<.85*len(previous.splitlines()): raise ValueError('Unexpected update coverage drop')
        if previous==encoded:
            save_checkpoint(checkpoint, tree_sha, encoded)
            print(f'{len(lines)} updates unchanged; {hits} cache hits'); return
    output.parent.mkdir(parents=True,exist_ok=True)
    tmp=output.with_suffix('.tmp')
    try:
        tmp.write_bytes(encoded); tmp.replace(output)
    finally: tmp.unlink(missing_ok=True)
    save_checkpoint(checkpoint, tree_sha, encoded)
    print(f'{len(lines)} updates; {hits} cache hits; {len(lines)-hits} XML downloads')


if __name__=='__main__': run()
