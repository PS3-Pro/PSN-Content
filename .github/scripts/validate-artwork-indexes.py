#!/usr/bin/env python3
import json
import sys
from pathlib import Path

CORE = {'ps1', 'ps2', 'ps3', 'ps4', 'psp', 'psvita'}
SYSTEMS = {'launchbox': CORE | {'pce', 'neogeo'}, 'libretro': CORE}

def read(path, provider, platform):
    data = json.loads(path.read_text(encoding='utf-8'))
    if data.get('version') != 1 or data.get('platform') != platform or not isinstance(data.get('titles'), dict):
        raise ValueError(f'Invalid index: {path}')
    titles = data['titles']
    if provider == 'launchbox':
        art, meta = data.get('art'), data.get('meta')
        if not isinstance(art, dict) or not isinstance(meta, dict):
            raise ValueError(f'Invalid LaunchBox data: {path}')
        if any(not isinstance(v, str) or v not in art and v not in meta for v in titles.values()):
            raise ValueError(f'Broken title reference: {path}')
        return len(titles), sum(bool(v.get('front')) for v in art.values()), sum(bool(v.get('background')) for v in art.values())
    if any(not isinstance(v, dict) for v in titles.values()):
        raise ValueError(f'Invalid Libretro data: {path}')
    return len(titles), sum(bool(v.get('front')) for v in titles.values()), sum(bool(v.get('screenshot')) for v in titles.values())

def main(staging, published):
    for provider, platforms in SYSTEMS.items():
        for platform in sorted(platforms):
            filename = platform + '.json'
            new = staging / provider / filename
            old = published / provider / filename
            if not new.is_file() or new.stat().st_size > 45 * 1024 * 1024:
                raise ValueError(f'Missing or oversized index: {new}')
            counts = read(new, provider, platform)
            if platform in CORE and (not counts[0] or not counts[1]):
                raise ValueError(f'Empty title or cover index: {new}')
            if old.is_file():
                previous = read(old, provider, platform)
                for current, before in zip(counts, previous):
                    if before and current < before * 0.75:
                        raise ValueError(f'Unexpected reduction in {new}: {previous} -> {counts}')
            print(f'{provider}/{filename}: {counts[0]} titles, {counts[1]} covers, {counts[2]} backgrounds/screenshots')
        if provider == 'launchbox' and not read(staging / provider / 'ps3.json', provider, 'ps3')[2]:
            raise ValueError('LaunchBox PS3 fanart index is empty')

if __name__ == '__main__':
    main(Path(sys.argv[1]), Path(sys.argv[2]))
