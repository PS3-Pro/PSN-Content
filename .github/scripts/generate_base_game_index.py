#!/usr/bin/env python3
import csv
from pathlib import Path

base_dir = Path("resources/database/content/official")
output = Path("resources/database/helpers/base_game_index.tsv")

def clean_field(value):
    return str(value or "").replace("\t", " ").replace("\r", " ").replace("\n", " ").strip()

def valid_title_id(value):
    value = clean_field(value).upper()
    return value if len(value) == 9 and value.isalnum() else ""

def read_rows(path):
    if not path.exists():
        return
    with path.open("r", encoding="utf-8-sig", errors="replace", newline="") as handle:
        for row in csv.reader(handle, delimiter="\t"):
            yield row

child_ids = set()
child_sources = [
    base_dir / "dlcs.tsv",
    base_dir / "pending" / "dlcs.tsv",
    base_dir / "avatars.tsv",
    base_dir / "pending" / "avatars.tsv",
    base_dir / "themes.tsv",
    base_dir / "pending" / "themes.tsv",
    base_dir / "game_updates.tsv",
]

for path in child_sources:
    for row in read_rows(path) or ():
        if not row:
            continue
        title_id = valid_title_id(row[0])
        if title_id:
            child_ids.add(title_id)

records = []
seen = set()

# Keep name/content ID for Games so the Classic can apply its exact
# existing Guide/Soundtrack/Video exclusions on the PS3.
for path in (base_dir / "games.tsv", base_dir / "pending" / "games.tsv"):
    for row in read_rows(path) or ():
        if not row:
            continue
        title_id = valid_title_id(row[0])
        if not title_id or title_id not in child_ids:
            continue
        name = clean_field(row[2] if len(row) > 2 else "")
        content_id = clean_field(row[5] if len(row) > 5 else "")
        key = ("G", title_id, name, content_id)
        if key not in seen:
            seen.add(key)
            records.append(key)

# Preserve optional apps.tsv support from the legacy loader if those
# files are ever present; absent files are simply ignored.
for path in (base_dir / "apps.tsv", base_dir / "pending" / "apps.tsv"):
    for row in read_rows(path) or ():
        if not row:
            continue
        title_id = valid_title_id(row[0])
        if not title_id or title_id not in child_ids:
            continue
        key = ("A", title_id)
        if key not in seen:
            seen.add(key)
            records.append(key)

for path in (base_dir / "demos.tsv", base_dir / "pending" / "demos.tsv"):
    for row in read_rows(path) or ():
        if not row:
            continue
        title_id = valid_title_id(row[0])
        if not title_id or title_id not in child_ids:
            continue
        key = ("D", title_id)
        if key not in seen:
            seen.add(key)
            records.append(key)

output.parent.mkdir(parents=True, exist_ok=True)
with output.open("w", encoding="utf-8", newline="\n") as handle:
    handle.write("# PS3 Classic base game index v1\n")
    handle.write("# G\\tTITLE_ID\\tNAME\\tCONTENT_ID\n")
    handle.write("# A\\tTITLE_ID\n")
    handle.write("# D\\tTITLE_ID\n")
    for record in records:
        handle.write("\t".join(record) + "\n")

print(
    "Generated {} | Child IDs: {} | Parent records: {}".format(
        output, len(child_ids), len(records)
    )
)
