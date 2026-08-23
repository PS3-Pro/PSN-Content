import datetime
import json
import os
import re
import sys


LATEST_LIMIT = 50
EVENT_SCHEMA_VERSION = 1
EVENT_INDEX_NAME = "index.json"
EVENT_SEEN_NAME = ".seen_ids.txt"
EVENT_TYPES = {
    "dlcs": "dlc",
    "avatars": "avatar",
    "themes": "theme",
}


def is_valid_item(parts):
    if len(parts) < 4:
        return False

    title_id = parts[0].strip()
    name = parts[2].strip()
    pkg_url = parts[3].strip()

    return (
        bool(title_id)
        and bool(name)
        and name.upper() != "MISSING"
        and bool(pkg_url)
        and pkg_url.upper() != "MISSING"
    )


def get_item_id(parts):
    if len(parts) > 5:
        value = parts[5].strip()
        if value and value.upper() != "MISSING":
            return value
    return parts[0].strip() if parts else None


def read_valid_latest_items(latest_path):
    if not os.path.exists(latest_path):
        return []

    with open(latest_path, "r", encoding="utf-8") as file:
        lines = file.readlines()

    valid_items = []
    for line in lines[1:]:
        parts = line.rstrip("\r\n").split("\t")
        if is_valid_item(parts):
            valid_items.append(line if line.endswith("\n") else line + "\n")

    return valid_items


def utc_now():
    return datetime.datetime.now(datetime.timezone.utc)


def normalize_event_key(parts):
    content_id = parts[5].strip().upper() if len(parts) > 5 and parts[5].strip().upper() != "MISSING" else ""
    if content_id:
        return content_id
    title_id = parts[0].strip().upper() if parts else ""
    name = parts[2].strip().casefold() if len(parts) > 2 else ""
    return f"{title_id}|{name}".strip("|")


def parse_legacy_added_at(value, fallback_ms):
    text = str(value or "").strip()
    if not text:
        return fallback_ms
    for fmt in ("%b %d, %Y", "%Y-%m-%d", "%Y-%m-%dT%H:%M:%S%z"):
        try:
            parsed = datetime.datetime.strptime(text, fmt)
            if parsed.tzinfo is None:
                # Old Recently Added rows only stored a date. Noon UTC avoids timezone-edge shifts.
                parsed = parsed.replace(hour=12, tzinfo=datetime.timezone.utc)
            return int(parsed.timestamp() * 1000)
        except ValueError:
            continue

    return fallback_ms


def build_catalog_event(parts, added_at_ms, source, event_type):
    if not is_valid_item(parts) or event_type not in EVENT_TYPES.values():
        return None

    key = normalize_event_key(parts)
    if not key:
        return None

    title_id = parts[0].strip().upper()
    region = parts[1].strip() if len(parts) > 1 else ""
    name = parts[2].strip() if len(parts) > 2 else ""
    content_id = parts[5].strip() if len(parts) > 5 and parts[5].strip().upper() != "MISSING" else ""
    return {
        "key": key,
        "type": event_type,
        "titleId": title_id,
        "contentId": content_id,
        "name": name,
        "region": region,
        "addedAt": int(added_at_ms),
        "source": str(source or "").strip().lower() or "unknown",
    }


def read_json(path, default):
    try:
        with open(path, "r", encoding="utf-8") as file:
            value = json.load(file)
        return value
    except (OSError, ValueError, TypeError):
        return default


def write_json(path, value):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8", newline="\n") as file:
        json.dump(value, file, ensure_ascii=False, indent=2, sort_keys=False)
        file.write("\n")


def get_event_month(added_at_ms):
    stamp = max(0, int(added_at_ms or 0)) / 1000.0
    return datetime.datetime.fromtimestamp(stamp, tz=datetime.timezone.utc).strftime("%Y-%m")


def iter_event_month_files(events_dir):
    if not os.path.isdir(events_dir):
        return []
    files = []
    for name in os.listdir(events_dir):
        if re.fullmatch(r"\d{4}-\d{2}\.json", name):
            files.append(os.path.join(events_dir, name))
    return sorted(files)


def load_seen_event_keys(events_dir):
    seen_path = os.path.join(events_dir, EVENT_SEEN_NAME)
    if os.path.exists(seen_path):
        with open(seen_path, "r", encoding="utf-8") as file:
            return {line.strip().upper() for line in file if line.strip()}

    # First migration: reconstruct the dedupe set from any existing monthly shards.
    seen = set()
    for path in iter_event_month_files(events_dir):
        payload = read_json(path, {})
        for event in payload.get("events", []) if isinstance(payload, dict) else []:
            key = str(event.get("key", "")).strip().upper()
            if key:
                seen.add(key)
    return seen


def write_seen_event_keys(events_dir, seen):
    os.makedirs(events_dir, exist_ok=True)
    path = os.path.join(events_dir, EVENT_SEEN_NAME)
    with open(path, "w", encoding="utf-8", newline="\n") as file:
        for key in sorted({str(value).strip().upper() for value in seen if str(value).strip()}):
            file.write(key + "\n")


def rebuild_event_index(events_dir, event_type, updated_at_ms, tracking_started_at_ms=0):
    index_path = os.path.join(events_dir, EVENT_INDEX_NAME)
    previous_index = read_json(index_path, {})
    previous_tracking_started_at = int(previous_index.get("trackingStartedAt", 0) or 0) if isinstance(previous_index, dict) else 0
    tracking_started_at = previous_tracking_started_at or max(0, int(tracking_started_at_ms or 0)) or max(0, int(updated_at_ms or 0))
    months = []
    latest_event_at = 0

    for path in iter_event_month_files(events_dir):
        payload = read_json(path, {})
        events = payload.get("events", []) if isinstance(payload, dict) else []
        valid_times = [
            int(event.get("addedAt", 0) or 0)
            for event in events
            if isinstance(event, dict)
            and str(event.get("type", "")).strip().lower() == event_type
            and int(event.get("addedAt", 0) or 0) > 0
        ]
        if not valid_times:
            continue
        month_id = os.path.basename(path)[:-5]
        min_at = min(valid_times)
        max_at = max(valid_times)
        latest_event_at = max(latest_event_at, max_at)
        months.append({
            "id": month_id,
            "file": os.path.basename(path),
            "minAt": min_at,
            "maxAt": max_at,
            "count": len(events),
        })

    write_json(index_path, {
        "schemaVersion": EVENT_SCHEMA_VERSION,
        "type": event_type,
        "trackingStartedAt": int(tracking_started_at),
        "updatedAt": int(updated_at_ms),
        "latestEventAt": int(latest_event_at),
        "months": months,
    })


def append_catalog_events(events_dir, event_type, events, seen, tracking_started_at_ms=0):
    pending = []
    for event in events:
        if not event or str(event.get("type", "")).strip().lower() != event_type:
            continue
        key = str(event.get("key", "")).strip().upper()
        if not key or key in seen:
            continue
        seen.add(key)
        pending.append(event)

    if not pending:
        return False

    by_month = {}
    for event in pending:
        by_month.setdefault(get_event_month(event.get("addedAt")), []).append(event)

    for month_id, month_events in by_month.items():
        path = os.path.join(events_dir, f"{month_id}.json")
        payload = read_json(path, {
            "schemaVersion": EVENT_SCHEMA_VERSION,
            "type": event_type,
            "month": month_id,
            "events": [],
        })
        if not isinstance(payload, dict):
            payload = {
                "schemaVersion": EVENT_SCHEMA_VERSION,
                "type": event_type,
                "month": month_id,
                "events": [],
            }

        existing = payload.get("events", [])
        if not isinstance(existing, list):
            existing = []

        existing_keys = {
            str(item.get("key", "")).strip().upper()
            for item in existing
            if isinstance(item, dict)
        }
        for event in month_events:
            key = str(event.get("key", "")).strip().upper()
            if key not in existing_keys:
                existing.append(event)
                existing_keys.add(key)

        existing.sort(key=lambda item: (int(item.get("addedAt", 0) or 0), str(item.get("key", ""))))
        payload.update({
            "schemaVersion": EVENT_SCHEMA_VERSION,
            "type": event_type,
            "month": month_id,
            "events": existing,
        })
        write_json(path, payload)

    write_seen_event_keys(events_dir, seen)
    rebuild_event_index(
        events_dir,
        event_type,
        max(int(event.get("addedAt", 0) or 0) for event in pending),
        tracking_started_at_ms,
    )
    return True


def seed_catalog_events_from_latest(events_dir, event_type, latest_path, seen, source="legacy-latest", tracking_started_at_ms=0):
    # First rollout seeds the current 50-item Recently Added window.
    # Clients baseline at trackingStartedAt, so old rows do not create retroactive spam.
    fallback_ms = max(0, int(tracking_started_at_ms or 0)) or int(utc_now().timestamp() * 1000)
    seed_events = []
    for line in read_valid_latest_items(latest_path):
        parts = line.rstrip("\r\n").split("\t")
        added_at_ms = parse_legacy_added_at(parts[11] if len(parts) > 11 else "", fallback_ms)
        seed_events.append(build_catalog_event(parts, added_at_ms, source, event_type))

    changed = append_catalog_events(events_dir, event_type, seed_events, seen, tracking_started_at_ms)
    if tracking_started_at_ms:
        rebuild_event_index(events_dir, event_type, tracking_started_at_ms, tracking_started_at_ms)
    return changed


def process_latest(old_path, new_path, latest_path, events_dir="", category="", source=""):
    now = utc_now()
    current_date = now.strftime("%b %d, %Y")
    now_ms = int(now.timestamp() * 1000)
    category_key = str(category or "").strip().lower()
    event_type = EVENT_TYPES.get(category_key, "")
    is_event_source = bool(event_type and events_dir)

    old_ids = set()
    if os.path.exists(old_path):
        with open(old_path, "r", encoding="utf-8") as file:
            for line in file:
                parts = line.rstrip("\r\n").split("\t")
                item_id = normalize_event_key(parts) if event_type else get_item_id(parts)
                if item_id:
                    old_ids.add(item_id)

    seen_event_keys = None
    if is_event_source:
        os.makedirs(events_dir, exist_ok=True)
        seen_event_keys = load_seen_event_keys(events_dir)
        if not os.path.exists(os.path.join(events_dir, EVENT_SEEN_NAME)):
            seed_catalog_events_from_latest(
                events_dir,
                event_type,
                latest_path,
                seen_event_keys,
                tracking_started_at_ms=now_ms,
            )
            # append_catalog_events may have written the file; if there were no seed rows,
            # persist the empty dedupe set and rollout boundary.
            if not os.path.exists(os.path.join(events_dir, EVENT_SEEN_NAME)):
                write_seen_event_keys(events_dir, seen_event_keys)
                rebuild_event_index(events_dir, event_type, now_ms, now_ms)

    header = ""
    new_items = []
    new_event_parts = []
    if os.path.exists(new_path):
        with open(new_path, "r", encoding="utf-8") as file:
            lines = file.readlines()

        if lines:
            header = lines[0]

        for line in lines[1:]:
            parts = line.rstrip("\r\n").split("\t")
            if not is_valid_item(parts):
                continue
            item_id = normalize_event_key(parts) if event_type else get_item_id(parts)
            if item_id and item_id not in old_ids:
                while len(parts) < 11:
                    parts.append("")
                if event_type:
                    new_event_parts.append(list(parts))
                parts.append(current_date)
                new_items.append("\t".join(parts) + "\n")

    if is_event_source and seen_event_keys is not None and new_event_parts:
        events = [
            build_catalog_event(parts, now_ms, source, event_type)
            for parts in new_event_parts
        ]
        append_catalog_events(events_dir, event_type, events, seen_event_keys, now_ms)

    existing_latest = read_valid_latest_items(latest_path)
    combined_latest = (new_items + existing_latest)[:LATEST_LIMIT]
    if header:
        with open(latest_path, "w", encoding="utf-8", newline="") as file:
            file.write(header if header.endswith("\n") else header + "\n")
            file.writelines(combined_latest)


if __name__ == "__main__":
    if len(sys.argv) < 4:
        print("Uso: python script.py old_database new_database latest_file [events_dir category source]")
    else:
        process_latest(
            sys.argv[1],
            sys.argv[2],
            sys.argv[3],
            sys.argv[4] if len(sys.argv) > 4 else "",
            sys.argv[5] if len(sys.argv) > 5 else "",
            sys.argv[6] if len(sys.argv) > 6 else "",
        )
