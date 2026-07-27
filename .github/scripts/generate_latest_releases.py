import datetime
import os
import sys


UNRESOLVED_NAMES = {"", "MISSING", "UNKNOWN TITLE"}


def normalize_name(name):
    return name.strip().upper()


def has_resolved_name(name):
    return normalize_name(name) not in UNRESOLVED_NAMES


def is_valid_item(parts):
    if len(parts) < 4:
        return False

    title_id = parts[0].strip()
    name = parts[2].strip()
    pkg_url = parts[3].strip()

    return (
        bool(title_id)
        and bool(name)
        and normalize_name(name) != "MISSING"
        and bool(pkg_url)
        and pkg_url.upper() != "MISSING"
    )


def get_content_id(parts):
    if len(parts) > 5:
        content_id = parts[5].strip()

        if content_id:
            return content_id

    return None


def get_item_id(parts):
    content_id = get_content_id(parts)

    if content_id:
        return content_id

    if parts:
        title_id = parts[0].strip()

        if title_id:
            return title_id

    return None


def get_reference_paths(old_path, new_path):
    old_path = os.path.normpath(old_path)
    parent_dir = os.path.dirname(old_path)
    filename = os.path.basename(old_path)

    if os.path.basename(parent_dir).lower() == "pending":
        official_path = os.path.join(
            os.path.dirname(parent_dir),
            filename,
        )

        return [
            official_path,
            new_path,
            old_path,
        ]

    pending_path = os.path.join(
        parent_dir,
        "pending",
        filename,
    )

    return [
        new_path,
        old_path,
        pending_path,
    ]


def buid_name_lookup(paths):

    name_lookup = {}

    for path in paths:
        if not path or not os.path.exists(path):
            continue

        with open(path, "r", encoding="utf-8") as file:
            lines = file.readlines()

        for line in lines[1:]:
            parts = line.rstrip("\r\n").split("\t")
            content_id = get_content_id(parts)

            if not content_id or len(parts) <= 2:
                continue

            name = parts[2].strip()

            if (
                has_resolved_name(name)
                and content_id not in name_lookup
            ):
                name_lookup[content_id] = name

    return name_lookup


def resolve_item_name(parts, name_lookup):
    """
    Se o item estiver com UNKNOWN TITLE, MISSING ou vazio,
    procura um título válido pelo mesmo Content ID.
    """

    if len(parts) <= 2:
        return parts

    current_name = parts[2].strip()

    if has_resolved_name(current_name):
        return parts

    content_id = get_content_id(parts)

    if not content_id:
        return parts

    resolved_name = name_lookup.get(content_id)

    if resolved_name:
        parts[2] = resolved_name

    return parts


def read_valid_latest_items(latest_path, name_lookup):
    if not os.path.exists(latest_path):
        return []

    with open(latest_path, "r", encoding="utf-8") as file:
        lines = file.readlines()

    valid_items = []

    for line in lines[1:]:
        parts = line.rstrip("\r\n").split("\t")

        resolve_item_name(parts, name_lookup)

        if is_valid_item(parts):
            valid_items.append(parts)

    return valid_items


def merge_without_duplicates(items):

    merged = []
    positions = {}

    for parts in items:
        item_id = get_item_id(parts)

        if not item_id:
            merged.append(parts)
            continue

        if item_id not in positions:
            positions[item_id] = len(merged)
            merged.append(parts)
            continue

        existing = merged[positions[item_id]]

        existing_name = (
            existing[2].strip()
            if len(existing) > 2
            else ""
        )

        duplicate_name = (
            parts[2].strip()
            if len(parts) > 2
            else ""
        )

        if (
            not has_resolved_name(existing_name)
            and has_resolved_name(duplicate_name)
        ):
            existing[2] = duplicate_name

    return merged


def process_latest(old_path, new_path, latest_path):
    current_date = datetime.datetime.now().strftime("%b %d, %Y")
    old_ids = set()

    if os.path.exists(old_path):
        with open(old_path, "r", encoding="utf-8") as file:
            lines = file.readlines()

        for line in lines[1:]:
            parts = line.rstrip("\r\n").split("\t")
            item_id = get_item_id(parts)

            if item_id:
                old_ids.add(item_id)

    reference_paths = get_reference_paths(
        old_path,
        new_path,
    )

    name_lookup = build_name_lookup(reference_paths)

    header = ""
    new_items = []

    if os.path.exists(new_path):
        with open(new_path, "r", encoding="utf-8") as file:
            lines = file.readlines()

        if lines:
            header = lines[0]

        for line in lines[1:]:
            parts = line.rstrip("\r\n").split("\t")

            resolve_item_name(parts, name_lookup)

            if not is_valid_item(parts):
                continue

            item_id = get_item_id(parts)

            if item_id and item_id not in old_ids:
                while len(parts) < 11:
                    parts.append("")

                parts.append(current_date)
                new_items.append(parts)

    existing_latest = read_valid_latest_items(
        latest_path,
        name_lookup,
    )

    combined_latest = merge_without_duplicates(
        new_items + existing_latest
    )[:50]

    if header:
        with open(
            latest_path,
            "w",
            encoding="utf-8",
            newline="",
        ) as file:
            file.write(
                header
                if header.endswith("\n")
                else header + "\n"
            )

            for parts in combined_latest:
                file.write("\t".join(parts) + "\n")


if __name__ == "__main__":
    if len(sys.argv) < 4:
        print(
            "Uso: python script.py "
            "old_database new_database latest_file"
        )
        sys.exit(1)

    process_latest(
        sys.argv[1],
        sys.argv[2],
        sys.argv[3],
    )