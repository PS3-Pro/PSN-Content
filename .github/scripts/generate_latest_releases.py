import datetime
import os
import sys


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
        return parts[5]
    return parts[0] if parts else None


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


def process_latest(old_path, new_path, latest_path):
    current_date = datetime.datetime.now().strftime("%b %d, %Y")
    old_ids = set()

    if os.path.exists(old_path):
        with open(old_path, "r", encoding="utf-8") as file:
            for line in file:
                parts = line.rstrip("\r\n").split("\t")
                item_id = get_item_id(parts)
                if item_id:
                    old_ids.add(item_id)

    header = ""
    new_items = []

    if os.path.exists(new_path):
        with open(new_path, "r", encoding="utf-8") as file:
            lines = file.readlines()

        if lines:
            header = lines[0]

        for line in lines[1:]:
            parts = line.rstrip("\r\n").split("\t")
            if not is_valid_item(parts):
                continue

            item_id = get_item_id(parts)
            if item_id and item_id not in old_ids:
                while len(parts) < 11:
                    parts.append("")
                parts.append(current_date)
                new_items.append("\t".join(parts) + "\n")

    existing_latest = read_valid_latest_items(latest_path)
    combined_latest = (new_items + existing_latest)[:50]

    if header:
        with open(latest_path, "w", encoding="utf-8", newline="") as file:
            file.write(header if header.endswith("\n") else header + "\n")
            file.writelines(combined_latest)


if __name__ == "__main__":
    if len(sys.argv) < 4:
        print("Uso: python script.py old_database new_database latest_file")
    else:
        process_latest(sys.argv[1], sys.argv[2], sys.argv[3])
