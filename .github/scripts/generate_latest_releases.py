import sys
import os
import datetime

def process_latest(old_path, new_path, latest_path):
    current_date = datetime.datetime.now().strftime("%b %d, %Y")
    
    old_ids = set()
    if os.path.exists(old_path):
        with open(old_path, 'r', encoding='utf-8') as f:
            for line in f:
                parts = line.strip().split('\t')
                if parts: old_ids.add(parts[0])

    new_items = []
    header = ""
    if os.path.exists(new_path):
        with open(new_path, 'r', encoding='utf-8') as f:
            lines = f.readlines()
            if lines:
                header = lines[0]
                for line in lines[1:]:
                    parts = line.strip().split('\t')
                    if parts and parts[0] not in old_ids:
                        while len(parts) < 11: parts.append("")
                        parts.append(current_date)
                        new_items.append("\t".join(parts) + "\n")

    if new_items:
        existing_latest = []
        if os.path.exists(latest_path):
            with open(latest_path, 'r', encoding='utf-8') as f:
                existing_latest = f.readlines()[1:]

        combined_latest = new_items + existing_latest
        with open(latest_path, 'w', encoding='utf-8') as f:
            f.write(header)
            f.writelines(combined_latest[:50]) 

if __name__ == "__main__":
    process_latest(sys.argv[1], sys.argv[2], sys.argv[3])