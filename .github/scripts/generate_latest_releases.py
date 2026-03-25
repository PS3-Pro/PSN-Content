import sys
import os
import datetime

def update_tsv(old_path, new_path):
    current_time = datetime.datetime.now().strftime("%d/%m/%Y - %H:%M")
    existing_dates = {}

    if os.path.exists(old_path):
        with open(old_path, 'r', encoding='utf-8') as f:
            for line in f:
                parts = line.strip().split('\t')
                if len(parts) > 11:
                    existing_dates[parts[0]] = parts[11]

    with open(new_path, 'r', encoding='utf-8') as f:
        lines = f.readlines()

    updated_lines = []
    if lines:
        updated_lines.append(lines[0].strip() + "\n")

        for line in lines[1:]:
            parts = line.strip().split('\t')
            if len(parts) < 4 or parts[3] == "MISSING": continue
            
            title_id = parts[0]
            date_to_use = existing_dates.get(title_id, current_time)

            while len(parts) < 11:
                parts.append("")
            
            if len(parts) == 11:
                parts.append(date_to_use)
            else:
                parts[11] = date_to_use
                
            updated_lines.append("\t".join(parts) + "\n")

    with open(old_path, 'w', encoding='utf-8') as f:
        f.writelines(updated_lines)

if __name__ == "__main__":
    update_tsv(sys.argv[1], sys.argv[2])