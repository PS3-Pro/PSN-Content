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
                if len(parts) > 5:
                    old_ids.add(parts[5])
                elif len(parts) > 0:
                    old_ids.add(parts[0])

    new_items = []
    header = ""
    
    if os.path.exists(new_path):
        with open(new_path, 'r', encoding='utf-8') as f:
            lines = f.readlines()
            if lines:
                header = lines[0]
                for line in lines[1:]:
                    parts = line.strip().split('\t')
                    
                    item_id = parts[5] if len(parts) > 5 else (parts[0] if parts else None)
                    
                    if item_id and item_id not in old_ids:
                        while len(parts) < 11: 
                            parts.append("")
                        
                        parts.append(current_date)
                        new_items.append("\t".join(parts) + "\n")

    if new_items:
        existing_latest = []
        if os.path.exists(latest_path):
            with open(latest_path, 'r', encoding='utf-8') as f:
                all_lines = f.readlines()
                if len(all_lines) > 0:
                    existing_latest = all_lines[1:]

        combined_latest = new_items + existing_latest
        
        with open(latest_path, 'w', encoding='utf-8') as f:
            f.write(header) # Mantém o cabeçalho
            f.writelines(combined_latest[:50]) 

if __name__ == "__main__":
    if len(sys.argv) < 4:
        print("Uso: python script.py old_database new_database latest_file")
    else:
        process_latest(sys.argv[1], sys.argv[2], sys.argv[3])