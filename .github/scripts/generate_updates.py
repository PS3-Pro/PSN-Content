import requests
import os
import re

def get_xml_info(xml_url, tid):
    try:
        r = requests.get(xml_url, timeout=10)
        if r.status_code == 200:
            c = r.text
            
            name_m = re.search(r'<TITLE>(.*?)</TITLE>', c, re.I)
            name = name_m.group(1).strip() if name_m else "Update: " + tid
            
            pkgs = re.findall(r'url="(http[^"]+\.pkg)"', c, re.I)
            url = "|".join(pkgs) if pkgs else xml_url

            last_url = pkgs[-1] if pkgs else xml_url
            cid_m = re.search(r'/([^/]+)\.pkg', last_url)
            cid = cid_m.group(1) if cid_m else ""
            
            size_m = re.findall(r'size="(\d+)"', c)
            total_size = sum(int(s) for s in size_m) if size_m else 0
            size = str(total_size)

            vers = re.findall(r'version="([\d\.]+)"', c)
            max_ver = max(vers) if vers else "01.00"
            version_str = f"v{max_ver}"

            return name, url, cid, size, version_str
    except:
        pass
    return "Update: " + tid, xml_url, "", "0", "v01.00"

def run():
    repo = "PS3-Pro/Game-Updates"
    api_url = f"https://api.github.com/repos/{repo}/git/trees/main?recursive=1"
    raw_url = f"https://raw.githubusercontent.com/{repo}/main"
    output_path = "resources/database/content/official/game_updates.tsv"

    try:
        response = requests.get(api_url)
        if response.status_code != 200: return

        tree = response.json().get("tree", [])
        lines = []
        
        regions = {"U": "US", "E": "EU", "J": "JP", "A": "ASIA", "H": "ASIA", "L": "ASIA"}

        for item in tree:
            path = item["path"]
            if path.startswith("np/np/") and path.endswith("-ver.xml"):
                parts = path.split("/")
                tid = parts[2]

                char_reg = tid[2] if len(tid) > 2 else ""
                reg = regions.get(char_reg, "ASIA") 
                
                full_xml_url = f"{raw_url}/{path}"
                name, pkg_url, cid, size, version = get_xml_info(full_xml_url, tid)

                line = f"{tid}\t{reg}\t{name}\t{pkg_url}\tNOT REQUIRED\t{cid}\t\t\t{size}\t\t{version}"
                lines.append(line)

        if lines:
            sorted_lines = sorted(lines, key=lambda x: x.split('\t')[2])
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            
            with open(output_path, "w", encoding="utf-8", newline='\n') as f:
                f.write("\n".join(sorted_lines))
        print("Update database generated successfully!")
    except Exception as e:
        print(f"Erro: {e}")

if __name__ == "__main__":
    run()