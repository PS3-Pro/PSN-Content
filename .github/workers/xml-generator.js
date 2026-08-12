export default {
  async fetch(request) {
    const url = new URL(request.url);
    const pathParts = url.pathname.split('/'); 
    const titleId = pathParts[2]?.toUpperCase().trim();
    const category = pathParts[3]?.replace('.xml', '').toLowerCase().trim();

    if (!titleId) {
      return new Response("Insert the Title ID.", { status: 400 });
    }

    const escapeXml = (unsafe) => {
      return unsafe.replace(/[<>&"']/g, (m) => {
        switch (m) {
          case '<': return '&lt;';
          case '>': return '&gt;';
          case '&': return '&amp;';
          case '"': return '&quot;';
          case "'": return '&apos;';
          default: return m;
        }
      });
    };

    const GITHUB_USER = "PS3-Pro";
    const GITHUB_REPO = "PSN-Content";
    const BASE_RAW = `https://raw.githubusercontent.com/${GITHUB_USER}/${GITHUB_REPO}/master/resources/database/`;
    const API_URL = `https://api.github.com/repos/${GITHUB_USER}/${GITHUB_REPO}/contents/resources/database/external`;

    let fileName = (category === 'updates') ? 'game_updates.tsv' : `${category}.tsv`;

    let filesToSearch = [
      `${BASE_RAW}official/${fileName}`,
      `${BASE_RAW}official/pending/${fileName}`,
      `${BASE_RAW}unofficial/${fileName}`
    ];

    try {
      if (category === 'games') {
        try {
          const apiRes = await fetch(API_URL, {
            headers: { 'User-Agent': 'Cloudflare-Worker-PS3-Pro' }
          });
          if (apiRes.ok) {
            const contents = await apiRes.json();
            const externalFiles = contents
              .filter(file => file.name.endsWith('.tsv'))
              .map(file => file.download_url);
            filesToSearch = [...filesToSearch, ...externalFiles];
          }
        } catch (e) { console.error("Erro API GitHub:", e); }
      }

      let pkgUrls = [];
      let totalSize = 0;

      for (const tsvUrl of filesToSearch) {
        const res = await fetch(tsvUrl);
        if (!res.ok) continue;

        const text = await res.text();
        const lines = text.split(/\r?\n/);

        for (let line of lines) {
          if (!line.trim()) continue;
          const parts = line.split('\t');
          
          if (parts[0]?.trim().toUpperCase() === titleId) {
            const pkgUrlRaw = parts[3]?.trim();
            
            let rawSizeBytes = 0;
            if (category === 'updates') {
              rawSizeBytes = parseInt(parts[8]?.trim(), 10) || 0;
            } else {
              for (let j = parts.length - 1; j >= 3; j--) {
                if (parts[j] && /^\s*\d+\s*$/.test(parts[j])) {
                  rawSizeBytes = parseInt(parts[j].trim(), 10);
                  break;
                }
              }
            }

            if (pkgUrlRaw && pkgUrlRaw.startsWith('http') && pkgUrlRaw !== "MISSING") {
              const urls = pkgUrlRaw.split('|');
              const sizePerPart = Math.floor(rawSizeBytes / urls.length);

              urls.forEach((u) => {
                pkgUrls.push({ url: u.trim(), size: sizePerPart });
              });
              
              totalSize = rawSizeBytes;
            }
          }
        }
        if (pkgUrls.length > 0) break;
      }

      if (pkgUrls.length === 0) {
        return new Response(`ID ${titleId} not found in ${fileName}.`, { 
          status: 404,
          headers: { "Access-Control-Allow-Origin": "*" }
        });
      }

      const piecesXml = pkgUrls.map((p, i) => 
        `<pieces file_size="${p.size}" hash_value="0000000000000000000000000000000000000000" index="${i}" url="${escapeXml(p.url)}"/>`
      ).join('\n');

      const xml = `<?xml version="1.0" encoding="UTF-8"?>
<hfs_manifest>
<file_name>${titleId}.pkg</file_name>
<file_size>${totalSize}</file_size>
<number_of_split_files>${pkgUrls.length}</number_of_split_files>
${piecesXml}
</hfs_manifest>`;

      return new Response(xml, {
        headers: { 
          "Content-Type": "application/xml; charset=utf-8",
          "Access-Control-Allow-Origin": "*" 
        }
      });

    } catch (e) {
      return new Response("Erro: " + e.message, { status: 500 });
    }
  }
};