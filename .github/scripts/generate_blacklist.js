const fs = require('fs');
const path = require('path');
const crypto = require('crypto');

const key = Buffer.from([
    0xF5, 0xDE, 0x66, 0xD2, 0x68, 0x0E, 0x25, 0x5B, 0x2D, 0xF7, 0x9E, 0x74, 0xF8, 0x90, 0xEB, 0xF3,
    0x49, 0x26, 0x2F, 0x61, 0x8B, 0xCA, 0xE2, 0xA9, 0xAC, 0xCD, 0xEE, 0x51, 0x56, 0xCE, 0x8D, 0xF2,
    0xCD, 0xF2, 0xD4, 0x8C, 0x71, 0x17, 0x3C, 0xDC, 0x25, 0x94, 0x46, 0x5B, 0x87, 0x40, 0x5D, 0x19,
    0x7C, 0xF1, 0xAE, 0xD3, 0xB7, 0xE9, 0x67, 0x1E, 0xEB, 0x56, 0xCA, 0x67, 0x53, 0xC2, 0xE6, 0xB0
]);

function getSonyHash(titleId) {
    const base = titleId + "_00";
    const hmac = crypto.createHmac('sha1', key);
    hmac.update(base);
    return base + "_" + hmac.digest('hex').toUpperCase();
}

const officialDbDir = 'resources/database/content/official'; 
const coversDir = 'resources/database/covers/compressed';
const hashesDir = '../Recently-Played-Games/np';
const outputPath = 'resources/database/covers/blacklist.tsv';

const missingCovers = [];

const files = fs.readdirSync(officialDbDir);

files.forEach(file => {
    if (!file.endsWith('.tsv')) return;

    const filePath = path.join(officialDbDir, file);
    const dbContent = fs.readFileSync(filePath, 'utf8');
    const lines = dbContent.split('\n');

    lines.forEach(line => {
        if (!line || line.trim() === '') return;
        
        const parts = line.split('\t');
        const titleId = parts[0];

        if (titleId && titleId.length === 9 && /^[A-Z]{4}[0-9]{5}$/.test(titleId)) {
            const hasLocalCover = fs.existsSync(path.join(coversDir, `${titleId}.JPG`));

            if (!hasLocalCover) {
                const hash = getSonyHash(titleId);
                const hasSonyCover = fs.existsSync(path.join(hashesDir, hash, 'ICON0.PNG'));

                if (!hasSonyCover) {
                    missingCovers.push(titleId);
                }
            }
        }
    });
});

const uniqueMissing = [...new Set(missingCovers)].sort();
fs.writeFileSync(outputPath, uniqueMissing.join('\n'));

const tsvCount = files.filter(f => f.endsWith('.tsv')).length;
console.log(`Success: Blacklist generated. Scanned ${tsvCount} TSV files. Found ${uniqueMissing.length} items without cover.`);