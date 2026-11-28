const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const readline = require('readline');

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
const checked = new Set();
const localCovers = new Set(fs.readdirSync(coversDir));
const sonyFolders = new Set(fs.readdirSync(hashesDir));

const files = fs.readdirSync(officialDbDir);

async function main() {
    for (const file of files) {
        if (!file.endsWith('.tsv')) continue;
        const lines = readline.createInterface({
            input: fs.createReadStream(path.join(officialDbDir, file), { encoding: 'utf8' }),
            crlfDelay: Infinity
        });
        for await (const line of lines) {
            const separator = line.indexOf('\t');
            const titleId = separator === -1 ? line : line.slice(0, separator);
            if (!/^[A-Z]{4}[0-9]{5}$/.test(titleId) || checked.has(titleId)) continue;
            checked.add(titleId);
            if (localCovers.has(`${titleId}.JPG`)) continue;
            const hash = getSonyHash(titleId);
            if (!sonyFolders.has(hash) || !fs.existsSync(path.join(hashesDir, hash, 'ICON0.PNG'))) {
                missingCovers.push(titleId);
            }
        }
    }

    const uniqueMissing = missingCovers.sort();
    const encoded = uniqueMissing.join('\n');
    if (!fs.existsSync(outputPath) || fs.readFileSync(outputPath, 'utf8') !== encoded) {
        const temporary = `${outputPath}.tmp`;
        try {
            fs.writeFileSync(temporary, encoded);
            fs.renameSync(temporary, outputPath);
        } finally {
            if (fs.existsSync(temporary)) fs.unlinkSync(temporary);
        }
    }

    const tsvCount = files.filter(f => f.endsWith('.tsv')).length;
    console.log(`Success: Blacklist generated. Scanned ${tsvCount} TSV files. Found ${uniqueMissing.length} items without cover.`);

}
main().catch(error => {
    console.error(error);
    process.exitCode = 1;
});
