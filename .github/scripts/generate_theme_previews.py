#!/usr/bin/env python3
"""
Extract PS3 theme preview PNGs from theme TSV rows and save them as:

    resources/database/themes/<TITLE_ID>/<CONTENT_ID>.png

This follows the avatar-cover workflow style, but themes usually store the
actual .p3t inside a .p3t.edat. The pipeline is:

    TSV row -> PKG -> .p3t.edat -> decrypted .p3t -> preview.png -> repo PNG

Dependencies:
    pip install pycryptodomex pillow

System dependencies for P3T/GIM conversion:
    php-cli php-gd git unzip

Optional, but recommended:
    Run from the repository root:
        python .github/scripts/generate_theme_previews.py --repo . --source pending --title-id BLES00354
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import io
import os
import re
import shutil
import struct
import subprocess
import sys
import tempfile
import urllib.parse
import urllib.request
import zlib
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Iterable

try:
    from Cryptodome.Cipher import AES
    from Cryptodome.Util import Counter
except ImportError:  # Allows running with pycryptodome too.
    from Crypto.Cipher import AES
    from Crypto.Util import Counter

try:
    from PIL import Image
except ImportError:
    Image = None


# -----------------------------------------------------------------------------
# Generic helpers
# -----------------------------------------------------------------------------

def safe_filename(value: str | None, fallback: str = "unknown") -> str:
    value = (value or fallback).strip()
    value = re.sub(r"[^A-Za-z0-9._-]+", "_", value)
    value = value.strip("._-")
    return value or fallback


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest().upper()


def download_file(url: str, dest: Path, log: Callable[[str], None]) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_suffix(dest.suffix + ".part")
    request = urllib.request.Request(
        url,
        headers={"User-Agent": "PS3-Pro-Theme-Preview-Generator/1.0"},
    )

    with urllib.request.urlopen(request, timeout=120) as response, tmp.open("wb") as out:
        total = int(response.headers.get("Content-Length") or 0)
        downloaded = 0

        while True:
            chunk = response.read(1024 * 1024)
            if not chunk:
                break

            out.write(chunk)
            downloaded += len(chunk)

        if total:
            log(f"    Downloaded {downloaded / 1024:.1f} KB / {total / 1024:.1f} KB")
        else:
            log(f"    Downloaded {downloaded / 1024:.1f} KB")

    tmp.replace(dest)


def extract_title_id_from_content_id(content_id: str) -> str:
    # Example: EP0002-BLES00354_00-CODWAWTHEME00001 -> BLES00354
    match = re.search(r"([A-Z]{4}\d{5})", content_id or "", flags=re.I)
    return match.group(1).upper() if match else ""


def extract_title_id_from_pkg_url(pkg_url: str) -> str:
    # Example: /EP0002/BLES00354_00/...pkg -> BLES00354
    match = re.search(r"/([A-Z]{4}\d{5})_", pkg_url or "", flags=re.I)
    return match.group(1).upper() if match else ""


def resolved_title_id(row_title_id: str, content_id: str, pkg_url: str) -> str:
    # Prefer IDs encoded in the Content ID / URL because they catch malformed TSV rows.
    return (
        extract_title_id_from_content_id(content_id)
        or extract_title_id_from_pkg_url(pkg_url)
        or (row_title_id or "").upper()
    )


# -----------------------------------------------------------------------------
# Theme TSV handling
# -----------------------------------------------------------------------------

@dataclass(frozen=True)
class ThemeEntry:
    title_id: str
    region: str
    name: str
    pkg_url: str
    rap_hex: str
    content_id: str
    sha256: str = ""
    size: str = ""
    date: str = ""


COLUMN_ALIASES = {
    "title_id": ["Title ID", "TitleID", "TITLE_ID", "id"],
    "region": ["Region", "REGION"],
    "name": ["Name", "Title", "NAME"],
    "pkg_url": ["PKG direct link", "PKG", "URL", "pkgUrl"],
    "rap_hex": ["RAP", "rap"],
    "content_id": ["Content ID", "ContentID", "CONTENT_ID", "contentId"],
    "sha256": ["SHA256", "sha256", "SHA-256"],
    "size": ["Size", "SIZE", "File Size"],
    "date": ["Date", "DATE", "Modified", "Last Modified"],
}


def get_col(row: dict[str, str], key: str) -> str:
    for name in COLUMN_ALIASES[key]:
        if name in row:
            return (row.get(name) or "").strip()
    return ""


def read_theme_tsv(path: Path) -> list[ThemeEntry]:
    entries: list[ThemeEntry] = []

    with path.open("r", encoding="utf-8-sig", newline="") as f:
        sample = f.read(4096)
        f.seek(0)

        # Headered TSV is preferred. If the file has no recognized header, fall back to positional.
        first_line = sample.splitlines()[0] if sample.splitlines() else ""
        has_header = any(alias in first_line for aliases in COLUMN_ALIASES.values() for alias in aliases)

        if has_header:
            reader = csv.DictReader(f, delimiter="\t")
            for row in reader:
                title_id = get_col(row, "title_id").upper()
                pkg_url = get_col(row, "pkg_url")
                content_id = get_col(row, "content_id")

                if not title_id or not pkg_url or pkg_url.upper() == "MISSING":
                    continue

                entries.append(
                    ThemeEntry(
                        title_id=title_id,
                        region=get_col(row, "region"),
                        name=get_col(row, "name"),
                        pkg_url=pkg_url,
                        rap_hex=get_col(row, "rap_hex"),
                        content_id=content_id,
                        sha256=get_col(row, "sha256"),
                        size=get_col(row, "size"),
                        date=get_col(row, "date"),
                    )
                )
        else:
            reader = csv.reader(f, delimiter="\t")
            for cols in reader:
                if not cols or len(cols) < 6:
                    continue

                title_id = cols[0].strip().upper()
                pkg_url = cols[3].strip()
                content_id = cols[5].strip()

                if not title_id or not pkg_url or pkg_url.upper() == "MISSING":
                    continue

                entries.append(
                    ThemeEntry(
                        title_id=title_id,
                        region=cols[1].strip() if len(cols) > 1 else "",
                        name=cols[2].strip() if len(cols) > 2 else "",
                        pkg_url=pkg_url,
                        rap_hex=cols[4].strip() if len(cols) > 4 else "",
                        content_id=content_id,
                        size=cols[8].strip() if len(cols) > 8 else "",
                        date=cols[11].strip() if len(cols) > 11 else "",
                    )
                )

    return entries


def read_all_entries(paths: Iterable[Path]) -> list[ThemeEntry]:
    entries: list[ThemeEntry] = []
    seen: set[tuple[str, str, str]] = set()

    for path in paths:
        if not path.exists():
            continue

        for entry in read_theme_tsv(path):
            key = (entry.title_id, entry.content_id, entry.pkg_url)
            if key in seen:
                continue

            seen.add(key)
            entries.append(entry)

    return entries


# -----------------------------------------------------------------------------
# PKG extractor core
# -----------------------------------------------------------------------------

PKG_MAGIC = b"\x7fPKG"
PKG_TYPE_PS3 = 0x0001
PKG_RELEASE_TYPE_DEBUG = 0x0000
PS3_NPDRM_KEY = bytes.fromhex("2e7b71d7c9c9a14ea3221f188828b8f8")
FLAG_DIR = 0x04
ITEM_RECORD_SIZE = 0x20
HDR_FMT = ">4sHHIIIIQQQ48s16s16s"
HDR_SIZE = struct.calcsize(HDR_FMT)
ITEM_FMT = ">IIQQII"


def read_pkg_header(f) -> dict:
    f.seek(0)
    raw = f.read(HDR_SIZE)
    if len(raw) < HDR_SIZE:
        raise ValueError("File too small to be a PKG")

    (
        magic,
        revision,
        pkg_type,
        _meta_offset,
        _meta_count,
        _header_size,
        item_count,
        _total_size,
        data_offset,
        _data_size,
        content_id_raw,
        digest,
        riv,
    ) = struct.unpack(HDR_FMT, raw)

    if magic != PKG_MAGIC:
        raise ValueError(f"Not a valid PKG file: magic={magic!r}")

    if pkg_type != PKG_TYPE_PS3:
        raise ValueError(f"Not a PS3 PKG: type={pkg_type:#06x}")

    content_id = content_id_raw.rstrip(b"\x00").decode("ascii", errors="replace")
    return {
        "item_count": item_count,
        "data_offset": data_offset,
        "content_id": content_id,
        "iv": riv,
        "qa_digest": digest,
        "release_type": revision,
    }


def sha1_compat(data: bytes) -> bytes:
    try:
        return hashlib.sha1(data, usedforsecurity=False).digest()
    except TypeError:
        return hashlib.sha1(data).digest()


def get_debug_keystream_block(qa_digest: bytes, block_index: int) -> bytes:
    qa_0 = qa_digest[0:8]
    qa_1 = qa_digest[8:16]
    buffer = bytearray(64)
    buffer[0:8] = qa_0
    buffer[8:16] = qa_0
    buffer[16:24] = qa_1
    buffer[24:32] = qa_1
    buffer[56:64] = struct.pack(">Q", block_index)
    return sha1_compat(buffer)[:16]


def decrypt_pkg_region(f, hdr: dict, stream_pos: int, size: int) -> bytes:
    if size <= 0:
        return b""

    data_offset = hdr["data_offset"]
    release_type = hdr["release_type"]
    block_start = stream_pos & ~0xF
    prefix_len = stream_pos - block_start
    num_bytes = prefix_len + size
    num_blocks = (num_bytes + 15) // 16

    f.seek(data_offset + block_start)
    enc = f.read(num_blocks * 16)

    if release_type == PKG_RELEASE_TYPE_DEBUG:
        qa_digest = hdr["qa_digest"]
        block_index = block_start // 16
        plaintext = bytearray()

        for i, offset in enumerate(range(0, len(enc), 16)):
            chunk = enc[offset:offset + 16]
            keystream = get_debug_keystream_block(qa_digest, block_index + i)
            plaintext.extend(a ^ b for a, b in zip(chunk, keystream))

        return bytes(plaintext)[prefix_len:prefix_len + size]

    iv = hdr["iv"]
    iv_int = int.from_bytes(iv, "big")
    ctr_val = (iv_int + block_start // 16) & ((1 << 128) - 1)
    ctr = Counter.new(128, initial_value=ctr_val)
    cipher = AES.new(PS3_NPDRM_KEY, AES.MODE_CTR, counter=ctr)
    plaintext = cipher.decrypt(enc)
    return plaintext[prefix_len:prefix_len + size]


def parse_sfo(data: bytes) -> dict | None:
    if data[:4] != b"\0PSF":
        return None

    key_table_start = struct.unpack_from("<I", data, 0x08)[0]
    data_table_start = struct.unpack_from("<I", data, 0x0C)[0]
    entry_count = struct.unpack_from("<I", data, 0x10)[0]
    result: dict[str, str] = {}

    for i in range(entry_count):
        entry_off = 0x14 + i * 0x10
        key_off, _fmt, size, _max_size, data_off = struct.unpack_from("<HHIII", data, entry_off)

        key_start = key_table_start + key_off
        key_end = data.find(b"\0", key_start)
        if key_end == -1:
            continue

        key = data[key_start:key_end].decode("utf-8", errors="ignore")
        val_start = data_table_start + data_off
        raw = data[val_start:val_start + size]
        value = raw.rstrip(b"\0").decode("utf-8", errors="ignore")
        result[key] = value

    return result or None


def is_path_pkg(item_names: list[str]) -> bool:
    return any(name.startswith("../") or name.startswith("..\\") for name in item_names)


def resolve_path_pkg_dest(raw_name: str, dest_root: Path) -> Path | None:
    name = raw_name.replace("\\", "/")
    parts = [p for p in name.split("/") if p]
    resolved: list[str] = []

    for part in parts:
        if part == "..":
            if resolved:
                resolved.pop()
        elif part != ".":
            resolved.append(part)

    if not resolved:
        return None

    result = dest_root.joinpath(*resolved)
    try:
        result.resolve().relative_to(dest_root.resolve())
    except ValueError:
        return None

    return result


def extract_pkg(pkg_path: Path, dest_root: Path, fallback_folder: str, log: Callable[[str], None]) -> Path:
    """Extract PKG and return the folder where the extracted files were written."""
    with pkg_path.open("rb") as f:
        hdr = read_pkg_header(f)
        item_count = hdr["item_count"]

        log(f"    PKG CONTENT-ID: {hdr['content_id']}")
        log(f"    PKG ITEMS: {item_count}")

        table_raw = decrypt_pkg_region(f, hdr, 0, item_count * ITEM_RECORD_SIZE)
        items = []

        for i in range(item_count):
            rec = table_raw[i * ITEM_RECORD_SIZE:(i + 1) * ITEM_RECORD_SIZE]
            name_off, name_size, item_data_off, item_data_size, flags, _ = struct.unpack(ITEM_FMT, rec)
            items.append((name_off, name_size, item_data_off, item_data_size, flags))

        raw_names: list[str] = []
        pkg_title_id = None

        for name_off, name_size, item_data_off, item_data_size, _flags in items:
            if name_size <= 0:
                raw_names.append("")
                continue

            raw_name = decrypt_pkg_region(f, hdr, name_off, name_size)
            name = raw_name.rstrip(b"\x00").decode("utf-8", errors="replace")
            raw_names.append(name)

            if name.upper().endswith("PARAM.SFO") and item_data_size > 0:
                sfo_data = decrypt_pkg_region(f, hdr, item_data_off, item_data_size)
                info = parse_sfo(sfo_data)
                if info and info.get("TITLE_ID"):
                    pkg_title_id = info["TITLE_ID"]

        path_pkg = is_path_pkg(raw_names)
        output_root = dest_root if path_pkg else dest_root / safe_filename(pkg_title_id or fallback_folder)

        if path_pkg:
            log("    Detected path-traversal style PKG; using virtual root extraction.")

        for (name_off, name_size, item_data_off, item_data_size, flags), raw_name in zip(items, raw_names):
            if name_size <= 0 or not raw_name or raw_name in (".", ".."):
                continue

            is_dir = bool(flags & FLAG_DIR)

            if path_pkg:
                dest = resolve_path_pkg_dest(raw_name, dest_root)
                if dest is None:
                    log(f"    SKIPPED unsafe path: {raw_name}")
                    continue
            else:
                name = raw_name.replace("\\", "/").strip("/")
                if not name or "\x00" in name:
                    continue
                dest = output_root / name

            if is_dir:
                if dest.exists() and dest.is_file():
                    dest.unlink()
                dest.mkdir(parents=True, exist_ok=True)
                continue

            if dest.parent.exists() and dest.parent.is_file():
                dest.parent.unlink()

            dest.parent.mkdir(parents=True, exist_ok=True)
            written = 0
            chunk_size = 512 * 1024

            with dest.open("wb") as out:
                while written < item_data_size:
                    size = min(chunk_size, item_data_size - written)
                    data = decrypt_pkg_region(f, hdr, item_data_off + written, size)

                    if not data:
                        raise IOError("Unexpected EOF while decrypting PKG")

                    out.write(data)
                    written += len(data)

        return output_root


# -----------------------------------------------------------------------------
# EDAT -> P3T decryptor
# -----------------------------------------------------------------------------

NP_KLIC_FREE = b"\x72\xF9\x90\x78\x8F\x9C\xFF\x74\x57\x25\xF0\x8E\x4C\x12\x83\x87"
EDAT_KEY_0 = b"\xBE\x95\x9C\xA8\x30\x8D\xEF\xA2\xE5\xE1\x80\xC6\x37\x12\xA9\xAE"
EDAT_KEY_1 = b"\x4C\xA9\xC1\x4B\x01\xC9\x53\x09\x96\x9B\xEC\x68\xAA\x0B\xC0\x81"
EDAT_IV = b"\x00" * 16

EDAT_COMPRESSED_FLAG = 0x00000001
EDAT_FLAG_0x02 = 0x00000002
EDAT_ENCRYPTED_KEY_FLAG = 0x00000008
EDAT_FLAG_0x10 = 0x00000010
EDAT_FLAG_0x20 = 0x00000020
SDAT_FLAG = 0x01000000


def maybe_zlib_decompress(data: bytes) -> bytes:
    for wbits in (zlib.MAX_WBITS, -zlib.MAX_WBITS):
        try:
            return zlib.decompress(data, wbits)
        except zlib.error:
            pass

    return data


def dec_section(metadata: bytes) -> tuple[int, int, int]:
    dec = bytearray(16)

    dec[0] = metadata[0xC] ^ metadata[0x8] ^ metadata[0x10]
    dec[1] = metadata[0xD] ^ metadata[0x9] ^ metadata[0x11]
    dec[2] = metadata[0xE] ^ metadata[0xA] ^ metadata[0x12]
    dec[3] = metadata[0xF] ^ metadata[0xB] ^ metadata[0x13]

    dec[4] = metadata[0x4] ^ metadata[0x8] ^ metadata[0x14]
    dec[5] = metadata[0x5] ^ metadata[0x9] ^ metadata[0x15]
    dec[6] = metadata[0x6] ^ metadata[0xA] ^ metadata[0x16]
    dec[7] = metadata[0x7] ^ metadata[0xB] ^ metadata[0x17]

    dec[8] = metadata[0xC] ^ metadata[0x0] ^ metadata[0x18]
    dec[9] = metadata[0xD] ^ metadata[0x1] ^ metadata[0x19]
    dec[10] = metadata[0xE] ^ metadata[0x2] ^ metadata[0x1A]
    dec[11] = metadata[0xF] ^ metadata[0x3] ^ metadata[0x1B]

    dec[12] = metadata[0x4] ^ metadata[0x0] ^ metadata[0x1C]
    dec[13] = metadata[0x5] ^ metadata[0x1] ^ metadata[0x1D]
    dec[14] = metadata[0x6] ^ metadata[0x2] ^ metadata[0x1E]
    dec[15] = metadata[0x7] ^ metadata[0x3] ^ metadata[0x1F]

    offset = struct.unpack(">Q", dec[0:8])[0]
    length = struct.unpack(">I", dec[8:12])[0] & 0xFFFFFFFF
    comp_end = struct.unpack(">I", dec[12:16])[0] & 0xFFFFFFFF
    return offset, length, comp_end


def decrypt_edat_to_bytes(edat_path: Path, log: Callable[[str], None]) -> bytes:
    data = edat_path.read_bytes()

    if len(data) < 0x100 or not data.startswith(b"NPD"):
        raise ValueError("Not an NPD/EDAT file")

    version = struct.unpack(">I", data[4:8])[0]
    license_type = struct.unpack(">I", data[8:12])[0]
    content_id = data[16:64].decode("ascii", errors="ignore").strip("\x00")
    digest = data[0x40:0x50]
    dev_hash = data[0x60:0x70]
    flags = struct.unpack(">I", data[0x80:0x84])[0]
    block_size = struct.unpack(">I", data[0x84:0x88])[0]
    file_size = struct.unpack(">Q", data[0x88:0x90])[0]

    log(f"    EDAT: {edat_path.name}")
    log(f"      content_id={content_id} version={version} license={license_type} flags={flags:#x}")
    log(f"      block_size={block_size} file_size={file_size}")

    if block_size <= 0 or file_size <= 0:
        raise ValueError("Invalid EDAT block_size/file_size")

    is_compressed = bool(flags & EDAT_COMPRESSED_FLAG)
    is_payload_plain = bool(flags & EDAT_FLAG_0x02)
    has_encrypted_key = bool(flags & EDAT_ENCRYPTED_KEY_FLAG)
    has_0x10 = bool(flags & EDAT_FLAG_0x10)
    has_0x20 = bool(flags & EDAT_FLAG_0x20)
    is_sdat = bool(flags & SDAT_FLAG)

    if is_sdat:
        sdat_key = b"\x0D\x65\x5E\xF8\xE6\x74\xA9\x8A\xB8\x50\x5C\xFA\x7D\x01\x29\x33"
        key_input = bytes(a ^ b for a, b in zip(dev_hash, sdat_key))
    else:
        # Free theme EDATs normally use NP_KLIC_FREE.
        key_input = NP_KLIC_FREE

    edat_key = EDAT_KEY_1 if version == 4 else EDAT_KEY_0
    meta_size = 0x20 if (is_compressed or has_0x20) else 0x10
    metadata_offset = 0x100
    num_blocks = (file_size + block_size - 1) // block_size
    dev_hash_12 = dev_hash[:12]
    output = bytearray()

    for i in range(num_blocks):
        is_last = i == num_blocks - 1
        this_size = (file_size % block_size) if (is_last and file_size % block_size) else block_size
        pad_len = (this_size + 15) & ~15

        if is_compressed:
            meta_pos = metadata_offset + (i * meta_size)
            metadata = data[meta_pos:meta_pos + meta_size]

            if len(metadata) < meta_size:
                break

            if version <= 1:
                data_offset = struct.unpack(">Q", metadata[0x10:0x18])[0]
                chunk_len = struct.unpack(">I", metadata[0x18:0x1C])[0]
            else:
                data_offset, chunk_len, _ = dec_section(metadata)

            read_len = (int(chunk_len) + 15) & ~15
        elif has_0x20:
            data_offset = metadata_offset + i * (meta_size + block_size) + meta_size
            chunk_len = this_size
            read_len = pad_len
        else:
            data_offset = metadata_offset + num_blocks * meta_size + i * block_size
            chunk_len = this_size
            read_len = pad_len

        if data_offset + read_len > len(data):
            log(f"      stopped: block {i} outside EDAT")
            break

        enc = data[int(data_offset):int(data_offset) + int(read_len)]

        if len(enc) < read_len:
            enc += b"\x00" * (int(read_len) - len(enc))

        block_key = dev_hash_12 + struct.pack(">I", i)
        key_result = AES.new(key_input, AES.MODE_ECB).encrypt(block_key)

        if has_0x10:
            key_result = AES.new(key_input, AES.MODE_ECB).encrypt(key_result)

        if has_encrypted_key:
            key_final = AES.new(edat_key, AES.MODE_CBC, iv=EDAT_IV).decrypt(key_result)
            iv_final = digest
        else:
            key_final = key_result
            iv_final = digest if version > 1 else EDAT_IV

        if is_payload_plain:
            dec_block = enc[:int(chunk_len)]
        else:
            dec_block = AES.new(key_final, AES.MODE_CBC, iv=iv_final).decrypt(enc)[:int(chunk_len)]

        if is_compressed:
            dec_block = maybe_zlib_decompress(dec_block)

        output.extend(dec_block)

    raw = bytes(output)
    p3tf_pos = raw.find(b"P3TF")

    if p3tf_pos >= 0:
        raw = raw[p3tf_pos:]

    return raw


def find_edats(root: Path) -> list[Path]:
    result: list[Path] = []

    for dirpath, _dirs, files in os.walk(root):
        for name in files:
            if name.lower().endswith(".edat"):
                result.append(Path(dirpath) / name)

    return sorted(result)


def find_direct_p3ts(root: Path) -> list[Path]:
    result: list[Path] = []

    for dirpath, _dirs, files in os.walk(root):
        for name in files:
            path = Path(dirpath) / name
            if name.lower().endswith(".p3t"):
                result.append(path)
                continue

            try:
                with path.open("rb") as f:
                    if f.read(4) == b"P3TF":
                        result.append(path)
            except OSError:
                pass

    return sorted(result)


# -----------------------------------------------------------------------------
# Original PHP P3T extractor setup
# -----------------------------------------------------------------------------

def patch_file_text(path: Path, replacements: list[tuple[str, str]]) -> None:
    text = path.read_text(encoding="utf-8", errors="replace")
    for old, new in replacements:
        text = text.replace(old, new)
    path.write_text(text, encoding="utf-8")


def setup_php_p3t_extractor(tool_root: Path, log: Callable[[str], None]) -> Path:
    """Clone and patch hoshadiq/ps3theme-p3t-extract. Return runner path."""
    extractor_dir = tool_root / "ps3theme-p3t-extract"
    runner = tool_root / "run_p3t_extract.php"

    if not extractor_dir.exists():
        log("[TOOL] Cloning ps3theme-p3t-extract")
        subprocess.run(
            [
                "git",
                "clone",
                "--depth",
                "1",
                "https://github.com/hoshsadiq/ps3theme-p3t-extract.git",
                str(extractor_dir),
            ],
            check=True,
        )

    (extractor_dir / "autoload_colab.php").write_text(
        """<?php
spl_autoload_register(function ($class) {
    $prefix = 'P3TExtractor\\\\';
    $base_dir = __DIR__ . '/src/P3TExtractor/';

    $len = strlen($prefix);
    if (strncmp($prefix, $class, $len) !== 0) {
        return;
    }

    $relative_class = substr($class, $len);
    $file = $base_dir . str_replace('\\\\', '/', $relative_class) . '.php';

    if (file_exists($file)) {
        require $file;
    }
});
""",
        encoding="utf-8",
    )

    gim_file = extractor_dir / "src/P3TExtractor/Gim.php"
    if gim_file.exists():
        patch_file_text(
            gim_file,
            [
                ("throw new Exception(", "throw new \\Exception("),
            ],
        )

    element_file = extractor_dir / "src/P3TExtractor/Element.php"
    if element_file.exists():
        patch_file_text(
            element_file,
            [
                (
                    "substr($this->rgba, $pos, ( $pos+4 ) )",
                    "substr($this->rgba, $pos, 4)",
                ),
            ],
        )

    runner.write_text(
        """<?php
require __DIR__ . '/ps3theme-p3t-extract/autoload_colab.php';

$input = $argv[1];
$output = rtrim($argv[2], "/") . "/";

if (!is_dir($output)) {
    mkdir($output, 0777, true);
}

$p3t = new P3TExtractor\\Extractor($input, $output, true);
$p3t->parse();

// png = converted GIM files, gim = raw GIM, jpg = backgrounds, p3t = generated XML.
$p3t->dump_files("png,gim,jpg,p3t");
""",
        encoding="utf-8",
    )

    return runner


def extract_p3t_with_php(p3t_path: Path, out_dir: Path, runner: Path, log: Callable[[str], None]) -> bool:
    out_dir.mkdir(parents=True, exist_ok=True)

    result = subprocess.run(
        ["php", str(runner), str(p3t_path), str(out_dir)],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        check=False,
    )

    if result.stdout.strip():
        for line in result.stdout.splitlines():
            log(f"      {line}")

    return result.returncode == 0


# -----------------------------------------------------------------------------
# Preview selector
# -----------------------------------------------------------------------------

def get_image_size(path: Path) -> tuple[int, int]:
    if Image is None:
        return (0, 0)

    try:
        with Image.open(path) as img:
            return img.size
    except Exception:
        return (0, 0)


def score_preview_candidate(path: Path) -> int:
    name = path.name.lower()
    stem = path.stem.lower()
    path_text = path.as_posix().lower()
    width, height = get_image_size(path)
    pixels = width * height
    score = 0

    # Absolute priority: the actual P3T preview asset.
    if name == "preview.png":
        score += 1_000_000
    elif stem == "preview":
        score += 900_000
    elif "preview" in name:
        score += 800_000
    elif "preview" in path_text:
        score += 700_000

    # Avoid accidentally selecting icon.png, PS3LOGO, or notification images.
    if "icon" in name or "icon" in path_text:
        score -= 500_000
    if "logo" in name or "logo" in path_text:
        score -= 500_000
    if "notification" in name or "notification" in path_text:
        score -= 300_000

    # Fallback if filenames are weird: previews/backgrounds are usually larger/wider.
    if width and height:
        score += min(pixels, 2_000_000) // 100

        if width == height and pixels <= 512 * 512:
            score -= 100_000

        if width > height:
            score += 50_000

    return score


def select_preview_image(root: Path) -> Path | None:
    candidates: list[Path] = []

    for ext in ("*.png", "*.jpg", "*.jpeg"):
        candidates.extend(p for p in root.rglob(ext) if p.is_file())

    candidates = [p for p in candidates if not p.name.lower().startswith("best_preview")]

    if not candidates:
        return None

    candidates.sort(key=lambda p: (score_preview_candidate(p), p.stat().st_size), reverse=True)
    return candidates[0]


def save_as_png(source: Path, dest: Path) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)

    if source.suffix.lower() == ".png":
        shutil.copy2(source, dest)
        return

    if Image is None:
        raise RuntimeError("Pillow is required to convert JPG/JPEG previews to PNG")

    image = Image.open(source).convert("RGBA")
    image.save(dest, format="PNG")


# -----------------------------------------------------------------------------
# Processing
# -----------------------------------------------------------------------------

def unique_output_path(path: Path, overwrite: bool) -> Path:
    if overwrite or not path.exists():
        return path

    stem = path.stem
    suffix = path.suffix

    for i in range(2, 10_000):
        candidate = path.with_name(f"{stem}__{i}{suffix}")
        if not candidate.exists():
            return candidate

    raise RuntimeError(f"Could not create unique output path for {path}")


def process_entry(
    entry: ThemeEntry,
    download_dir: Path,
    output_root: Path,
    work_root: Path,
    tool_runner: Path,
    overwrite: bool,
    redownload: bool,
    keep_extracted: bool,
    log: Callable[[str], None],
) -> tuple[int, str]:
    fixed_title_id = safe_filename(resolved_title_id(entry.title_id, entry.content_id, entry.pkg_url))
    content_name = safe_filename(entry.content_id or Path(urllib.parse.urlparse(entry.pkg_url).path).stem, fallback="theme")
    title_folder = output_root / fixed_title_id
    expected_output = title_folder / f"{content_name}.png"

    if expected_output.exists() and not overwrite:
        log(f"[SKIP] {fixed_title_id} / {entry.content_id}: PNG already exists")
        return 0, ""

    pkg_name = safe_filename(entry.content_id or Path(urllib.parse.urlparse(entry.pkg_url).path).stem, fallback="theme") + ".pkg"
    pkg_path = download_dir / pkg_name

    if redownload and pkg_path.exists():
        pkg_path.unlink()

    if not pkg_path.exists():
        log(f"[GET ] {fixed_title_id} - {entry.name}")
        download_file(entry.pkg_url, pkg_path, log)
    else:
        log(f"[CACHE] {pkg_path.name}")

    if entry.sha256 and re.fullmatch(r"[0-9a-fA-F]{64}", entry.sha256):
        got_hash = sha256_file(pkg_path)
        if got_hash != entry.sha256.upper():
            raise ValueError(f"SHA256 mismatch for {pkg_path.name}: {got_hash} != {entry.sha256}")

    extract_base = work_root / safe_filename(entry.content_id or fixed_title_id)
    if extract_base.exists():
        shutil.rmtree(extract_base)
    extract_base.mkdir(parents=True, exist_ok=True)

    pkg_extract_dir = extract_base / "pkg"
    decrypted_dir = extract_base / "p3t"
    p3t_extract_dir = extract_base / "p3t_extract"
    decrypted_dir.mkdir(parents=True, exist_ok=True)
    p3t_extract_dir.mkdir(parents=True, exist_ok=True)

    extracted_root = extract_pkg(pkg_path, pkg_extract_dir, fallback_folder=entry.content_id or fixed_title_id, log=log)

    p3t_files: list[Path] = []

    for direct_p3t in find_direct_p3ts(extracted_root):
        out_p3t = decrypted_dir / f"{safe_filename(direct_p3t.stem)}.p3t"
        if direct_p3t.suffix.lower() == ".p3t" and direct_p3t.read_bytes()[:4] == b"P3TF":
            shutil.copy2(direct_p3t, out_p3t)
            p3t_files.append(out_p3t)

    edats = find_edats(extracted_root)
    if not edats:
        edats = find_edats(pkg_extract_dir)

    for idx, edat_path in enumerate(edats, start=1):
        try:
            raw_p3t = decrypt_edat_to_bytes(edat_path, log=log)
        except Exception as exc:
            log(f"[FAIL] EDAT decrypt failed for {edat_path.name}: {exc}")
            continue

        p3tf_pos = raw_p3t.find(b"P3TF")
        if p3tf_pos >= 0:
            raw_p3t = raw_p3t[p3tf_pos:]

        if not raw_p3t.startswith(b"P3TF"):
            log(f"[MISS] EDAT decrypted but no P3TF magic: {edat_path.name}")
            continue

        out_p3t = decrypted_dir / f"{idx:02d}_{safe_filename(edat_path.stem)}.p3t"
        out_p3t.write_bytes(raw_p3t)
        p3t_files.append(out_p3t)
        log(f"[P3T ] {out_p3t.name} ({out_p3t.stat().st_size} bytes)")

    if not p3t_files:
        if not keep_extracted:
            shutil.rmtree(extract_base, ignore_errors=True)
        return 0, "no_p3t_found"

    extracted_ok = False
    for idx, p3t_path in enumerate(p3t_files, start=1):
        out_dir = p3t_extract_dir / f"{idx:02d}_{safe_filename(p3t_path.stem)}"
        if extract_p3t_with_php(p3t_path, out_dir, tool_runner, log=log):
            extracted_ok = True

    if not extracted_ok:
        if not keep_extracted:
            shutil.rmtree(extract_base, ignore_errors=True)
        return 0, "p3t_extractor_failed"

    selected = select_preview_image(p3t_extract_dir)
    if not selected:
        if not keep_extracted:
            shutil.rmtree(extract_base, ignore_errors=True)
        return 0, "no_preview_image_extracted"

    final_out = unique_output_path(expected_output, overwrite=overwrite)
    save_as_png(selected, final_out)
    log(f"[SAVE] {final_out.relative_to(output_root)} <- {selected.name}")

    if not keep_extracted:
        shutil.rmtree(extract_base, ignore_errors=True)

    return 1, ""


# -----------------------------------------------------------------------------
# CLI
# -----------------------------------------------------------------------------

def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Extract PS3 theme preview PNGs from themes.tsv PKG entries.")
    parser.add_argument("--repo", type=Path, default=Path.cwd(), help="PSN-Content repository root. Default: current folder")
    parser.add_argument("--tsv", type=Path, action="append", help="Custom theme TSV path. Can be used more than once.")
    parser.add_argument(
        "--source",
        choices=["official", "pending", "all"],
        default="official",
        help="Theme TSV source to process when --tsv is not used. Default: official.",
    )
    parser.add_argument(
        "--include-pending",
        action="store_true",
        help="Backward-compatible alias that makes --source behave as all.",
    )
    parser.add_argument("--title-id", action="append", help="Only process this Title ID/game ID. Can be used more than once.")
    parser.add_argument("--content-id", action="append", help="Only process this Content ID. Can be used more than once.")
    parser.add_argument("--limit", type=int, default=0, help="Limit how many entries are processed. 0 means no limit.")
    parser.add_argument("--start", type=int, default=0, help="Skip the first N matching entries.")
    parser.add_argument("--overwrite", action="store_true", help="Overwrite existing PNG files.")
    parser.add_argument("--redownload", action="store_true", help="Download PKGs again even when they already exist in cache.")
    parser.add_argument("--keep-extracted", action="store_true", help="Keep temporary extracted PKG/P3T files.")
    parser.add_argument("--download-dir", type=Path, help="PKG cache folder. Default: <repo>/.cache/theme_pkgs")
    parser.add_argument("--output", type=Path, help="Output folder. Default: <repo>/resources/database/themes")
    parser.add_argument("--tool-dir", type=Path, help="Tool cache folder. Default: <repo>/.cache/theme_tools")
    return parser


def main() -> int:
    args = build_arg_parser().parse_args()
    repo = args.repo.resolve()

    if args.tsv:
        tsv_paths = args.tsv
    else:
        source = "all" if args.include_pending else args.source
        tsv_paths = []
        official_tsv = repo / "resources/database/content/official/themes.tsv"
        pending_tsv = repo / "resources/database/content/official/pending/themes.tsv"

        if source in ("official", "all"):
            tsv_paths.append(official_tsv)

        if source in ("pending", "all"):
            tsv_paths.append(pending_tsv)

    output_root = (args.output or repo / "resources/database/themes").resolve()
    download_dir = (args.download_dir or repo / ".cache/theme_pkgs").resolve()
    tool_dir = (args.tool_dir or repo / ".cache/theme_tools").resolve()

    print(f"Repo: {repo}")
    print(f"Output: {output_root}")
    print(f"Temporary PKG folder: {download_dir}")
    print(f"Tool folder: {tool_dir}")
    print("TSV:")
    for path in tsv_paths:
        print(f"  - {path}")

    entries = read_all_entries(tsv_paths)

    title_filter = {x.upper() for x in args.title_id or []}
    content_filter = {x.upper() for x in args.content_id or []}

    if title_filter:
        entries = [
            e for e in entries
            if e.title_id.upper() in title_filter
            or resolved_title_id(e.title_id, e.content_id, e.pkg_url).upper() in title_filter
        ]

    if content_filter:
        entries = [e for e in entries if e.content_id.upper() in content_filter]

    if args.start > 0:
        entries = entries[args.start:]

    if args.limit > 0:
        entries = entries[: args.limit]

    print(f"Entries to process: {len(entries)}")
    if not entries:
        return 0

    output_root.mkdir(parents=True, exist_ok=True)
    download_dir.mkdir(parents=True, exist_ok=True)
    tool_dir.mkdir(parents=True, exist_ok=True)

    try:
        tool_runner = setup_php_p3t_extractor(tool_dir, log=print)
    except Exception as exc:
        print(f"[ERROR] Could not set up P3T extractor: {exc}")
        return 1

    if args.keep_extracted:
        work_context = None
        work_root = repo / ".cache/theme_extract"
        work_root.mkdir(parents=True, exist_ok=True)
    else:
        work_context = tempfile.TemporaryDirectory(prefix="theme_extract_")
        work_root = Path(work_context.name)

    total_saved = 0
    total_errors = 0
    failed_report_rows: list[list[str]] = []

    try:
        for number, entry in enumerate(entries, start=1):
            fixed_title_id = resolved_title_id(entry.title_id, entry.content_id, entry.pkg_url)
            print(f"\n[{number}/{len(entries)}] {fixed_title_id} | {entry.content_id} | {entry.name}")

            try:
                saved, reason = process_entry(
                    entry=entry,
                    download_dir=download_dir,
                    output_root=output_root,
                    work_root=work_root,
                    tool_runner=tool_runner,
                    overwrite=args.overwrite,
                    redownload=args.redownload,
                    keep_extracted=args.keep_extracted,
                    log=print,
                )
                total_saved += saved

                if reason:
                    failed_report_rows.append([
                        "theme_preview_failed",
                        fixed_title_id,
                        entry.content_id,
                        entry.name,
                        entry.pkg_url,
                        reason,
                    ])
            except KeyboardInterrupt:
                raise
            except Exception as exc:
                print(f"[ERROR] {entry.content_id or entry.pkg_url}: {exc}")
                total_errors += 1
                failed_report_rows.append([
                    "entry_error",
                    fixed_title_id,
                    entry.content_id,
                    entry.name,
                    entry.pkg_url,
                    str(exc),
                ])
    finally:
        if work_context is not None:
            work_context.cleanup()

    print("\nDone.")
    print(f"Saved PNGs: {total_saved}")
    print(f"Entry errors: {total_errors}")
    print(f"Failures: {len(failed_report_rows)}")

    if failed_report_rows:
        report_path = output_root / "_failed_theme_previews.tsv"
        with report_path.open("w", encoding="utf-8", newline="") as handle:
            handle.write("type\ttitle_id\tcontent_id\tname\tpkg_url\treason\n")
            for row in failed_report_rows:
                safe_row = [str(cell).replace("\t", " ").replace("\r", " ").replace("\n", " ") for cell in row]
                handle.write("\t".join(safe_row) + "\n")
        print(f"Failure report: {report_path}")

    if total_errors:
        print("WARNING: Some theme entries had errors, but generated PNGs will still be committed.")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
