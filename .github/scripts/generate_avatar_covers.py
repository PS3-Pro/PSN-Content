#!/usr/bin/env python3
"""
Extract PSN avatar PNGs from the avatar TSV and save them as:

    resources/database/avatars/<TITLE_ID>/<CONTENT_ID>.png

The TITLE_ID folder comes from the avatar list, not from PARAM.SFO.

Dependencies:
    pip install pycryptodomex

Optional, but recommended:
    Run from the repository root:
        python .github/scripts/generate_avatar_covers.py --repo .
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import re
import shutil
import struct
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


# -----------------------------------------------------------------------------
# PKG extractor core, based on your PKG extraction script.
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


def get_debug_keystream_block(qa_digest: bytes, block_index: int) -> bytes:
    qa_0 = qa_digest[0:8]
    qa_1 = qa_digest[8:16]
    buffer = bytearray(64)
    buffer[0:8] = qa_0
    buffer[8:16] = qa_0
    buffer[16:24] = qa_1
    buffer[24:32] = qa_1
    buffer[56:64] = struct.pack(">Q", block_index)
    return hashlib.sha1(buffer, usedforsecurity=False).digest()[:16]


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
            chunk = enc[offset : offset + 16]
            keystream = get_debug_keystream_block(qa_digest, block_index + i)
            plaintext.extend(a ^ b for a, b in zip(chunk, keystream))
        return bytes(plaintext)[prefix_len : prefix_len + size]

    iv = hdr["iv"]
    iv_int = int.from_bytes(iv, "big")
    ctr_val = (iv_int + block_start // 16) & ((1 << 128) - 1)
    ctr = Counter.new(128, initial_value=ctr_val)
    cipher = AES.new(PS3_NPDRM_KEY, AES.MODE_CTR, counter=ctr)
    plaintext = cipher.decrypt(enc)
    return plaintext[prefix_len : prefix_len + size]


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
        raw = data[val_start : val_start + size]
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

        log(f"    CONTENT-ID: {hdr['content_id']}")
        log(f"    ITEMS: {item_count}")

        table_raw = decrypt_pkg_region(f, hdr, 0, item_count * ITEM_RECORD_SIZE)
        items = []
        for i in range(item_count):
            rec = table_raw[i * ITEM_RECORD_SIZE : (i + 1) * ITEM_RECORD_SIZE]
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
        if path_pkg:
            log("    Detected path-traversal style PKG; using virtual root extraction.")
            output_root = dest_root
        else:
            # Avatars may not have a usable PARAM.SFO, so never depend on it.
            output_root = dest_root / safe_filename(pkg_title_id or fallback_folder)

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
# EDAT/UNEDAT PNG extractor core, based on your EDAT avatar script.
# -----------------------------------------------------------------------------

NP_KLIC_FREE = b"\x72\xF9\x90\x78\x8F\x9C\xFF\x74\x57\x25\xF0\x8E\x4C\x12\x83\x87"
EDAT_KEY_0 = b"\xBE\x95\x9C\xA8\x30\x8D\xEF\xA2\xE5\xE1\x80\xC6\x37\x12\xA9\xAE"
EDAT_KEY_1 = b"\x4C\xA9\xC1\x4B\x01\xC9\x53\x09\x96\x9B\xEC\x68\xAA\x0B\xC0\x81"
EDAT_IV = b"\x00" * 16


class PS3Crypto:
    @staticmethod
    def rap_to_rif(rap_data: bytes) -> bytes | None:
        rap_key = b"\x86\x9F\x77\x45\xC1\x3F\xD8\x90\xCC\xF2\x91\x88\xE3\xCC\x3E\xDF"
        rap_pbox = [0x0C, 0x03, 0x06, 0x04, 0x01, 0x0B, 0x0F, 0x08, 0x02, 0x07, 0x00, 0x05, 0x0A, 0x0E, 0x0D, 0x09]
        rap_e1 = [0xA9, 0x3E, 0x1F, 0xD6, 0x7C, 0x55, 0xA3, 0x29, 0xB7, 0x5F, 0xDD, 0xA6, 0x2A, 0x95, 0xC7, 0xA5]
        rap_e2 = [0x67, 0xD4, 0x5D, 0xA3, 0x29, 0x6D, 0x00, 0x6A, 0x4E, 0x7C, 0x53, 0x7B, 0xF5, 0x53, 0x8C, 0x74]

        if len(rap_data) < 16:
            return None

        key = bytearray(16)
        iv = bytearray(16)
        cipher = AES.new(rap_key, AES.MODE_CBC, iv)
        key[:] = cipher.decrypt(rap_data[:16])

        for _round_num in range(5):
            for i in range(16):
                p = rap_pbox[i]
                key[p] ^= rap_e1[p]

            for i in range(15, 0, -1):
                p = rap_pbox[i]
                pp = rap_pbox[i - 1]
                key[p] ^= key[pp]

            carry = 0
            for i in range(16):
                p = rap_pbox[i]
                kc = key[p] - carry
                ec2 = rap_e2[p]
                if carry != 1 or kc != 0xFF:
                    carry = 1 if kc < ec2 else 0
                    key[p] = (kc - ec2) & 0xFF
                else:
                    key[p] = kc & 0xFF

        return bytes(key)


class PS3EdatDecryptor:
    def __init__(self, filepath: Path, klic_map: dict[str, bytes]):
        self.filepath = filepath
        self.klic_map = klic_map

    @staticmethod
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

    @staticmethod
    def get_block_key(block: int, dev_hash: bytes, version: int) -> bytes:
        key = bytearray(16)
        if version <= 1:
            key[:12] = b"\x00" * 12
        else:
            key[:12] = dev_hash[:12]
        key[12:] = struct.pack(">I", block)
        return bytes(key)

    def decrypt_to_png(self) -> tuple[bytes | None, str | None]:
        data = self.filepath.read_bytes()

        if len(data) < 4 or not data.startswith(b"NPD"):
            return self.extract_png_from_buffer(data), None

        version = struct.unpack(">I", data[4:8])[0]
        license_type = struct.unpack(">I", data[8:12])[0]
        content_id = data[16:64].decode("ascii", errors="ignore").strip("\x00")
        digest = data[64:80]
        dev_hash = data[96:112]

        flags = struct.unpack(">I", data[128:132])[0]
        block_size = struct.unpack(">I", data[132:136])[0]
        file_size = struct.unpack(">Q", data[136:144])[0]

        edat_compressed_flag = 0x00000001
        edat_flag_0x02 = 0x00000002
        edat_encrypted_key_flag = 0x00000008
        edat_flag_0x20 = 0x00000020
        sdat_flag = 0x01000000

        is_compressed = bool(flags & edat_compressed_flag)
        is_payload_encrypted = not bool(flags & edat_flag_0x02)
        is_encrypted_key = bool(flags & edat_encrypted_key_flag)
        has_0x20_flag = bool(flags & edat_flag_0x20)
        is_sdat = bool(flags & sdat_flag)

        if is_sdat:
            sdat_key = b"\x0D\x65\x5E\xF8\xE6\x74\xA9\x8A\xB8\x50\x5C\xFA\x7D\x01\x29\x33"
            key_input = bytes(a ^ b for a, b in zip(dev_hash, sdat_key))
        else:
            license_mask = license_type & 0x3
            if license_mask == 0x3:
                key_input = self.klic_map.get(content_id, NP_KLIC_FREE)
            else:
                rap_data = self.klic_map.get(content_id)
                key_input = PS3Crypto.rap_to_rif(rap_data) if rap_data else NP_KLIC_FREE
                if key_input is None:
                    key_input = NP_KLIC_FREE

        if block_size <= 0:
            return None, content_id

        num_blocks = (file_size + block_size - 1) // block_size
        meta_size = 32 if (is_compressed or has_0x20_flag) else 16
        metadata_offset = 0x100

        png_data = bytearray()
        found_png = False
        png_start_markers = [b"\x89PNG\r\n\x1a\n", b"PSNA", b"IHDR"]

        for i in range(num_blocks):
            if is_compressed:
                meta_pos = metadata_offset + (i * meta_size)
                if meta_pos + meta_size > len(data):
                    break
                metadata = data[meta_pos : meta_pos + meta_size]
                if version <= 1:
                    data_offset = struct.unpack(">Q", metadata[0x10:0x18])[0]
                    chunk_len = struct.unpack(">I", metadata[0x18:0x1C])[0]
                else:
                    data_offset, chunk_len, _ = self.dec_section(metadata)
            elif has_0x20_flag:
                meta_pos = metadata_offset + (i * (meta_size + block_size))
                data_offset = meta_pos + meta_size
                chunk_len = min(block_size, file_size - (i * block_size))
            else:
                data_offset = metadata_offset + (num_blocks * meta_size) + (i * block_size)
                chunk_len = min(block_size, file_size - (i * block_size))

            b_key_input = self.get_block_key(i, dev_hash, version)
            key_result = AES.new(key_input, AES.MODE_ECB).encrypt(b_key_input)

            if is_encrypted_key:
                edat_key = EDAT_KEY_1 if version == 4 else EDAT_KEY_0
                cipher_key = AES.new(edat_key, AES.MODE_CBC, iv=EDAT_IV)
                key_final = cipher_key.decrypt(key_result)
            else:
                key_final = key_result

            read_len = (int(chunk_len) + 15) & ~15
            if data_offset + read_len > len(data):
                break

            block_enc = data[data_offset : data_offset + read_len]
            if is_payload_encrypted:
                iv_payload = b"\x00" * 16 if version <= 1 else digest
                cipher_payload = AES.new(key_final, AES.MODE_CBC, iv=iv_payload)
                dec_block = cipher_payload.decrypt(block_enc)[:chunk_len]
            else:
                dec_block = block_enc[:chunk_len]

            if is_compressed:
                dec_block = maybe_zlib_decompress(dec_block)

            if not found_png:
                for marker in png_start_markers:
                    pos = dec_block.find(marker)
                    if pos != -1:
                        png_data.extend(dec_block[pos:])
                        found_png = True
                        break
            else:
                png_data.extend(dec_block)

        if png_data:
            return self.extract_png_from_buffer(bytes(png_data)), content_id
        return None, content_id

    @staticmethod
    def extract_png_from_buffer(buffer: bytes) -> bytes | None:
        png_header = b"\x89PNG\r\n\x1a\n"
        if buffer.startswith(png_header):
            end = buffer.find(b"IEND")
            return buffer[: end + 8] if end != -1 else buffer

        ihdr_pos = buffer.find(b"IHDR")
        if ihdr_pos != -1:
            reconstructed = png_header + b"\x00\x00\x00\r" + buffer[ihdr_pos:]
            end = reconstructed.find(b"IEND")
            return reconstructed[: end + 8] if end != -1 else reconstructed
        return None


def maybe_zlib_decompress(data: bytes) -> bytes:
    # Some EDATs flagged as compressed are raw zlib streams; others are already usable.
    for wbits in (zlib.MAX_WBITS, -zlib.MAX_WBITS):
        try:
            return zlib.decompress(data, wbits)
        except zlib.error:
            pass
    return data


# -----------------------------------------------------------------------------
# Secondary EDAT image extractor, based on the standalone avatar EDAT script.
# -----------------------------------------------------------------------------

EDAT_FLAG_0x10 = 0x00000010


def find_png_end(data: bytes, start: int) -> int:
    """Find the real PNG end by walking chunks."""
    i = start + 8
    while i + 12 <= len(data):
        chunk_len = struct.unpack(">I", data[i:i + 4])[0]
        chunk_type = data[i + 4:i + 8]
        i += 4 + 4 + chunk_len + 4
        if chunk_type == b"IEND":
            return i
    return -1


def extract_image_from_buffer(buffer: bytes) -> tuple[bytes | None, str | None]:
    png_header = b"\x89PNG\r\n\x1a\n"

    png_off = buffer.find(png_header)
    if png_off >= 0:
        png_end = find_png_end(buffer, png_off)
        return (buffer[png_off:png_end] if png_end > 0 else buffer[png_off:], "png")

    ihdr_off = buffer.find(b"IHDR")
    if ihdr_off >= 0:
        reconstructed = png_header + b"\x00\x00\x00\r" + buffer[ihdr_off:]
        png_end = find_png_end(reconstructed, 0)
        return (reconstructed[:png_end] if png_end > 0 else reconstructed, "png")

    jpg_off = buffer.find(b"\xff\xd8\xff")
    if jpg_off >= 0:
        jpg = buffer[jpg_off:]
        jpg_end = jpg.find(b"\xff\xd9")
        if jpg_end >= 0:
            jpg = jpg[:jpg_end + 2]
        return jpg, "jpg"

    return None, None


def convert_jpg_to_png_bytes(jpg_data: bytes, log: Callable[[str], None]) -> bytes | None:
    try:
        from PIL import Image
        import io

        image = Image.open(io.BytesIO(jpg_data))
        output = io.BytesIO()
        image.save(output, format="PNG")
        return output.getvalue()
    except Exception as exc:
        log(f"[ALT ] JPG found but PNG conversion failed: {exc}")
        return None


def decrypt_edat_image_secondary(edat_path: Path, log: Callable[[str], None]) -> tuple[bytes | None, str | None, str | None]:
    """Try KLIC_FREE + FLAG_0x10, based on the shared standalone extractor."""
    data = edat_path.read_bytes()

    if len(data) < 0x100 or not data.startswith(b"NPD"):
        image, image_type = extract_image_from_buffer(data)
        if image_type == "jpg" and image:
            png = convert_jpg_to_png_bytes(image, log)
            return png, None, "jpg" if png else None
        return image, None, image_type

    version = struct.unpack(">I", data[4:8])[0]
    license_type = struct.unpack(">I", data[8:12])[0]
    content_id = data[16:64].decode("ascii", errors="ignore").strip("\x00")
    digest = data[0x40:0x50]
    dev_hash = data[0x60:0x70]
    flags = struct.unpack(">I", data[0x80:0x84])[0]
    block_size = struct.unpack(">I", data[0x84:0x88])[0]
    file_size = struct.unpack(">Q", data[0x88:0x90])[0]

    if not block_size or not file_size:
        return None, content_id, None

    license_mask = license_type & 0x3
    if license_mask not in (0x2, 0x3):
        return None, content_id, None

    edat_compressed_flag = 0x00000001
    edat_flag_0x02 = 0x00000002
    edat_encrypted_key_flag = 0x00000008
    edat_flag_0x20 = 0x00000020

    is_compressed = bool(flags & edat_compressed_flag)
    has_0x20_flag = bool(flags & edat_flag_0x20)
    has_encrypted_key = bool(flags & edat_encrypted_key_flag)
    has_0x10 = bool(flags & EDAT_FLAG_0x10)
    has_0x02 = bool(flags & edat_flag_0x02)

    key_input = NP_KLIC_FREE
    edat_key = EDAT_KEY_1 if version == 4 else EDAT_KEY_0
    meta_size = 0x20 if (is_compressed or has_0x20_flag) else 0x10
    metadata_offset = 0x100
    dev_hash_12 = dev_hash[:12]
    num_blocks = (file_size + block_size - 1) // block_size

    output = bytearray()

    for i in range(num_blocks):
        is_last = i == num_blocks - 1
        this_size = (file_size % block_size) if (is_last and file_size % block_size) else block_size
        pad_len = (this_size + 15) & ~15

        if is_compressed:
            meta_pos = metadata_offset + (i * meta_size)
            if meta_pos + meta_size > len(data):
                break
            metadata = data[meta_pos:meta_pos + meta_size]
            if version <= 1:
                data_offset = struct.unpack(">Q", metadata[0x10:0x18])[0]
                chunk_len = struct.unpack(">I", metadata[0x18:0x1C])[0]
            else:
                data_offset, chunk_len, _ = PS3EdatDecryptor.dec_section(metadata)
            read_len = (int(chunk_len) + 15) & ~15
        elif has_0x20_flag:
            data_offset = metadata_offset + i * (meta_size + block_size) + meta_size
            chunk_len = this_size
            read_len = pad_len
        else:
            data_offset = metadata_offset + i * block_size + num_blocks * meta_size
            chunk_len = this_size
            read_len = pad_len

        if data_offset + read_len > len(data):
            break

        enc = data[int(data_offset):int(data_offset) + read_len]
        if len(enc) < read_len:
            enc += b"\x00" * (read_len - len(enc))

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

        if has_0x02:
            dec_block = enc[:int(chunk_len)]
        else:
            dec_block = AES.new(key_final, AES.MODE_CBC, iv=iv_final).decrypt(enc)[:int(chunk_len)]

        if is_compressed:
            dec_block = maybe_zlib_decompress(dec_block)

        output.extend(dec_block)

    raw = bytes(output[:file_size])
    image, image_type = extract_image_from_buffer(raw)

    if not image:
        return None, content_id, None

    if image_type == "jpg":
        png = convert_jpg_to_png_bytes(image, log)
        return png, content_id, "jpg" if png else None

    return image, content_id, image_type



# -----------------------------------------------------------------------------
# Enhanced EDAT image extractor.
#
# This keeps the old primary/secondary extractors intact, but adds the same
# multi-key path used by the theme preview extractor:
#   - RAP -> RIF
#   - RAP -> RIF encrypted with dev_hash
#   - raw RAP as KLIC
#   - raw RAP encrypted with dev_hash
#   - NP_KLIC_FREE fallback
# It also tries both 0x20 layouts and digest/zero IV variants. This helps EDATs
# that have license type 2 and flags such as 0x3c.
# -----------------------------------------------------------------------------

EDAT_FLAG_COMPRESSED = 0x00000001
EDAT_FLAG_PLAIN_PAYLOAD = 0x00000002
EDAT_FLAG_ENCRYPTED_KEY = 0x00000008
EDAT_FLAG_0x20 = 0x00000020
EDAT_FLAG_SDAT = 0x01000000


def _dedupe_key_candidates(candidates: list[tuple[str, bytes | None]]) -> list[tuple[str, bytes]]:
    out: list[tuple[str, bytes]] = []
    seen: set[bytes] = set()

    for label, key in candidates:
        if not key or len(key) != 16:
            continue
        if key in seen:
            continue
        seen.add(key)
        out.append((label, key))

    return out


def build_enhanced_key_candidates(rap_hex: str, dev_hash: bytes) -> list[tuple[str, bytes]]:
    candidates: list[tuple[str, bytes | None]] = []
    rap = rap_hex_to_bytes(rap_hex)

    if rap:
        rif = PS3Crypto.rap_to_rif(rap)

        if rif:
            candidates.append(("rap_to_rif", rif))
            try:
                candidates.append(("rap_to_rif_x_dev_hash", AES.new(rif, AES.MODE_ECB).encrypt(dev_hash)))
            except Exception:
                pass

        candidates.append(("rap_raw_klic", rap))
        try:
            candidates.append(("rap_raw_x_dev_hash", AES.new(rap, AES.MODE_ECB).encrypt(dev_hash)))
        except Exception:
            pass

    candidates.append(("np_klic_free", NP_KLIC_FREE))
    return _dedupe_key_candidates(candidates)


def decrypt_edat_payload_variant(
    data: bytes,
    key_input: bytes,
    layout_mode: str,
    flag10_mode: str,
    iv_mode: str,
) -> tuple[bytes, str | None]:
    version = struct.unpack(">I", data[4:8])[0]
    content_id = data[16:64].decode("ascii", errors="ignore").strip("\x00")
    digest = data[0x40:0x50]
    dev_hash = data[0x60:0x70]
    flags = struct.unpack(">I", data[0x80:0x84])[0]
    block_size = struct.unpack(">I", data[0x84:0x88])[0]
    file_size = struct.unpack(">Q", data[0x88:0x90])[0]

    if block_size <= 0 or file_size <= 0:
        raise ValueError("Invalid EDAT block_size/file_size")

    is_compressed = bool(flags & EDAT_FLAG_COMPRESSED)
    is_payload_plain = bool(flags & EDAT_FLAG_PLAIN_PAYLOAD)
    has_encrypted_key = bool(flags & EDAT_FLAG_ENCRYPTED_KEY)
    has_0x10 = bool(flags & EDAT_FLAG_0x10)
    has_0x20 = bool(flags & EDAT_FLAG_0x20)
    is_sdat = bool(flags & EDAT_FLAG_SDAT)

    if is_sdat:
        sdat_key = b"\x0D\x65\x5E\xF8\xE6\x74\xA9\x8A\xB8\x50\x5C\xFA\x7D\x01\x29\x33"
        key_input = bytes(a ^ b for a, b in zip(dev_hash, sdat_key))

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
                data_offset, chunk_len, _ = PS3EdatDecryptor.dec_section(metadata)

            read_len = (int(chunk_len) + 15) & ~15

        elif has_0x20 and layout_mode == "inline_20":
            data_offset = metadata_offset + i * (meta_size + block_size) + meta_size
            chunk_len = this_size
            read_len = pad_len

        elif has_0x20 and layout_mode == "table_20":
            data_offset = metadata_offset + num_blocks * meta_size + i * block_size
            chunk_len = this_size
            read_len = pad_len

        else:
            data_offset = metadata_offset + num_blocks * meta_size + i * block_size
            chunk_len = this_size
            read_len = pad_len

        if data_offset + read_len > len(data):
            break

        enc = data[int(data_offset):int(data_offset) + int(read_len)]
        if len(enc) < read_len:
            enc += b"\x00" * (int(read_len) - len(enc))

        block_key = dev_hash_12 + struct.pack(">I", i)
        key_result = AES.new(key_input, AES.MODE_ECB).encrypt(block_key)

        if has_0x10 and flag10_mode == "respect_0x10":
            key_result = AES.new(key_input, AES.MODE_ECB).encrypt(key_result)

        if has_encrypted_key:
            key_final = AES.new(edat_key, AES.MODE_CBC, iv=EDAT_IV).decrypt(key_result)
        else:
            key_final = key_result

        if iv_mode == "digest":
            iv_final = digest if version > 1 else EDAT_IV
        else:
            iv_final = EDAT_IV

        if is_payload_plain:
            dec_block = enc[:int(chunk_len)]
        else:
            dec_block = AES.new(key_final, AES.MODE_CBC, iv=iv_final).decrypt(enc)[:int(chunk_len)]

        if is_compressed:
            dec_block = maybe_zlib_decompress(dec_block)

        output.extend(dec_block)

    return bytes(output[:file_size]), content_id or None


def decrypt_edat_image_enhanced(
    edat_path: Path,
    rap_hex: str,
    log: Callable[[str], None],
) -> tuple[bytes | None, str | None, str | None]:
    data = edat_path.read_bytes()

    if len(data) < 0x100 or not data.startswith(b"NPD"):
        image, image_type = extract_image_from_buffer(data)
        if image_type == "jpg" and image:
            png = convert_jpg_to_png_bytes(image, log)
            return png, None, "raw_jpg" if png else None
        return image, None, "raw_png" if image else None

    version = struct.unpack(">I", data[4:8])[0]
    license_type = struct.unpack(">I", data[8:12])[0]
    content_id = data[16:64].decode("ascii", errors="ignore").strip("\x00")
    dev_hash = data[0x60:0x70]
    flags = struct.unpack(">I", data[0x80:0x84])[0]
    block_size = struct.unpack(">I", data[0x84:0x88])[0]
    file_size = struct.unpack(">Q", data[0x88:0x90])[0]

    if not block_size or not file_size:
        return None, content_id, None

    has_0x20 = bool(flags & EDAT_FLAG_0x20)
    layout_modes = ["inline_20", "table_20"] if has_0x20 else ["normal"]
    flag10_modes = ["respect_0x10", "ignore_0x10"]
    iv_modes = ["digest", "zero"]
    key_candidates = build_enhanced_key_candidates(rap_hex, dev_hash)
    debug = os.environ.get("AVATAR_EDAT_DEBUG", "").lower() in ("1", "true", "yes")

    if debug:
        log(
            f"[ENH ] {edat_path.name}: version={version} license={license_type} "
            f"flags={flags:#x} block={block_size} file={file_size} candidates={len(key_candidates)}"
        )

    for key_label, key_input in key_candidates:
        for layout_mode in layout_modes:
            for flag10_mode in flag10_modes:
                for iv_mode in iv_modes:
                    attempt_label = f"{key_label}|{layout_mode}|{flag10_mode}|iv_{iv_mode}"
                    try:
                        raw, edat_content_id = decrypt_edat_payload_variant(
                            data=data,
                            key_input=key_input,
                            layout_mode=layout_mode,
                            flag10_mode=flag10_mode,
                            iv_mode=iv_mode,
                        )
                    except Exception as exc:
                        if debug:
                            log(f"[ENH ] {edat_path.name}: {attempt_label} error: {exc}")
                        continue

                    image, image_type = extract_image_from_buffer(raw)
                    if not image:
                        if debug:
                            log(f"[ENH ] {edat_path.name}: {attempt_label} no image first16={raw[:16].hex()}")
                        continue

                    if image_type == "jpg":
                        png = convert_jpg_to_png_bytes(image, log)
                        if not png:
                            continue
                        image = png

                    log(f"[ENH ] Enhanced extractor recovered {edat_path.name} ({attempt_label}, {image_type})")
                    return image, edat_content_id or content_id, attempt_label

    return None, content_id, None


# -----------------------------------------------------------------------------
# Avatar list handling and automation.
# -----------------------------------------------------------------------------

@dataclass(frozen=True)
class AvatarEntry:
    title_id: str
    region: str
    name: str
    pkg_url: str
    rap_hex: str
    content_id: str
    sha256: str = ""


COLUMN_ALIASES = {
    "title_id": ["Title ID", "TitleID", "TITLE_ID", "id"],
    "region": ["Region", "REGION"],
    "name": ["Name", "Title", "NAME"],
    "pkg_url": ["PKG direct link", "PKG", "URL", "pkgUrl"],
    "rap_hex": ["RAP", "rap"],
    "content_id": ["Content ID", "ContentID", "CONTENT_ID", "contentId"],
    "sha256": ["SHA256", "sha256", "SHA-256"],
}


def get_col(row: dict[str, str], key: str) -> str:
    for name in COLUMN_ALIASES[key]:
        if name in row:
            return (row.get(name) or "").strip()
    return ""


def read_avatar_tsv(path: Path) -> list[AvatarEntry]:
    entries: list[AvatarEntry] = []
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f, delimiter="\t")
        for row in reader:
            title_id = get_col(row, "title_id").upper()
            pkg_url = get_col(row, "pkg_url")
            content_id = get_col(row, "content_id")
            if not title_id or not pkg_url or pkg_url.upper() == "MISSING":
                continue
            entries.append(
                AvatarEntry(
                    title_id=title_id,
                    region=get_col(row, "region"),
                    name=get_col(row, "name"),
                    pkg_url=pkg_url,
                    rap_hex=get_col(row, "rap_hex"),
                    content_id=content_id,
                    sha256=get_col(row, "sha256"),
                )
            )
    return entries


def read_all_entries(paths: Iterable[Path]) -> list[AvatarEntry]:
    entries: list[AvatarEntry] = []
    seen: set[tuple[str, str, str]] = set()
    for path in paths:
        if not path.exists():
            continue
        for entry in read_avatar_tsv(path):
            key = (entry.title_id, entry.content_id, entry.pkg_url)
            if key in seen:
                continue
            seen.add(key)
            entries.append(entry)
    return entries


def rap_hex_to_bytes(rap_hex: str) -> bytes | None:
    rap_hex = rap_hex.strip()
    if not rap_hex or rap_hex.upper() == "MISSING":
        return None
    if not re.fullmatch(r"[0-9a-fA-F]{32}", rap_hex):
        return None
    return bytes.fromhex(rap_hex)


def safe_filename(value: str | None, fallback: str = "unknown") -> str:
    value = (value or fallback).strip()
    value = re.sub(r"[^A-Za-z0-9._-]+", "_", value)
    value = value.strip("._-")
    return value or fallback


def download_file(url: str, dest: Path, log: Callable[[str], None]) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_suffix(dest.suffix + ".part")
    request = urllib.request.Request(url, headers={"User-Agent": "PSN-Content-AvatarExtractor/1.0"})
    with urllib.request.urlopen(request, timeout=60) as response, tmp.open("wb") as out:
        total = int(response.headers.get("Content-Length") or 0)
        downloaded = 0
        while True:
            chunk = response.read(512 * 1024)
            if not chunk:
                break
            out.write(chunk)
            downloaded += len(chunk)
        if total:
            log(f"    Downloaded {downloaded / 1024:.1f} KB / {total / 1024:.1f} KB")
        else:
            log(f"    Downloaded {downloaded / 1024:.1f} KB")
    tmp.replace(dest)


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest().upper()


def find_edats(root: Path) -> list[Path]:
    result: list[Path] = []
    for dirpath, _dirs, files in os.walk(root):
        for name in files:
            if name.lower().endswith((".edat", ".unedat")):
                result.append(Path(dirpath) / name)
    return sorted(result)


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


# -----------------------------------------------------------------------------
# External PNG fallback repositories.
# -----------------------------------------------------------------------------

EXTERNAL_AVATAR_REPOS = [
    {
        "label": "MrJasonDEX/ModdingShop",
        "repo": "MrJasonDEX/ModdingShop",
        "branch": "main",
        "png_prefix": "Avaters/PNG_files",
        "list_path": "Avaters/png_file_list.txt",
        "folder_url": "https://github.com/MrJasonDEX/ModdingShop/tree/main/Avaters/PNG_files",
    },

    # Disabled for now:
    # This repository can cause wrong/default-looking matches for some avatars.
    #
    # {
    #     "label": "lI-Isekai-Il/PS3-Avatars-Edat",
    #     "repo": "lI-Isekai-Il/PS3-Avatars-Edat",
    #     "branch": "main",
    #     "png_prefix": "PS3 Avatars Packs",
    #     "list_path": "",
    #     "folder_url": "https://github.com/lI-Isekai-Il/PS3-Avatars-Edat/tree/main/PS3%20Avatars%20Packs",
    # },
]

_EXTERNAL_FILE_LIST_CACHE: dict[str, list[str]] = {}


def normalize_lookup_text(value: str) -> str:
    return re.sub(r"[^A-Z0-9]+", "", (value or "").upper())


def github_raw_url(repo: str, branch: str, path: str) -> str:
    return (
        f"https://raw.githubusercontent.com/{repo}/"
        f"{urllib.parse.quote(branch, safe='')}/"
        f"{urllib.parse.quote(path, safe='/')}"
    )


def download_text(url: str) -> str:
    headers = {
        "User-Agent": "PS3-Pro-Avatar-Cover-Generator",
        "Accept": "text/plain,*/*",
    }

    token = os.environ.get("GITHUB_TOKEN")
    if token and "api.github.com" in url:
        headers["Authorization"] = f"Bearer {token}"

    req = urllib.request.Request(url, headers=headers)
    with urllib.request.urlopen(req, timeout=90) as response:
        return response.read().decode("utf-8", errors="replace")


def parse_external_png_list(text: str) -> list[str]:
    names: list[str] = []

    for line in text.splitlines():
        line = line.strip().strip(",").strip()
        if not line:
            continue

        # list.txt format is usually:
        # "PSNA_0002AFA0AA32398A10233F9D2BB4CE1EA6110219.png",
        line = line.strip("'\"").strip()
        if not line:
            continue

        if line.lower().endswith((".png", ".jpg", ".jpeg")):
            names.append(line)

    return names


def get_external_image_paths(repo_info: dict, log: Callable[[str], None]) -> list[str]:
    """Return external image paths.

    MrJasonDEX stores an explicit Avaters/png_file_list.txt index containing
    names like PSNA_XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX.png.
    We use that list and build Avaters/PNG_files/<name> directly.
    """
    repo = repo_info["repo"]
    branch = repo_info["branch"]
    png_prefix = repo_info["png_prefix"].strip("/")
    list_path = (repo_info.get("list_path") or "").strip("/")
    cache_key = f"{repo}@{branch}:{png_prefix}:{list_path}"

    if cache_key in _EXTERNAL_FILE_LIST_CACHE:
        return _EXTERNAL_FILE_LIST_CACHE[cache_key]

    paths: list[str] = []

    if list_path:
        list_url = github_raw_url(repo, branch, list_path)
        try:
            list_text = download_text(list_url)
            names = parse_external_png_list(list_text)
            for name in names:
                clean_name = name.strip().strip("/")
                if "/" in clean_name:
                    paths.append(clean_name)
                else:
                    paths.append(f"{png_prefix}/{clean_name}")
            log(f"[EXT ] Indexed {len(paths)} image(s) from {repo}/{list_path}")
        except Exception as exc:
            log(f"[EXT ] Could not read external list {list_url}: {exc}")

    # Fallback only if the list could not be read. Keep it simple and exact-folder based.
    if not paths:
        api_url = (
            f"https://api.github.com/repos/{repo}/contents/"
            f"{urllib.parse.quote(png_prefix, safe='/')}?ref={urllib.parse.quote(branch, safe='')}"
        )
        try:
            headers = {
                "User-Agent": "PS3-Pro-Avatar-Cover-Generator",
                "Accept": "application/vnd.github+json",
            }
            token = os.environ.get("GITHUB_TOKEN")
            if token:
                headers["Authorization"] = f"Bearer {token}"
            req = urllib.request.Request(api_url, headers=headers)
            with urllib.request.urlopen(req, timeout=90) as response:
                payload = json.loads(response.read().decode("utf-8"))
            if isinstance(payload, dict):
                payload = [payload]
            for item in payload:
                if item.get("type") == "file":
                    path = item.get("path") or ""
                    if path.lower().endswith((".png", ".jpg", ".jpeg")):
                        paths.append(path)
            log(f"[EXT ] Indexed {len(paths)} image(s) from {repo}/{png_prefix}")
        except Exception as exc:
            log(f"[EXT ] Could not read external folder {api_url}: {exc}")

    _EXTERNAL_FILE_LIST_CACHE[cache_key] = paths
    return paths


def download_external_image(repo: str, branch: str, path: str, output_path: Path, log: Callable[[str], None]) -> bool:
    raw_url = github_raw_url(repo, branch, path)

    try:
        req = urllib.request.Request(raw_url, headers={"User-Agent": "PS3-Pro-Avatar-Cover-Generator"})
        with urllib.request.urlopen(req, timeout=90) as response:
            data = response.read()
    except Exception as exc:
        log(f"[EXT ] Download failed: {raw_url} ({exc})")
        return False

    if data.startswith(b"\x89PNG\r\n\x1a\n"):
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_bytes(data)
        return True

    if data.startswith(b"\xff\xd8\xff"):
        try:
            from PIL import Image
            import io

            image = Image.open(io.BytesIO(data))
            output_path.parent.mkdir(parents=True, exist_ok=True)
            image.save(output_path, format="PNG")
            return True
        except Exception as exc:
            log(f"[EXT ] JPG found but Pillow conversion failed: {exc}")
            return False

    log(f"[EXT ] Remote file is not PNG/JPG: {path}")
    return False


def normalize_external_match_name(value: str) -> str:
    value = (value or "").upper()
    value = value.rsplit("/", 1)[-1]
    value = re.sub(r"\.(PNG|JPG|JPEG|EDAT|UNEDAT|BIN|DAT)$", "", value, flags=re.IGNORECASE)
    return normalize_lookup_text(value)


def build_failed_psna_names(failed_edat_names: Iterable[str] | None = None) -> list[str]:
    """Build PSNA names from the actual EDAT/UNEDAT files that failed.

    Example:
      local failed EDAT: PSNA_0002AFA0AA32398A10233F9D2BB4CE1EA6110219.edat
      external PNG:      PSNA_0002AFA0AA32398A10233F9D2BB4CE1EA6110219.png
    """
    out: list[str] = []
    seen: set[str] = set()

    for name in failed_edat_names or []:
        stem = normalize_external_match_name(name)
        if not stem:
            continue

        # Only accept actual PSNA stems from the extracted package.
        if not stem.startswith("PSNA"):
            continue

        if stem not in seen:
            seen.add(stem)
            out.append(stem)

    return out


def find_external_avatar_cover(
    entry: AvatarEntry,
    output_path: Path,
    log: Callable[[str], None],
    failed_edat_names: Iterable[str] | None = None,
) -> bool:
    match_names = build_failed_psna_names(failed_edat_names)

    if not match_names:
        log("[EXT ] No failed PSNA EDAT name available for external fallback")
        return False

    log(f"[EXT ] Looking for external PSNA: {', '.join(match_names)}")

    for repo_info in EXTERNAL_AVATAR_REPOS:
        label = repo_info["label"]
        repo = repo_info["repo"]
        branch = repo_info["branch"]

        paths = get_external_image_paths(repo_info, log)
        if not paths:
            continue

        candidates: list[tuple[int, str]] = []

        for path in paths:
            external_stem = normalize_external_match_name(path)

            for match_name in match_names:
                if external_stem == match_name:
                    score = 1000
                    if path.lower().endswith(".png"):
                        score += 5
                    candidates.append((score, path))
                    break

        if not candidates:
            log(f"[EXT ] No external PSNA match in {label} for {', '.join(match_names)}")
            continue

        candidates.sort(key=lambda item: (-item[0], len(item[1])))

        for score, path in candidates[:5]:
            log(f"[EXT ] Trying {label}: {path}")
            if download_external_image(repo, branch, path, output_path, log):
                log(f"[EXT ] Saved external cover: {output_path}")
                return True

    return False


def process_entry(
    entry: AvatarEntry,
    download_dir: Path,
    output_root: Path,
    work_root: Path,
    overwrite: bool,
    redownload: bool,
    keep_extracted: bool,
    log: Callable[[str], None],
) -> tuple[int, int]:
    """Return (saved_png_count, failed_edat_count)."""
    title_folder = output_root / safe_filename(entry.title_id)
    title_folder.mkdir(parents=True, exist_ok=True)

    pkg_name = safe_filename(entry.content_id or Path(entry.pkg_url).name, fallback="avatar") + ".pkg"
    pkg_path = download_dir / pkg_name

    expected_output = title_folder / f"{safe_filename(entry.content_id or entry.name)}.png"
    if expected_output.exists() and not overwrite:
        log(f"[SKIP] {entry.title_id} / {entry.content_id}: PNG already exists")
        return 0, 0

    if redownload and pkg_path.exists():
        pkg_path.unlink()

    if not pkg_path.exists():
        log(f"[GET ] {entry.title_id} - {entry.name}")
        download_file(entry.pkg_url, pkg_path, log)
    else:
        log(f"[CACHE] {pkg_path.name}")

    if entry.sha256 and re.fullmatch(r"[0-9a-fA-F]{64}", entry.sha256):
        got_hash = sha256_file(pkg_path)
        if got_hash != entry.sha256.upper():
            raise ValueError(f"SHA256 mismatch for {pkg_path.name}: {got_hash} != {entry.sha256}")

    extract_base = work_root / safe_filename(entry.content_id or entry.title_id)
    if extract_base.exists():
        shutil.rmtree(extract_base)
    extract_base.mkdir(parents=True, exist_ok=True)

    extracted_root = extract_pkg(pkg_path, extract_base, fallback_folder=entry.content_id or entry.title_id, log=log)
    edats = find_edats(extracted_root)
    if not edats:
        # Path-style PKGs may place files one level above the returned root.
        edats = find_edats(extract_base)

    if not edats:
        log(f"[MISS] No EDAT/UNEDAT found in {pkg_path.name}")
        if find_external_avatar_cover(entry, expected_output, log):
            if not keep_extracted:
                shutil.rmtree(extract_base, ignore_errors=True)
            return 1, 0
        if not keep_extracted:
            shutil.rmtree(extract_base, ignore_errors=True)
        return 0, 1

    klic_map: dict[str, bytes] = {}
    rap_bytes = rap_hex_to_bytes(entry.rap_hex)
    if rap_bytes and entry.content_id:
        klic_map[entry.content_id] = rap_bytes

    saved = 0
    failed = 0
    failed_edat_names: list[str] = []

    for index, edat_path in enumerate(edats, start=1):
        png_data = None
        edat_content_id = None

        try:
            png_data, edat_content_id = PS3EdatDecryptor(edat_path, klic_map).decrypt_to_png()
        except Exception as exc:
            log(f"[FAIL] Primary extractor failed for {edat_path.name}: {exc}")

        if not png_data:
            try:
                enhanced_data, enhanced_content_id, enhanced_label = decrypt_edat_image_enhanced(edat_path, entry.rap_hex, log)
                if enhanced_data:
                    png_data = enhanced_data
                    edat_content_id = edat_content_id or enhanced_content_id
            except Exception as exc:
                log(f"[ENH ] Enhanced extractor failed for {edat_path.name}: {exc}")

        if not png_data:
            try:
                alt_data, alt_content_id, alt_type = decrypt_edat_image_secondary(edat_path, log)
                if alt_data:
                    png_data = alt_data
                    edat_content_id = edat_content_id or alt_content_id
                    log(f"[ALT ] Secondary extractor recovered {edat_path.name} ({alt_type})")
            except Exception as exc:
                log(f"[ALT ] Secondary extractor failed for {edat_path.name}: {exc}")

        if not png_data:
            log(f"[FAIL] Could not find image in {edat_path.name}")
            failed_edat_names.append(edat_path.name)
            failed += 1
            continue

        base_name = safe_filename(entry.content_id or edat_content_id or edat_path.stem)
        if len(edats) > 1:
            base_name = f"{base_name}__{index}_{safe_filename(edat_path.stem)}"

        out_path = unique_output_path(title_folder / f"{base_name}.png", overwrite=overwrite)
        out_path.write_bytes(png_data)
        log(f"[SAVE] {out_path.relative_to(output_root)}")
        saved += 1

    if saved == 0 and failed:
        if find_external_avatar_cover(entry, expected_output, log, failed_edat_names=failed_edat_names):
            saved += 1
            failed = 0

    if saved > 0:
        failed = 0

    if not keep_extracted:
        shutil.rmtree(extract_base, ignore_errors=True)

    return saved, failed


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Extract avatar PNG covers from PS3 avatar PKGs listed in avatars.tsv.")
    parser.add_argument("--repo", type=Path, default=Path.cwd(), help="PSN-Content repository root. Default: current folder")
    parser.add_argument("--tsv", type=Path, action="append", help="Custom avatar TSV path. Can be used more than once.")
    parser.add_argument(
        "--source",
        choices=["official", "pending", "all"],
        default="official",
        help="Avatar TSV source to process when --tsv is not used. Default: official.",
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
    parser.add_argument("--keep-extracted", action="store_true", help="Keep temporary extracted PKG files.")
    parser.add_argument("--download-dir", type=Path, help="PKG cache folder. Default: <repo>/.cache/avatar_pkgs")
    parser.add_argument("--output", type=Path, help="Output folder. Default: <repo>/resources/database/avatars")
    return parser


def main() -> int:
    args = build_arg_parser().parse_args()
    repo = args.repo.resolve()

    if args.tsv:
        tsv_paths = args.tsv
    else:
        source = "all" if args.include_pending else args.source
        tsv_paths = []

        official_tsv = repo / "resources/database/content/official/avatars.tsv"
        pending_tsv = repo / "resources/database/content/official/pending/avatars.tsv"

        if source in ("official", "all"):
            tsv_paths.append(official_tsv)

        if source in ("pending", "all"):
            tsv_paths.append(pending_tsv)
    output_root = (args.output or repo / "resources/database/avatars").resolve()
    download_dir = (args.download_dir or repo / ".cache/avatar_pkgs").resolve()

    print(f"Repo: {repo}")
    print(f"Output: {output_root}")
    print(f"Temporary PKG folder: {download_dir}")
    print("TSV:")
    for path in tsv_paths:
        print(f"  - {path}")

    entries = read_all_entries(tsv_paths)

    title_filter = {x.upper() for x in args.title_id or []}
    content_filter = {x.upper() for x in args.content_id or []}

    if title_filter:
        entries = [e for e in entries if e.title_id.upper() in title_filter]
    if content_filter:
        entries = [e for e in entries if e.content_id.upper() in content_filter]
    if args.start > 0:
        entries = entries[args.start :]
    if args.limit > 0:
        entries = entries[: args.limit]

    print(f"Entries to process: {len(entries)}")
    if not entries:
        return 0

    output_root.mkdir(parents=True, exist_ok=True)
    download_dir.mkdir(parents=True, exist_ok=True)

    if args.keep_extracted:
        work_context = None
        work_root = repo / ".cache/avatar_extract"
        work_root.mkdir(parents=True, exist_ok=True)
    else:
        work_context = tempfile.TemporaryDirectory(prefix="avatar_extract_")
        work_root = Path(work_context.name)

    total_saved = 0
    total_failed = 0
    total_errors = 0
    failed_report_rows = []

    try:
        for number, entry in enumerate(entries, start=1):
            print(f"\n[{number}/{len(entries)}] {entry.title_id} | {entry.content_id} | {entry.name}")
            try:
                saved, failed = process_entry(
                    entry=entry,
                    download_dir=download_dir,
                    output_root=output_root,
                    work_root=work_root,
                    overwrite=args.overwrite,
                    redownload=args.redownload,
                    keep_extracted=args.keep_extracted,
                    log=print,
                )
                total_saved += saved
                total_failed += failed

                if failed:
                    failed_report_rows.append([
                        "failed_edat",
                        entry.title_id,
                        entry.content_id,
                        entry.name,
                        entry.pkg_url,
                        f"{failed} EDAT(s) had no PNG",
                    ])
            except KeyboardInterrupt:
                raise
            except Exception as exc:
                print(f"[ERROR] {entry.content_id or entry.pkg_url}: {exc}")
                total_errors += 1
                failed_report_rows.append([
                    "entry_error",
                    entry.title_id,
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
    print(f"Failed EDATs: {total_failed}")
    print(f"Entry errors: {total_errors}")

    if failed_report_rows:
        report_path = output_root / "_failed_avatar_covers.tsv"
        with report_path.open("w", encoding="utf-8", newline="") as handle:
            handle.write("type\ttitle_id\tcontent_id\tname\tpkg_url\treason\n")
            for row in failed_report_rows:
                safe_row = [str(cell).replace("\t", " ").replace("\r", " ").replace("\n", " ") for cell in row]
                handle.write("\t".join(safe_row) + "\n")
        print(f"Failure report: {report_path}")

    if total_errors or total_failed:
        print("WARNING: Some avatar entries failed, but generated PNGs will still be committed.")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
