#!/usr/bin/env python3
import argparse
import csv
import gzip
import heapq
import json
import os
import re
import shutil
import time
import unicodedata
from collections import Counter, defaultdict
from functools import lru_cache
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

ROOT = Path(__file__).resolve().parents[2]
OUTPUT = 'resources/database/helpers/franchises.json'
CACHE_FILE = '.cache/franchises-igdb-v1/games.json.gz'
CACHE_VERSION = 1
FULL_REFRESH_SECONDS = 7 * 86400
UPDATE_OVERLAP_SECONDS = 3600
FILES = ('official/games.tsv', 'official/pending/games.tsv', 'external/nostalgia_vault.tsv', 'official/demos.tsv', 'official/pending/demos.tsv')
PLATFORMS = {
    'ps3': {9}, 'ps2': {8}, 'psx': {7}, 'psp': {38}, 'minis': {9, 38},
    'pce': {86, 150, 128}, 'neogeo': {79, 80, 136},
}
PLATFORM_NAMES = {'pce': {'turbografx16pcengine', 'turbografx16pcenginecd', 'pcenginesupergrafx'}, 'neogeo': {'neogeoaes', 'neogeomvs', 'neogeocd'}}
ALLOWED_TYPES = {'main game', 'remake', 'remaster', 'port', 'bundle', 'standalone expansion', 'expanded game'}
ID = re.compile(r'^[A-Z0-9]{9}$')
PSN_CONTENT_ID = re.compile(r'^[A-Z]{2}[0-9]{4}-([A-Z]{4}[0-9]{5})_[0-9]{2}-[A-Z0-9]{16}$')
TITLE_ID_SUFFIX = re.compile(r'\s*\(\s*([A-Z0-9]{9})\s*\)\s*$', re.I)
NON_ALNUM = re.compile(r'[\W_]+')
WORD_PATTERN = re.compile(r'[^\W_]+')
MARKS = str.maketrans('', '', '™®©')
TRIAL = re.compile(r'\btrial\s+to\s+full\b', re.I)
TECH = re.compile(r'\s*[\[(]\s*(?:PS[123XP]|PS[12]\s*classics?|minis?|PC\s*Engine|TurboGrafx(?:-16)?|Neo\s*Geo(?:\s*Station)?|Nostalgia Vault|(?:English|French|German|Spanish|Italian|Japanese)(?:\s+Version)?|digital version|trial to full|part\s*\d+|disc\s*\d+)\s*[\])]\s*', re.I)


@lru_cache(maxsize=4096)
def norm(value):
    value = unicodedata.normalize('NFKD', str(value).translate(MARKS)).casefold()
    return NON_ALNUM.sub('', value)


@lru_cache(maxsize=4096)
def names(value):
    raw = str(value).strip()
    cleaned = TECH.sub(' ', raw)
    cleaned = TRIAL.sub(' ', cleaned)
    cleaned = re.sub(r'\s+(?:digital version|(?:French|English|German|Italian|Spanish|Japanese) version)\s*$', '', cleaned, flags=re.I)
    return tuple(dict.fromkeys(x for x in (norm(raw), norm(cleaned)) if x))


# Secondary keys are deliberately separate from the original exact index.
ROMAN = {'i':'1','ii':'2','iii':'3','iv':'4','v':'5','vi':'6','vii':'7','viii':'8','ix':'9','x':'10','xi':'11','xii':'12','xiii':'13','xiv':'14','xv':'15','xvi':'16'}
VAULT = re.compile(r'\s*\(Nostalgia Vault\)\s*$', re.I)
VAULT_PART = re.compile(r'\s+(?:part|disc)\s*\d+\s*$', re.I)
PLATFORM_PREFIX = re.compile(r'^\s*(?:PSP|PS[123X]|NEO\s*GEO\s+STATION|PC\s*ENGINE)\s*(?:[-:–—]\s*|\s+)', re.I)
EDITION_SUFFIX = re.compile(r'(?:\s*[-:–—]\s*|\s+)(?:game of the year(?: edition)?|goty(?: edition)?|(?:deluxe|complete|collector[’\']?s|special|templar|ultimate|digital) edition|classics? hd|hd)\s*$', re.I)

REGIONAL_LABEL = r'(?:EN|ENG|ENGLISH|FR|FRA|FRE|FRENCH|FRANCE|DE|DEU|GER|GERMAN|GERMANY|IT|ITA|ITALIAN|ITALY|ES|SPA|ESP|SPANISH|SPAIN|JP|JPN|JAPANESE|JAPAN|KR|KOR|KOREAN|CN|CHN|CHINESE|PL|POL|POLISH|RU|RUS|RUSSIAN|RUSSIA|PT|POR|PORTUGUESE|BR|BRAZIL|UK|ENGLAND|US|USA|EU|EUR|ANZ|TUR|TURKISH)'
REGIONAL_SUFFIX = re.compile(r'\s*\(\s*' + REGIONAL_LABEL + r'(?:\s*[-/,]\s*' + REGIONAL_LABEL + r')*\s*\)\s*$', re.I)
DIGITAL_SUFFIX = re.compile(r'\s*\(\s*(?:digital|downloadable ver(?:sion)?\.?)\s*\)\s*$', re.I)


def catalog_match_name(name, tid):
    # Some regional catalog names repeat their own Title ID as a suffix.
    match = TITLE_ID_SUFFIX.search(name)
    return name[:match.start()] if match and match[1].upper() == tid else name


@lru_cache(maxsize=4096)
def recovery_base(value, catalog):
    raw = unicodedata.normalize('NFKC', str(value).translate(MARKS)).strip()
    if catalog and VAULT.search(raw):
        raw = VAULT_PART.sub('', VAULT.sub('', raw))
    return TRIAL.sub(' ', TECH.sub(' ', raw)).strip()


@lru_cache(maxsize=4096)
def recovery_keys(value, catalog=False):
    raw = recovery_base(value, catalog)
    if catalog:
        raw = REGIONAL_SUFFIX.sub('', raw).strip()
        raw = re.sub(r'\s*[\[(](?:ENG|FRA|FRE|GER|DEU|ITA|SPA|ESP|JPN|KOR|POR)[\])]\s*$', '', raw, flags=re.I).strip()
        raw = DIGITAL_SUFFIX.sub('', raw).strip()
    if catalog:
        raw = PLATFORM_PREFIX.sub('', raw).strip()
    def key(title):
        words = franchise_words(title.replace('&', ' and '))
        return ''.join(ROMAN.get(w, w) if i and (w != 'i' or i == len(words)-1) else w for i,w in enumerate(words))
    strong = {norm(raw), key(raw)} - {''}
    article = re.fullmatch(r'(.+),\s*(the|an|a)', raw, re.I)
    if article:
        reordered = article[2] + ' ' + article[1]
        strong.update((norm(reordered), key(reordered)))
    # A numeric brand (e.g. 007) may be preceded by a publisher's IP name
    # in the official title. Keep the entire branded suffix, never just words.
    words = franchise_words(raw)
    for i,w in enumerate(words[:4]):
        if re.fullmatch(r'0\d{2,3}', w) and len(words)-i >= 2:
            strong.add(key(' '.join(words[i:])))
    weak = set()
    if catalog:
        stripped = EDITION_SUFFIX.sub('', raw).strip()
        if stripped != raw and len(franchise_words(stripped)) >= 2:
            weak.update((norm(stripped), key(stripped)))
    return frozenset(strong), frozenset(weak - strong)



REISSUE_SUFFIX = re.compile(r'\s*(?:PlayStation\s*3|PS3)\s+the\s+Best\s*$', re.I)
LANG_DASH_SUFFIX = re.compile(r'\s+[-–—]\s+(?:EN|ENG|DE|GER|FR|FRA|IT|ITA|ES|SPA|JP|JPN|KR|KOR|RU|RUS|PT|POR)\s*$', re.I)
VERSION_SUFFIX = re.compile(r'\s*\(\s*(?:app\s+)?version\s+\d+(?:\.\d+){1,3}\s*\)\s*$', re.I)
HD_EDITION = re.compile(r'\s*(?:[-:–—]\s*)?HD\s+Edition\s*$', re.I)
ORDINAL_WORDS = {'first':'1st','second':'2nd','third':'3rd','fourth':'4th','fifth':'5th','sixth':'6th','seventh':'7th','eighth':'8th','ninth':'9th','tenth':'10th'}
ORDINAL_WORD = re.compile(r'\b(?:' + '|'.join(ORDINAL_WORDS) + r')\b', re.I)
ORDINAL_NUMBER = re.compile(r'\b(?:' + '|'.join(ORDINAL_WORDS.values()) + r')\b', re.I)
NUMBER_PAIR = re.compile(r'(?<!\w)([IVXLCDM]+|\d+)\s+(?:and|&)\s+(?=(?:[IVXLCDM]+|\d+)(?:\b|[-/]))', re.I)
NON_GAME_SUFFIX = re.compile(r'(?:\s+(?:Official|Gameplay|Launch)\s+Trailer|\s*[-–—]\s*Console Application)\s*$', re.I)
MANUAL_SUFFIX = re.compile(r'(?:\b(?:DigiManual|Digital Manual|Manual)\b)(?:\s*\(Digital\))?\s*$', re.I)


def is_explicit_non_game(title, tid):
    title = unicodedata.normalize('NFKC', title).replace('™', '').replace('®', '').strip()
    return bool(NON_GAME_SUFFIX.search(title) or (tid.startswith('NPUO') and MANUAL_SUFFIX.search(title)))


@lru_cache(maxsize=4096)
def late_recovery_keys(value, catalog=False):
    raw = recovery_base(value, catalog)
    if catalog:
        raw = PLATFORM_PREFIX.sub('', raw).strip()
        raw = REGIONAL_SUFFIX.sub('', raw).strip()
        raw = DIGITAL_SUFFIX.sub('', raw).strip()
    keys = set()
    clean = REISSUE_SUFFIX.sub('', raw).strip()
    if catalog:
        clean = LANG_DASH_SUFFIX.sub('', clean).strip()
        clean = VERSION_SUFFIX.sub('', clean).strip()
    if clean != raw:
        keys.update(recovery_keys(clean, catalog)[0])
    # Compare only explicit pairs of installment numbers, not arbitrary conjunctions.
    paired = NUMBER_PAIR.sub(r'\1/', clean)
    if paired != clean:
        keys.update(recovery_keys(paired, catalog)[0])
    # An official numbered HD edition may include a subtitle omitted by PSN.
    # Keep both the installment number and the explicit HD edition marker.
    if not catalog and HD_EDITION.search(clean):
        prefix = re.split(r'[:–—]|\s-\s', clean, maxsplit=1)[0].strip()
        words = franchise_words(prefix)
        if len(words) >= 3 and ROMAN.get(words[-1], words[-1]).isdigit():
            keys.update(recovery_keys(prefix + ' HD Edition')[0])
    # Also support a one-word base only for the explicit GOTY designation.
    if catalog:
        base = re.sub(r'\s+(?:GOTY(?: Edition)?|Game of the Year(?: Edition)?)\s*$', '', clean, flags=re.I).strip()
        if base != clean and len(franchise_words(base)) == 1 and len(base) >= 5:
            keys.update(recovery_keys(base)[0])
    return frozenset(keys - {''})


@lru_cache(maxsize=4096)
def ordinal_recovery_keys(value, catalog=False):
    raw = recovery_base(value, catalog)
    if catalog:
        raw = DIGITAL_SUFFIX.sub('', REGIONAL_SUFFIX.sub('', PLATFORM_PREFIX.sub('', raw))).strip()
    if not (ORDINAL_WORD.search(raw) or ORDINAL_NUMBER.search(raw)) or len(franchise_words(raw)) < 3:
        return frozenset()
    canonical = ORDINAL_WORD.sub(lambda match: ORDINAL_WORDS[match[0].lower()], raw)
    return recovery_keys(canonical, catalog)[0]


# Catalog-only release labels. Never remove an installment number or subtitle.
CATALOG_RELEASE_SUFFIX = re.compile(
    r'\s*(?:\(\s*(?:PS[123XP]|PS3\s*/\s*PSP|(?:English|French|German|Spanish|Italian|Japanese)\s+Ver\.?|(?:SuperLite\s+\d+|World Greatest Hits)\s+Series|base game)\s*\)'
    r'|\s+(?:[-–—]\s*)?PS[123XP](?:\s+full game)?|\s+full game|\s+v(?:ersion\s*)?\d+\.\d+(?:\.\d+)*|\s+(?:download|製品版))\s*$', re.I)
EXTRA_REGION_SUFFIX = re.compile(r'\s*(?:\(\s*(?:UK|US|EU)\s+(?:English|French|German|Spanish|Italian)\s*\)|\s+[-–—]\s+(?:EU|EUR|UK|US|USA|JPN|ASIA))\s*$', re.I)
POSSESSIVE = re.compile(r"(?<=\w)[’']s\b", re.I)
NUMBER_K = re.compile(r'\b([1-9]\d{0,2})k\b', re.I)


@lru_cache(maxsize=8192)
def extended_title_words(value, catalog=False):
    raw = recovery_base(value, catalog)
    if catalog:
        for _ in range(5):
            clean = CATALOG_RELEASE_SUFFIX.sub('', raw).strip()
            clean = EXTRA_REGION_SUFFIX.sub('', clean).strip()
            clean = REGIONAL_SUFFIX.sub('', clean).strip()
            clean = DIGITAL_SUFFIX.sub('', clean).strip()
            clean = VERSION_SUFFIX.sub('', clean).strip()
            clean = REISSUE_SUFFIX.sub('', clean).strip()
            clean = LANG_DASH_SUFFIX.sub('', clean).strip()
            if clean == raw:
                break
            raw = clean
        raw = PLATFORM_PREFIX.sub('', raw).strip()
    article = re.fullmatch(r'(.+),\s*(the|an|a)', raw, re.I)
    if article:
        raw = article[2] + ' ' + article[1]
    words = franchise_words(raw.replace('&', ' and ').replace('+', ' plus '))
    return tuple(ROMAN.get(w, w) if i and (w != 'i' or i == len(words)-1) else w for i, w in enumerate(words))


def extended_name_key(words):
    return ''.join(words) if words else ''


@lru_cache(maxsize=8192)
def equivalent_title_keys(value, catalog=False):
    # Exact equivalents only: articles, possessives and explicit numeric units.
    # The full normalized name is tested before these weaker alternatives.
    words=extended_title_words(value,catalog)
    variants={words}
    if POSSESSIVE.search(value):
        variants.add(extended_title_words(POSSESSIVE.sub('',value),catalog))
    if NUMBER_K.search(value):
        variants.add(extended_title_words(NUMBER_K.sub(lambda m: str(int(m[1])*1000),value),catalog))
    for parts in tuple(variants):
        if len(parts)>=3:
            no_article=tuple(w for w in parts if w!='the')
            if len(no_article)>=2 and sum(map(len,no_article))>=10:
                variants.add(no_article)
        # Explicit edition language is optional only next to another edition
        # marker; never strip a bare numbered title or an arbitrary subtitle.
        if 'edition' in parts and any(w in ('hd','remix','remastered','remaster') for w in parts):
            variants.add(tuple(w for w in parts if w!='edition'))
    return frozenset(extended_name_key(parts) for parts in variants if parts)


CATALOG_STORE_SUFFIX = re.compile(r"\s+(?:PS3|PlayStation\s*3|EU|USA|EUR|JPN|\(Download Ver\.\))\s*$", re.I)


def store_title_keys(value, catalog=False):
    raw = recovery_base(value, catalog)
    if catalog:
        raw = CATALOG_STORE_SUFFIX.sub('', raw).strip()
    words = extended_title_words(raw, catalog)
    variants = {words}
    if len(words) >= 3:
        without_articles = tuple(w for w in words if w != 'the')
        if len(without_articles) >= 2:
            variants.add(without_articles)
        for suffix in (('the','video','game'), ('video','game'), ('legacy','edition')):
            if words[-len(suffix):] == suffix and len(words) - len(suffix) >= 2:
                variants.add(words[:-len(suffix)])
    # Keep numbers intact except explicit zero-padding and volume syntax;
    # do not equate different installments or remove a numbered sequel.
    for parts in tuple(variants):
        if len(parts) < 3:
            continue
        numbers = tuple(str(int(w)) if re.fullmatch(r'0[1-9]',w) or (i and parts[i-1] in ('vol','volume') and re.fullmatch(r'0\d+',w)) else w for i,w in enumerate(parts))
        variants.add(numbers)
        if re.fullmatch(r'v[1-9]',parts[-1]):
            variants.add(parts[:-1]+(parts[-1][1:],))
        if re.fullmatch(r'[1-9]',parts[-1]):
            variants.add(parts[:-1]+('v'+parts[-1],))
        # Explicit episode/part numbering, never discard the number.
        episode = tuple('part' if w in ('episode','ep') and i+1<len(parts) and parts[i+1].isdigit() else w for i,w in enumerate(parts))
        variants.add(episode)
        if 'n' in parts and all(w.isascii() for w in parts):
            variants.add(tuple('and' if w=='n' else w for w in parts))
        if len(parts)>=4 and parts[-2]=='episode' and parts[-1] in ('one','two','three','four','five','six','seven','eight','nine'):
            variants.add(parts[:-1]+(str(('one','two','three','four','five','six','seven','eight','nine').index(parts[-1])+1),))
    return {extended_name_key(w) for w in variants if len(w) >= 2 and sum(map(len,w)) >= 8}


def supported_store_keys(game, titles, accepted):
    keys = set()
    kind = (game.get('game_type') or {}).get('type','').lower()
    labels = {franchise_words(label) for _,label in accepted}
    for title in titles:
        keys.update(store_title_keys(title))
        words = extended_title_words(title)
        # Strip an explicit brand only at a subtitle boundary and only when
        # the same brand is present in the official membership metadata.
        parts = subtitle_parts(title)
        if parts and franchise_words(parts[1]) in labels:
            suffix = extended_title_words(parts[2])
            if len(suffix) >= 2 and sum(map(len,suffix)) >= 9:
                keys.update(store_title_keys(parts[2]))
        # A possessive brand is unambiguous at the start, with a full title
        # remaining. Require an explicit series/collection to support it.
        if accepted:
            match = re.match(r"^(.{3,35}?[’']s)\s+(.+)$", title, re.I)
            if match and any(label_in_name(label, match[2]) for _,label in accepted):
                keys.update(store_title_keys(match[2]))
        if kind == 'remaster':
            for suffix in (('hd','edition'),('hd',),('remastered',),('remaster',)):
                if words[-len(suffix):] == suffix:
                    base = words[:-len(suffix)]
                    if len(base) >= 2 and sum(map(len,base)) >= 9:
                        keys.add(extended_name_key(base))
                        keys.add(extended_name_key(tuple(w for w in base if w != 'the')))
            if len(words) >= 2 and words[-1] != 'hd':
                keys.add(extended_name_key(words+('hd',)))
    return keys


def romanized_japanese_key(words):
    if len(words)<3 or sum(map(len,words))<15 or not all(w.isascii() for w in words):
        return ''
    return ''.join(w.replace('ou','o').replace('uu','u') for w in words)


def publisher_prefixes(game):
    result=set()
    for item in game.get('involved_companies') or []:
        if not isinstance(item,dict) or item.get('publisher') is not True: continue
        company=item.get('company') or {}
        if not isinstance(company,dict): continue
        words=franchise_words(company.get('name',''))
        while words and words[-1] in ('inc','ltd','llc','corp','corporation','co','limited','company','games','interactive','entertainment','studios','studio','software','computer'):
            words=words[:-1]
        if words and sum(map(len,words))>=4:
            result.add(words)
    return result


def branded_title_keys(prefixes, words):
    # The publisher must be supplied by IGDB; never infer it from a title.
    if len(words)<2 or sum(map(len,words))<9: return set()
    result=set()
    for prefix in prefixes:
        if words[:len(prefix)]==prefix:
            remainder=words[len(prefix):]
            if remainder[:1]==('s',): remainder=remainder[1:]
            if len(remainder)>=2 and sum(map(len,remainder))>=9:
                result.add(extended_name_key(remainder))
        else:
            result.add(extended_name_key(prefix+words))
            result.add(extended_name_key(prefix+('s',)+words))
    return result


def typo_slots(words):
    # At least two independent words; the altered word cannot be a number,
    # an edition designation, or a short franchise identifier.
    if len(words) < 2 or len(words) > 14 or not all(w.isascii() for w in words):
        return
    total = sum(map(len, words))
    if total < 13:
        return
    for i, word in enumerate(words):
        if len(word) < 5 or not word.isalpha() or word in ROMAN or SERIES_EDITION.fullmatch(word):
            continue
        if total - len(word) >= 7:
            yield (i, words[:i], words[i+1:]), word


def one_typo(a, b):
    if a == b or abs(len(a)-len(b)) > 1 or min(len(a),len(b)) < 5:
        return False
    if len(a) == len(b):
        diffs = [i for i,(x,y) in enumerate(zip(a,b)) if x != y]
        return len(diffs) == 1 or (len(diffs) == 2 and diffs[1] == diffs[0]+1 and a[diffs[0]] == b[diffs[1]] and a[diffs[1]] == b[diffs[0]])
    if len(a) > len(b):
        a,b=b,a
    i=next((i for i,(x,y) in enumerate(zip(a,b)) if x != y),len(a))
    return a[i:] == b[i+1:]


BILINGUAL_SUFFIX = re.compile(r'^(.+?)\s*[（(]([^()（）]+)[）)]\s*$')
JAPANESE_SCRIPT = re.compile(r'[\u3040-\u30ff\u3400-\u9fff]')
LATIN_SCRIPT = re.compile(r'[A-Za-z]')


def bilingual_title_keys(value, catalog=False):
    # A release may put a Japanese translation beside the Latin title.
    # Do not split ordinary subtitles, edition labels or same-script names.
    if not JAPANESE_SCRIPT.search(value) or not LATIN_SCRIPT.search(value):
        return frozenset()
    raw = recovery_base(value, catalog)
    match = BILINGUAL_SUFFIX.fullmatch(raw)
    if not match:
        return frozenset()
    a,b=(part.strip() for part in match.groups())
    latin=lambda part: bool(LATIN_SCRIPT.search(part)) and not JAPANESE_SCRIPT.search(part)
    japanese=lambda part: bool(JAPANESE_SCRIPT.search(part)) and not LATIN_SCRIPT.search(part)
    if not ((latin(a) and japanese(b)) or (latin(b) and japanese(a))):
        return frozenset()
    # Only index the Latin half: a Japanese suffix can be a sales label, not a translation.
    key=extended_name_key(extended_title_words(a if latin(a) else b,catalog))
    return frozenset((key,)) if len(key)>=4 else frozenset()


def psn_title_id(value):
    uid=str(value or '').strip().upper()
    if ID.fullmatch(uid):
        return uid
    match=PSN_CONTENT_ID.fullmatch(uid)
    return match[1] if match else ''


def localized_game_names(game):
    return tuple(dict.fromkeys(str(item['name']) for item in game.get('game_localizations') or []
        if isinstance(item,dict) and item.get('name')))


def platform(title, tid, cid):
    if 'NEOGEOSTATION' in cid.upper(): return 'neogeo'
    for regex, tag in [(r'\(\s*PS2[^)]*\)|\bPS2\s*CLASSICS?\b','ps2'), (r'\(\s*PS[1X]\s*\)|\bPSONE\b|\bPS1\s*CLASSICS?\b','psx'), (r'\(\s*MINIS?\s*\)','minis'), (r'\(\s*PSP\s*\)','psp'), (r'\(\s*(?:PC\s*ENGINE|TURBO\s*GRAFX(?:\s*-\s*16)?)\s*\)','pce'), (r'\bNEO\s*GEO(?:\s*STATION)?\b','neogeo')]:
        if re.search(regex,title,re.I): return tag
    if tid[:2] in ('UC','UL'): return 'psp'
    if tid.startswith(('SLUS0', 'SLES0', 'SCES0', 'SCUS94')): return 'psx'
    if tid[:2] in ('SL','SC'): return 'ps2'
    if tid[:2] == 'NP':
        if tid[3] in 'ZX': return 'minis'
        if tid[:4] == 'NPUF' or tid.startswith('NPJJ3'): return 'pce'
        if tid[3] in 'GH': return 'psp'
        if tid[3] in 'DC': return 'ps2'
        if tid[3] in {'U':'JFI','J':'JI','E':'FE'}.get(tid[2],''): return 'psx'
    return 'ps3'


def compile_browse_regex(pattern):
    # JavaScript's non-Unicode \b is based on ASCII word characters.
    boundary = r'(?-i:(?:(?<![A-Za-z0-9_])(?=[A-Za-z0-9_])|(?<=[A-Za-z0-9_])(?![A-Za-z0-9_])))'
    return re.compile(pattern.replace(r'\b', boundary), re.I)


class BrowseContentFilter:
    def __init__(self, modern):
        source = modern.read_text(encoding='utf-8-sig')
        block = re.search(r'const DICT_APPS_FILTER\s*=\s*\{(.*?)\};', source, re.S)
        if not block:
            raise RuntimeError('Missing Browse DICT_APPS_FILTER; previous index preserved')
        self.apps = set(re.findall(r'"([A-Z0-9]{9})"\s*:\s*1\b', block[1]))
        if not self.apps:
            raise RuntimeError('Empty Browse app filter; previous index preserved')
        rules = {}
        for match in re.finditer(r'const\s+(PSN_(?:SOUNDTRACK|VIDEO)_[A-Z_]+_REGEX)\s*=\s*/((?:\\.|[^/\n])+)/i\s*;', source):
            rules.setdefault(match[1], match[2])
        def regex(name):
            if name not in rules:
                raise RuntimeError(f'Missing Browse rule {name}; previous index preserved')
            return compile_browse_regex(rules[name])
        self.soundtrack = regex('PSN_SOUNDTRACK_TITLE_SIGNAL_REGEX')
        self.video_name = regex('PSN_VIDEO_NAME_SIGNAL_REGEX')
        self.video_cid = regex('PSN_VIDEO_CONTENT_ID_SIGNAL_REGEX')
        self.video_cid_exclusion = regex('PSN_VIDEO_CONTENT_ID_EXCLUSION_REGEX')
        self.video_name_exclusion = regex('PSN_VIDEO_NAME_EXCLUSION_REGEX')
        self.video_test = regex('PSN_VIDEO_TEST_VERSION_REGEX')
        self.physical_game_id = re.compile(r'^[BS][CL][A-Z]{2}\d{5}$', re.I)
        analysis = re.search(r'function buildStoreRowAnalysis\(.*?\n\s*function getStoreRowAnalysis\(', source, re.S)
        guide = re.search(r'const isGuide\s*=\s*/((?:\\.|[^/\n])+)/i\.test\(rawName\);', analysis[0] if analysis else '')
        if not guide:
            raise RuntimeError('Missing Browse guide rule; previous index preserved')
        self.guide = compile_browse_regex(guide[1])
        # Read the same literal demo rules as isDemoLikeTitle, in source order.
        demo = re.search(r'function isDemoLikeTitle\(value\)\s*\{(.*?)\n\s*\}', source, re.S)
        rules = re.findall(r'if\s*\(/((?:\\.|[^/\n])+)/i\.test\(text\)\)\s*return true;', demo[1] if demo else '')
        if not demo or not rules or 'isKnownNonDemoTitle(text)' not in demo[1] or 'isTrialToFullTitle(text)' not in demo[1]:
            raise RuntimeError('Missing Browse demo rules; previous index preserved')
        self.demo_rules = tuple(compile_browse_regex(rule) for rule in rules)
        known = re.search(r'function isKnownNonDemoTitle\(value\).*?return /((?:\\.|[^/\n])+)/i\.test\(text\);', source, re.S)
        if not known:
            raise RuntimeError('Missing Browse non-demo exception; previous index preserved')
        self.non_demo = compile_browse_regex(known[1])

    def category(self, title, tid, cid, explicit_demo=False):
        title = VAULT.sub('', title).strip()
        if tid in self.apps:
            return 'app'
        if self.guide.search(title):
            return 'guide'
        if self.soundtrack.search(title):
            return 'soundtrack'
        if not (self.video_test.search(title) or self.video_name_exclusion.search(title) or self.video_cid_exclusion.search(cid)):
            if self.video_name.search(title) or (not self.physical_game_id.fullmatch(tid) and self.video_cid.search(cid)):
                return 'video'
        normalized = unicodedata.normalize('NFKC', title).strip()
        if explicit_demo:
            if not TRIAL.search(title):
                return 'demo'
        elif normalized and not TRIAL.search(normalized) and not self.non_demo.search(normalized):
            if any(rule.search(normalized) for rule in self.demo_rules):
                return 'demo'
        upper = title.upper()
        if 'FIRSTPLAY' in upper or 'FIRST PLAY' in upper or 'QORE' in upper:
            return 'app'
        if is_explicit_non_game(title, tid):
            return 'explicit_non_game'
        return 'eligible_rows'


def read_catalog(root, diagnostic=None):
    base = root / 'resources/database/content'
    if not (base / FILES[0]).is_file(): raise RuntimeError('Missing official games.tsv')
    modern = root / 'files/website/modern.html'
    if not modern.is_file():
        raise RuntimeError(f'Missing Browse source at {modern}; previous index preserved')
    browse_filter = BrowseContentFilter(modern)
    result = defaultdict(set)
    sources = {}
    for file in FILES:
        path = base / file
        if not path.is_file():
            sources[file] = {'missing': True}
            continue
        counts = Counter()
        sources[file] = counts
        explicit_demo = file.endswith('demos.tsv')
        with path.open(encoding='utf-8-sig', newline='') as handle:
            for row in csv.reader(handle, delimiter='\t', quoting=csv.QUOTE_NONE):
                counts['rows'] += 1
                if len(row) < 6:
                    counts['invalid_row'] += 1
                    continue
                tid, _, title, url, _, cid = (x.strip() for x in row[:6])
                tid = tid.upper()
                if not ID.fullmatch(tid): reason = 'invalid_id_or_header'
                elif len(title) < 2 or title.upper() == 'MISSING': reason = 'missing_title'
                elif not url.lower().startswith(('http://','https://')): reason = 'missing_download_url'
                else:
                    reason = browse_filter.category(title, tid, cid, explicit_demo)
                    if reason == 'eligible_rows':
                        result[tid].add((title, platform(title,tid,cid)))
                counts[reason] += 1
    if diagnostic is not None:
        diagnostic['catalog_sources'] = sources
        diagnostic['apps_filter_available'] = True
        diagnostic['browse_filter_app_ids'] = len(browse_filter.apps)
    if not result: raise RuntimeError('Catalog is empty')
    return result


def igdb_credentials(root, config_path=None):
    client = os.environ.get('IGDB_CLIENT_ID', '').strip()
    secret = os.environ.get('IGDB_CLIENT_SECRET', '').strip()
    if client and secret:
        return client, secret
    path = Path(config_path) if config_path else Path('resources/server/server-secrets.json')
    if not path.is_absolute():
        path = root / path
    try:
        config = json.loads(path.read_text(encoding='utf-8-sig'))
        if not isinstance(config, dict):
            raise ValueError()
        pair = tuple(config.get(key) for key in ('IGDB_CLIENT_ID', 'IGDB_CLIENT_SECRET'))
        if not all(isinstance(value, str) and value.strip() for value in pair):
            raise ValueError()
        return tuple(value.strip() for value in pair)
    except (OSError, ValueError, UnicodeError):
        raise RuntimeError(f'IGDB credentials unavailable at {path}: check file location, JSON format and IGDB keys, or configure Actions secrets') from None


class IGDB:
    def __init__(self, root=ROOT, config_path=None):
        self.client, self.secret = igdb_credentials(root, config_path)
        self.token = self.authenticate()
        self.next_request = 0
        self.requests = 0

    def authenticate(self):
        req = Request('https://id.twitch.tv/oauth2/token', data=urlencode({'client_id':self.client,'client_secret':self.secret,'grant_type':'client_credentials'}).encode(), headers={'Content-Type':'application/x-www-form-urlencoded'})
        try:
            with urlopen(req,timeout=30) as response: return json.load(response)['access_token']
        except (HTTPError, URLError, KeyError, ValueError): raise RuntimeError('IGDB authentication failed') from None

    def query(self, endpoint, body):
        for attempt in range(5):
            time.sleep(max(0,self.next_request-time.monotonic()))
            self.next_request=time.monotonic()+0.35
            self.requests+=1
            req=Request('https://api.igdb.com/v4/'+endpoint, data=body.encode(), headers={'Client-ID':self.client,'Authorization':'Bearer '+self.token,'Accept':'application/json','Content-Type':'text/plain'})
            try:
                with urlopen(req,timeout=60) as response: data=json.load(response)
                if not isinstance(data,list): raise RuntimeError('Invalid IGDB response')
                return data
            except HTTPError as err:
                if err.code==401 and attempt==0: self.token=self.authenticate();continue
                if err.code not in (429,500,502,503,504): raise RuntimeError(f'IGDB {endpoint}: HTTP {err.code}') from None
            except (URLError, TimeoutError): pass
            time.sleep(min(30,2**(attempt+1)))
        raise RuntimeError(f'IGDB {endpoint}: retries exhausted')

    def pages(self, endpoint, fields, where=''):
        last=0
        while True:
            rows=self.query(endpoint,f'fields {fields}; where id > {last}{" & " + where if where else ""}; sort id asc; limit 500;')
            if not rows: return
            ids=[row.get('id') if isinstance(row,dict) else None for row in rows]
            if any(type(gid) is not int for gid in ids) or ids != sorted(set(ids)) or ids[0]<=last:
                raise RuntimeError('Invalid IGDB pagination')
            new=ids[-1]
            yield from rows
            last=new
            if len(rows)<500:return


class InvalidMetadataCache(Exception):
    pass


def cache_header(path):
    try:
        with gzip.open(path, 'rt', encoding='utf-8') as handle:
            header = json.loads(handle.readline())
        if not isinstance(header, dict):
            raise ValueError()
        return header
    except (OSError, EOFError, ValueError, UnicodeError) as error:
        raise InvalidMetadataCache('missing or invalid cache header') from error


def cached_games(path):
    try:
        with gzip.open(path, 'rt', encoding='utf-8') as handle:
            handle.readline()
            previous = 0
            for line in handle:
                game = json.loads(line)
                if not isinstance(game, dict) or type(game.get('id')) is not int or game['id'] <= previous:
                    raise ValueError('invalid game ID or order')
                previous = game['id']
                yield game
            if not previous:
                raise ValueError('empty game cache')
    except (OSError, EOFError, ValueError, UnicodeError) as error:
        raise InvalidMetadataCache('invalid or truncated game cache') from error


def write_metadata_cache(path, header, games):
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_suffix('.tmp')
    count = 0
    previous = 0
    try:
        with gzip.open(temp, 'wt', encoding='utf-8', compresslevel=3, newline='\n') as handle:
            handle.write(json.dumps(header, separators=(',', ':')) + '\n')
            for game in games:
                if not isinstance(game, dict) or type(game.get('id')) is not int or game['id'] <= previous:
                    raise RuntimeError('Invalid IGDB game ID or cache order')
                previous = game['id']
                handle.write(json.dumps(game, ensure_ascii=False, separators=(',', ':')) + '\n')
                count += 1
        if not count:
            raise RuntimeError('Empty IGDB metadata; previous cache preserved')
        temp.replace(path)
    finally:
        temp.unlink(missing_ok=True)
    return count


def metadata_snapshot(api, path, fields, platform_ids, force_full=False, now=None):
    started = int(time.time() if now is None else now)
    where = 'platforms = (' + ','.join(map(str, platform_ids)) + ')'
    signature = {'version': CACHE_VERSION, 'platforms': platform_ids, 'fields': fields}
    previous = None
    if not force_full and path.is_file():
        try:
            header = cache_header(path)
            full, watermark = header.get('full_at'), header.get('watermark')
            if (all(header.get(key) == value for key, value in signature.items())
                and type(full) is int and type(watermark) is int
                and 0 < full <= watermark <= started
                and started - full < FULL_REFRESH_SECONDS):
                previous = header
            else:
                print('IGDB cache expired or metadata scope changed; full refresh')
        except InvalidMetadataCache:
            print('IGDB cache header invalid; full refresh')
    if previous is not None:
        since = max(0, previous['watermark'] - UPDATE_OVERLAP_SECONDS)
        changes = {game['id']: game for game in api.pages('games', fields, f'{where} & updated_at >= {since}')}
        try:
            old = (game for game in cached_games(path) if game['id'] not in changes)
            merged = heapq.merge(old, (changes[key] for key in sorted(changes)), key=lambda game: game['id'])
            count = write_metadata_cache(path, {**signature, 'full_at': previous['full_at'], 'watermark': started}, merged)
            print(f'IGDB incremental refresh: {len(changes)} updated records; {count} cached records')
            return cached_games(path)
        except InvalidMetadataCache:
            print('IGDB cache damaged; recovering with full refresh')
    count = write_metadata_cache(path, {**signature, 'full_at': started, 'watermark': started}, api.pages('games', fields, where))
    print(f'IGDB full refresh: {count} records cached')
    return cached_games(path)


def metadata(api, cache_path, force_full=False):
    platform_map={k:set(v) for k,v in PLATFORMS.items()}
    platforms=list(api.pages('platforms','id,name'))
    for tag, variants in PLATFORM_NAMES.items():
        platform_map[tag]={r['id'] for r in platforms if norm(r.get('name','')) in variants}
    if any(not ids for ids in platform_map.values()):raise RuntimeError('IGDB platform mapping incomplete')
    sources={r['id'] for r in api.pages('external_game_sources','id,name') if 'playstation' in norm(r.get('name','')) or norm(r.get('name','')).startswith('psn')}
    all_platforms=sorted(set().union(*platform_map.values()))
    fields='id,name,platforms,game_type.type,alternative_names.name,collections.id,collections.name,franchises.id,franchises.name,franchise.id,franchise.name,external_games.uid,external_games.external_game_source,external_games.platform,external_games.name,game_localizations.name,involved_companies.publisher,involved_companies.company.name,version_parent,remakes,remasters,ports,expanded_games,standalone_expansions'
    games = metadata_snapshot(api, cache_path, fields, all_platforms, force_full)
    relations = list(api.pages('collection_relations', 'id,child_collection.id,child_collection.name,parent_collection.id,parent_collection.name,type.name'))
    return games,platform_map,sources,relations


@lru_cache(maxsize=4096)
def franchise_words(value):
    value = unicodedata.normalize('NFKD', str(value).translate(MARKS)).casefold()
    if not value.isascii():
        value = ''.join(c for c in value if not unicodedata.combining(c))
    return tuple(WORD_PATTERN.findall(value))


@lru_cache(maxsize=2048)
def franchise_name_key(name):
    plain = re.sub(r'\s+(?:series|franchise)\s*$', '', str(name).strip(), flags=re.I)
    return norm(plain) or norm(name)


@lru_cache(maxsize=8192)
def label_in_name(label, title):
    # Whole words, with punctuation/spacing variants (e.g. apostrophes).
    label_key = franchise_name_key(label)
    words = franchise_words(title)
    if not label_key:
        return False
    for i in range(len(words)):
        candidate = ''
        for word in words[i:]:
            candidate += word
            if candidate == label_key:
                return True
            if len(candidate) >= len(label_key) or not label_key.startswith(candidate):
                break
    return False


def game_names(game):
    return tuple(dict.fromkeys(str(name) for name in [game.get('name', '')] +
        [item.get('name', '') for item in game.get('alternative_names') or [] if isinstance(item, dict)] if name))


def source_entries(game, field, prefix):
    items = list(game.get(field) or [])
    if field == 'franchises' and isinstance(game.get('franchise'), dict):
        items.append(game['franchise'])
    return set((prefix + ':' + str(item['id']), str(item['name']).strip())
               for item in items if isinstance(item, dict) and isinstance(item.get('id'), int) and str(item.get('name') or '').strip())


def select_game_groups(game):
    collections = source_entries(game, 'collections', 'c')
    franchises = source_entries(game, 'franchises', 'f')
    if len({franchise_name_key(name) for _, name in franchises}) <= 1:
        return collections | franchises, set()
    titles = game_names(game)
    # Several IPs on one game are often character/crossover associations.
    # Prefer the game's series and labels supported by its own names.
    # A collection's name may also establish a parent franchise.
    anchors = titles + tuple(name for _, name in collections)
    supported = {entry for entry in franchises if any(label_in_name(entry[1], title) for title in anchors)}
    return collections | supported, franchises - supported


def franchise_label_order(entry):
    key, name = entry
    qualified = bool(re.search(r'\s+(?:series|franchise)\s*$', name.strip(), re.I))
    return qualified, name.islower(), key.startswith('f:'), name.casefold(), name, key


def acronym(name):
    words = franchise_words(name)
    return ''.join(word if word.isdigit() else word[0] for word in words) if len(words) > 1 else ''


def collection_parent_links(relations):
    links = defaultdict(set)
    for relation in relations:
        kind = relation.get('type') or {}
        child = relation.get('child_collection') or {}
        parent = relation.get('parent_collection') or {}
        if not all(isinstance(value, dict) for value in (kind, child, parent)):
            continue
        label = norm(kind.get('name', ''))
        if not any(token in label for token in ('subseries', 'subcollection', 'subfranchise', 'partof', 'parentchild', 'spinoff')) and label not in {'parent', 'child', 'contains', 'haspart'}:
            continue
        if not all(isinstance(item.get('id'), int) and str(item.get('name') or '').strip() for item in (child, parent)):
            continue
        if child['id'] != parent['id']:
            links['c:' + str(child['id'])].add(('c:' + str(parent['id']), str(parent['name']).strip()))
    return links


def flatten_collection_parents(groups, relations):
    links = collection_parent_links(relations)
    if not links:
        return groups
    result = defaultdict(dict)
    def root(entry):
        seen = {entry[0]}
        current = entry
        while len(links.get(current[0], ())) == 1:
            parent = next(iter(links[current[0]]))
            if parent[0] in seen:
                return entry
            seen.add(parent[0])
            current = parent
        return current
    roots = {entry: root(entry) for entry in groups}
    promoted = defaultdict(set)
    for entry, target in roots.items():
        if target != entry:
            promoted[franchise_name_key(entry[1])].add(target)
    for entry, members in sorted(groups.items(), key=lambda item: franchise_label_order(item[0])):
        target = roots[entry]
        alternatives = promoted.get(franchise_name_key(entry[1]), ())
        if target == entry and len(alternatives) == 1:
            target = next(iter(alternatives))
        if target != entry:
            print(f'Merge IGDB hierarchy: {entry[1]} -> {target[1]}')
        result[target].update(members)
    return result


def flatten_named_subseries(components, records=None):
    records = records or {}
    words = [franchise_words(entry[1]) for entry, _ in components]
    signatures = [set(members.values()) for _, members in components]
    by_game = defaultdict(list)
    for i, signature in enumerate(signatures):
        for gid in signature:
            by_game[gid].append(i)
    support = {}
    def name_support(i, gid):
        key = (i, gid)
        if key not in support:
            support[key] = any(label_in_name(components[i][0][1], title) for title in game_names(records.get(gid, {})))
        return support[key]
    parents = {}
    for i, parts in enumerate(words):
        a = signatures[i]
        if not a or not parts:
            continue
        overlaps = defaultdict(int)
        for gid in a:
            for j in by_game[gid]:
                if j != i:
                    overlaps[j] += 1
        candidates = []
        for j, count in overlaps.items():
            b = signatures[j]
            other = words[j]
            if not other:
                continue
            coverage = count / len(a)
            prefix = len(other) < len(parts) and parts[:len(other)] == other
            suffix = len(other) < len(parts) and parts[-len(other):] == other
            shared_start = len(parts[0]) >= 4 and parts[0] == other[0]
            # Directional ranks prevent cycles, including equal-size aliases.
            if len(b) <= len(a) and not (prefix or suffix):
                continue
            if prefix and coverage >= .5 or suffix and coverage >= .75:
                strength = 4
            elif shared_start and len(b) > len(a) and coverage >= .5 and (count >= 2 or len(a) == 2):
                strength = 3
            elif count >= 2 and coverage == 1 and len(b) <= 2 * len(a) and len(b) > len(a) and components[j][0][0].startswith('c:'):
                # Exact canonical subset, not a coincidental single shared game.
                strength = 2
            elif records and count >= 2 and coverage >= 2/3 and len(b) > len(a) and components[i][0][0].startswith('c:') and all(not name_support(i,gid) for gid in a) and all(name_support(j,gid) for gid in a & b):
                # An umbrella collection with a different label, backed by game titles.
                strength = 1
            else:
                continue
            candidates.append((strength, coverage, count, j))
        if candidates:
            candidates.sort(key=lambda item: (-item[0], -item[1], -item[2], len(words[item[3]]), -len(signatures[item[3]]), franchise_label_order(components[item[3]][0])))
            best = candidates[0]
            # Do not guess between unrelated, equally plausible parents.
            if len(candidates) > 1 and best[:3] == candidates[1][:3] and best[0] <= 2:
                continue
            parents[i] = best[3]
    result = {}
    for i, (entry, members) in enumerate(components):
        target = i
        seen = set()
        while target in parents and target not in seen:
            seen.add(target)
            target = parents[target]
        if target in seen:
            target = i
        if target != i:
            print(f'Merge subseries/alias: {entry[1]} -> {components[target][0][1]}')
        result.setdefault(target, {}).update(members)
    print(f'Subseries consolidation: {len(parents)} candidate groups absorbed')
    return [(components[i][0], members) for i, members in sorted(result.items())]


def prune_incidental_views(components, records):
    if not records:
        return components
    by_game = defaultdict(list)
    signatures = [set(members.values()) for _, members in components]
    for i, signature in enumerate(signatures):
        for gid in signature:
            by_game[gid].append(i)
    labels = {}
    def supported(i, gid):
        key = (i, gid)
        if key not in labels:
            labels[key] = any(label_in_name(components[i][0][1], title) for title in game_names(records.get(gid, {})))
        return labels[key]
    rejected = defaultdict(set)
    # A game with a clear collection name should not be listed in a different
    # series whose own name is absent. Keep valid multi-name crossovers.
    for gid, indexes in by_game.items():
        if len(indexes) < 2:
            continue
        anchors = [i for i in indexes if components[i][0][0].startswith('c:') and supported(i,gid)]
        if not anchors:
            continue
        for i in indexes:
            if i in anchors or supported(i,gid):
                continue
            if any(supported(i, other) for other in signatures[i] - {gid}):
                rejected[i].add(gid)
    # Brand/franchise-only views that merely repeat several independent named
    # collections are redundant. Require strong coverage, not just a crossover.
    for i, (entry, _) in enumerate(components):
        if not entry[0].startswith('f:'):
            continue
        a = signatures[i]
        coverage = defaultdict(set)
        for gid in a:
            for j in by_game[gid]:
                if j != i and components[j][0][0].startswith('c:') and supported(j,gid):
                    coverage[j].add(gid)
        distinct = [ids for j, ids in coverage.items() if len(ids) >= 2 and len(ids) / len(signatures[j]) >= .75]
        if len(distinct) >= 2:
            combined = set().union(*distinct)
            if len(combined) >= .85 * len(a) and sum(map(len, distinct)) <= 1.2 * len(combined):
                rejected[i].update(combined)
    cleaned = []
    for i, (entry, members) in enumerate(components):
        if rejected[i]:
            print(f'Remove incidental/redundant game memberships: {entry[1]} ({len(rejected[i])})')
            members = {tid:gid for tid,gid in members.items() if gid not in rejected[i]}
        cleaned.append((entry,members))
    # Pruning must not erase a title simply because its remaining group fell
    # below the two-game threshold. Restore only affected groups if needed.
    before = {tid for _, members in components if len(set(members.values())) >= 2 for tid in members}
    covered = {tid for _, members in cleaned if len(set(members.values())) >= 2 for tid in members}
    missing = before - covered
    if missing:
        for i, (entry, members) in enumerate(components):
            if len(set(members.values())) >= 2 and missing.intersection(members):
                cleaned[i] = (entry, members)
                missing.difference_update(members)
                print(f'Preserve unique catalog coverage: {entry[1]}')
                if not missing:
                    break
    return cleaned


REISSUE_COLLECTION = re.compile(r'\b(?:complete works|classics|remasters?|remastered|anthology|trilogy|hd collection)\b', re.I)


def flatten_reissue_collections(groups, records):
    # A reissue collection is not an independent franchise when every
    # distinct game explicitly belongs to the same established franchise.
    plans = []
    for entry, members in groups.items():
        if not entry[0].startswith('c:') or not REISSUE_COLLECTION.search(entry[1]):
            continue
        ids = set(members.values())
        if len(ids) < 2:
            continue
        common = None
        for gid in ids:
            franchises = source_entries(records.get(gid, {}), 'franchises', 'f')
            if len(franchises) != 1:
                common = set()
                break
            common = franchises if common is None else common & franchises
            if not common:
                break
        if not common or len(common) != 1:
            continue
        target = next(iter(common))
        parent_ids = set(groups.get(target, {}).values())
        if not ids <= parent_ids or len(parent_ids - ids) < 2:
            continue
        plans.append((entry, target))
    for entry, target in plans:
        groups[target].update(groups.pop(entry))
        print(f'Merge official reissue collection: {entry[1]} -> {target[1]}')
    return len(plans)


def consolidate_groups(groups, records=None, relations=(), diagnostic=None):
    groups = flatten_collection_parents(groups, relations)
    records = records or {}
    groups = defaultdict(dict, groups)
    reissue_merges = flatten_reissue_collections(groups, records)
    if diagnostic is not None: diagnostic['reissue_collection_merges'] = reissue_merges
    entries = sorted(groups, key=franchise_label_order)
    parents = list(range(len(entries)))
    by_source, by_name = {}, {}

    def find(index):
        while parents[index] != index:
            parents[index] = parents[parents[index]]
            index = parents[index]
        return index

    def union(a, b):
        a, b = find(a), find(b)
        if a != b:
            parents[max(a,b)] = min(a,b)

    for i, (key, name) in enumerate(entries):
        if key in by_source:
            union(i, by_source[key])
        else:
            by_source[key] = i
        normalized = franchise_name_key(name)
        if normalized in by_name:
            union(i, by_name[normalized])
        else:
            by_name[normalized] = i

    merged = defaultdict(list)
    for i, entry in enumerate(entries):
        merged[find(i)].append(entry)

    components = []
    name_merges = 0
    for component in merged.values():
        chosen = min(component, key=franchise_label_order)
        members = {}
        for entry in component:
            members.update(groups[entry])
        if len(component) > 1:
            name_merges += len(component)-1
            print(f'Merge by name/source: {component!r} -> {chosen!r}')
        if members:
            components.append((chosen, members))

    # Merge abbreviations only with strong canonical-game overlap. Other
    # differently named, partly overlapping series stay separate.
    keys = defaultdict(list)
    for i, ((_, name), _) in enumerate(components):
        keys[franchise_name_key(name)].append(i)
    parents = list(range(len(components)))
    signatures = [set(members.values()) for _, members in components]
    abbreviation_merges = 0
    for i, ((_, name), members) in enumerate(components):
        short = acronym(name)
        if len(short) < 2:
            continue
        a = signatures[i]
        for j in keys.get(short, []):
            if i == j or find(i) == find(j):
                continue
            b = signatures[j]
            overlap = len(a & b)
            if overlap >= 3 and overlap / (len(a) + len(b) - overlap) >= .75:
                union(i,j)
                abbreviation_merges += 1
                print(f'Merge abbreviation: {components[j][0][1]} -> {name}')
    combined = defaultdict(list)
    for i, item in enumerate(components):
        combined[find(i)].append(item)

    # Resolve duplicate views by actual game identity. A collection or a
    # label supported by the games is preferable to an incidental IP label.
    def rank(entry, signature):
        support = sum(any(label_in_name(entry[1], title) for title in game_names(records.get(gid, {}))) for gid in signature)
        return -support, franchise_label_order(entry)

    flattened = []
    for component in combined.values():
        members = {}
        for _, part in component:
            members.update(part)
        signature = frozenset(members.values())
        chosen = component[0][0] if len(component) == 1 else min((entry for entry, _ in component), key=lambda entry: rank(entry, signature))
        flattened.append((chosen, members))
    by_games = {}
    content_merges = 0
    for chosen, members in prune_incidental_views(flatten_named_subseries(flattened, records), records):
        signature = frozenset(members.values())
        if len(signature) < 2:
            if diagnostic is not None:
                diagnostic.setdefault('discarded_groups', []).append({
                    'id': chosen[0], 'name': chosen[1], 'distinct_igdb_games': len(signature),
                    'titles': sorted(members), 'igdb_ids': sorted(signature)})
            continue
        previous = by_games.get(signature)
        if previous is None:
            by_games[signature] = (chosen, members)
        else:
            previous_entry, previous_members = previous
            winner = min(previous_entry, chosen, key=lambda entry: rank(entry, signature))
            previous_members.update(members)
            by_games[signature] = (winner, previous_members)
            content_merges += 1
            print(f'Consolidate identical game view ({len(signature)}): {previous_entry!r}, {chosen!r} -> {winner!r}')

    result = [{'id': key, 'name': name[:1].upper() + name[1:] if name.islower() else name, 'titles': sorted(members)}
              for (key, name), members in by_games.values()]
    result.sort(key=lambda g: (g['name'].casefold(), g['id']))
    print(f'Consolidation: {name_merges} name/source duplicates; {abbreviation_merges} abbreviations; {content_merges} identical-game views')
    return result


def validate_coverage(previous, result, catalog):
    # A successful consolidation reduces the number of groups, not the coverage
    # of existing catalog entries. Do not reject a valid deduplication as shrinkage.
    old_titles = {tid for group in previous.get('franchises', []) for tid in group.get('titles', [])}
    old_titles.intersection_update(catalog)
    new_titles = {tid for group in result['franchises'] for tid in group['titles']}
    retained = len(old_titles & new_titles)
    print(f'Coverage of previously indexed catalog IDs: {retained}/{len(old_titles)}')
    if len(old_titles) >= 20 and retained < len(old_titles)*0.75:
        raise RuntimeError('Unexpected index coverage loss; previous index preserved')


def is_game_record(game):
    kind = game.get('game_type') or {}
    return isinstance(kind, dict) and str(kind.get('type', '')).lower().replace('_', ' ') in ALLOWED_TYPES


def resolve_catalog_game(tid, variants, primary, aliases, ext):
    direct, exact, alternate = set(), set(), set()
    for tag, keys in variants:
        direct.update(ext.get((tag, tid), ()))
        for key in keys:
            found = primary.get((tag, key), ())
            if found:
                exact.update(found)
                break
        for key in keys:
            found = aliases.get((tag, key), ())
            if found:
                alternate.update(found)
                break
    if direct:
        # A platform-specific external ID is stronger than an alternate name.
        # Explicitly conflicting evidence is not guessed away.
        for evidence in (exact, alternate):
            if evidence:
                common = direct & evidence
                if common:
                    direct = common
                elif len(direct) > 1 or evidence is exact:
                    return direct | evidence
        return direct
    return exact or alternate


def plus_identity(value):
    return norm(value.replace('+', ' plus '))


def resolve_plus_variant(tid, candidates, catalog_variants, records, ext):
    if len(candidates) < 2 or any(ext.get((tag, tid)) for _, tag in catalog_variants):
        return candidates
    keys = set()
    for name, _ in catalog_variants:
        clean = TECH.sub(' ', name)
        clean = TRIAL.sub(' ', clean)
        if '+' not in clean:
            return candidates
        keys.add(plus_identity(clean))
    if len(keys) != 1:
        return candidates
    found = {gid for gid in candidates if '+' in records[gid].get('name', '') and plus_identity(records[gid]['name']) in keys}
    return found if len(found) == 1 else candidates


def attach_exact_series_roots(groups, pending, records, rejected_games=frozenset()):
    # An unclassified, matched game can be the original of an established
    # series. Only exact official names and established series qualify.
    by_name = defaultdict(list)
    for entry, members in groups.items():
        if members:
            by_name[franchise_name_key(entry[1])].append(entry)
    recovered = 0
    for gid, tids in sorted(pending.items()):
        if gid in rejected_games: continue
        name = records[gid].get('name', '')
        entries = [entry for entry in by_name.get(franchise_name_key(name), ()) if any(other != gid for other in groups[entry].values())]
        if not entries:
            continue
        # All candidates share the same normalized label and will be merged.
        entry = min(entries, key=franchise_label_order)
        groups[entry].update((tid, gid) for tid in tids)
        recovered += len(tids)
        print(f'Recover exact series root: {name} -> {entry[1]} ({len(tids)} catalog IDs)')
    return recovered


def attach_numbered_series(groups, pending, records, rejected_games=frozenset()):
    # Require an existing IGDB group, an explicit sequel marker immediately
    # after its label and at least one already named member with another ID.
    labels=defaultdict(list)
    for entry,members in groups.items():
        words=franchise_words(entry[1])
        if not words:
            continue
        ids=set(members.values())
        if len(ids)<2:
            continue
        anchors={gid for gid in ids if label_in_name(entry[1],records.get(gid,{}).get('name',''))}
        if len(words)>=2 and anchors:
            labels[words].append(entry)
        elif len(words)==1 and len(words[0])>=4:
            # Single-word series need two independently named members, at
            # least one with an explicit number, before inferring a sequel.
            named=[franchise_words(records.get(gid,{}).get('name','')) for gid in anchors]
            rooted=[title for title in named if title[:1]==words and (len(title)==1 or (len(title)>1 and ROMAN.get(title[1],title[1]).isdigit()))]
            if len(rooted)>=2 and any(len(title)>1 for title in rooted):
                labels[words].append(entry)
    recovered=0
    already={tid for members in groups.values() for tid in members}
    for gid,tids in sorted(pending.items()):
        if gid in rejected_games: continue
        tids=[tid for tid in tids if tid not in already]
        if not tids: continue
        words=tuple(franchise_words(records[gid].get('name','')))
        matches=[]
        for i in range(1,len(words)):
            number=ROMAN.get(words[i],words[i])
            if not number.isdigit() or not 2<=int(number)<=99: continue
            for entry in labels.get(words[:i],()):
                matches.append((i,entry))
        if not matches: continue
        best=max(i for i,_ in matches)
        entries=[entry for i,entry in matches if i==best]
        if len({franchise_name_key(entry[1]) for entry in entries})!=1: continue
        entry=min(entries,key=franchise_label_order)
        groups[entry].update((tid,gid) for tid in tids)
        recovered+=len(tids)
        print(f'Recover numbered series member: {records[gid].get("name")} -> {entry[1]} ({len(tids)} catalog IDs)')
    return recovered


def attach_exact_numbered_pairs(groups, pending, records, catalog, rejected_games):
    # Infer an original + explicit second installment only from distinct,
    # uniquely matched official names on a shared catalog platform.
    by_name = defaultdict(set)
    grouped = defaultdict(set)
    ids_to_tids = defaultdict(set)
    for entry, members in groups.items():
        for tid, gid in members.items():
            grouped[gid].add(entry)
            ids_to_tids[gid].add(tid)
    for gid, tids in pending.items():
        ids_to_tids[gid].update(tids)
    for gid in ids_to_tids:
        if gid not in rejected_games:
            by_name[franchise_words(records[gid].get('name', ''))].add(gid)
    tags = {gid: {tag for tid in tids for _, tag in catalog[tid]}
            for gid, tids in ids_to_tids.items()}
    labels = defaultdict(list)
    for entry in groups:
        labels[franchise_name_key(entry[1])].append(entry)
    recovered = 0
    for gid in sorted(pending):
        if gid in grouped or gid in rejected_games:
            continue
        words = franchise_words(records[gid].get('name', ''))
        if len(words) < 3 or ROMAN.get(words[-1], words[-1]) != '2':
            continue
        root_words = words[:-1]
        if root_words[-1] == 'chapter':
            root_words = root_words[:-1]
        if len(root_words) < 2:
            continue
        root_ids = by_name.get(root_words, set())
        # All equivalent second-installment spellings must identify one record.
        sequel_ids = set().union(*(by_name.get(root_words + suffix, ()) for suffix in
            (('2',), ('ii',), ('chapter', '2'), ('chapter', 'ii'))))
        if len(root_ids) != 1 or sequel_ids != {gid}:
            continue
        original = next(iter(root_ids))
        if original == gid or not tags[original] & tags[gid]:
            continue
        name = records[original]['name']
        key = franchise_name_key(name)
        known = labels.get(key, [])
        if grouped.get(original):
            if not known or any(franchise_name_key(entry[1]) != key for entry in grouped[original]):
                continue
            entries = [entry for entry in known if entry in grouped[original]]
            if not entries:
                continue
            entry = min(entries, key=franchise_label_order)
        else:
            if known or original not in pending:
                continue
            entry = ('s:' + str(original), name)
            labels[key].append(entry)
            groups[entry].update((tid, original) for tid in pending[original])
            grouped[original].add(entry)
            recovered += len(pending[original])
        groups[entry].update((tid, gid) for tid in pending[gid])
        grouped[gid].add(entry)
        recovered += len(pending[gid])
        print(f'Recover exact numbered pair: {name} -> {records[gid]["name"]}')
    return recovered


SERIES_SUBTITLE = re.compile(r'^(.+?):\s*(\S.+)$')
SERIES_SECOND = re.compile(r'^(.+?)\s+(?:2|II)$', re.I)
SERIES_EDITION = re.compile(r'\b(?:edition|collection|bundle|remaster(?:ed)?|compact|pack|version|deluxe|gold|goty|complete)\b', re.I)


def subtitle_parts(name):
    match = SERIES_SUBTITLE.fullmatch(name.strip())
    if not match:
        return None
    root, subtitle = match.groups()
    return franchise_name_key(root.replace('&', ' and ')), root, subtitle


def attach_subtitle_series(groups, pending, records, catalog, rejected_games):
    # Exact official series prefix, with independent evidence from known members.
    grouped = defaultdict(set)
    tags = defaultdict(set)
    for entry, members in groups.items():
        for tid, gid in members.items():
            grouped[gid].add(entry)
            tags[gid].update(tag for _, tag in catalog[tid])
    for gid, tids in pending.items():
        for tid in tids:
            tags[gid].update(tag for _, tag in catalog[tid])
    labels = defaultdict(list)
    for entry in groups:
        labels[franchise_name_key(entry[1].replace('&', ' and '))].append(entry)
    by_root = defaultdict(list)
    for gid in tags:
        if gid in rejected_games:
            continue
        name = records[gid].get('name', '')
        parts = subtitle_parts(name)
        if parts:
            by_root[parts[0]].append((gid, parts[1], parts[2]))
    recovered = 0
    for key, entries in sorted(labels.items()):
        candidates = by_root.get(key, ())
        if not candidates or len({franchise_name_key(e[1]) for e in entries}) != 1:
            continue
        members = set().union(*(set(groups[e].values()) for e in entries))
        anchors = {gid for gid, _, _ in candidates if gid in members}
        if not anchors:
            continue
        entry = min(entries, key=franchise_label_order)
        words = franchise_words(entry[1])
        if len(words) < 2 and (len(key) < 5 or len(anchors) < 2):
            continue
        anchor_tags = set().union(*(tags[gid] for gid in anchors))
        for gid, _, subtitle in candidates:
            if gid not in pending or grouped[gid] or not tags[gid] & anchor_tags:
                continue
            # Do not create a two-game series from an edition of its only member.
            if len(members) < 2 and SERIES_EDITION.search(subtitle):
                continue
            groups[entry].update((tid, gid) for tid in pending[gid])
            grouped[gid].add(entry)
            recovered += len(pending[gid])
            print(f'Recover anchored subtitle: {records[gid]["name"]} -> {entry[1]}')
    # A uniquely identified second installment can establish a series with a
    # uniquely identified subtitled original, on a shared catalog platform.
    seconds = defaultdict(list)
    exact_names = set()
    for gid in tags:
        name = records[gid].get('name', '')
        exact_names.add(franchise_name_key(name.replace('&', ' and ')))
        match = SERIES_SECOND.fullmatch(name)
        if match and len(franchise_words(match[1])) >= 2:
            seconds[franchise_name_key(match[1].replace('&', ' and '))].append(gid)
    for key, gids in sorted(seconds.items()):
        candidates = by_root.get(key, ())
        if key in labels or len(gids) != 1 or len(candidates) != 1:
            continue
        sequel = gids[0]
        original, root, subtitle = candidates[0]
        if sequel not in pending or grouped[sequel] or sequel in rejected_games or original == sequel or original not in pending or grouped[original] or not tags[original] & tags[sequel] or SERIES_EDITION.search(subtitle):
            continue
        # Avoid inventing a root if an exact-title original also exists.
        if key in exact_names:
            continue
        entry = ('s:' + str(original), root)
        for gid in (original, sequel):
            groups[entry].update((tid, gid) for tid in pending[gid])
            grouped[gid].add(entry)
            recovered += len(pending[gid])
        print(f'Recover subtitled numbered pair: {root} -> {records[sequel]["name"]}')
    return recovered


def attach_shared_series_names(groups, pending, records, catalog, rejected_games):
    # Require independent official names and shared catalog platforms. Never
    # override an existing assignment or treat regional IDs as distinct games.
    grouped = defaultdict(set)
    tags = defaultdict(set)
    for entry, members in groups.items():
        for tid, gid in members.items():
            grouped[gid].add(entry)
            tags[gid].update(tag for _, tag in catalog[tid])
    for gid, tids in pending.items():
        for tid in tids:
            tags[gid].update(tag for _, tag in catalog[tid])
    labels = defaultdict(list)
    for entry in groups:
        labels[franchise_name_key(entry[1])].append(entry)
    names_by_words = defaultdict(set)
    subtitles = defaultdict(list)
    suffixes = defaultdict(list)
    for gid in sorted(tags):
        name = records[gid].get('name', '')
        words = franchise_words(name)
        names_by_words[words].add(gid)
        if gid in rejected_games:
            continue
        parts = subtitle_parts(name)
        if parts:
            _, root, subtitle = parts
            subtitles[franchise_words(root)].append((gid, root, subtitle))
        if len(words) >= 3 and len(words[-1]) >= 3 and not SERIES_EDITION.search(words[-1]) and not words[-1].isdigit() and words[-1] not in ROMAN:
            suffixes[words[:-1]].append((gid, words[-1]))
    recovered = 0

    def attach(entry, gid):
        nonlocal recovered
        groups[entry].update((tid, gid) for tid in pending[gid])
        grouped[gid].add(entry)
        recovered += len(pending[gid])
        print(f'Recover shared series name: {records[gid]["name"]} -> {entry[1]}')

    # Extend a known multiword series only with exact word prefixes and
    # independent anchors, or three distinct single-word installment suffixes.
    for key, entries in sorted(labels.items()):
        entry = min(entries, key=franchise_label_order)
        root = franchise_words(entry[1])
        if len(root) < 2 or len({franchise_words(e[1]) for e in entries}) != 1:
            continue
        members = set().union(*(set(groups[e].values()) for e in entries))
        anchors = {gid for gid, _, sub in subtitles.get(root, ()) if gid in members and not SERIES_EDITION.search(sub)}
        single = suffixes.get(root, ())
        plain_anchors = {gid for gid, _ in single if gid in members}
        candidates = [(gid, sub) for gid, sub in single if gid in pending and not grouped[gid] and len(names_by_words[root + (sub,)]) == 1]
        if len(anchors) >= 2:
            anchor_tags = set().union(*(tags[gid] for gid in anchors))
            for gid, _ in candidates:
                if tags[gid] & anchor_tags:
                    attach(entry, gid)
        elif len(root) >= 3 and plain_anchors and len(candidates) >= 2:
            for tag in sorted(set().union(*(tags[gid] for gid in plain_anchors))):
                compatible = [(gid, sub) for gid, sub in candidates if tag in tags[gid] and not grouped[gid]]
                anchor_suffixes = {sub for gid, sub in single if gid in plain_anchors and tag in tags[gid]}
                if anchor_suffixes and len({sub for _, sub in compatible} - anchor_suffixes) >= 2:
                    for gid, sub in compatible:
                        if sub not in anchor_suffixes:
                            attach(entry, gid)
    # A new subtitle-only series needs a distinctive shared root, different
    # subtitles, no conflicting assignments and a platform common to all.
    for root, candidates in sorted(subtitles.items()):
        if len(root) < 2 or len(candidates) < (2 if len(root) >= 3 else 3):
            continue
        if len(''.join(root)) < 8 or names_by_words.get(root):
            continue
        gid, label, _ = candidates[0]
        if franchise_name_key(label) in labels:
            continue
        if any(g not in pending or grouped[g] or SERIES_EDITION.search(sub) for g, _, sub in candidates):
            continue
        identities = {franchise_words(sub) for _, _, sub in candidates}
        if len(identities) != len(candidates) or any(not identity for identity in identities):
            continue
        if not set.intersection(*(tags[g] for g, _, _ in candidates)):
            continue
        entry = ('s:' + str(gid), label)
        for g, _, _ in candidates:
            attach(entry, g)
    return recovered


def attach_linked_versions(groups, pending, records, rejected_games):
    # Explicit IGDB version/port/remake links, never similarity recommendations,
    # generic parent_game (which can be a bundle) or shared publisher alone.
    grouped=defaultdict(set)
    for entry,members in groups.items():
        for gid in set(members.values()):
            grouped[gid].add(entry)
    linked=defaultdict(set)
    for gid,game in records.items():
        neighbors=[game.get('version_parent')]
        for field in ('remakes','remasters','ports','expanded_games','standalone_expansions'):
            neighbors.extend(game.get(field) or [])
        for other in neighbors:
            if type(other) is int and other != gid and other in records:
                linked[gid].add(other)
                linked[other].add(gid)
    # A single pass over the original assignments: no chain of inferred links.
    decisions=[]
    for gid,tids in sorted(pending.items()):
        if grouped.get(gid) or gid in rejected_games:
            continue
        entries=set().union(*(grouped.get(other, ()) for other in linked.get(gid, ())))
        if not entries or len({franchise_name_key(e[1]) for e in entries}) != 1:
            continue
        eligible=[e for e in entries if len(set(groups[e].values())) >= 2 and any(
            label_in_name(e[1], records[other].get('name','')) for other in linked[gid] if e in grouped.get(other, ()))]
        if eligible:
            decisions.append((min(eligible,key=franchise_label_order),gid,tids))
    for entry,gid,tids in decisions:
        groups[entry].update((tid,gid) for tid in tids)
        print(f'Recover explicit IGDB version link: {records[gid]["name"]} -> {entry[1]}')
    return sum(len(tids) for _,_,tids in decisions)


def attach_external_version_parents(groups, pending, records, rejected_games, parents):
    # A parent may be in the IGDB snapshot without being sold on PSN. Its
    # explicit franchise can still establish a version's membership, but only
    # if that franchise already has at least two independent catalog games.
    grouped={gid for members in groups.values() for gid in members.values()}
    labels=defaultdict(list)
    for entry,members in groups.items():
        if len(set(members.values()))>=2:
            labels[entry[0]].append(entry)
    decisions=[]
    for gid,tids in sorted(pending.items()):
        if gid in grouped or gid in rejected_games: continue
        parent=records[gid].get('version_parent')
        if type(parent) is not int or parent in grouped or parent==gid: continue
        evidence=parents.get(parent)
        if not evidence: continue
        entries={entry for key,name in evidence for entry in labels.get(key, ()) if franchise_name_key(entry[1])==franchise_name_key(name)}
        if len({franchise_name_key(e[1]) for e in entries}) != 1: continue
        decisions.append((min(entries,key=franchise_label_order),gid,tids))
    for entry,gid,tids in decisions:
        groups[entry].update((tid,gid) for tid in tids)
        print(f'Recover non-catalog IGDB version parent: {records[gid]["name"]} -> {entry[1]}')
    return sum(len(tids) for _,_,tids in decisions)


def attach_publisher_subtitle_pairs(groups, pending, records, catalog, rejected_games):
    # New pairs require a unique original title, a distinct subtitle, the
    # same official publisher, a shared catalog platform and no existing
    # memberships. An edition alone never establishes a new franchise.
    occupied = {gid for members in groups.values() for gid in members.values()}
    free = set(pending) - occupied - rejected_games
    roots = defaultdict(set)
    tags = {gid: {tag for tid in pending[gid] for _, tag in catalog[tid]} for gid in free}
    for gid in free:
        words = extended_title_words(records[gid].get('name',''))
        if words and sum(map(len,words)) >= 7:
            roots[words].add(gid)
    proposals = []
    labels = {franchise_name_key(entry[1]) for entry in groups}
    for gid in sorted(free):
        title = records[gid].get('name','')
        parts = subtitle_parts(title)
        if not parts:
            continue
        _, label, subtitle = parts
        if SERIES_EDITION.search(subtitle) or len(extended_title_words(subtitle)) < 2:
            continue
        candidates = roots.get(extended_title_words(label), set()) - {gid}
        if len(candidates) != 1:
            continue
        other = next(iter(candidates))
        if not tags[gid] & tags[other] or not publisher_prefixes(records[gid]) & publisher_prefixes(records[other]):
            continue
        if franchise_name_key(label) in labels:
            continue
        proposals.append((gid,other,label))
    counts = Counter(g for a,b,_ in proposals for g in (a,b))
    recovered = 0
    for a,b,label in proposals:
        if counts[a] != 1 or counts[b] != 1:
            continue
        entry = ('s:'+str(b),label)
        for gid in (a,b):
            groups[entry].update((tid,gid) for tid in pending[gid])
            recovered += len(pending[gid])
        print(f'Recover publisher-supported subtitle pair: {label}')
    return recovered


def attach_extended_series(groups, pending, records, catalog, rejected_games):
    # Distinctive exact shared title roots. Inferred proposals compete globally;
    # a title with multiple plausible series is not assigned by iteration order.
    grouped=defaultdict(set); tags=defaultdict(set)
    for entry,members in groups.items():
        for tid,gid in members.items():
            grouped[gid].add(entry)
            tags[gid].update(tag for _,tag in catalog[tid])
    for gid,tids in pending.items():
        for tid in tids:
            tags[gid].update(tag for _,tag in catalog[tid])
    names={gid:franchise_words(records[gid].get('name','')) for gid in tags}
    by_prefix=defaultdict(set)
    for gid,words in names.items():
        for i in range(2,len(words)):
            if i<=8 and sum(map(len,words[:i]))>=8:
                by_prefix[words[:i]].add(gid)
    labels=defaultdict(list)
    for entry in groups:
        labels[franchise_words(entry[1])].append(entry)
    free={gid for gid in pending if not grouped.get(gid) and gid not in rejected_games}
    proposals=defaultdict(set)
    # Existing series: two independent members establish a shared alternative
    # root (e.g. regional titles), or a singleton plus an explicit first marker.
    for root,ids in by_prefix.items():
        possible=ids & free
        if not possible: continue
        known=ids-free-rejected_games
        anchors=defaultdict(set)
        for gid in known:
            for entry in grouped.get(gid, ()):
                anchors[entry].add(gid)
        for entry,gg in anchors.items():
            label=franchise_words(entry[1])
            if label != root:
                if len(root)<3 or sum(map(len,root))<12 or len(gg)<2 or root[0] in ('the','a','an'):
                    continue
                if not all((parts:=subtitle_parts(records[g]['name'])) and franchise_words(parts[1])==root for g in gg):
                    continue
            for gid in possible:
                suffix=names[gid][len(root):]
                same=[other for other in gg if tags[gid]&tags[other]]
                if not same or not suffix: continue
                if any(names[other]==names[gid] for other in same): continue
                is_edition=bool(SERIES_EDITION.search(' '.join(suffix)))
                explicit_first=suffix in (('first',),('1',),('i',))
                subtitle_anchor=any((parts:=subtitle_parts(records[other]['name'])) and franchise_words(parts[1])==root for other in same)
                if len(same)>=2 or (explicit_first and subtitle_anchor and not is_edition):
                    proposals[gid].add(entry)
    # New two-game series: one official title has a colon, the other shares
    # the exact root and a single substantive suffix. No generic label or
    # competing root, no version/edition marker, and a shared platform.
    new_pairs=[]
    for root,ids in by_prefix.items():
        if root in labels or len(ids)!=2 or not ids <= free or len(root)<2: continue
        a,b=sorted(ids)
        if not tags[a]&tags[b] or any(names[g]==root for g in ids): continue
        suffixes=[names[g][len(root):] for g in (a,b)]
        if any(not sub or SERIES_EDITION.search(' '.join(sub)) for sub in suffixes): continue
        if any(names[g]==root+('first',) for g in ids): continue
        colon=[g for g in (a,b) if (parts:=subtitle_parts(records[g]['name'])) and franchise_words(parts[1])==root]
        if len(colon)!=1: continue
        original=colon[0]; other=b if original==a else a
        suffix=names[other][len(root):]
        if len(suffix)!=1 or len(suffix[0])<4 or not suffix[0].isalpha() or suffix[0] in ROMAN: continue
        if sum(map(len,root))<9 or any(proposals.get(g) for g in ids): continue
        new_pairs.append((root,original,other))
    pair_counts=Counter(g for _,a,b in new_pairs for g in (a,b))
    decisions=[]
    for gid,entries in proposals.items():
        if len({franchise_name_key(e[1]) for e in entries})==1:
            decisions.append((min(entries,key=franchise_label_order),gid))
    for root,a,b in new_pairs:
        if pair_counts[a]!=1 or pair_counts[b]!=1: continue
        label=subtitle_parts(records[a]['name'])[1]
        entry=('s:'+str(a),label)
        decisions.extend(((entry,a),(entry,b)))
    for entry,gid in sorted(decisions):
        groups[entry].update((tid,gid) for tid in pending[gid])
        print(f'Recover extended series root: {records[gid]["name"]} -> {entry[1]}')
    return sum(len(pending[gid]) for _,gid in decisions)



# Conservative final-stage title recovery. Keep installment numbers and plus
# markers, and never turn a named DLC or a promotional SKU into its base game.
TITLE_FILLERS = frozenset(('the','of','a','an','in','on','for','to','s'))
SUBTITLE_BLOCK = re.compile(r"\b(?:demo|beta|trial|trailer|soundtrack|album|avatar|patch|upgrade|season pass|skin pack|costume|early access|multiplayer|add.on|dlc)\b", re.I)
TITLE_SEPARATOR = re.compile(r'[:–—]|\s-\s')
INSTALL_PART = re.compile(r'\s*[-–—]?\s*part\s+\d+\s*\(install both parts separately\)\s*$',re.I)

def final_title_keys(name, catalog=False, labels=()):
    raw = recovery_base(name,catalog)
    if catalog:
        raw = INSTALL_PART.sub('',raw).strip()
        if SUBTITLE_BLOCK.search(TRIAL.sub('',raw)):
            return set()
    words = extended_title_words(raw,catalog)
    if catalog:
        suffix=re.fullmatch(r'(.+?)\s*\(([A-Z]{3,8})\)',raw)
        if suffix:
            base=extended_title_words(suffix[1],True)
            if ''.join(w[0] for w in base if w).upper()==suffix[2]:
                words=base
    core = tuple(w for w in words if w not in TITLE_FILLERS)
    result = set()
    if len(core)>=2 and sum(map(len,core))>=9:
        result.add(('ordered',core))
    if len(core)>=3 and sum(map(len,core))>=10:
        # Reordering is only allowed for an explicit title/subtitle delimiter
        # on the catalog side, or the exact same substantive word sequence.
        if not catalog or TITLE_SEPARATOR.search(raw):
            result.add(('reordered',tuple(sorted(core))))
    if catalog and len(words)>=2:
        # Explicit regional names are alternatives, never arbitrary slash
        # separated bundles. Retain all candidates to expose conflicts.
        dual=re.fullmatch(r'(.+?)\s*\((?:EU|US|UK|USA|EUR|JPN)\)\s*/\s*(.+?)\s*\((?:EU|US|UK|USA|EUR|JPN)\)',raw,re.I)
        if dual:
            for part in dual.groups():
                result.add(('literal',extended_title_words(part,True)))
        for suffix in (('premium','edition'),('complete','edition'),('complete',),('full','series')):
            if words[-len(suffix):]==suffix and len(words)-len(suffix)>=2 and sum(map(len,words[:-len(suffix)]))>=10:
                result.add(('literal',words[:-len(suffix)]))
        result.add(('literal',words))
        result.add(('numbered',words))
        if len(core)>=2 and sum(map(len,core))>=10:
            result.add(('abbreviated',words))
    # A short function word may be misspelled without changing the distinctive
    # title. Match a dropped letter only (no substitute or changed number).
    for i,w in enumerate(words):
        if len(w)>=2 and w.isalpha() and sum(map(len,words))-len(w)>=10:
            if catalog and any(w==x[:j]+x[j+1:] for x in TITLE_FILLERS if len(x)==len(w)+1 for j in range(len(x))):
                for x in TITLE_FILLERS:
                    if len(x)==len(w)+1 and any(w==x[:j]+x[j+1:] for j in range(len(x))):
                        result.add(('literal',words[:i]+(x,)+words[i+1:]))
    if not catalog:
        result.add(('literal',words))
        # A volume designation is optional, its number is not.
        for i,w in enumerate(words[:-1]):
            if w in ('vol','volume') and words[i+1].isdigit() and i>=2:
                result.add(('literal',words[:i]+words[i+1:]))
    if not catalog:
        parts=subtitle_parts(raw)
        if parts:
            prefix=extended_title_words(parts[1])
            if len(prefix)>=2 and sum(map(len,prefix))>=8 and prefix[-1].isdigit():
                result.add(('numbered',prefix))
            if labels and len(prefix)>=2 and sum(map(len,prefix))>=10 and any(label_in_name(label,parts[1]) and franchise_name_key(label)!=franchise_name_key(parts[1]) for label in labels):
                result.add(('abbreviated',prefix))
    return result


def resolve_linked_port_family(tid, candidates, variants, records, ext):
    # Explicit ports of the same named game are one identity, not competing
    # sequels. Only collapse when one official parent links every candidate.
    if len(candidates)<2 or any(ext.get((tag,tid)) for _,tag in variants):
        return candidates
    if len({norm(records[g]['name']) for g in candidates}) != 1:
        return candidates
    # A conflicting catalog record must never be resolved by this rule.
    if len({extended_title_words(catalog_match_name(n,tid),True) for n,_ in variants})!=1:
        return candidates
    roots=[]
    for gid in candidates:
        game=records[gid]
        ports=set(game.get('ports') or ())
        if candidates-{gid} <= ports and all((records[other].get('game_type') or {}).get('type','').lower()=='port' for other in candidates-{gid}):
            roots.append(gid)
    if len(roots)==1:
        return {roots[0]}
    return candidates


def attach_verified_series_versions(groups,pending,records,catalog,rejected_games):
    # Existing series only. A publisher-supported exact game root + edition
    # or a shared regional root established by multiple distinct members.
    occupied=defaultdict(set); tagsets=defaultdict(set)
    for entry,members in groups.items():
        for tid,gid in members.items():
            occupied[gid].add(entry)
            tagsets[gid].update(tag for _,tag in catalog[tid])
    free=set(pending)-set(occupied)-rejected_games
    publishers={g:publisher_prefixes(records[g]) for g in set(occupied)|free}
    words={g:extended_title_words(records[g]['name']) for g in publishers}
    anchors=defaultdict(set)
    for gid in occupied:
        w=words[gid]
        for i in range(1,len(w)+1):
            if sum(map(len,w[:i]))>=7:
                anchors[w[:i]].add(gid)
    decisions=[]
    for gid in sorted(free):
        w=words[gid]; tags={tag for tid in pending[gid] for _,tag in catalog[tid]}
        proposals=set()
        for i in range(1,len(w)):
            root=w[:i]; suffix=w[i:]
            candidates={g for g in anchors.get(root,()) if publishers[gid]&publishers[g] and tags&tagsets[g]}
            if not candidates:continue
            # Explicit remaster/edition or first game with a verified sequel.
            exact={g for g in candidates if words[g]==root}
            if exact and SERIES_EDITION.search(' '.join(suffix)):
                proposals.update(e for g in exact for e in occupied[g] if len(set(groups[e].values())-{g})>=1)
            if len(root)>=3 and len(candidates)>=2:
                # The same regional subtitle root, independently supported
                # by two games, not just a common publisher/product line.
                roots={g for g in candidates if (parts:=subtitle_parts(records[g]['name'])) and extended_title_words(parts[1])==root}
                if len(roots)>=2:
                    proposals.update(e for e in occupied[next(iter(roots))] if all(e in occupied[g] for g in roots))
        for g in anchors.get(w,()):
            if words[g]==w+('2',) and publishers[gid]&publishers[g] and tags&tagsets[g]:
                proposals.update(occupied[g])
        if len({franchise_name_key(e[1]) for e in proposals})==1:
            decisions.append((min(proposals,key=franchise_label_order),gid))
    for entry,gid in decisions:
        groups[entry].update((tid,gid) for tid in pending[gid])
        print(f'Recover verified series version: {records[gid]["name"]} -> {entry[1]}')
    return sum(len(pending[gid]) for _,gid in decisions)


# Last-stage recovery for explicit release labels, verified subtitle variants
# and short spelling differences. Earlier exact matches always take priority.
EXTRA_RELEASE_LABEL = re.compile(r'\s*(?:\(\s*(?:download\s+ver\.?|(?:chinese|korean|japanese|english|french|german|spanish|italian)\s+(?:sub|ver\.?|version)|(?:with\s+[A-Z0-9]+\s+story))\s*\)|\s+\(digital download\)|\s+playstation\s*3\s+edition|\s+ps[123x]\s*$|\s+製品版|\s+完全版)\s*$',re.I)
SCRIPT_BOUNDARY = re.compile(r'(?<=[\u3040-\u30ff\u3400-\u9fff])(?=[A-Za-z0-9])|(?<=[A-Za-z0-9])(?=[\u3040-\u30ff\u3400-\u9fff])')
EXTRA_NON_GAME = re.compile(r'\b(?:early release|early access|preview|unlock|add.on|dlc|patch|upgrade|trial|demo|beta|avatar|costume|soundtrack|album|manual|multiplayer|starter pack|season pass)\b|(?:解説書|サウンドトラック|サントラ|リハーサル版|解除キー|デモ版|데모)',re.I)
EDITION_TYPES = frozenset(('port','remaster','remake','expanded game','bundle'))


def supplemental_words(value,catalog=False):
    raw=recovery_base(value,catalog)
    if catalog:
        for _ in range(3):
            new=EXTRA_RELEASE_LABEL.sub('',raw).strip()
            if new==raw:break
            raw=new
    return extended_title_words(SCRIPT_BOUNDARY.sub(' ',raw),catalog)


def supplemental_title_keys(value,catalog=False,labels=(),kind='',japanese=False):
    raw=recovery_base(value,catalog)
    if catalog and EXTRA_NON_GAME.search(raw):return set()
    words=supplemental_words(value,catalog)
    if not words:return set()
    variants={words}
    if catalog:
        # Display branding repeated as a full acronym is redundant only when
        # the other words independently spell that acronym.
        if len(words)>=4 and len(words[0])>=3:
            tail=words[1:]; text=tuple(w for w in tail if not w.isdigit())
            if ''.join(w[0] for w in text)==words[0]:
                variants.add(text+tuple(w for w in tail if w.isdigit()))
        # 3D is a display qualifier, not installment 3. Require a subtitle.
        for i,w in enumerate(words):
            if w=='3d' and i and len(words[i+1:])>=2 and sum(map(len,words[i+1:]))>=8:
                variants.add(words[:i]+words[i+1:])
    else:
        for label in labels:
            root=supplemental_words(label)
            if not root or words[:len(root)]!=root:continue
            tail=words[len(root):]
            if len(tail)>=3 and (tail[0].isdigit() or tail[0] in ('z','xx','3d')) and sum(map(len,tail[1:]))>=9:
                # Distinctive complete subtitle remains; never substitute one
                # explicit installment number for another.
                variants.add(root+tail[1:])
            if kind in EDITION_TYPES and len(tail)>=2:
                for i,w in enumerate(words):
                    if w=='edition' and i==len(words)-1:
                        for count in (2,3):
                            base=words[:-count]
                            if len(base)>len(root) and (sum(map(len,base))>=9 or any(x.isdigit() for x in base)):
                                variants.add(base)
                        if len(words)>=len(root)+4 and words[-3] in ('ultimate','special','limited','collector','deluxe'):
                            variants.add(words[:-2])
                if tail[-1] in ('hd','remastered','remaster','deluxe') and len(words)>len(root)+1:
                    variants.add(words[:-1])
    # Romanized Japanese particles are orthographic variants, not dropped
    # words; retain numeric installments and require a long complete title.
    keys=set()
    for w in variants:
        total=sum(map(len,w))
        if total>=8 or (len(w)>=2 and w[-1].isdigit() and total>=6):
            keys.add(('exact',''.join(w)))
        if japanese and len(w)>=3 and total>=13 and all(x.isascii() for x in w):
            normalized=tuple({'wo':'o','he':'e'}.get(x,x) for x in w)
            keys.add(('jp',romanized_japanese_key(normalized) or ''.join(normalized)))
    return keys


@lru_cache(maxsize=8192)
def catalog_identity_words(value, catalog=False):
    raw = recovery_base(value, catalog)
    if catalog and (EXTRA_NON_GAME.search(raw) or SUBTITLE_BLOCK.search(raw)):
        return ()
    words = supplemental_words(value, catalog)
    if not words:
        return ()
    # Generic media suffixes are not distinguishing subtitles. Keep every
    # numeral, content word and version marker in the underlying title.
    for suffix in (('the','video','game'), ('the','videogame'), ('video','game'), ('videogame',)):
        if words[-len(suffix):] == suffix and len(words) > len(suffix)+1:
            words = words[:-len(suffix)]
            break
    return tuple(w for w in words if w != 'the')


JAPANESE_SCRIPT = re.compile(r'[\u3040-\u30ff\u3400-\u9fff]')
JAPANESE_HD = re.compile(r'(?<![a-z])hd(?![a-z])',re.I)


def japanese_hd_key(value, catalog=False):
    text=unicodedata.normalize('NFKC',recovery_base(value,catalog)).casefold()
    if not JAPANESE_SCRIPT.search(text) or (catalog and (not JAPANESE_HD.search(text) or EXTRA_NON_GAME.search(text))):
        return None
    text=JAPANESE_HD.sub('',text).replace('アンド','&')
    # Do not strip voiced sound marks: Japanese kana identity depends on them.
    key=''.join(c for c in text if c.isalnum() or c=='&')
    return ('jp_hd',key) if len(key)>=7 else None


def mixed_crossover_keys(official, localized):
    # Construct mixed-script crossover titles only from the two complete
    # documented names for the very same game and the same explicit joiner.
    def parts(title):
        value=unicodedata.normalize('NFKC',title)
        return re.fullmatch(r'(.+?)\s+[xX]\s+(.+)',value)
    base=parts(official)
    if not base or JAPANESE_SCRIPT.search(official):
        return set()
    out=set()
    for title in localized:
        alt=parts(title)
        if not alt or not all(JAPANESE_SCRIPT.search(x) for x in alt.groups()):
            continue
        for candidate in (base[1]+' X '+alt[2],alt[1]+' X '+base[2]):
            w=catalog_identity_words(candidate)
            if len(w)>=3 and sum(map(len,w))>=9:
                out.add(('packaging',''.join(w)))
    return out


def last_identity_keys(value, catalog=False, labels=(), kind=''):
    words = catalog_identity_words(value, catalog)
    if not words:
        return set()
    variants = {words}
    # These are display/packaging qualifiers, not arbitrary subtitle words.
    # Preserve numbered installments, and leave edition ambiguities unresolved.
    if words[-1] in ('edition','3d') and len(words) >= 3 and sum(map(len, words[:-1])) >= 8:
        variants.add(words[:-1])
    if words[-1]=='hd' and len(words)>=3 and sum(map(len,words[:-1]))>=9:
        variants.add(words[:-1])
    if not catalog and kind in ('remake','remaster','port'):
        # A platform-specific rerelease can be sold under the original title.
        # Remove only the explicitly documented standard rerelease suffix.
        for suffix in (('special','edition'),('anniversary','edition'),('hd','edition'),('remastered',)):
            if words[-len(suffix):]==suffix:
                base=words[:-len(suffix)]
                if suffix==('anniversary','edition') and base and re.fullmatch(r'\d+(?:st|nd|rd|th)',base[-1]):
                    base=base[:-1]
                if len(base)>=2 and sum(map(len,base))>=9:
                    variants.add(base)
    if not catalog:
        for label in labels:
            root = catalog_identity_words(label)
            if not root or len(root) < 3 or words[:len(root)] != root:
                continue
            acronym = ''.join(w[0] for w in root)
            tail = words[len(root):]
            if tail and sum(map(len,tail)) >= 8 and all(w.isascii() for w in root):
                variants.add((acronym,)+tail)
                # A metadata alias may have an explicit number after the
                # acronym; only use the full distinctive subtitle without it.
                if tail[0].isdigit() and sum(map(len,tail[1:])) >= 12:
                    variants.add((acronym,)+tail[1:])
    result={('packaging', ''.join(w)) for w in variants if len(w)>=2 and sum(map(len,w))>=9}
    # Single-word HD store listings are safe only with a distinctive complete
    # title and a literal HD qualifier on one side. Never strip a number.
    if catalog and len(words)==2 and words[-1]=='hd' and words[0].isalpha() and len(words[0])>=6:
        result.add(('single_hd',words[0]))
    if not catalog and len(words)==1 and words[0].isalpha() and len(words[0])>=6:
        result.add(('single_hd',words[0]))
    if catalog or kind=='remaster':
        jp=japanese_hd_key(value,catalog)
        if jp:result.add(jp)
    return result


def identity_typo_slots(words):
    if len(words)<3 or sum(map(len,words))<14:
        return ()
    return tuple((words[:i],words[i+1:],word) for i,word in enumerate(words)
                 if word.isascii() and word.isalpha() and len(word)>=5
                 and sum(len(w) for j,w in enumerate(words) if j!=i and w.isalpha())>=8)


def identity_title_variants(name, labels):
    words = catalog_identity_words(name)
    result = {words}
    # Publisher/creator prefixes are removable only when the remaining
    # official title starts with a recorded series label.
    if re.match(r"^.+?[’']s\s+",name):
        for label in labels:
            root = catalog_identity_words(label)
            if len(root)<2:
                continue
            for i in range(1,min(5,len(words))):
                if words[i:i+len(root)]==root:
                    result.add(words[i:])
    return result


def verified_extended_brand_keys(official,aliases,labels):
    parts=subtitle_parts(official)
    if not parts:
        return set()
    _,prefix,subtitle=parts
    w=catalog_identity_words(prefix);tail=catalog_identity_words(subtitle)
    if not w or sum(map(len,tail))<8:
        return set()
    roots={catalog_identity_words(label) for label in labels}
    alt={catalog_identity_words(name) for name in aliases}
    out=set()
    for root in roots:
        if len(root)<2 or w[:len(root)]!=root or len(w)<=len(root):
            continue
        rest=w[len(root):]
        for long in roots:
            if len(long)!=len(root)+1 or long[:-1]!=root or not all(x.isalpha() and x.isascii() for x in long):
                continue
            acronym=''.join(x[0] for x in long)
            if (acronym,)+rest in alt:
                out.add(('packaging',''.join(long+rest+tail)))
    return out


def verified_camelcase_number_keys(official, aliases, labels):
    parts=subtitle_parts(official)
    if not parts:
        return set()
    _, prefix, subtitle=parts
    root=catalog_identity_words(prefix);tail=catalog_identity_words(subtitle)
    if len(root)!=1 or len(tail)<2 or sum(map(len,tail))<10:
        return set()
    initials=''.join(re.findall(r'[A-Z]',prefix))
    if not 2<=len(initials)<=4 or not any(catalog_identity_words(label)==root for label in labels):
        return set()
    result=set()
    for alias in aliases:
        m=re.fullmatch(re.escape(initials.lower())+r'(\d+)',norm(alias))
        if m:
            result.add(('packaging',''.join(root+(m[1],)+tail)))
    return result


def confirmed_omitted_word_index(catalog,catalog_keys,primary,aliases,ext,records):
    # Require another exact, already confirmed regional catalog release and
    # an official title/series relationship. No fuzzy title similarity alone.
    anchors=defaultdict(set)
    for tid,prepared in catalog_keys.items():
        found=resolve_catalog_game(tid,prepared,primary,aliases,ext)
        if len(found)!=1:
            continue
        gid=next(iter(found));official=catalog_identity_words(records[gid]['name'])
        for name,tag in catalog[tid]:
            if official and catalog_identity_words(catalog_match_name(name,tid),True)==official:
                anchors[gid,tag].add(tid[:4])
    index=defaultdict(set)
    for (gid,tag),regions in anchors.items():
        game=records[gid];w=catalog_identity_words(game['name'])
        if len(w)<5 or not any(x.isdigit() for x in w):
            continue
        accepted,_=select_game_groups(game)
        roots={catalog_identity_words(label) for _,label in accepted}
        # At least one official label and a numeric installment must precede
        # the omitted word; the complete remaining suffix is retained.
        for i in range(2,len(w)-1):
            if not w[i].isalpha() or len(w[i])<3 or SERIES_EDITION.search(w[i]):
                continue
            prefix=w[:i];suffix=w[i+1:]
            if not any(x.isdigit() for x in prefix) or sum(map(len,suffix))<5:
                continue
            if not any(root and any(prefix[j:j+len(root)]==root for j in range(len(prefix))) for root in roots):
                continue
            # Require an explicit short alternate title, or a technical
            # rerelease suffix, independently distinguishing this installment.
            short=any(a==prefix or (len(a)>=3 and a[-1].isdigit() and len(prefix)==len(a)+1 and prefix[:len(a)]==a)
                      for n in game_names(game)[1:] if (a:=catalog_identity_words(n)))
            technical=suffix in (('hd','remix'),('hd','remaster'),('hd','edition'))
            if short or technical:
                index[tag,prefix+suffix].update((gid,region) for region in regions)
    return index


def resolve_omitted_word(tid,variants,index):
    evidence=[]
    for name,tag in variants:
        key=catalog_identity_words(catalog_match_name(name,tid),True)
        evidence.append({gid for gid,region in index.get((tag,key),()) if region!=tid[:4]})
    return set.intersection(*evidence) if evidence and all(evidence) else set()


def resolve_mixed_crossover_parent(tid,candidates,variants,records,ext):
    if len(candidates)<2 or any(ext.get((tag,tid)) for _,tag in variants):
        return candidates
    names={catalog_identity_words(catalog_match_name(n,tid),True) for n,_ in variants}
    if len(names)!=1 or not next(iter(names)):
        return candidates
    key=('packaging',''.join(next(iter(names))))
    choices=set()
    for gid in candidates:
        game=records[gid]
        if key not in mixed_crossover_keys(game['name'],localized_game_names(game)):
            continue
        if all(records[child].get('version_parent')==gid and SERIES_EDITION.search(records[child]['name']) for child in candidates-{gid}):
            choices.add(gid)
    return choices if len(choices)==1 else candidates


def release_distribution_identity(tid, candidates, variants, records, ext, digital_named):
    if len(candidates)<2 or any(ext.get((tag,tid)) for _,tag in variants):
        return candidates
    titles = {catalog_identity_words(catalog_match_name(n,tid),True) for n,_ in variants}
    if len(titles)!=1 or not next(iter(titles)):
        return candidates
    w = next(iter(titles))
    tags = {tag for _,tag in variants}
    if any(catalog_identity_words(records[g]['name']) != w for g in candidates):
        return candidates
    # The PSN PS1 Classics SKU refers to the emulated PS1 release, not a
    # separate PS1-only regional record. Require both download platforms.
    if tags == {'psx'} and tid.startswith(('NPEF','NPJF','NPJJ','NPUJ','NPUI','NPHI')):
        digital = {g for g in candidates if {7,9,38} <= set(records[g].get('platforms') or ())}
        if len(digital)==1 and all(7 in records[g].get('platforms',()) for g in candidates):
            return digital
    # A linked version carrying an explicit PlayStation Store listing is
    # more specific than the original multi-platform record with no listing.
    if tags == {'ps3'}:
        digital = candidates & digital_named
        if len(digital)==1:
            gid=next(iter(digital))
            if all(gid in explicit_version_links(records[other]) for other in candidates-{gid}):
                return digital
    return candidates


def supplemental_typo_keys(key):
    if len(key)<10 or len(key)>80 or not key.isascii():return ()
    numbers=tuple(re.findall(r'\d+',key))
    return tuple(hash((numbers,key[:i]+key[i+1:])) for i,c in enumerate(key) if c.isalpha())+(hash((numbers,key)),)



def strict_full_title_typo(a,b):
    if not one_typo(a,b) or re.findall(r'\d+',a)!=re.findall(r'\d+',b):return False
    # A changed qualifier attached to a number (2X, 2D, 3D, etc.) is not a typo.
    if re.findall(r'\d+[a-z]?',a)!=re.findall(r'\d+[a-z]?',b):return False
    # The exact one-character edit must be in letters, not a number boundary.
    if abs(len(a)-len(b))==1:
        short,long=sorted((a,b),key=len)
        i=next((i for i,(x,y) in enumerate(zip(short,long)) if x!=y),len(short))
        if (i and long[i-1].isdigit()) or (i+1<len(long) and long[i+1].isdigit()):return False
    return True


def corroborated_candidates(variants, lookup):
    # Multiple genuinely different catalog names must independently support
    # the same game. Missing evidence is not a vote for the other name.
    evidence=[set(lookup(variant)) for variant in variants]
    if not evidence or not all(evidence):return set()
    union=set().union(*evidence)
    common=set.intersection(*evidence)
    return common if common else union


def resolve_release_ambiguity(tid,candidates,variants,records,ext,classic_confirmed=frozenset()):
    if len(candidates)<2 or any(ext.get((tag,tid)) for _,tag in variants):return candidates
    nameset={supplemental_words(re.sub(r'\s+digital edition\s*$','',catalog_match_name(n,tid),flags=re.I),True) for n,_ in variants}
    tags={tag for _,tag in variants}
    if len(nameset)!=1:return candidates
    w=next(iter(nameset))
    # A named standard game is not its separately named limited/collector
    # edition. Require an explicit parent relationship and exact parent name.
    for gid in sorted(candidates):
        if all(records[x].get('version_parent')==gid for x in candidates-{gid}):
            children=candidates-{gid}
            if SERIES_EDITION.search(' '.join(w)):
                editions={x for x in children if SERIES_EDITION.search(records[x]['name']) and w in {supplemental_words(n) for n in game_names(records[x])+localized_game_names(records[x])}}
                if len(editions)==1 and not SERIES_EDITION.search(records[gid]['name']):return editions
            elif w in {supplemental_words(n) for n in game_names(records[gid])+localized_game_names(records[gid])}:
                if all(supplemental_words(records[x]['name'])!=w for x in children):return {gid}
    # Digital PS3 release versus a PS1 classic explicitly identified as such
    # by the catalog platform. Only resolve this narrow generational conflict.
    if tags=={'ps3'}:
        native={gid for gid in candidates if 9 in records[gid].get('platforms',()) and not {7,8,38}&set(records[gid].get('platforms',()))}
        if len(native)==1 and all(x in classic_confirmed and 7 in records[x].get('platforms',()) for x in candidates-native):
            if all(supplemental_words(records[x]['name'])==w for x in candidates):return native
    return candidates


def developer_names(game):
    return {franchise_words((item.get('company') or {}).get('name','')) for item in game.get('involved_companies') or () if item.get('publisher') is False and (item.get('company') or {}).get('name')}


def attach_supported_series(groups,pending,records,catalog,rejected_games):
    occupied=defaultdict(set);tags=defaultdict(set)
    for entry,members in groups.items():
        for tid,gid in members.items():
            occupied[gid].add(entry);tags[gid].update(tag for _,tag in catalog[tid])
    for gid,tids in pending.items():
        for tid in tids:tags[gid].update(tag for _,tag in catalog[tid])
    free=set(pending)-set(occupied)-rejected_games
    pubs={gid:{(item.get('company') or {}).get('id') for item in records[gid].get('involved_companies') or () if item.get('publisher') is True and (item.get('company') or {}).get('id')} for gid in tags}
    devs={gid:developer_names(records[gid]) for gid in tags}
    words={gid:supplemental_words(records[gid]['name']) for gid in tags}
    labels=defaultdict(list)
    for entry in groups:labels[supplemental_words(entry[1])].append(entry)
    proposals=defaultdict(set)
    for gid in sorted(free):
        w=words[gid]
        for i in range(1,len(w)):
            root=w[:i];suffix=w[i:]
            if sum(map(len,root))<7:continue
            entries=labels.get(root,())
            if not entries:continue
            entry=min(entries,key=franchise_label_order)
            members=set().union(*(set(groups[e].values()) for e in entries))
            anchors={x for x in members if tags[gid]&tags[x] and pubs[gid]&pubs[x] and words[x][:i]==root}
            if not anchors:continue
            # Disjoint numbered compilations are distinct releases, not
            # two editions of the same game.
            if (records[gid].get('game_type') or {}).get('type','').lower()=='bundle':
                numbers={ROMAN.get(x,x) for x in suffix if ROMAN.get(x,x).isdigit()}
                if len(numbers)>=2 and any(len(ns:={ROMAN.get(n,n) for n in words[x][i:] if ROMAN.get(n,n).isdigit()})>=2 and not ns&numbers for x in anchors):
                    proposals[gid].add(entry)
            # An explicitly named spin-off/HD edition with the same
            # developer and publisher can join an established series.
            elif len(members)>=2 and any(devs[gid]&devs[x] for x in anchors) and (SERIES_EDITION.search(' '.join(suffix)) or suffix==('hd',) or 'gaiden' in suffix):
                proposals[gid].add(entry)
    recovered=0
    for gid,entries in sorted(proposals.items()):
        if len({franchise_name_key(e[1]) for e in entries})!=1:continue
        entry=min(entries,key=franchise_label_order)
        groups[entry].update((tid,gid) for tid in pending[gid]);free.discard(gid);recovered+=len(pending[gid])
        print(f'Recover independently supported membership: {records[gid]["name"]} -> {entry[1]}')
    # An original and named standalone sequel, backed by identical publisher
    # and developer. No mere edition suffix or one shared generic word.
    exact=defaultdict(set)
    for gid in free:exact[words[gid]].add(gid)
    pairs=[]
    for gid in sorted(free):
        w=words[gid]
        if len(w)<2:continue
        root=w[:-1];suffix=w[-1]
        if len(''.join(root))<8 or root in labels or len(suffix)<4 or not suffix.isalpha() or SERIES_EDITION.search(suffix) or suffix in ('special','plus','gold','ultimate','premium','black','classic','original','online','digital','anniversary','limited','reloaded','remix'):continue
        candidates=exact.get(root,set())-{gid}
        if len(candidates)!=1:continue
        other=next(iter(candidates))
        if not tags[gid]&tags[other] or not pubs[gid]&pubs[other] or not devs[gid]&devs[other]:continue
        if any((records[x].get('game_type') or {}).get('type','').lower()!='main game' for x in (gid,other)):continue
        pairs.append((other,gid))
    counts=Counter(x for pair in pairs for x in pair)
    for a,b in pairs:
        if counts[a]!=1 or counts[b]!=1:continue
        entry=('s:'+str(a),records[a]['name'])
        for gid in (a,b):
            groups[entry].update((tid,gid) for tid in pending[gid]);recovered+=len(pending[gid])
        print(f'Recover developer-supported series: {entry[1]}')
    # Two explicit singleton official labels may be spelling aliases or a
    # parent and a named subseries with the same developer/publisher. Keep
    # the shorter label only when every title supports that label.
    singletons={entry:next(iter(set(members.values()))) for entry,members in groups.items() if len(set(members.values()))==1}
    singleton_roots=defaultdict(list)
    for entry,gid in singletons.items():
        w=supplemental_words(entry[1]);singleton_roots[w].append(entry)
    merge_plans=defaultdict(set)
    for entry,gid in singletons.items():
        w=supplemental_words(entry[1]);title=words[gid]
        if len(w)>=2 and w[-1]=='s' and title[:len(w)]==w:
            for other in singleton_roots.get(w[:-1],()):
                q=singletons[other]
                if q!=gid and words[q][:len(w)]==w and pubs[gid]&pubs[q]:merge_plans[other].add(entry)
        if len(w)>=3 and title[:len(w)]==w:
            root=w[:-1]
            if sum(map(len,root))<8:continue
            for other in singleton_roots.get(root,()):
                q=singletons[other]
                if q!=gid and words[q][:len(root)]==root and words[q]!=title and pubs[gid]&pubs[q] and devs[gid]&devs[q]:merge_plans[entry].add(other)
    for child,parents in sorted(merge_plans.items()):
        if len(parents)!=1:continue
        parent=next(iter(parents))
        if child==parent or parent in merge_plans:continue
        gid=singletons[child]
        if parent not in groups or child not in groups:continue
        groups[parent].update(groups.pop(child));recovered+=len([t for t,g in groups[parent].items() if g==gid])
        print(f'Merge independently supported singleton alias: {child[1]} -> {parent[1]}')
    return recovered




def attach_catalog_volume_aliases(groups,pending,records,catalog,rejected_games):
    # A documented official alternate title with an explicit series volume
    # can recover a regional release even when its translated name is unrelated.
    occupied={gid for members in groups.values() for gid in members.values()}
    labels=defaultdict(list)
    for entry,members in groups.items():
        root=supplemental_words(entry[1]);ids=set(members.values())
        if len(ids)<2 or not root or sum(map(len,root))<6:
            continue
        support=defaultdict(set)
        for tid,gid in members.items():
            tags={tag for _,tag in catalog[tid]}
            for pub in publisher_ids(records[gid]):
                support[pub].update(tags)
        labels[root].append((entry,support))
    decisions=[]
    for gid,tids in sorted(pending.items()):
        if gid in occupied or gid in rejected_games:
            continue
        game=records[gid];pubs=publisher_ids(game)
        if not pubs:
            continue
        tags={tag for tid in tids for _,tag in catalog[tid]}
        proposals=set()
        for alias in game_names(game)[1:]+localized_game_names(game):
            w=supplemental_words(alias)
            for i in range(1,len(w)-3):
                if w[i:i+2] not in (('series','vol'),('series','volume')) or not w[i+2].isdigit():
                    continue
                for entry,support in labels.get(w[:i],()):
                    if any(tags&support.get(pub,set()) for pub in pubs):
                        proposals.add(entry)
        if proposals and len({franchise_name_key(e[1]) for e in proposals})==1:
            decisions.append((min(proposals,key=franchise_label_order),gid,tids))
    for entry,gid,tids in decisions:
        groups[entry].update((tid,gid) for tid in tids)
        print(f'Recover official series-volume alias: {records[gid]["name"]} -> {entry[1]}')
    return sum(len(tids) for _,_,tids in decisions)


def attach_explicit_editions(groups,pending,records,rejected_games):
    by_game=defaultdict(set)
    for entry,members in groups.items():
        for gid in set(members.values()):by_game[gid].add(entry)
    recovered=0
    for gid,tids in sorted(pending.items()):
        if gid in by_game or gid in rejected_games:continue
        game=records[gid];parent=game.get('version_parent')
        if parent not in by_game or parent in rejected_games:continue
        original=records[parent];base=supplemental_words(original['name']);title=supplemental_words(game['name'])
        if not base or title[:len(base)]!=base or not SERIES_EDITION.search(' '.join(title[len(base):])):continue
        franchises=source_entries(original,'franchises','f')
        if len(franchises)!=1:continue
        entry=next(iter(franchises))
        if entry not in by_game[parent] or len(set(groups[entry].values()))<2:continue
        if not any(label_in_name(label,original['name']) for _,label in select_game_groups(original)[0]):continue
        groups[entry].update((tid,gid) for tid in tids);recovered+=len(tids)
        print(f'Recover explicit edition membership: {game["name"]} -> {entry[1]}')
    return recovered


def flatten_verified_character_subsets(groups,records):
    signatures={entry:set(members.values()) for entry,members in groups.items()}
    explicit=defaultdict(set)
    for entry,ids in signatures.items():
        if entry[0].startswith('f:') and len(ids)>=2:explicit[entry[0]].add(entry)
    plans={}
    for child,ids in signatures.items():
        if not child[0].startswith(('f:','c:')) or len(ids)<2:continue
        child_words=supplemental_words(child[1])
        if not child_words:continue
        common=None
        for gid in ids:
            labels={key for key,_ in source_entries(records[gid],'franchises','f')}
            common=labels if common is None else common&labels
        if not common:continue
        if child[0].startswith('f:') and child[0] not in common:continue
        if child[0].startswith('c:') and not all(child in source_entries(records[gid],'collections','c') for gid in ids):continue
        choices=set()
        for key in common-{child[0]}:
            for parent in explicit.get(key,()):
                if not ids < signatures[parent] or len(signatures[parent]-ids)<2:continue
                root=supplemental_words(parent[1])
                if not root or len(root)<2 or root[-1]=='s':continue
                if child[0].startswith('c:') and set(root)&set(child_words):continue
                if not all(any(supplemental_words(n)[:len(root)]==root for n in game_names(records[gid])) for gid in ids):continue
                if any(any(supplemental_words(n)[:len(child_words)]==child_words for n in game_names(records[gid])) for gid in ids):continue
                choices.add(parent)
        if len(choices)==1:plans[child]=next(iter(choices))
    for child,parent in sorted(plans.items()):
        if parent in plans or child not in groups or parent not in groups:continue
        groups[parent].update(groups.pop(child))
        print(f'Merge explicitly supported character subseries: {child[1]} -> {parent[1]}')
    return sum(1 for child in plans if child not in groups)


def compact_game(game):
    # External store rows are consumed by the indexes and not needed later.
    return {key:value for key,value in game.items() if key!='external_games'}


def publisher_ids(game):
    return {item['company']['id'] for item in game.get('involved_companies') or () if item.get('publisher') is True and isinstance(item.get('company'),dict) and isinstance(item['company'].get('id'),int)}


def explicit_version_links(game):
    return {x for x in [game.get('version_parent')]+[v for field in ('ports','remasters','remakes','expanded_games') for v in game.get(field) or ()] if type(x) is int}


def release_alias_bridges(records,requested,bridge_records,platform_tags):
    # Learn only from explicit links or exact official titles shared by an
    # original and a port/remaster from the same publisher. No cross-platform
    # catalog guesses: the resulting target must actually support its tag.
    by_name=defaultdict(list);by_id={g['id']:g for g in bridge_records}
    for g in bridge_records:by_name[supplemental_words(g['name'])].append(g['id'])
    out=defaultdict(set)
    kind=lambda g:(g.get('game_type') or {}).get('type','').lower()
    tags=lambda g:set().union(*(platform_tags.get(p,()) for p in g.get('platforms') or ()))
    publications={g['id']:publisher_ids(g) for g in bridge_records}
    links={g['id']:explicit_version_links(g) for g in bridge_records}
    reverse=defaultdict(set)
    for gid,neighbors in links.items():
        for other in neighbors:reverse[other].add(gid)
    for target in bridge_records:
        gid=target['id'];t=kind(target)
        if t not in ('port','remaster','remake'):continue
        labels=tuple(label for _,label in select_game_groups(target)[0]); roots={supplemental_words(label) for label in labels}
        if not labels:continue
        sources=set(by_name[supplemental_words(target['name'])]) | links[gid] | reverse[gid]
        for other in sources-{gid}:
            source=by_id.get(other)
            if source is None or kind(source) not in ('main game','expanded game','port','remaster'):continue
            linked=other in links[gid] or gid in links[other]
            if not linked and (t=='remake' or kind(source) not in ('main game','expanded game') or not publications[gid]&publications[other]):continue
            shared={franchise_name_key(label) for _,label in select_game_groups(source)[0]}&{franchise_name_key(label) for label in labels}
            if not shared:continue
            target_tags=tags(target)-tags(source)
            if not target_tags:continue
            for name in game_names(source)+localized_game_names(source):
                if linked and t=='remaster':
                    jp=japanese_hd_key(name)
                    if jp:
                        for tag in target_tags:
                            if jp in requested[tag]:
                                out[tag,jp].add(gid);records.setdefault(gid,target)
                w=supplemental_words(name)
                if not w or w in roots or len(''.join(w))<7 or len(w)<2 or name.lower().endswith('.exe'):continue
                variants={('exact',''.join(w))}
                if t=='remaster' and w[-1]!='hd':variants.add(('exact',''.join(w+('hd',))))
                for tag in target_tags:
                    for key in variants & requested[tag]:
                        out[tag,key].add(gid);records.setdefault(gid,target)
    return out


def regional_brand_bridges(records,requested,bridge_records,platform_tags):
    # Learn alternate series prefixes from independent official titles whose
    # complete suffix agrees. A single alias is not enough to establish a
    # regional naming rule. Publisher and series IDs must agree as well.
    evidence=defaultdict(set);targets=defaultdict(list)
    for game in bridge_records:
        gid=game['id'];official=supplemental_words(game['name']);pubs=publisher_ids(game)
        if not pubs or len(official)<2:continue
        accepted,rejected=select_game_groups(game)
        for entry in accepted:
            root=supplemental_words(entry[1]);tail=official[len(root):]
            if not root or official[:len(root)]!=root or not tail or len(''.join(root))<6:continue
            if not any(w.isdigit() for w in tail) and sum(map(len,tail))<5:continue
            for pub in pubs:
                label=(entry[0],root,pub)
                targets[label].append((game,tail))
                for alias in game_names(game)[1:]+localized_game_names(game):
                    w=supplemental_words(alias)
                    if len(w)<=len(tail) or w[-len(tail):]!=tail:continue
                    prefix=w[:-len(tail)]
                    if prefix==root or not 1<=len(prefix)<=5 or len(''.join(prefix))<5 or any(x.isdigit() for x in prefix):continue
                    if not all(x.isascii() and x.isalpha() for x in prefix):continue
                    evidence[label,prefix].add(tail)
    out=defaultdict(set)
    for (label,prefix),tails in sorted(evidence.items()):
        if len(tails)<2:continue
        for game,tail in targets[label]:
            keys={('exact',''.join(prefix+tail))}
            root=label[1]
            for name in game_names(game)[1:]+localized_game_names(game):
                w=supplemental_words(name)
                if w[:len(root)+len(tail)]==root+tail and w[len(root)+len(tail):] in (('hd',),('hd','remaster'),('remastered',),('remaster',)):
                    keys.add(('exact',''.join(prefix+w[len(root):])))
            for p in game.get('platforms') or ():
                for tag in platform_tags.get(p,()):
                    for key in keys & requested[tag]:out[tag,key].add(game['id']);records.setdefault(game['id'],game)
    return out


# Narrow final fallback; keep the full literal title and never replace a
# previous decision. Store qualifiers and alternate glyphs are not sequels.
CJK_GLYPHS = str.maketrans('雙國體學劍龍戰圖樂實傳說','双国体学剣竜戦図楽実伝説')
STORE_STORY = re.compile(r'\s*\(with\s+[A-Z0-9]+\s+story\)\s+full\s+game\s*$',re.I)
STORE_YEAR = re.compile(r'\s*\((?:19|20)\d{2}\)\s*$')
STORE_BILINGUAL = re.compile(r'^([A-Za-z][A-Za-z0-9]{3,})\s*[-–—]\s*[\u30a0-\u30ff]{2,}\s*[-–—]\s*(.+)$')


def terminal_title_key(name):
    raw=unicodedata.normalize('NFKC',name.translate(MARKS)).casefold().translate(CJK_GLYPHS)
    return NON_ALNUM.sub('',raw)


def terminal_catalog_keys(name,tag,tid):
    raw=recovery_base(catalog_match_name(name,tid),True)
    if EXTRA_NON_GAME.search(raw) or SUBTITLE_BLOCK.search(raw):return set()
    out=set()
    literal=terminal_title_key(raw)
    if len(literal)>=5 and JAPANESE_SCRIPT.search(raw):
        out.add(('localized',literal))
    if STORE_STORY.search(raw):
        out.add(('literal',terminal_title_key(STORE_STORY.sub('',raw))))
    if m:=STORE_BILINGUAL.fullmatch(raw):
        if JAPANESE_SCRIPT.search(m[2]) and len(terminal_title_key(m[2]))>=12:
            out.add(('localized',terminal_title_key(m[1]+' '+m[2])))
    if re.search(r'\bthe game$',raw,re.I):
        out.add(('literal',terminal_title_key(re.sub(r'\bthe game$', 'the video game',raw,flags=re.I))))
    if tag=='ps2' and tid.startswith('NP') and STORE_YEAR.search(raw):
        out.add(('classic_year',terminal_title_key(STORE_YEAR.sub('',raw))))
    if len(literal)>=12 and TITLE_SEPARATOR.search(raw) and not re.search(r'\d',raw):
        out.add(('numbered_subtitle',literal))
    return out


def terminal_metadata_keys(game,titles,localized,accepted):
    out=set()
    for name in titles+localized:
        key=terminal_title_key(name)
        if not key:continue
        out.update((('literal',key),('classic_year',key),('numbered_subtitle',key)))
        if JAPANESE_SCRIPT.search(name):out.add(('localized',key))
    # A complete distinctive subtitle with an omitted installment number:
    # the exact numbered short name and official collection must both exist.
    if parts:=subtitle_parts(game['name']):
        root=supplemental_words(parts[1]); tail=parts[2]
        if len(root)>=2 and root[-1].isdigit() and len(root)>=3 and len(terminal_title_key(tail))>=7:
            prefix=root[:-1]
            if any(supplemental_words(label)==prefix for _,label in accepted) and any(supplemental_words(a)==root for a in titles[1:]):
                out.add(('numbered_subtitle',terminal_title_key(' '.join(prefix)+' '+tail)))
    return out


def terminal_resolution(tid,variants,index,anchors):
    evidence=[]
    for tag,keys in variants:
        possible=set()
        for kind,key in keys:
            found=index.get((tag,kind,key),set())
            # A competing literal or derived title must remain ambiguous.
            if len(found)>1:return set()
            if not found:continue
            gid=next(iter(found))
            other=anchors.get((gid,tag),set())
            if not other:continue
            if kind=='classic_year' and not any(x.startswith(('SL','SC')) and x[4:]==tid[4:] for x in other):continue
            possible.add(gid)
        evidence.append(possible)
    if not evidence or not all(evidence):return set()
    common=set.intersection(*evidence)
    return common if len(common)==1 else set()


def resolve_regional_alias_conflict(tid,candidates,variants,records,ext,anchors):
    # A Western title which is the exact official name of one confirmed
    # release and only an alias of a separately named Japanese release.
    if len(candidates)!=2 or not tid.startswith(('NPE','NPU')) or any(ext.get((tag,tid)) for _,tag in variants):return candidates
    matches=set()
    for gid in candidates:
        game=records[gid];w=supplemental_words(game['name'])
        if not w or len(w)<3 or not all(supplemental_words(catalog_match_name(n,tid),True)==w and anchors.get((gid,tag)) for n,tag in variants):continue
        other=records[next(iter(candidates-{gid}))]
        if supplemental_words(other['name'])==w or not any(JAPANESE_SCRIPT.search(n) for n in localized_game_names(other)):continue
        if not any(supplemental_words(n)==w for n in game_names(other)[1:]):continue
        if not publisher_ids(game)&publisher_ids(other):continue
        if not {e[0] for e in select_game_groups(game)[0]}&{e[0] for e in select_game_groups(other)[0]}:continue
        matches.add(gid)
    return matches if len(matches)==1 else candidates


def attach_confirmed_spinoffs(groups,pending,records,catalog,rejected_games):
    # A specifically labeled Japanese side story can join an official
    # collection if two independently documented titles share the label,
    # platform and two publishing companies. No publisher-only grouping.
    occupied={gid for members in groups.values() for gid in members.values()}
    plans=[]
    for gid,tids in sorted(pending.items()):
        if gid in occupied or gid in rejected_games:continue
        game=records[gid];w=supplemental_words(game['name']);pubs=publisher_ids(game)
        if len(pubs)<2 or 'gaiden' not in w:continue
        tags={tag for tid in tids for _,tag in catalog[tid]}
        entries=[]
        for entry,members in groups.items():
            if not entry[0].startswith('c:'):continue
            root=supplemental_words(entry[1]);n=len(root)
            if not root or sum(map(len,root))<7 or w[:n+1]!=root+('gaiden',):continue
            anchors={g for tid,g in members.items() if len(pubs&publisher_ids(records[g]))>=2 and tags&{tag for _,tag in catalog[tid]} and any(label_in_name(entry[1],name) for name in game_names(records[g]))}
            direct={g for g in anchors if entry in source_entries(records[g],'collections','c')}
            # An independently matched, exact original/numbered pair can
            # corroborate the one directly documented collection member.
            corroborated=any(supplemental_words(records[a]['name'])==supplemental_words(records[b]['name'])+('2',) or supplemental_words(records[b]['name'])==supplemental_words(records[a]['name'])+('2',) for a in direct for b in anchors-{a})
            if len(direct)>=2 or corroborated:entries.append(entry)
        if len({franchise_name_key(e[1]) for e in entries})==1:plans.append((min(entries,key=franchise_label_order),gid,tids))
    for entry,gid,tids in plans:
        groups[entry].update((tid,gid) for tid in tids)
        print(f'Recover confirmed side story: {records[gid]["name"]} -> {entry[1]}')
    return sum(len(tids) for _,_,tids in plans)


def attach_verified_singleton_parents(groups,pending,records,rejected_games,parents):
    # An explicitly linked original and an independently documented sequel
    # can complete a singleton, but never create a series from two editions
    # of the same game. The parent need not be in the local catalog.
    occupied={gid for members in groups.values() for gid in members.values()}
    plans=[]
    for gid,tids in sorted(pending.items()):
        if gid in occupied or gid in rejected_games:continue
        parent=records[gid].get('version_parent')
        if type(parent) is not int or parent in occupied or parent==gid:continue
        entries=[]
        for entry in parents.get(parent,()):
            if entry not in groups or len(set(groups[entry].values()))!=1:continue
            anchor=next(iter(groups[entry].values()));other=records[anchor]
            if anchor==parent or anchor==gid or other.get('version_parent')==parent:continue
            root=supplemental_words(entry[1]);w=supplemental_words(records[gid]['name'])
            if not root or w[:len(root)]!=root or not SERIES_EDITION.search(' '.join(w[len(root):])):continue
            if not publisher_ids(records[gid])&publisher_ids(other):continue
            # Independent explicit second-installment alias on the sequel.
            if not any(supplemental_words(a)==root+('2',) for a in game_names(other)):continue
            entries.append(entry)
        if len(entries)==1:plans.append((entries[0],gid,tids))
    for entry,gid,tids in plans:
        groups[entry].update((tid,gid) for tid in tids)
        print(f'Recover verified original edition and sequel: {records[gid]["name"]} -> {entry[1]}')
    return sum(len(tids) for _,_,tids in plans)


def build(catalog, games, platform_map, psn_sources=frozenset({36}), relations=(), diagnostic=None):
    primary=defaultdict(set); aliases=defaultdict(set); ext=defaultdict(set); records={}
    requested_names=defaultdict(set); requested_ids=defaultdict(set)
    catalog_keys={}; recovery_catalog={}; recovery_requested=defaultdict(set); recovery_index=defaultdict(set)
    late_catalog={}; late_requested=defaultdict(set); late_extra_requested=defaultdict(set); late_index=defaultdict(set)
    ordinal_catalog={}; ordinal_requested=defaultdict(set); ordinal_index=defaultdict(set)
    extended_catalog={}; extended_requested=defaultdict(set); extended_index=defaultdict(set)
    typo_catalog={}; typo_requested=defaultdict(set); typo_index=defaultdict(set)
    equivalent_catalog={}; equivalent_requested=defaultdict(set); equivalent_index=defaultdict(set)
    japanese_catalog={}; japanese_requested=defaultdict(set); japanese_index=defaultdict(set)
    bilingual_catalog={}; bilingual_requested=defaultdict(set); bilingual_index=defaultdict(set); bilingual_extra_requested=defaultdict(set)
    for tid, variants in catalog.items():
        extended=[]; typos=[]; equivalents=[]; japanese=[]; bilingual=[]
        prepared=[]; recovery=[]; late_prepared=[]; ordinal_prepared=[]
        for name, tag in sorted(variants):
            keys=names(name)
            prepared.append((tag,keys))
            requested_names[tag].update(keys)
            requested_ids[tag].add(tid)
            match_name=catalog_match_name(name, tid)
            words=extended_title_words(match_name, True)
            key=extended_name_key(words)
            if key:
                extended.append((tag,key))
                extended_requested[tag].add(key)
            split_keys=bilingual_title_keys(match_name,True)
            bilingual.append((tag,key,split_keys))
            bilingual_requested[tag].update(split_keys)
            if key: bilingual_extra_requested[tag].add(key)
            for alt in equivalent_title_keys(match_name,True):
                equivalents.append((tag,alt))
                equivalent_requested[tag].add(alt)
            if tid.startswith(('NPJ','SLPS','SLPM','SCPS')):
                japanese_key=romanized_japanese_key(words)
                if japanese_key:
                    japanese.append((tag,japanese_key))
                    japanese_requested[tag].add(japanese_key)
            for slot,word in typo_slots(words):
                typos.append((tag,slot,word))
                typo_requested[tag].add(slot)
            strong,weak=recovery_keys(match_name, True)
            extra=late_recovery_keys(match_name, True)
            ordinal=ordinal_recovery_keys(match_name, True)
            if ordinal:
                ordinal_prepared.append((tag,ordinal))
                ordinal_requested[tag].update(ordinal)
            recovery.append((tag,strong,weak))
            late_prepared.append((tag,extra,strong))
            recovery_requested[tag].update(strong)
            recovery_requested[tag].update(weak)
            late_requested[tag].update(extra)
            late_requested[tag].update(strong)
            late_extra_requested[tag].update(extra)
        bilingual_catalog[tid]=bilingual
        extended_catalog[tid]=extended
        equivalent_catalog[tid]=equivalents
        if japanese: japanese_catalog[tid]=japanese
        if typos: typo_catalog[tid]=typos
        catalog_keys[tid]=prepared
        recovery_catalog[tid]=recovery
        late_catalog[tid]=late_prepared
        if ordinal_prepared:
            ordinal_catalog[tid]=ordinal_prepared
    tags_by_platform=defaultdict(set)
    for tag, platforms in platform_map.items():
        if requested_ids.get(tag):
            for platform_id in platforms:
                tags_by_platform[platform_id].add(tag)
    store_catalog={tid:[(tag,store_title_keys(catalog_match_name(name,tid),True)) for name,tag in sorted(variants)] for tid,variants in catalog.items()}
    store_requested=defaultdict(set); store_index=defaultdict(set)
    for variants in store_catalog.values():
        for tag,keys in variants: store_requested[tag].update(keys)
    final_catalog={tid:[(tag,final_title_keys(catalog_match_name(n,tid),True)) for n,tag in sorted(variants)] for tid,variants in catalog.items()}
    final_requested=defaultdict(set); final_index=defaultdict(set)
    for variants in final_catalog.values():
        for tag,keys in variants: final_requested[tag].update(keys)
    extra_catalog={}; extra_requested=defaultdict(set); extra_index=defaultdict(set)
    short_typo_catalog={}; short_typo_requested=defaultdict(set); short_typo_index=defaultdict(set)
    identity_typo_catalog={}; identity_typo_requested=defaultdict(set); identity_typo_index=defaultdict(set)
    for tid,variants in catalog.items():
        extra=[]; typo=[]; identity_typos=[]
        japanese=tid.startswith(('NPJ','SLPS','SLPM','SCPS'))
        for name,tag in sorted(variants):
            n=catalog_match_name(name,tid)
            legacy_keys=supplemental_title_keys(n,True,japanese=japanese)
            keys=legacy_keys | last_identity_keys(n,True)
            slots=identity_typo_slots(catalog_identity_words(n,True))
            if slots:
                identity_typos.append((tag,slots))
                identity_typo_requested[tag].update((left,right) for left,right,_ in slots)
            extra.append((tag,keys));extra_requested[tag].update(keys)
            if legacy_keys:
                w=supplemental_words(n,True); key=''.join(w)
                slots=supplemental_typo_keys(key) if len(w)>=2 else ()
                if slots:
                    typo.append((tag,key));short_typo_requested[tag].update(slots)
        extra_catalog[tid]=extra
        if typo:short_typo_catalog[tid]=typo
        if len(identity_typos)==len(variants):identity_typo_catalog[tid]=identity_typos
    terminal_catalog={tid:[(tag,terminal_catalog_keys(n,tag,tid)) for n,tag in sorted(variants)] for tid,variants in catalog.items()}
    terminal_requested=defaultdict(set);terminal_index=defaultdict(set)
    for variants in terminal_catalog.values():
        for tag,keys in variants:terminal_requested[tag].update(keys)
    metadata_count=0; version_parent_groups={}; bridge_records=[]; digital_named=set()
    for game in games:
        metadata_count+=1
        if not is_game_record(game): continue
        gid=game['id']
        # Keep only explicit, title-supported memberships for possible version
        # parents, including metadata records not matched to the catalog.
        accepted,rejected=select_game_groups(game)
        if accepted or rejected:
            if accepted and not rejected:
                supported=tuple(sorted(e for e in accepted if any(label_in_name(e[1],title) for title in game_names(game))))
                if supported and len({franchise_name_key(e[1]) for e in supported})==1:
                    version_parent_groups[gid]=supported
        tags=set()
        for platform_id in game.get('platforms') or []:
            tags.update(tags_by_platform.get(platform_id, ()))
        if not tags: continue
        if accepted:
            bridge_records.append(compact_game(game))
        official = game.get('name', '')
        titles = game_names(game)
        official_keys = set(names(official)) if official else set()
        keys = official_keys.copy()
        secondary = set(); late = set(); ordinals = set()
        for title in titles:
            if title != official:
                keys.update(names(title))
            secondary.update(recovery_keys(title)[0])
            late.update(late_recovery_keys(title))
            ordinals.update(ordinal_recovery_keys(title))
        relevant=False
        all_names=titles + localized_game_names(game)
        terminal_keys=terminal_metadata_keys(game,titles,localized_game_names(game),accepted)
        for tag in tags:
            for kind,key in terminal_keys & terminal_requested[tag]:
                terminal_index[tag,kind,key].add(gid);relevant=True
        extended_words={extended_title_words(title) for title in all_names}
        extended_keys={extended_name_key(words) for words in extended_words} - {''}
        bilingual_keys=set().union(*(bilingual_title_keys(title) for title in all_names))
        equivalent_keys=set().union(*(equivalent_title_keys(title) for title in all_names))
        prefixes=publisher_prefixes(game)
        if prefixes:
            for words in extended_words:
                equivalent_keys.update(branded_title_keys(prefixes,words))
        japanese_keys={romanized_japanese_key(words) for words in extended_words} - {''}
        typo_keys={pair for words in extended_words for pair in typo_slots(words)}
        store_keys=supported_store_keys(game, all_names, accepted)
        final_labels=tuple(label for _,label in accepted)
        final_keys=set().union(*(final_title_keys(title,labels=final_labels) for title in all_names))
        if not accepted:
            final_keys={key for key in final_keys if key[0] not in ('numbered','abbreviated')}
        # An incidental IP must not veto an exact, numbered collection title,
        # but only allow it if the title contains a retained collection label.
        elif rejected:
            final_keys={key for key in final_keys if key[0]!='numbered' or any(label_in_name(label,' '.join(key[1])) for _,label in accepted)}
        # A store listing may prepend an IP owner or publisher to the exact
        # game title. Only labels explicitly attached to this metadata record
        # are accepted, and never strip a substring from an arbitrary title.
        brand_parts={franchise_words(label) for _,label in source_entries(game,'franchises','f')}
        brand_parts.update(publisher_prefixes(game))
        brand_parts={b for b in brand_parts if b and len(b)<=3 and sum(map(len,b))>=4}
        single={b for b in brand_parts if len(b)==1}
        brand_parts.update(a+b for a in single for b in single if a!=b)
        for title in all_names:
            w=extended_title_words(title)
            if not w or sum(map(len,w))<4:continue
            for brand in brand_parts:
                if w[:len(brand)]==brand:continue
                if len(w)==1 and len(brand)>1:continue
                final_keys.add(('literal',brand+w))
                final_keys.add(('literal',brand+('s',)+w))
        # Combine an explicitly documented numbered short name with its full
        # subtitle, rather than inferring an installment number from context.
        if (parts:=subtitle_parts(official)):
            prefix=extended_title_words(parts[1]); suffix=extended_title_words(parts[2])
            for title in titles[1:]:
                alias=extended_title_words(title)
                if len(alias)==len(prefix)+1 and alias[:-1]==prefix and alias[-1].isdigit():
                    final_keys.add(('literal',alias+suffix))
        # A shortened possessive brand is supported by the exact full brand
        # in the same official title, not a global acronym table.
        brand=re.match(r"^(.+?)[’']s\s+(.+)$",official,re.I)
        if brand and accepted:
            initials=''.join(w[0] for w in franchise_words(brand[1]) if w)
            tail=extended_title_words(brand[2])
            if len(initials) in (2,3,4) and len(tail)>=3 and sum(map(len,tail))>=12:
                final_keys.add(('literal',(initials,'s')+tail))
        extra_keys=set().union(*(supplemental_title_keys(n,labels=final_labels,kind=(game.get('game_type') or {}).get('type','').lower(),japanese=True) for n in all_names))
        extra_keys.update(key for n in all_names for key in last_identity_keys(n,labels=final_labels,kind=(game.get('game_type') or {}).get('type','').lower()))
        extra_keys.update(verified_camelcase_number_keys(official,titles[1:],final_labels))
        extra_keys.update(verified_extended_brand_keys(official,titles[1:],final_labels))
        extra_keys.update(mixed_crossover_keys(official,localized_game_names(game)))
        identity_slots={slot for n in all_names for w in identity_title_variants(n,final_labels) for slot in identity_typo_slots(w)}
        official_words=supplemental_words(official)
        if official_words and official_words[-1].isdigit():
            for _,label in source_entries(game,'collections','c'):
                root=supplemental_words(label)
                middle=official_words[:-1]
                if len(root)>=3 and len(middle)==len(root)+1 and any(middle[:i]+middle[i+1:]==root and middle[i].isalpha() for i in range(1,len(middle)-1)):
                    extra_keys.add(('exact',''.join(root+(official_words[-1],))))
        if (game.get('game_type') or {}).get('type','').lower()=='port':
            for n in titles[1:]:
                w=supplemental_words(n)
                if len(w)>=3 and w[-1]=='console' and sum(map(len,w[:-1]))>=8:extra_keys.add(('exact',''.join(w[:-1])))
        # A localized official name can verify an exact additional brand
        # prefix for another language's title in the same record.
        label_words={supplemental_words(label) for label in final_labels}
        local_brands=set()
        for n in all_names:
            w=supplemental_words(n)
            for i in (1,2):
                if len(w)>i and any(root and w[i:i+len(root)]==root for root in label_words) and all(x.isascii() and x.isalpha() for x in w[:i]):local_brands.add(w[:i])
        for brand in local_brands:
            for n in all_names:
                w=supplemental_words(n)
                if any(root and w[:len(root)]==root for root in label_words):extra_keys.add(('exact',''.join(brand+w)))
        # The documented acronym of a numbered alternate title can supply
        # an installment number missing from the full official subtitle.
        if (parts:=subtitle_parts(official)):
            root=supplemental_words(parts[1]);tail=supplemental_words(parts[2]); initials=''.join(w[0] for w in root)
            if len(initials)>=2 and sum(map(len,tail))>=8:
                for title in titles[1:]:
                    m=re.fullmatch(re.escape(initials)+r'(\d+)',norm(title))
                    if m:extra_keys.add(('exact',''.join(root+(m[1],)+tail)))
        typo_words={supplemental_words(n) for n in all_names}
        for tag in tags:
            for left,right,word in identity_slots:
                if (left,right) in identity_typo_requested[tag]:
                    identity_typo_index[tag,left,right].add((word,gid));relevant=True
            for key in extra_keys & extra_requested[tag]:
                extra_index[tag,key].add(gid);relevant=True
            for w in typo_words:
                for slot in supplemental_typo_keys(''.join(w)) if len(w)>=2 else ():
                    if slot in short_typo_requested[tag]:
                        short_typo_index[tag,slot].add((''.join(w),gid));relevant=True
            # Exact names from a platform-specific external retail release
            # are independent evidence; never use names from other platforms.
            for item in game.get('external_games') or ():
                if item.get('external_game_source')!=20 or tag not in tags_by_platform.get(item.get('platform'),()) or not item.get('name'):continue
                for key in supplemental_title_keys(item['name'],True)&extra_requested[tag]:
                    extra_index[tag,key].add(gid);relevant=True
            for key in final_keys & final_requested[tag]:
                final_index[tag,key].add(gid)
                relevant=True
            for key in store_keys & store_requested[tag]:
                store_index[tag,key].add(gid)
                relevant=True
            for key in (extended_keys & bilingual_requested[tag]) | (bilingual_keys & (bilingual_requested[tag] | bilingual_extra_requested[tag])):
                bilingual_index[tag,key].add(gid)
                relevant=True
            for key in extended_keys & extended_requested[tag]:
                extended_index[tag,key].add(gid)
                relevant=True
            for key in equivalent_keys & equivalent_requested[tag]:
                equivalent_index[tag,key].add(gid)
                relevant=True
            for key in japanese_keys & japanese_requested[tag]:
                japanese_index[tag,key].add(gid)
                relevant=True
            for slot,word in typo_keys:
                if slot in typo_requested[tag]:
                    typo_index[tag,slot].add((word,gid))
                    relevant=True
            for key in ordinals & ordinal_requested[tag]:
                ordinal_index[tag,key].add(gid)
                relevant=True
            for key in (late & late_requested[tag]) | (secondary & late_extra_requested[tag]):
                late_index[tag,key].add(gid)
                relevant=True
            for key in secondary & recovery_requested[tag]:
                recovery_index[tag,key].add(gid)
                relevant=True
            for key in official_keys & requested_names[tag]:
                primary[tag,key].add(gid)
            for key in keys & requested_names[tag]:
                aliases[tag,key].add(gid)
                relevant=True
        for item in game.get('external_games') or []:
            if not isinstance(item,dict) or item.get('external_game_source') not in psn_sources: continue
            uid=psn_title_id(item.get('uid'))
            external_platform = item.get('platform')
            compatible = tags if external_platform is None else tags.intersection(tags_by_platform.get(external_platform, ()))
            external_name = item.get('name')
            external_key = extended_name_key(extended_title_words(external_name, True)) if isinstance(external_name,str) else ''
            for tag in compatible:
                if external_key and external_key in extended_requested[tag]:
                    extended_index[tag,external_key].add(gid)
                    relevant=True
                if uid and uid in requested_ids[tag]:
                    ext[tag,uid].add(gid)
                    relevant=True
        if relevant:
            records[gid]=compact_game(game)
            if any(item.get('external_game_source') in psn_sources and item.get('name') and catalog_identity_words(item['name'],True)==catalog_identity_words(official) for item in game.get('external_games') or ()):
                digital_named.add(gid)
    release_bridges=release_alias_bridges(records,extra_requested,bridge_records,tags_by_platform)
    brand_bridges=regional_brand_bridges(records,extra_requested,bridge_records,tags_by_platform)
    del bridge_records
    print(f'IGDB metadata rows: {metadata_count}; relevant records indexed: {len(records)}')
    groups=defaultdict(dict); matched=0; ambiguous=0; canonical={}; memberships={}; rejected_games=set(); rejected_links=0
    pending=defaultdict(list); unresolved=[]
    classic_confirmed=set()
    for tid,variants in catalog_keys.items():
        if {tag for tag,_ in variants}=={'psx'}:
            found=resolve_catalog_game(tid,variants,primary,aliases,ext)
            if len(found)==1:classic_confirmed.update(found)
    omitted_word_index=confirmed_omitted_word_index(catalog,catalog_keys,primary,aliases,ext,records)
    terminal_anchors=defaultdict(set)
    for tid,variants in catalog_keys.items():
        found=resolve_catalog_game(tid,variants,primary,aliases,ext)
        if len(found)==1:
            gid=next(iter(found))
            for tag,_ in variants:terminal_anchors[gid,tag].add(tid)
    rejected_memberships = {}; recovered_matches = Counter(); match_methods = {}
    for tid, variants in sorted(catalog_keys.items()):
        candidates=resolve_catalog_game(tid, variants, primary, aliases, ext)
        method='baseline'
        if not candidates:
            possible=set()
            used_edition=False
            for tag,strong,weak in recovery_catalog[tid]:
                found=set()
                for key in strong:
                    found.update(recovery_index.get((tag,key), ()))
                if not found:
                    for key in weak:
                        found.update(recovery_index.get((tag,key), ()))
                    used_edition = used_edition or bool(found)
                possible.update(found)
            candidates=possible
            if len(possible)==1:
                method='edition' if used_edition else 'normalized'
                recovered_matches[method] += 1
        if not candidates:
            # Do not override any existing match or resolve existing ambiguity by guessing.
            possible=set()
            for tag,extra,original in late_catalog[tid]:
                for key in extra | original:
                    possible.update(late_index.get((tag,key), ()))
            if len(possible)==1:
                method='reissue_or_edition'
                recovered_matches[method] += 1
            candidates=possible
        if not candidates and tid in ordinal_catalog:
            possible=set()
            for tag,keys in ordinal_catalog[tid]:
                for key in keys:
                    possible.update(ordinal_index.get((tag,key), ()))
            if len(possible)==1:
                method='ordinal'
                recovered_matches[method] += 1
            candidates=possible
        if not candidates:
            candidates=set().union(*(extended_index.get((tag,key), ()) for tag,key in extended_catalog[tid]))
            if len(candidates)==1:
                method='regional_release_name'
                recovered_matches[method] += 1
        if not candidates:
            candidates=set().union(*(equivalent_index.get((tag,key), ()) for tag,key in equivalent_catalog[tid]))
            if len(candidates)==1:
                method='title_equivalence'
                recovered_matches[method] += 1
        if not candidates:
            candidates=set().union(*(bilingual_index.get((tag,k), ()) for tag,key,split_keys in bilingual_catalog[tid] for k in split_keys | ({key} if key else set())))
            if len(candidates)==1:
                method='bilingual_title'
                recovered_matches[method] += 1
        if not candidates and tid in japanese_catalog:
            candidates=set().union(*(japanese_index.get((tag,key), ()) for tag,key in japanese_catalog[tid]))
            if len(candidates)==1:
                method='japanese_romanization'
                recovered_matches[method] += 1
        if not candidates and tid in typo_catalog:
            candidates={gid for tag,slot,word in typo_catalog[tid]
                        for candidate,gid in typo_index.get((tag,slot), ()) if one_typo(word,candidate)}
            if len(candidates)==1:
                method='strict_typo'
                recovered_matches[method] += 1
        if not candidates:
            candidates={gid for tag,keys in store_catalog[tid] for key in keys for gid in store_index.get((tag,key), ())}
            if len(candidates)==1:
                method='store_title'
                recovered_matches[method] += 1
        if not candidates:
            candidates={gid for tag,keys in final_catalog[tid] for key in keys for gid in final_index.get((tag,key),())}
            if len(candidates)==1:
                method='verified_title_variants'
                recovered_matches[method] += 1
        if not candidates:
            candidates=corroborated_candidates(extra_catalog[tid],lambda item: {gid for key in item[1] if key[0] not in ('packaging','single_hd','jp_hd') for gid in extra_index.get((item[0],key),())})
            if len(candidates)==1:
                method='release_evidence';recovered_matches[method]+=1
        if not candidates:
            candidates=corroborated_candidates(extra_catalog[tid],lambda item: {gid for key in item[1] for gid in release_bridges.get((item[0],key),())})
            if len(candidates)==1:method='official_version_alias';recovered_matches[method]+=1
        if not candidates:
            candidates=corroborated_candidates(extra_catalog[tid],lambda item: {gid for key in item[1] for gid in brand_bridges.get((item[0],key),())})
            if len(candidates)==1:method='verified_regional_brand';recovered_matches[method]+=1
        if not candidates and tid in short_typo_catalog:
            candidates=corroborated_candidates(short_typo_catalog[tid],lambda item: {gid for slot in supplemental_typo_keys(item[1]) for candidate,gid in short_typo_index.get((item[0],slot),()) if strict_full_title_typo(item[1],candidate)}) if len(short_typo_catalog[tid])==len(catalog[tid]) else set()
            if len(candidates)==1:
                method='strict_title_typo';recovered_matches[method]+=1
        if not candidates:
            candidates=corroborated_candidates(extra_catalog[tid],lambda item: {gid for key in item[1] if key[0] in ('packaging','single_hd','jp_hd') for gid in extra_index.get((item[0],key),())})
            if len(candidates)==1:
                method='packaging_identity';recovered_matches[method]+=1
        if not candidates and tid in identity_typo_catalog:
            candidates=corroborated_candidates(identity_typo_catalog[tid],lambda item: {gid for left,right,word in item[1] for other,gid in identity_typo_index.get((item[0],left,right),()) if word==other or one_typo(word,other)})
            if len(candidates)==1:
                method='word_identity';recovered_matches[method]+=1
        if not candidates:
            candidates=resolve_omitted_word(tid,catalog[tid],omitted_word_index)
            if len(candidates)==1:
                method='confirmed_omitted_word';recovered_matches[method]+=1
        if not candidates:
            candidates=terminal_resolution(tid,terminal_catalog[tid],terminal_index,terminal_anchors)
            if len(candidates)==1:
                method='terminal_release_identity';recovered_matches[method]+=1
        if len(candidates)>1:
            refined=resolve_plus_variant(tid, candidates, catalog[tid], records, ext)
            if len(refined)==1:
                method='explicit_plus'
                recovered_matches[method] += 1
            candidates=refined
        if len(candidates)>1:
            refined=resolve_linked_port_family(tid,candidates,catalog[tid],records,ext)
            if len(refined)==1:
                method='linked_port_identity'
                recovered_matches[method] += 1
            candidates=refined
        if len(candidates)>1:
            refined=resolve_release_ambiguity(tid,candidates,catalog[tid],records,ext,classic_confirmed)
            if len(refined)==1:
                method='release_identity';recovered_matches[method]+=1
            candidates=refined
        if len(candidates)>1:
            refined=release_distribution_identity(tid,candidates,catalog[tid],records,ext,digital_named)
            if len(refined)==1:
                method='distribution_identity';recovered_matches[method]+=1
            candidates=refined
        if len(candidates)>1:
            refined=resolve_mixed_crossover_parent(tid,candidates,catalog[tid],records,ext)
            if len(refined)==1:
                method='mixed_crossover_identity';recovered_matches[method]+=1
            candidates=refined
        if len(candidates)>1:
            refined=resolve_regional_alias_conflict(tid,candidates,catalog[tid],records,ext,terminal_anchors)
            if len(refined)==1:
                method='confirmed_regional_alias';recovered_matches[method]+=1
            candidates=refined
        if len(candidates)!=1:
            ambiguous+=bool(candidates)
            unresolved.append((tid, variants, candidates))
            continue
        gid=next(iter(candidates));game=records[gid]
        matched+=1
        canonical[tid]=gid
        if diagnostic is not None: match_methods[tid]=method
        if gid not in memberships:
            accepted, rejected = select_game_groups(game)
            memberships[gid] = accepted
            if rejected:
                if diagnostic is not None: rejected_memberships[gid] = rejected
                rejected_games.add(gid)
                rejected_links += len(rejected)
                print(f'Ignore incidental IP associations: {game.get("name", gid)} ({gid}): {", ".join(sorted(name for _,name in rejected))}')
        if not memberships[gid]:
            pending[gid].append(tid)
        for entry in memberships[gid]:
            groups[entry][tid]=gid
    del short_typo_catalog, short_typo_requested, short_typo_index, extra_catalog, extra_requested, extra_index, release_bridges, brand_bridges, identity_typo_catalog, identity_typo_requested, identity_typo_index, omitted_word_index
    recovered=attach_exact_series_roots(groups, pending, records, rejected_games)
    numbered_recovered=attach_numbered_series(groups, pending, records, rejected_games)
    pair_recovered=attach_exact_numbered_pairs(groups, pending, records, catalog, rejected_games)
    subtitle_recovered=attach_subtitle_series(groups, pending, records, catalog, rejected_games)
    shared_recovered=attach_shared_series_names(groups, pending, records, catalog, rejected_games)
    linked_recovered=attach_linked_versions(groups, pending, records, rejected_games)
    external_parent_recovered=attach_external_version_parents(groups, pending, records, rejected_games, version_parent_groups)
    extended_recovered=attach_extended_series(groups, pending, records, catalog, rejected_games)
    publisher_pair_recovered=attach_publisher_subtitle_pairs(groups,pending,records,catalog,rejected_games)
    verified_series_recovered=attach_verified_series_versions(groups,pending,records,catalog,rejected_games)
    supported_recovered=attach_supported_series(groups,pending,records,catalog,rejected_games)
    volume_alias_recovered=attach_catalog_volume_aliases(groups,pending,records,catalog,rejected_games)
    explicit_edition_recovered=attach_explicit_editions(groups,pending,records,rejected_games)
    singleton_parent_recovered=attach_verified_singleton_parents(groups,pending,records,rejected_games,version_parent_groups)
    spinoff_recovered=attach_confirmed_spinoffs(groups,pending,records,catalog,rejected_games)
    character_merges=flatten_verified_character_subsets(groups,records)
    if diagnostic is not None:
        diagnostic['confirmed_spinoff_recoveries']=spinoff_recovered
        diagnostic['singleton_parent_recoveries']=singleton_parent_recovered
        diagnostic['volume_alias_recoveries']=volume_alias_recovered
        diagnostic['explicit_edition_recoveries']=explicit_edition_recovered
        diagnostic['character_subseries_merges']=character_merges
        diagnostic['supported_series_recoveries']=supported_recovered
        diagnostic['verified_series_version_recoveries']=verified_series_recovered
        diagnostic['publisher_subtitle_pair_recoveries']=publisher_pair_recovered
        diagnostic['external_version_parent_recoveries']=external_parent_recovered
        diagnostic['linked_version_recoveries']=linked_recovered
        diagnostic['extended_series_recoveries']=extended_recovered
        diagnostic['shared_series_name_recoveries']=shared_recovered
        diagnostic['subtitle_series_recoveries']=subtitle_recovered
        diagnostic['numbered_series_recoveries']=numbered_recovered
        diagnostic['exact_numbered_pair_recoveries']=pair_recovered
    series_keys={franchise_name_key(entry[1]) for entry in groups}
    for tid, variants, candidates in unresolved:
        if any(key in series_keys for _, keys in variants for key in keys):
            print(f'Unresolved exact series-title candidate: {tid}; IGDB candidates: {sorted(candidates)}')
    print(f'Exact series-root recovery: {recovered} catalog IDs')
    print(f'Secondary name recovery: {dict(recovered_matches)}')
    if diagnostic is not None: diagnostic['secondary_match_recoveries'] = dict(recovered_matches)
    print(f'Incidental IP associations: {rejected_links} removed across {len(rejected_games)} games')
    output = consolidate_groups(groups, records, relations, diagnostic)
    print(f'Catalog: {len(catalog)} IDs | matched: {matched} | ambiguous: {ambiguous} | franchises: {len(output)}')
    used={tid for group in output for tid in group['titles']}
    if diagnostic is not None:
        collect_diagnostics(diagnostic, catalog, records, canonical, unresolved, groups,
                            memberships, rejected_memberships, output, metadata_count, recovered, match_methods)
    result = {'version':1,'franchises':output,'games':{tid:canonical[tid] for tid in sorted(used)}}
    for cached in (norm, names, recovery_base, recovery_keys, late_recovery_keys, ordinal_recovery_keys, extended_title_words, equivalent_title_keys, franchise_words, franchise_name_key, label_in_name, catalog_identity_words):
        cached.cache_clear()
    return result


STATUS_LABELS = {
    'indexed': 'Indexed in a franchise',
    'no_match': 'No eligible IGDB match',
    'ambiguous': 'Multiple IGDB candidates',
    'no_franchise_metadata': 'Matched; no franchise or collection metadata',
    'incidental_only': 'Matched; only incidental IP associations',
    'single_game_franchise': 'Matched; final group has fewer than two distinct games',
    'excluded_during_consolidation': 'Matched; not retained during consolidation',
}


def collect_diagnostics(report, catalog, records, canonical, unresolved, groups,
                        memberships, rejected, output, metadata_count, recovered, match_methods=None):
    final = defaultdict(list)
    assigned = defaultdict(list)
    discarded = defaultdict(list)
    for group in output:
        for tid in group['titles']:
            final[tid].append(group['name'])
    for (_, name), members in groups.items():
        for tid in members:
            assigned[tid].append(name)
    for group in report.get('discarded_groups', []):
        for tid in group['titles']:
            discarded[tid].append(group['name'])
    candidates_by_tid = {tid: candidates for tid, _, candidates in unresolved}
    counts = Counter()
    distinct = defaultdict(set)
    by_platform = defaultdict(Counter)
    rows = []
    for tid, variants in sorted(catalog.items()):
        gid = canonical.get(tid)
        candidates = candidates_by_tid.get(tid, ())
        if tid in final: status = 'indexed'
        elif gid is None: status = 'ambiguous' if candidates else 'no_match'
        elif tid in discarded: status = 'single_game_franchise'
        elif tid in assigned: status = 'excluded_during_consolidation'
        elif rejected.get(gid): status = 'incidental_only'
        else: status = 'no_franchise_metadata'
        counts[status] += 1
        if gid is not None: distinct[status].add(gid)
        tags = sorted({tag for _, tag in variants})
        for tag in tags: by_platform[tag][status] += 1
        rows.append({
            'title_id': tid, 'catalog_names': sorted({name for name, _ in variants}),
            'platforms': tags, 'status': status, 'igdb_id': gid,
            'match_method': (match_methods or {}).get(tid,''),
            'igdb_name': records[gid].get('name', '') if gid is not None else '',
            'candidates': [{'id': candidate, 'name': records.get(candidate, {}).get('name', '')}
                           for candidate in sorted(candidates)],
            'accepted_groups': sorted({name for _, name in memberships.get(gid, ())}),
            'rejected_groups': sorted({name for _, name in rejected.get(gid, ())}),
            'assigned_groups': sorted(set(assigned.get(tid, ()))),
            'discarded_groups': sorted(set(discarded.get(tid, ()))),
            'final_groups': sorted(set(final.get(tid, ()))),
        })
    report.update({
        'schema_version': 1, 'catalog_title_ids': len(catalog),
        'matched_title_ids': len(canonical), 'distinct_matched_igdb_games': len(set(canonical.values())),
        'indexed_title_ids': len(final),
        'distinct_indexed_igdb_games': len({canonical[tid] for tid in final}),
        'unindexed_title_ids': len(catalog) - len(final),
        'franchises': len(output), 'metadata_rows': metadata_count,
        'recovered_series_root_title_ids': recovered,
        'status_counts': {key: counts[key] for key in STATUS_LABELS},
        'distinct_matched_games_by_status': {key: len(distinct[key]) for key in STATUS_LABELS},
        'platform_status_counts': {tag: dict(counter) for tag, counter in sorted(by_platform.items())},
        'titles': rows,
    })
    if sum(counts.values()) != len(catalog) or counts['indexed'] != len(final):
        raise RuntimeError('Diagnostic counts inconsistent')


def write_diagnostics(report, directory):
    directory.mkdir(parents=True, exist_ok=True)
    (directory / 'coverage.json').write_text(json.dumps(report, ensure_ascii=False, indent=2)+'\n', encoding='utf-8')
    fields = ('title_id', 'catalog_names', 'platforms', 'status', 'igdb_id', 'igdb_name', 'match_method',
              'candidates', 'accepted_groups', 'rejected_groups', 'assigned_groups', 'discarded_groups', 'final_groups')
    with (directory / 'titles.tsv').open('w', encoding='utf-8', newline='') as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, delimiter='\t')
        writer.writeheader()
        for row in report['titles']:
            writer.writerow({key: json.dumps(value, ensure_ascii=False, separators=(',', ':')) if isinstance(value, list) else value
                             for key, value in row.items()})
    lines = ['## Franchise coverage', '',
             f"Eligible catalog Title IDs: **{report['catalog_title_ids']}** | Indexed: **{report['indexed_title_ids']}** | Not indexed: **{report['unindexed_title_ids']}**",
             '', f"Distinct IGDB games: **{report['distinct_matched_igdb_games']} matched / {report['distinct_indexed_igdb_games']} indexed** | Franchise cards: **{report['franchises']}**",
             '', '| Result | Title IDs | Distinct matched IGDB games |', '|---|---:|---:|']
    for key, label in STATUS_LABELS.items():
        lines.append(f"| {label} | {report['status_counts'][key]} | {report['distinct_matched_games_by_status'][key]} |")
    lines += ['', f"Groups discarded below minimum: **{len(report.get('discarded_groups', []))}**. Exact series-root recoveries: **{report['recovered_series_root_title_ids']}**.",
              '', 'Title ID statuses are mutually exclusive. Distinct IGDB counts by status may overlap; unmatched records have no confirmed IGDB ID. The website may count regional groups differently.',
              '', '### By platform', '', '| Platform | Eligible IDs | Indexed IDs | No match | Ambiguous | Other unindexed |', '|---|---:|---:|---:|---:|---:|']
    for tag, counter in report['platform_status_counts'].items():
        total = sum(counter.values()); indexed = counter.get('indexed', 0); missing = counter.get('no_match', 0); ambiguous = counter.get('ambiguous', 0)
        lines.append(f'| {tag} | {total} | {indexed} | {missing} | {ambiguous} | {total-indexed-missing-ambiguous} |')
    lines += ['', 'Platform counts may overlap when a Title ID has multiple catalog classifications.', '',
              '### TSV input', '', '| Source | Rows | Eligible rows | Excluded rows |', '|---|---:|---:|---:|']
    for file, counts in report.get('catalog_sources', {}).items():
        if counts.get('missing'): lines.append(f'| {file} | missing | — | — |')
        else: lines.append(f"| {file} | {counts.get('rows',0)} | {counts.get('eligible_rows',0)} | {counts.get('rows',0)-counts.get('eligible_rows',0)} |")
    lines += ['', 'Rows include headers, duplicates and regional releases. See coverage.json for exclusions by reason.',
              '', 'Download the franchise-diagnostics artifact for all Title IDs, candidate names, accepted/rejected memberships and discarded groups. No credentials or download URLs are included.', '']
    excluded = Counter()
    for counts in report.get('catalog_sources', {}).values():
        excluded.update({key: count for key, count in counts.items() if key in ('app', 'guide', 'soundtrack', 'video', 'demo', 'explicit_non_game')})
    lines += ['### Content excluded before matching', '', '| Category | TSV rows |', '|---|---:|']
    for key, label in (('app', 'Apps'), ('guide', 'Guides'), ('soundtrack', 'Soundtracks'), ('video', 'Videos / movies'), ('demo', 'Demos'), ('explicit_non_game', 'Explicit non-game content')):
        lines.append(f'| {label} | {excluded[key]} |')
    lines += ['', 'Counts are source rows, not unique Title IDs.', '']
    recoveries = report.get('secondary_match_recoveries', {})
    lines += ['### Secondary recovery', '', f"Normalized names: **{recoveries.get('normalized', 0)}** | Edition fallback: **{recoveries.get('edition', 0)}** | Ordinal spelling: **{recoveries.get('ordinal', 0)}** | Reissue / alternate spelling: **{recoveries.get('reissue_or_edition', 0)}** | Explicit plus variants: **{recoveries.get('explicit_plus', 0)}** | Numbered series memberships: **{report.get('numbered_series_recoveries', 0)}** | Exact numbered pair memberships: **{report.get('exact_numbered_pair_recoveries', 0)}** | Subtitle series memberships: **{report.get('subtitle_series_recoveries', 0)}** | Shared series names: **{report.get('shared_series_name_recoveries', 0)}** | Regional release names: **{recoveries.get('regional_release_name', 0)}** | Title equivalents: **{recoveries.get('title_equivalence', 0)}** | Bilingual titles: **{recoveries.get('bilingual_title', 0)}** | Japanese romanization: **{recoveries.get('japanese_romanization', 0)}** | Strict typos: **{recoveries.get('strict_typo', 0)}** | Store title recovery: **{recoveries.get('store_title', 0)}** | Linked versions: **{report.get('linked_version_recoveries', 0)}** | Non-catalog version parents: **{report.get('external_version_parent_recoveries', 0)}** | Extended series roots: **{report.get('extended_series_recoveries', 0)}** | Publisher subtitle pairs: **{report.get('publisher_subtitle_pair_recoveries', 0)}** | Reissue collection merges: **{report.get('reissue_collection_merges', 0)}** | Verified title variants: **{recoveries.get('verified_title_variants', 0)}** | Linked port identities: **{recoveries.get('linked_port_identity', 0)}** | Verified series versions: **{report.get('verified_series_version_recoveries', 0)}**.", '']
    lines += [f"Release evidence: **{recoveries.get('release_evidence',0)}** | Strict full-title typos: **{recoveries.get('strict_title_typo',0)}** | Release identities: **{recoveries.get('release_identity',0)}** | Supported series memberships: **{report.get('supported_series_recoveries',0)}** | Official version aliases: **{recoveries.get('official_version_alias',0)}** | Verified regional brands: **{recoveries.get('verified_regional_brand',0)}** | Explicit edition memberships: **{report.get('explicit_edition_recoveries',0)}** | Explicit subseries merges: **{report.get('character_subseries_merges',0)}**.", '']
    lines += [f"Packaging identity: **{recoveries.get('packaging_identity',0)}** | Digital distribution identity: **{recoveries.get('distribution_identity',0)}** | Token identity: **{recoveries.get('word_identity',0)}** | Mixed-script crossover identity: **{recoveries.get('mixed_crossover_identity',0)}** | Official series-volume aliases: **{report.get('volume_alias_recoveries',0)}** | Confirmed regional omitted word: **{recoveries.get('confirmed_omitted_word',0)}**.", '']
    lines += [f"Terminal release identities: **{recoveries.get('terminal_release_identity',0)}** | Verified original edition and sequel: **{report.get('singleton_parent_recoveries',0)}** | Confirmed side stories: **{report.get('confirmed_spinoff_recoveries',0)}** | Confirmed regional aliases: **{recoveries.get('confirmed_regional_alias',0)}**.", '']
    summary = '\n'.join(lines)
    (directory / 'summary.md').write_text(summary, encoding='utf-8')
    print(summary)


def main():
    p=argparse.ArgumentParser();p.add_argument('--root',type=Path,default=ROOT);p.add_argument('--metadata',type=Path,help='Offline JSON fixture for testing');p.add_argument('--replay',type=Path,help='Offline diagnostic replay directory (does not update the published index)');p.add_argument('--secrets-file', type=Path, default=os.environ.get('PSN_SECRETS_FILE') or None, help='Path to private JSON credentials');p.add_argument('--full-refresh', action='store_true', help='Ignore previous IGDB metadata cache');p.add_argument('--diagnostics', type=Path, help='Write coverage reports to this directory');p.add_argument('--replay-output',type=Path,help='Write the replay index into a separate file');args=p.parse_args()
    if args.replay_output and not args.replay: p.error('--replay-output requires --replay')
    diagnostic = {} if args.diagnostics else None
    if args.replay:
        if args.metadata or args.full_refresh:
            p.error('--replay cannot be combined with --metadata or --full-refresh')
        context=json.loads((args.replay/'replay-context.json').read_text(encoding='utf-8'))
        catalog={tid:{tuple(pair) for pair in variants} for tid,variants in context['catalog'].items()}
        platforms={tag:set(ids) for tag,ids in context['platforms'].items()}
        sources=set(context['psn_sources']);relations=context['collection_relations']
        if diagnostic is not None:
            diagnostic.update(context.get('catalog_diagnostics', {}))
        games=cached_games(args.replay/'metadata-snapshot.json.gz')
        result=build(catalog,games,platforms,sources,relations,diagnostic)
        if diagnostic is not None: write_diagnostics(diagnostic,args.diagnostics)
        if args.replay_output:
            if args.replay_output.resolve() == (args.root/OUTPUT).resolve(): p.error('Replay output cannot overwrite the published index')
            args.replay_output.parent.mkdir(parents=True,exist_ok=True)
            args.replay_output.write_text(json.dumps(result,ensure_ascii=False,separators=(',',':'))+'\n',encoding='utf-8')
        print(f'Offline replay: {len(result["franchises"])} franchises; {len(result["games"])} indexed IDs')
        return
    catalog=read_catalog(args.root, diagnostic)
    if args.metadata:
        fixture=json.loads(args.metadata.read_text()); platforms=PLATFORMS; sources={36}
        games,relations=(fixture['games'],fixture.get('relations',[])) if isinstance(fixture,dict) else (fixture,[])
    else:
        api=IGDB(args.root,args.secrets_file);games,platforms,sources,relations=metadata(api, args.root/CACHE_FILE, args.full_refresh)
    result=build(catalog,games,platforms,sources,relations,diagnostic)
    if diagnostic is not None:
        write_diagnostics(diagnostic, args.diagnostics)
        if not args.metadata:
            # Include the exact public metadata used in this run so all future
            # matcher revisions can be replayed offline without running CI.
            shutil.copyfile(args.root/CACHE_FILE,args.diagnostics/'metadata-snapshot.json.gz')
            replay={'catalog':{tid:sorted(variants) for tid,variants in sorted(catalog.items())},
                    'platforms':{tag:sorted(ids) for tag,ids in platforms.items()},
                    'psn_sources':sorted(sources),'collection_relations':relations,
                    'catalog_diagnostics':{key:diagnostic[key] for key in ('catalog_sources','apps_filter_available','browse_filter_app_ids') if key in diagnostic}}
            (args.diagnostics/'replay-context.json').write_text(json.dumps(replay,ensure_ascii=False,separators=(',',':'))+'\n',encoding='utf-8')
    if not args.metadata: print(f'IGDB requests: {api.requests}')
    if not result['franchises']:raise RuntimeError('No franchises matched; previous index preserved')
    path=args.root/OUTPUT
    if path.exists():
        validate_coverage(json.loads(path.read_text(encoding='utf-8')), result, catalog)
    path.parent.mkdir(parents=True,exist_ok=True)
    content=json.dumps(result,ensure_ascii=False,separators=(',',':'))+'\n'
    if not path.exists() or path.read_text(encoding='utf-8')!=content:
        temp=path.with_suffix('.tmp');temp.write_text(content,encoding='utf-8',newline='\n');temp.replace(path)


if __name__=='__main__':main()
