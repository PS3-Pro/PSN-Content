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


def consolidate_groups(groups, records=None, relations=(), diagnostic=None):
    groups = flatten_collection_parents(groups, relations)
    records = records or {}
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
    metadata_count=0; version_parent_groups={}
    for game in games:
        metadata_count+=1
        if not is_game_record(game): continue
        gid=game['id']
        # Keep only explicit, title-supported memberships for possible version
        # parents, including metadata records not matched to the catalog.
        if game.get('collections') or game.get('franchises') or game.get('franchise'):
            accepted,rejected=select_game_groups(game)
            if accepted and not rejected:
                supported=tuple(sorted(e for e in accepted if any(label_in_name(e[1],title) for title in game_names(game))))
                if supported and len({franchise_name_key(e[1]) for e in supported})==1:
                    version_parent_groups[gid]=supported
        tags=set()
        for platform_id in game.get('platforms') or []:
            tags.update(tags_by_platform.get(platform_id, ()))
        if not tags: continue
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
        for tag in tags:
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
            records[gid]=game
    print(f'IGDB metadata rows: {metadata_count}; relevant records indexed: {len(records)}')
    groups=defaultdict(dict); matched=0; ambiguous=0; canonical={}; memberships={}; rejected_games=set(); rejected_links=0
    pending=defaultdict(list); unresolved=[]
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
        if len(candidates)>1:
            refined=resolve_plus_variant(tid, candidates, catalog[tid], records, ext)
            if len(refined)==1:
                method='explicit_plus'
                recovered_matches[method] += 1
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
    recovered=attach_exact_series_roots(groups, pending, records, rejected_games)
    numbered_recovered=attach_numbered_series(groups, pending, records, rejected_games)
    pair_recovered=attach_exact_numbered_pairs(groups, pending, records, catalog, rejected_games)
    subtitle_recovered=attach_subtitle_series(groups, pending, records, catalog, rejected_games)
    shared_recovered=attach_shared_series_names(groups, pending, records, catalog, rejected_games)
    linked_recovered=attach_linked_versions(groups, pending, records, rejected_games)
    external_parent_recovered=attach_external_version_parents(groups, pending, records, rejected_games, version_parent_groups)
    extended_recovered=attach_extended_series(groups, pending, records, catalog, rejected_games)
    if diagnostic is not None:
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
    for cached in (norm, names, recovery_base, recovery_keys, late_recovery_keys, ordinal_recovery_keys, extended_title_words, equivalent_title_keys, franchise_words, franchise_name_key, label_in_name):
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
    lines += ['### Secondary recovery', '', f"Normalized names: **{recoveries.get('normalized', 0)}** | Edition fallback: **{recoveries.get('edition', 0)}** | Ordinal spelling: **{recoveries.get('ordinal', 0)}** | Reissue / alternate spelling: **{recoveries.get('reissue_or_edition', 0)}** | Explicit plus variants: **{recoveries.get('explicit_plus', 0)}** | Numbered series memberships: **{report.get('numbered_series_recoveries', 0)}** | Exact numbered pair memberships: **{report.get('exact_numbered_pair_recoveries', 0)}** | Subtitle series memberships: **{report.get('subtitle_series_recoveries', 0)}** | Shared series names: **{report.get('shared_series_name_recoveries', 0)}** | Regional release names: **{recoveries.get('regional_release_name', 0)}** | Title equivalents: **{recoveries.get('title_equivalence', 0)}** | Bilingual titles: **{recoveries.get('bilingual_title', 0)}** | Japanese romanization: **{recoveries.get('japanese_romanization', 0)}** | Strict typos: **{recoveries.get('strict_typo', 0)}** | Linked versions: **{report.get('linked_version_recoveries', 0)}** | Non-catalog version parents: **{report.get('external_version_parent_recoveries', 0)}** | Extended series roots: **{report.get('extended_series_recoveries', 0)}**.", '']
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
