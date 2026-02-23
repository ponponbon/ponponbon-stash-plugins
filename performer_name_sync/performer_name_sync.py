# testupdate - verify CI pipeline packages all plugins correctly
"""
Performer Name Sync - Stash Plugin
===================================
Cross-references JavStash and StashDB stash-box endpoints to update
performer names from Japanese to English and enrich local metadata
from StashDB.

For every performer with a JavStash stash-box ID whose primary name
contains non-Latin characters:
  1. Query JavStash for the performer's aliases
  2. Find the first Latin-character alias (English fallback)
  3. Search StashDB for a matching performer
  4. Update the local name (StashDB match preferred, else JavStash alias)
  5. Attach the StashDB stash_id to the performer if a match is found
  6. Preserve the original Japanese name in aliases
  7. Enrich local metadata from StashDB (fills empty fields only):
     disambiguation, gender, birthdate, ethnicity, country,
     eye/hair color, height, measurements, career length, tattoos,
     piercings, URL, and breast type

For performers already synced (Latin name), re-running will:
  - Skip if multiple stash-box IDs are present (already cross-referenced)
  - Proceed through the full pipeline if only JavStash stash-box ID exists
  - Attach a missing StashDB stash_id if a match is found
  - Update the name to StashDB's recommended name if it differs
  - Enrich metadata fields from StashDB

StashDB is treated as the preferred source of truth for enrichment,
while preserving all existing JavStash links, aliases, and locally
curated data.  For multi-value fields (aliases, URLs, tattoos,
piercings), new entries from StashDB are merged rather than skipped
when local data already exists.

Requires: stashapp-tools  (pip install stashapp-tools)
"""

import sys
import json
import re
import requests

# ---------------------------------------------------------------------------
# Logging helpers (Stash raw plugin protocol via stderr)
# ---------------------------------------------------------------------------
def log(msg):
    print(f"\x03{msg}", file=sys.stderr, flush=True)

def log_warn(msg):
    print(f"\x04{msg}", file=sys.stderr, flush=True)

def log_err(msg):
    print(f"\x05{msg}", file=sys.stderr, flush=True)

def log_progress(pct):
    print(f"\x06{pct:.2f}", file=sys.stderr, flush=True)

# ---------------------------------------------------------------------------
# Regex for detecting Latin-only names
# Allows letters, digits, common punctuation, spaces
# ---------------------------------------------------------------------------
LATIN_RE = re.compile(r'^[A-Za-z0-9\s\-\'.,()&!?]+$')

def is_latin(name: str) -> bool:
    """Return True if name consists only of Latin/ASCII characters."""
    return bool(name and LATIN_RE.match(name.strip()))

# ---------------------------------------------------------------------------
# Stash-box GraphQL client (for querying JavStash / StashDB directly)
# ---------------------------------------------------------------------------
class StashBoxClient:
    def __init__(self, endpoint: str, api_key: str):
        self.endpoint = endpoint.rstrip('/')
        # Ensure we hit the graphql endpoint
        if not self.endpoint.endswith('/graphql'):
            self.endpoint += '/graphql'
        self.session = requests.Session()
        self.session.headers['Content-Type'] = 'application/json'
        if api_key:
            self.session.headers['ApiKey'] = api_key

    def _query(self, query: str, variables: dict = None):
        body = {'query': query}
        if variables:
            body['variables'] = variables
        r = self.session.post(self.endpoint, json=body, timeout=30)
        r.raise_for_status()
        j = r.json()
        if 'errors' in j:
            for e in j['errors']:
                log_err(f"StashBox GQL: {e.get('message', e)}")
        return j.get('data', {})

    def find_performer(self, performer_id: str):
        """Fetch a performer by ID (JavStash)."""
        data = self._query("""
            query FindPerformer($id: ID!) {
                findPerformer(id: $id) {
                    id
                    name
                    aliases
                }
            }
        """, {'id': performer_id})
        return data.get('findPerformer')

    def search_performer(self, term: str):
        """Search performers by name (StashDB). Returns summarised results."""
        data = self._query("""
            query QueryPerformers($input: PerformerSearchInput!) {
                searchPerformer(input: $input) {
                    id
                    name
                    aliases
                }
            }
        """, {'input': {'term': term}})
        return data.get('searchPerformer', [])

    def find_performer_full(self, performer_id: str):
        """
        Fetch full performer metadata from StashDB by ID.
        Returns the raw performer dict with all enrichment fields.
        """
        data = self._query("""
            query FindPerformerFull($id: ID!) {
                findPerformer(id: $id) {
                    id
                    name
                    disambiguation
                    aliases
                    gender
                    birth_date
                    ethnicity
                    country
                    eye_color
                    hair_color
                    height
                    cup_size
                    band_size
                    waist_size
                    hip_size
                    breast_type
                    career_start_year
                    career_end_year
                    tattoos  { location description }
                    piercings { location description }
                    urls { url type }
                }
            }
        """, {'id': performer_id})
        return data.get('findPerformer')


# ---------------------------------------------------------------------------
# Identify stash-box endpoints from Stash configuration
# ---------------------------------------------------------------------------
def identify_stashboxes(stash):
    """
    Query the Stash configuration for stash-box endpoints and identify
    which is JavStash and which is StashDB by URL substring matching.
    Returns (javstash_box, stashdb_box) as dicts with 'endpoint' and 'api_key'.
    """
    result = stash.call_GQL("""
        query { configuration { general { stashBoxes {
            endpoint api_key name
        }}}}
    """)
    boxes = result.get('configuration', {}).get('general', {}).get('stashBoxes', [])

    javstash = None
    stashdb = None

    for box in boxes:
        ep = (box.get('endpoint') or '').lower()
        name = (box.get('name') or '').lower()
        if 'javstash' in ep or 'javstash' in name:
            javstash = box
        elif 'stashdb' in ep or 'stashdb' in name:
            stashdb = box

    return javstash, stashdb


def endpoint_matches(performer_endpoint: str, box_endpoint: str) -> bool:
    """Check if a performer's stash_id endpoint matches a stash-box endpoint."""
    # Normalize: strip trailing slashes and /graphql
    def norm(u):
        u = u.rstrip('/')
        if u.endswith('/graphql'):
            u = u[:-len('/graphql')]
        return u.rstrip('/').lower()
    return norm(performer_endpoint) == norm(box_endpoint)


# ---------------------------------------------------------------------------
# Find the best StashDB match
# ---------------------------------------------------------------------------
def find_stashdb_match(stashdb_client: StashBoxClient, english_name: str):
    """
    Search StashDB for a performer matching english_name.
    Returns (name, stashdb_id) if a good match is found, otherwise (None, None).
    """
    results = stashdb_client.search_performer(english_name)
    if not results:
        return None, None

    # Look for an exact case-insensitive match first
    target = english_name.strip().lower()
    for p in results:
        if p.get('name', '').strip().lower() == target:
            return p['name'], p['id']
        # Also check aliases
        for alias in (p.get('aliases') or []):
            if alias.strip().lower() == target:
                return p['name'], p['id']

    return None, None


# ---------------------------------------------------------------------------
# Metadata enrichment helpers
# ---------------------------------------------------------------------------

def _is_empty(value) -> bool:
    """Return True if a local field should be considered unpopulated."""
    if value is None:
        return True
    if isinstance(value, str) and value.strip() == '':
        return True
    return False


def _format_body_mod(mods) -> str:
    """Convert a list of {location, description} dicts from StashDB into a string."""
    if not mods or not isinstance(mods, list):
        return ''
    parts = []
    for m in mods:
        if not isinstance(m, dict):
            continue
        loc = (m.get('location') or '').strip()
        desc = (m.get('description') or '').strip()
        if loc and desc:
            parts.append(f"{loc}: {desc}")
        elif loc:
            parts.append(loc)
        elif desc:
            parts.append(desc)
    return ', '.join(parts)


def _compose_measurements(stashdb_p: dict) -> str:
    """
    Build a measurements string from StashDB band_size, cup_size, waist_size, hip_size.
    Format: "32B-24-34".  Returns empty string if insufficient data.
    """
    band = stashdb_p.get('band_size')
    cup = (stashdb_p.get('cup_size') or '').strip()
    waist = stashdb_p.get('waist_size')
    hip = stashdb_p.get('hip_size')
    if band and cup:
        bust_part = f"{band}{cup}"
        parts = [bust_part]
        if waist:
            parts.append(str(waist))
        if hip:
            parts.append(str(hip))
        return '-'.join(parts)
    return ''


def _map_breast_type(breast_type) -> str:
    """
    Map StashDB breast_type enum to a Stash-compatible fake_tits string.
    NATURAL -> 'No', FAKE / AUGMENTED -> 'Yes', NA / None / unknown -> ''
    """
    if not breast_type:
        return ''
    bt = str(breast_type).upper()
    if bt in ('FAKE', 'AUGMENTED'):
        return 'Yes'
    if bt == 'NATURAL':
        return 'No'
    return ''


def _merge_aliases(stashdb_aliases: list, local_aliases: list, current_name: str) -> list:
    """
    Return a list of StashDB aliases that are NOT already present in local_aliases
    or matching the current primary name.  Case-insensitive dedup.
    """
    if not stashdb_aliases:
        return []
    existing = {a.strip().lower() for a in local_aliases}
    if current_name:
        existing.add(current_name.strip().lower())
    new_aliases = []
    for alias in stashdb_aliases:
        alias = alias.strip() if isinstance(alias, str) else ''
        if alias and alias.lower() not in existing:
            existing.add(alias.lower())
            new_aliases.append(alias)
    return new_aliases


def _merge_urls(stashdb_urls: list, local_p: dict) -> list:
    """
    Return a list of URL strings from StashDB that are NOT already present
    in the local performer's urls list.  Case-insensitive, trailing-slash
    normalised comparison.
    """
    if not stashdb_urls:
        return []
    # Gather existing local URLs (both singular 'url' and plural 'urls')
    local_urls = set()
    singular = (local_p.get('url') or '').strip()
    if singular:
        local_urls.add(singular.rstrip('/').lower())
    for u in (local_p.get('urls') or []):
        if isinstance(u, str):
            local_urls.add(u.strip().rstrip('/').lower())

    new_urls = []
    for entry in stashdb_urls:
        url = ''
        if isinstance(entry, dict):
            url = (entry.get('url') or '').strip()
        elif isinstance(entry, str):
            url = entry.strip()
        if url and url.rstrip('/').lower() not in local_urls:
            local_urls.add(url.rstrip('/').lower())
            new_urls.append(url)
    return new_urls


def _merge_body_mods(stashdb_mods, local_value: str) -> str:
    """
    Merge StashDB body-modification entries (tattoos / piercings) with existing
    local text.  Parses local comma-separated entries, appends any StashDB
    entries whose normalised text is not already present, returns the combined
    string.  If local is empty, returns the full StashDB string.
    """
    stashdb_str = _format_body_mod(stashdb_mods)
    if not stashdb_str:
        return ''
    if _is_empty(local_value):
        return stashdb_str
    # Parse existing entries from local (comma-separated)
    existing = {e.strip().lower() for e in local_value.split(',') if e.strip()}
    new_parts = []
    for part in stashdb_str.split(','):
        part = part.strip()
        if part and part.lower() not in existing:
            existing.add(part.lower())
            new_parts.append(part)
    if new_parts:
        return local_value.rstrip().rstrip(',') + ', ' + ', '.join(new_parts)
    return ''


def build_enrichment_data(stashdb_p: dict, local_p: dict) -> dict:
    """
    Compare a full StashDB performer record against the current local performer
    record and return a dict of fields to apply to the local record.

    Rules:
      - For scalar fields: only fills when empty/None locally (never overwrites).
      - For multi-value text fields (tattoos, piercings): merges new entries
        from StashDB that are not already present locally.
      - Does NOT touch name, alias_list, urls, or stash_ids (managed separately).
    """
    enriched = {}

    # Disambiguation
    if _is_empty(local_p.get('disambiguation')):
        val = (stashdb_p.get('disambiguation') or '').strip()
        if val:
            enriched['disambiguation'] = val

    # Gender
    if _is_empty(local_p.get('gender')):
        val = stashdb_p.get('gender')
        if val:
            enriched['gender'] = val

    # Birthdate  (StashDB field: birth_date)
    if _is_empty(local_p.get('birthdate')):
        bd = stashdb_p.get('birth_date') or ''
        if isinstance(bd, dict):
            bd = (bd.get('date') or '').strip()
        bd = str(bd).strip() if bd else ''
        if bd:
            enriched['birthdate'] = bd

    # Ethnicity
    if _is_empty(local_p.get('ethnicity')):
        val = stashdb_p.get('ethnicity')
        if val:
            enriched['ethnicity'] = val

    # Country
    if _is_empty(local_p.get('country')):
        val = (stashdb_p.get('country') or '').strip()
        if val:
            enriched['country'] = val

    # Eye color
    if _is_empty(local_p.get('eye_color')):
        val = stashdb_p.get('eye_color')
        if val:
            enriched['eye_color'] = val

    # Hair color
    if _is_empty(local_p.get('hair_color')):
        val = stashdb_p.get('hair_color')
        if val:
            enriched['hair_color'] = val

    # Height (StashDB: integer cm -> local height_cm: integer)
    if _is_empty(local_p.get('height_cm')):
        val = stashdb_p.get('height')
        if val:
            try:
                enriched['height_cm'] = int(val)
            except (ValueError, TypeError):
                pass

    # Measurements (composed from StashDB band/cup/waist/hip)
    if _is_empty(local_p.get('measurements')):
        val = _compose_measurements(stashdb_p)
        if val:
            enriched['measurements'] = val

    # Fake tits (derived from StashDB breast_type)
    if _is_empty(local_p.get('fake_tits')):
        val = _map_breast_type(stashdb_p.get('breast_type'))
        if val:
            enriched['fake_tits'] = val

    # Career length (composed from career_start_year / career_end_year)
    if _is_empty(local_p.get('career_length')):
        start = stashdb_p.get('career_start_year')
        end = stashdb_p.get('career_end_year')
        if start:
            career_str = str(start)
            if end:
                career_str += f"-{end}"
            enriched['career_length'] = career_str

    # Tattoos (merge: append new entries from StashDB not already in local)
    tattoo_merged = _merge_body_mods(stashdb_p.get('tattoos'), local_p.get('tattoos') or '')
    if tattoo_merged:
        enriched['tattoos'] = tattoo_merged

    # Piercings (merge: append new entries from StashDB not already in local)
    piercing_merged = _merge_body_mods(stashdb_p.get('piercings'), local_p.get('piercings') or '')
    if piercing_merged:
        enriched['piercings'] = piercing_merged

    # URL (singular legacy field) - fill only if empty
    if _is_empty(local_p.get('url')):
        urls = stashdb_p.get('urls') or []
        if isinstance(urls, list):
            chosen_url = ''
            for u in urls:
                if isinstance(u, dict) and (u.get('type') or '').upper() == 'HOME':
                    chosen_url = u.get('url', '')
                    break
            if not chosen_url and urls and isinstance(urls[0], dict):
                chosen_url = urls[0].get('url', '')
            if chosen_url:
                enriched['url'] = chosen_url
    # Note: plural 'urls' merge is handled separately via _merge_urls()

    return enriched


# ---------------------------------------------------------------------------
# Core processing
# ---------------------------------------------------------------------------
def process(stash, dry_run=False):
    log("Identifying stash-box endpoints from configuration...")
    javstash_box, stashdb_box = identify_stashboxes(stash)

    if not javstash_box:
        log_err("Could not find JavStash stash-box endpoint in configuration. "
                "Make sure a stash-box with 'javstash' in the URL or name is configured.")
        return
    log(f"  JavStash: {javstash_box['endpoint']}")

    if not stashdb_box:
        log_warn("Could not find StashDB stash-box endpoint in configuration. "
                 "Will use JavStash Latin aliases only (no StashDB cross-reference).")
    else:
        log(f"  StashDB:  {stashdb_box['endpoint']}")

    javstash_client = StashBoxClient(javstash_box['endpoint'], javstash_box.get('api_key', ''))
    stashdb_client = StashBoxClient(stashdb_box['endpoint'], stashdb_box.get('api_key', '')) if stashdb_box else None

    # Fetch all performers (including full metadata for enrichment comparison)
    log("Fetching all performers from local Stash...")
    result = stash.call_GQL("""
        query {
            findPerformers(filter: { per_page: -1 }) {
                performers {
                    id
                    name
                    disambiguation
                    alias_list
                    gender
                    birthdate
                    ethnicity
                    country
                    eye_color
                    hair_color
                    height_cm
                    weight
                    measurements
                    fake_tits
                    career_length
                    tattoos
                    piercings
                    url
                    urls
                    details
                    stash_ids { endpoint stash_id }
                }
            }
        }
    """)
    performers = result.get('findPerformers', {}).get('performers', [])
    log(f"  {len(performers)} performer(s) found")

    # Filter to those with JavStash stash_ids
    javstash_ep = javstash_box['endpoint']
    candidates = []
    for p in performers:
        stash_ids = p.get('stash_ids') or []
        for sid in stash_ids:
            if endpoint_matches(sid.get('endpoint', ''), javstash_ep):
                candidates.append((p, sid['stash_id']))
                break

    log(f"  {len(candidates)} performer(s) with JavStash stash-box IDs")

    # Process
    stats = {'updated': 0, 'skipped_latin': 0, 'skipped_no_alias': 0,
             'stashdb_match': 0, 'javstash_fallback': 0, 'stashdb_linked': 0,
             'enriched': 0, 'errors': 0}
    total = len(candidates)

    for i, (performer, jav_stash_id) in enumerate(candidates):
        log_progress(i / max(total, 1))
        p_id = performer['id']
        current_name = performer.get('name', '')
        current_aliases = performer.get('alias_list') or []

        # --------------------------------------------------------------
        # Skip check: Already-Latin name with multiple stash-box IDs
        #   means already cross-referenced → safe to skip.
        #   Single stash-box ID (JavStash only) → fall through to the
        #   full pipeline below (query JavStash for aliases, search
        #   StashDB, enrich, etc.)
        # --------------------------------------------------------------
        if is_latin(current_name):
            existing_stash_ids = performer.get('stash_ids') or []
            if len(existing_stash_ids) > 1:
                log(f"  [{i+1}/{total}] {current_name}: already has {len(existing_stash_ids)} stash-box IDs, skipping")
                stats['skipped_latin'] += 1
                continue
            # Single stash-box ID — proceed through full pipeline below
            log(f"  [{i+1}/{total}] {current_name}: Latin name with single stash-box ID, re-processing")

        # --------------------------------------------------------------
        # Full enrichment pipeline (PATH B)
        # Handles both non-Latin names and Latin names with single
        # stash-box ID that need cross-referencing / enrichment.
        # --------------------------------------------------------------

        # Query JavStash for performer details
        try:
            jav_performer = javstash_client.find_performer(jav_stash_id)
        except Exception as e:
            log_err(f"  [{i+1}/{total}] {current_name}: JavStash query failed: {e}")
            stats['errors'] += 1
            continue

        if not jav_performer:
            log_warn(f"  [{i+1}/{total}] {current_name}: not found on JavStash (id={jav_stash_id})")
            stats['errors'] += 1
            continue

        jav_name = jav_performer.get('name', '')
        jav_aliases = jav_performer.get('aliases') or []

        # Find first Latin alias from JavStash
        english_name = None
        for alias in jav_aliases:
            if is_latin(alias):
                english_name = alias.strip()
                break

        if not english_name:
            if is_latin(jav_name):
                english_name = jav_name.strip()

        if not english_name:
            log(f"  [{i+1}/{total}] {current_name}: no Latin alias found on JavStash, skipping")
            stats['skipped_no_alias'] += 1
            continue

        # Try StashDB cross-reference
        new_name = None
        stashdb_performer_id = None
        if stashdb_client:
            try:
                stashdb_name, stashdb_performer_id = find_stashdb_match(stashdb_client, english_name)
                if stashdb_name:
                    new_name = stashdb_name
                    stats['stashdb_match'] += 1
                    log(f"  [{i+1}/{total}] {current_name} -> {new_name} (StashDB match, id={stashdb_performer_id})")
            except Exception as e:
                log_warn(f"  [{i+1}/{total}] {current_name}: StashDB search failed: {e}")

        if not new_name:
            new_name = english_name
            stats['javstash_fallback'] += 1
            log(f"  [{i+1}/{total}] {current_name} -> {new_name} (JavStash alias)")

        # Build updated aliases: preserve existing + add original Japanese name
        updated_aliases = list(current_aliases)
        if current_name and current_name not in updated_aliases:
            updated_aliases.append(current_name)
        updated_aliases = [a for a in updated_aliases if a.strip().lower() != new_name.strip().lower()]
        seen = set()
        deduped = []
        for a in updated_aliases:
            key = a.strip().lower()
            if key not in seen:
                seen.add(key)
                deduped.append(a)
        updated_aliases = deduped

        # Build updated stash_ids: preserve existing + add StashDB if matched
        existing_stash_ids = performer.get('stash_ids') or []
        updated_stash_ids = [
            {'endpoint': s['endpoint'], 'stash_id': s['stash_id']}
            for s in existing_stash_ids
        ]
        if stashdb_performer_id and stashdb_box:
            stashdb_ep = stashdb_box['endpoint']
            already_has = any(
                endpoint_matches(s['endpoint'], stashdb_ep)
                for s in updated_stash_ids
            )
            if not already_has:
                updated_stash_ids.append({
                    'endpoint': stashdb_ep,
                    'stash_id': stashdb_performer_id,
                })
                stats['stashdb_linked'] += 1
                log(f"    Attaching StashDB stash_id {stashdb_performer_id}")

        # Fetch full StashDB data for metadata enrichment + merge
        enrichment = {}
        alias_merge = []
        url_merge = []
        if stashdb_performer_id and stashdb_client:
            try:
                stashdb_full = stashdb_client.find_performer_full(stashdb_performer_id)
                if stashdb_full:
                    enrichment = build_enrichment_data(stashdb_full, performer)
                    # Merge StashDB aliases into local alias list
                    alias_merge = _merge_aliases(
                        stashdb_full.get('aliases') or [],
                        updated_aliases,
                        new_name,
                    )
                    # Merge StashDB URLs into local URLs
                    url_merge = _merge_urls(stashdb_full.get('urls') or [], performer)
                    if enrichment:
                        log(f"    Enrichment fields from StashDB: {list(enrichment.keys())}")
                    if alias_merge:
                        log(f"    Merging {len(alias_merge)} new alias(es) from StashDB")
                    if url_merge:
                        log(f"    Merging {len(url_merge)} new URL(s) from StashDB")
                    if enrichment or alias_merge or url_merge:
                        stats['enriched'] += 1
            except Exception as e:
                log_warn(f"  [{i+1}/{total}] {current_name}: StashDB enrichment fetch failed: {e}")

        # Incorporate alias merge
        if alias_merge:
            updated_aliases = updated_aliases + alias_merge
            seen = set()
            deduped = []
            for a in updated_aliases:
                key = a.strip().lower()
                if key not in seen:
                    seen.add(key)
                    deduped.append(a)
            updated_aliases = deduped

        if dry_run:
            log(f"    [DRY] Would update: name='{new_name}', aliases={updated_aliases}, "
                f"stash_ids={updated_stash_ids}, enrichment={list(enrichment.keys())}"
                f"{', url_merge=' + str(url_merge) if url_merge else ''}")
            stats['updated'] += 1
            continue

        # Build final update payload: base fields + enrichment
        update_payload = {
            'id': p_id,
            'name': new_name,
            'alias_list': updated_aliases,
            'stash_ids': updated_stash_ids,
        }
        update_payload.update(enrichment)

        # Merge URLs if any new ones from StashDB
        if url_merge:
            local_urls = list(performer.get('urls') or [])
            update_payload['urls'] = local_urls + url_merge

        # Update the performer
        try:
            stash.update_performer(update_payload)
            stats['updated'] += 1
        except Exception as e:
            log_err(f"  [{i+1}/{total}] {current_name}: update failed: {e}")
            stats['errors'] += 1

    log_progress(1.0)
    log("=" * 50)
    log(f"Done!  Updated={stats['updated']}  "
        f"StashDB={stats['stashdb_match']}  StashDBLinked={stats['stashdb_linked']}  "
        f"Enriched={stats['enriched']}  "
        f"JavFallback={stats['javstash_fallback']}  "
        f"SkippedLatin={stats['skipped_latin']}  SkippedNoAlias={stats['skipped_no_alias']}  "
        f"Errors={stats['errors']}")
    if dry_run:
        log("(Dry run - no changes were made)")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
def main():
    raw = sys.stdin.read()
    try:
        plugin_input = json.loads(raw)
    except Exception:
        plugin_input = {}

    server = plugin_input.get('server_connection', {})
    scheme = server.get('Scheme', 'http')
    host = server.get('Host', 'localhost')
    if host in ('0.0.0.0', ''):
        host = 'localhost'
    port = server.get('Port', 9999)
    api_key = server.get('ApiKey', '')

    mode = plugin_input.get('args', {}).get('mode', 'live')
    dry_run = mode == 'dry_run'
    log(f"Mode: {'DRY RUN' if dry_run else 'LIVE'}")

    # Connect via stashapp-tools
    try:
        from stashapi.stashapp import StashInterface
    except ImportError:
        log_err("stashapp-tools is not installed. Run: pip install stashapp-tools")
        print(json.dumps({'output': 'error'}))
        sys.exit(1)

    # StashInterface expects a logging.Logger-compatible object
    import logging
    logger = logging.getLogger("performer_name_sync")
    logger.setLevel(logging.DEBUG)
    handler = logging.StreamHandler(sys.stderr)
    handler.setLevel(logging.DEBUG)
    logger.addHandler(handler)

    stash = StashInterface({
        'scheme': scheme,
        'host': host,
        'port': port,
        'ApiKey': api_key,
        'logger': logger,
    })

    process(stash, dry_run)
    print(json.dumps({'output': 'ok'}))


if __name__ == '__main__':
    main()
