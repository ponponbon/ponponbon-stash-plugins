"""
Performer Name Sync - Stash Plugin
===================================
Cross-references JavStash and StashDB stash-box endpoints to update
performer names from Japanese to English and enrich local metadata
from StashDB.

For every performer with a JavStash stash-box ID:
  1. If already has multiple stash-box IDs (cross-referenced) → skip
  2. Query JavStash for the performer's aliases
  3. Determine the best Latin search term (JavStash alias or current name)
  4. Search StashDB for a matching performer
  5. Update the local name (StashDB match preferred, else JavStash alias)
  6. Attach the StashDB stash_id if a match is found
  7. Preserve the original name in aliases
  8. Enrich local metadata from StashDB, merging multi-value fields
     (aliases, URLs, tattoos, piercings) and filling empty scalar fields

Requires: stashapp-tools  (pip install stashapp-tools)
"""

import sys
import json
import re
import time
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
# Latin-character detection
# ---------------------------------------------------------------------------
LATIN_RE = re.compile(r'^[A-Za-z0-9\s\-\'.,()&!?]+$')

def is_latin(name: str) -> bool:
    return bool(name and LATIN_RE.match(name.strip()))

# ---------------------------------------------------------------------------
# GraphQL helper with retries
# ---------------------------------------------------------------------------
def graphql_request(query, variables, endpoint, api_key, retries=3):
    headers = {'Content-Type': 'application/json'}
    if api_key:
        headers['ApiKey'] = api_key
    for attempt in range(retries):
        try:
            r = requests.post(endpoint, json={'query': query, 'variables': variables},
                              headers=headers, timeout=30)
            r.raise_for_status()
            j = r.json()
            if 'errors' in j:
                for e in j['errors']:
                    log_err(f"GQL error: {e.get('message', e)}")
                return None
            return j.get('data')
        except requests.exceptions.RequestException as e:
            log_err(f"GQL request failed (attempt {attempt+1}/{retries}): {e}")
            if attempt < retries - 1:
                time.sleep(2 ** attempt)
            else:
                return None

# ---------------------------------------------------------------------------
# Stash-box queries
# ---------------------------------------------------------------------------
FIND_PERFORMER_QUERY = """
query FindPerformer($id: ID!) {
    findPerformer(id: $id) {
        id
        name
        aliases
    }
}
"""

SEARCH_PERFORMER_QUERY = """
query SearchPerformer($input: PerformerSearchInput!) {
    searchPerformer(input: $input) {
        id
        name
        aliases
    }
}
"""

FIND_PERFORMER_FULL_QUERY = """
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
"""

# ---------------------------------------------------------------------------
# Local Stash queries
# ---------------------------------------------------------------------------
ALL_PERFORMERS_QUERY = """
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
"""

STASH_CONFIG_QUERY = """
query { configuration { general { stashBoxes {
    endpoint api_key name
}}}}
"""

# ---------------------------------------------------------------------------
# Endpoint helpers
# ---------------------------------------------------------------------------
def norm_endpoint(u):
    u = (u or '').rstrip('/')
    if u.endswith('/graphql'):
        u = u[:-len('/graphql')]
    return u.rstrip('/').lower()

def endpoint_matches(a, b):
    return norm_endpoint(a) == norm_endpoint(b)

def ensure_graphql(ep):
    ep = ep.rstrip('/')
    if not ep.endswith('/graphql'):
        ep += '/graphql'
    return ep

# ---------------------------------------------------------------------------
# Identify stash-box endpoints
# ---------------------------------------------------------------------------
def identify_stashboxes(stash):
    result = stash.call_GQL(STASH_CONFIG_QUERY)
    boxes = result.get('configuration', {}).get('general', {}).get('stashBoxes', [])
    javstash = stashdb = None
    for box in boxes:
        ep = (box.get('endpoint') or '').lower()
        name = (box.get('name') or '').lower()
        if 'javstash' in ep or 'javstash' in name:
            javstash = box
        elif 'stashdb' in ep or 'stashdb' in name:
            stashdb = box
    return javstash, stashdb

# ---------------------------------------------------------------------------
# StashDB search — find exact match by name or alias
# ---------------------------------------------------------------------------
def search_stashdb(term, stashdb_ep, stashdb_key):
    data = graphql_request(SEARCH_PERFORMER_QUERY,
                           {'input': {'term': term}},
                           ensure_graphql(stashdb_ep), stashdb_key)
    if not data:
        return None, None
    results = data.get('searchPerformer', [])
    target = term.strip().lower()
    for p in results:
        if p.get('name', '').strip().lower() == target:
            return p['name'], p['id']
        for alias in (p.get('aliases') or []):
            if alias.strip().lower() == target:
                return p['name'], p['id']
    return None, None

# ---------------------------------------------------------------------------
# Fetch full performer from a stash-box
# ---------------------------------------------------------------------------
def fetch_performer(performer_id, endpoint, api_key):
    data = graphql_request(FIND_PERFORMER_QUERY, {'id': performer_id},
                           ensure_graphql(endpoint), api_key)
    if data:
        return data.get('findPerformer')
    return None

def fetch_performer_full(performer_id, endpoint, api_key):
    data = graphql_request(FIND_PERFORMER_FULL_QUERY, {'id': performer_id},
                           ensure_graphql(endpoint), api_key)
    if data:
        return data.get('findPerformer')
    return None

# ---------------------------------------------------------------------------
# Enrichment / merge helpers
# ---------------------------------------------------------------------------
def _is_empty(value):
    if value is None:
        return True
    if isinstance(value, str) and value.strip() == '':
        return True
    return False

def _format_body_mod(mods):
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

def _merge_body_mods(stashdb_mods, local_value):
    """Merge body-mod entries, appending new ones from StashDB."""
    stashdb_str = _format_body_mod(stashdb_mods)
    if not stashdb_str:
        return ''
    if _is_empty(local_value):
        return stashdb_str
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

def merge_aliases(stashdb_aliases, local_aliases, current_name, new_name):
    """Return list of new aliases from StashDB not already in local."""
    if not stashdb_aliases:
        return []
    existing = {a.strip().lower() for a in local_aliases}
    if current_name:
        existing.add(current_name.strip().lower())
    if new_name:
        existing.add(new_name.strip().lower())
    new = []
    for alias in stashdb_aliases:
        a = alias.strip() if isinstance(alias, str) else ''
        if a and a.lower() not in existing:
            existing.add(a.lower())
            new.append(a)
    return new

def merge_urls(stashdb_urls, local_performer):
    """Return list of new URL strings from StashDB not already local."""
    if not stashdb_urls:
        return []
    local = set()
    singular = (local_performer.get('url') or '').strip()
    if singular:
        local.add(singular.rstrip('/').lower())
    for u in (local_performer.get('urls') or []):
        if isinstance(u, str):
            local.add(u.strip().rstrip('/').lower())
    new = []
    for entry in stashdb_urls:
        url = ''
        if isinstance(entry, dict):
            url = (entry.get('url') or '').strip()
        elif isinstance(entry, str):
            url = entry.strip()
        if url and url.rstrip('/').lower() not in local:
            local.add(url.rstrip('/').lower())
            new.append(url)
    return new

def build_enrichment(stashdb_p, local_p):
    """Build dict of fields to enrich from StashDB. Scalar: fill empty only.
    Multi-value (tattoos, piercings): merge."""
    e = {}

    if _is_empty(local_p.get('disambiguation')):
        v = (stashdb_p.get('disambiguation') or '').strip()
        if v:
            e['disambiguation'] = v

    if _is_empty(local_p.get('gender')):
        v = stashdb_p.get('gender')
        if v:
            e['gender'] = v

    if _is_empty(local_p.get('birthdate')):
        bd = stashdb_p.get('birth_date') or ''
        if isinstance(bd, dict):
            bd = (bd.get('date') or '').strip()
        bd = str(bd).strip() if bd else ''
        if bd:
            e['birthdate'] = bd

    if _is_empty(local_p.get('ethnicity')):
        v = stashdb_p.get('ethnicity')
        if v:
            e['ethnicity'] = v

    if _is_empty(local_p.get('country')):
        v = (stashdb_p.get('country') or '').strip()
        if v:
            e['country'] = v

    if _is_empty(local_p.get('eye_color')):
        v = stashdb_p.get('eye_color')
        if v:
            e['eye_color'] = v

    if _is_empty(local_p.get('hair_color')):
        v = stashdb_p.get('hair_color')
        if v:
            e['hair_color'] = v

    if _is_empty(local_p.get('height_cm')):
        v = stashdb_p.get('height')
        if v:
            try:
                e['height_cm'] = int(v)
            except (ValueError, TypeError):
                pass

    if _is_empty(local_p.get('measurements')):
        band = stashdb_p.get('band_size')
        cup = (stashdb_p.get('cup_size') or '').strip()
        waist = stashdb_p.get('waist_size')
        hip = stashdb_p.get('hip_size')
        if band and cup:
            parts = [f"{band}{cup}"]
            if waist:
                parts.append(str(waist))
            if hip:
                parts.append(str(hip))
            e['measurements'] = '-'.join(parts)

    if _is_empty(local_p.get('fake_tits')):
        bt = (str(stashdb_p.get('breast_type') or '')).upper()
        if bt in ('FAKE', 'AUGMENTED'):
            e['fake_tits'] = 'Yes'
        elif bt == 'NATURAL':
            e['fake_tits'] = 'No'

    if _is_empty(local_p.get('career_length')):
        start = stashdb_p.get('career_start_year')
        end = stashdb_p.get('career_end_year')
        if start:
            e['career_length'] = f"{start}-{end}" if end else str(start)

    # Tattoos & piercings: merge
    tattoo_merged = _merge_body_mods(stashdb_p.get('tattoos'), local_p.get('tattoos') or '')
    if tattoo_merged:
        e['tattoos'] = tattoo_merged

    piercing_merged = _merge_body_mods(stashdb_p.get('piercings'), local_p.get('piercings') or '')
    if piercing_merged:
        e['piercings'] = piercing_merged

    # URL (singular legacy) - fill only if empty
    if _is_empty(local_p.get('url')):
        urls = stashdb_p.get('urls') or []
        if isinstance(urls, list):
            chosen = ''
            for u in urls:
                if isinstance(u, dict) and (u.get('type') or '').upper() == 'HOME':
                    chosen = u.get('url', '')
                    break
            if not chosen and urls and isinstance(urls[0], dict):
                chosen = urls[0].get('url', '')
            if chosen:
                e['url'] = chosen

    return e

def dedup_aliases(alias_list):
    seen = set()
    result = []
    for a in alias_list:
        key = a.strip().lower()
        if key and key not in seen:
            seen.add(key)
            result.append(a)
    return result

# ---------------------------------------------------------------------------
# Process a single performer
# ---------------------------------------------------------------------------
def process_performer(performer, javstash_box, stashdb_box, dry_run=False):
    """Process one performer. Returns a status string for stats tracking."""
    p_id = performer['id']
    current_name = performer.get('name', '')
    current_aliases = performer.get('alias_list') or []
    existing_stash_ids = performer.get('stash_ids') or []

    javstash_ep = javstash_box['endpoint']
    javstash_key = javstash_box.get('api_key', '')

    # Find JavStash stash_id for this performer
    jav_stash_id = None
    for sid in existing_stash_ids:
        if endpoint_matches(sid.get('endpoint', ''), javstash_ep):
            jav_stash_id = sid['stash_id']
            break

    if not jav_stash_id:
        return 'skip'  # No JavStash ID — not our performer

    # Already has multiple stash-box IDs → already cross-referenced, skip
    if len(existing_stash_ids) > 1:
        log(f"  {current_name}: already has {len(existing_stash_ids)} stash-box IDs, skipping")
        return 'skipped_multi'

    # ---- Query JavStash for performer details ----
    jav_performer = fetch_performer(jav_stash_id, javstash_ep, javstash_key)
    if not jav_performer:
        log_warn(f"  {current_name}: not found on JavStash (id={jav_stash_id})")
        return 'error'

    jav_aliases = jav_performer.get('aliases') or []
    jav_name = jav_performer.get('name', '')

    # Find best Latin name from JavStash
    english_name = None
    for alias in jav_aliases:
        if is_latin(alias):
            english_name = alias.strip()
            break
    if not english_name and is_latin(jav_name):
        english_name = jav_name.strip()

    # Also consider current local name if already Latin (re-run case)
    search_term = english_name
    if not search_term and is_latin(current_name):
        search_term = current_name.strip()

    if not search_term:
        log(f"  {current_name}: no Latin name found on JavStash, skipping")
        return 'skipped_no_alias'

    # ---- Search StashDB ----
    stashdb_name = None
    stashdb_pid = None
    stashdb_full = None

    if stashdb_box:
        stashdb_ep = stashdb_box['endpoint']
        stashdb_key = stashdb_box.get('api_key', '')

        stashdb_name, stashdb_pid = search_stashdb(search_term, stashdb_ep, stashdb_key)

        # If the current name is Latin and different from the JavStash alias,
        # also try searching by current name as fallback
        if not stashdb_name and is_latin(current_name) and current_name.strip().lower() != search_term.lower():
            stashdb_name, stashdb_pid = search_stashdb(current_name.strip(), stashdb_ep, stashdb_key)

        if stashdb_pid:
            stashdb_full = fetch_performer_full(stashdb_pid, stashdb_ep, stashdb_key)

    # ---- Determine new name ----
    if stashdb_name:
        new_name = stashdb_name
        log(f"  {current_name} -> {new_name} (StashDB match, id={stashdb_pid})")
    elif english_name:
        new_name = english_name
        log(f"  {current_name} -> {new_name} (JavStash alias)")
    else:
        # Current name is already Latin, no StashDB match — keep current
        new_name = current_name

    # If name hasn't changed and no StashDB data, check if there's anything to do
    name_changed = new_name.strip().lower() != current_name.strip().lower()

    # ---- Build aliases ----
    updated_aliases = list(current_aliases)
    # Preserve original name in aliases if it's changing
    if current_name and current_name not in updated_aliases:
        updated_aliases.append(current_name)
    # Remove new name from aliases (it's the primary name now)
    updated_aliases = [a for a in updated_aliases if a.strip().lower() != new_name.strip().lower()]

    # Merge StashDB aliases
    alias_merge = []
    if stashdb_full:
        alias_merge = merge_aliases(
            stashdb_full.get('aliases') or [],
            updated_aliases, current_name, new_name
        )
        if alias_merge:
            updated_aliases = updated_aliases + alias_merge
            log(f"    Merging {len(alias_merge)} new alias(es) from StashDB")

    updated_aliases = dedup_aliases(updated_aliases)

    # ---- Build stash_ids ----
    updated_stash_ids = [
        {'endpoint': s['endpoint'], 'stash_id': s['stash_id']}
        for s in existing_stash_ids
    ]
    stashdb_linked = False
    if stashdb_pid and stashdb_box:
        stashdb_ep = stashdb_box['endpoint']
        already_has = any(endpoint_matches(s['endpoint'], stashdb_ep)
                         for s in updated_stash_ids)
        if not already_has:
            updated_stash_ids.append({
                'endpoint': stashdb_ep,
                'stash_id': stashdb_pid,
            })
            stashdb_linked = True
            log(f"    Attaching StashDB stash_id {stashdb_pid}")

    # ---- Enrichment ----
    enrichment = {}
    url_merge = []
    if stashdb_full:
        enrichment = build_enrichment(stashdb_full, performer)
        url_merge = merge_urls(stashdb_full.get('urls') or [], performer)
        if enrichment:
            log(f"    Enrichment fields: {list(enrichment.keys())}")
        if url_merge:
            log(f"    Merging {len(url_merge)} new URL(s) from StashDB")

    # ---- Check if anything actually changed ----
    if not name_changed and not stashdb_linked and not enrichment and not alias_merge and not url_merge:
        log(f"  {current_name}: no changes needed, skipping")
        return 'skipped_no_change'

    # ---- Build update payload ----
    update_payload = {
        'id': p_id,
        'name': new_name,
        'alias_list': updated_aliases,
        'stash_ids': updated_stash_ids,
    }
    update_payload.update(enrichment)

    if url_merge:
        local_urls = list(performer.get('urls') or [])
        update_payload['urls'] = local_urls + url_merge

    if dry_run:
        log(f"    [DRY] Would update: {update_payload}")
        return 'updated'

    return update_payload  # Return payload for caller to execute


# ---------------------------------------------------------------------------
# Main processing loop
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
        log_warn("No StashDB endpoint found. Will use JavStash aliases only.")
    else:
        log(f"  StashDB:  {stashdb_box['endpoint']}")

    # Fetch all performers
    log("Fetching all performers from local Stash...")
    result = stash.call_GQL(ALL_PERFORMERS_QUERY)
    performers = result.get('findPerformers', {}).get('performers', [])
    log(f"  {len(performers)} performer(s) found")

    # Filter to those with JavStash stash_ids
    javstash_ep = javstash_box['endpoint']
    candidates = []
    for p in performers:
        for sid in (p.get('stash_ids') or []):
            if endpoint_matches(sid.get('endpoint', ''), javstash_ep):
                candidates.append(p)
                break

    log(f"  {len(candidates)} performer(s) with JavStash stash-box IDs")

    stats = {'updated': 0, 'skipped_multi': 0, 'skipped_no_alias': 0,
             'skipped_no_change': 0, 'errors': 0}
    total = len(candidates)

    for i, performer in enumerate(candidates):
        log_progress(i / max(total, 1))

        result = process_performer(performer, javstash_box, stashdb_box, dry_run)

        if isinstance(result, dict):
            # It's an update payload — execute it
            try:
                stash.update_performer(result)
                stats['updated'] += 1
            except Exception as e:
                log_err(f"  {performer['name']}: update failed: {e}")
                stats['errors'] += 1
        elif result == 'updated':
            stats['updated'] += 1
        elif result == 'skipped_multi':
            stats['skipped_multi'] += 1
        elif result == 'skipped_no_alias':
            stats['skipped_no_alias'] += 1
        elif result == 'skipped_no_change':
            stats['skipped_no_change'] += 1
        elif result == 'error':
            stats['errors'] += 1

    log_progress(1.0)
    log("=" * 50)
    log(f"Done!  Updated={stats['updated']}  "
        f"SkippedMultiID={stats['skipped_multi']}  "
        f"SkippedNoAlias={stats['skipped_no_alias']}  "
        f"SkippedNoChange={stats['skipped_no_change']}  "
        f"Errors={stats['errors']}")
    if dry_run:
        log("(Dry run — no changes were made)")


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

    try:
        from stashapi.stashapp import StashInterface
    except ImportError:
        log_err("stashapp-tools is not installed. Run: pip install stashapp-tools")
        print(json.dumps({'output': 'error'}))
        sys.exit(1)

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
