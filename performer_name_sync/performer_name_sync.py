# Performer Name Sync
# For performers linked to JavStash: pick a romanized (Latin) display name from
# JavStash, keep the old name as an alias, and enrich/link from StashDB.
# Run after Performer Cleanup, which handles duplicates, orphans and linking.

import sys
import json
import os
import re
import time
import requests

# ---------------------------------------------------------------------------
# Logging helpers (Stash raw plugin protocol via stderr)
# ---------------------------------------------------------------------------
def stash_log(level, msg):
    print(f"\x01{level}\x02{msg}", file=sys.stderr, flush=True)

def log(msg):
    stash_log("i", msg)

def log_warn(msg):
    stash_log("w", msg)

def log_err(msg):
    stash_log("e", msg)

def log_progress(pct):
    stash_log("p", f"{pct:.2f}")

def first_nonempty(*values):
    for value in values:
        if value is None:
            continue
        text = str(value).strip()
        if text:
            return text
    return ""

def api_key_from_input(plugin_input):
    args = plugin_input.get('args', {})
    server = plugin_input.get('server_connection', {})
    return first_nonempty(
        args.get('api_key'),
        args.get('f_api_key'),
        server.get('ApiKey'),
        os.environ.get('STASH_API_KEY'),
    )

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
            status = getattr(getattr(e, 'response', None), 'status_code', None)
            log_err(f"GQL request failed (attempt {attempt+1}/{retries}): {e}")
            if status == 422:
                return None  # Semantic rejection, don't retry
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
query SearchPerformer($term: String!) {
    searchPerformer(term: $term) {
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
            tags { id name }
            stash_ids { endpoint stash_id }
        }
    }
}
"""

FIND_TAGS_QUERY = """
query FindTags($filter: FindFilterType) {
    findTags(filter: $filter) { tags { id name } }
}
"""

TAG_CREATE_MUTATION = """
mutation TagCreate($input: TagCreateInput!) {
    tagCreate(input: $input) { id name }
}
"""

PERFORMER_TAGS_UPDATE_MUTATION = """
mutation PerformerUpdate($input: PerformerUpdateInput!) {
    performerUpdate(input: $input) { id }
}
"""

REVIEW_TAG_NAME = "Review: Possible Duplicate"

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
# Stash-box search - find exact match by name or alias
# ---------------------------------------------------------------------------
def search_stashbox(term, endpoint, api_key, label='stash-box',
                    cache=None, require_unique=False):
    cache_key = None
    if cache is not None:
        cache_key = (norm_endpoint(endpoint), (term or '').strip().lower())
        if cache_key in cache:
            return cache[cache_key]

    data = graphql_request(SEARCH_PERFORMER_QUERY,
                           {'term': term},
                           ensure_graphql(endpoint), api_key)
    if not data:
        log_warn(f"    {label} search '{term}': no response data")
        result = (None, None)
        if cache is not None:
            cache[cache_key] = result
        return result
    results = data.get('searchPerformer', [])
    if not results:
        log(f"    {label} search '{term}': 0 results")
        result = (None, None)
        if cache is not None:
            cache[cache_key] = result
        return result
    result_names = [p.get('name', '') for p in results]
    log(f"    {label} search '{term}': {len(results)} result(s) -> {result_names}")
    target = term.strip().lower()
    exact_matches = []
    for p in results:
        if p.get('name', '').strip().lower() == target:
            exact_matches.append(p)
            continue
        for alias in (p.get('aliases') or []):
            if alias.strip().lower() == target:
                exact_matches.append(p)
                break

    if exact_matches:
        unique = {}
        for p in exact_matches:
            unique[p['id']] = p
        if require_unique and len(unique) > 1:
            names = [p.get('name', '') for p in unique.values()]
            log_warn(f"    {label} search '{term}': ambiguous exact matches -> {names}")
            result = (None, None)
        else:
            match = next(iter(unique.values()))
            result = (match['name'], match['id'])
        if cache is not None:
            cache[cache_key] = result
        return result

    log(f"    {label} search '{term}': no exact name/alias match in results")
    result = (None, None)
    if cache is not None:
        cache[cache_key] = result
    return result


# ---------------------------------------------------------------------------
# StashDB search - compatibility wrapper
# ---------------------------------------------------------------------------
def search_stashdb(term, stashdb_ep, stashdb_key, cache=None):
    # require_unique: an ambiguous exact match is logged by search_stashbox
    # and returned as no match instead of linking the first hit.
    return search_stashbox(term, stashdb_ep, stashdb_key, 'StashDB',
                           cache=cache, require_unique=True)

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
# Rename-collision check
# ---------------------------------------------------------------------------
def normalize_name(name):
    if not isinstance(name, str):
        return ''
    return ' '.join(name.casefold().split())


def build_name_index(performers):
    """{normalized name: [performer, ...]} for collision checks."""
    index = {}
    for p in performers:
        key = normalize_name(p.get('name'))
        if key:
            index.setdefault(key, []).append(p)
    return index


def find_rename_collision(performer_id, new_name, new_disambiguation, name_index):
    """Return a different performer that already uses new_name, or None.
    Two performers whose disambiguations are both set and differ are
    explicitly distinct, so that is not treated as a collision."""
    new_dis = normalize_name(new_disambiguation)
    for other in name_index.get(normalize_name(new_name), []):
        if str(other.get('id')) == str(performer_id):
            continue
        other_dis = normalize_name(other.get('disambiguation'))
        if new_dis and other_dis and new_dis != other_dis:
            continue
        return other
    return None


def reindex_rename(name_index, performer, new_name):
    """Move performer to new_name in the index (used after a planned rename)."""
    old_key = normalize_name(performer.get('name'))
    if old_key in name_index:
        name_index[old_key] = [p for p in name_index[old_key]
                               if str(p.get('id')) != str(performer.get('id'))]
    name_index.setdefault(normalize_name(new_name), []).append(performer)
    performer['name'] = new_name


# ---------------------------------------------------------------------------
# Process a single performer
# ---------------------------------------------------------------------------
def process_performer(performer, javstash_box, stashdb_box, dry_run=False,
                      name_index=None, collisions=None):
    """Process one performer. Returns a status string, or the
    PerformerUpdateInput payload when something should change. dry_run is
    accepted for compatibility; the caller decides whether to write.
    When name_index is given, a rename onto another performer's name is
    skipped and (performer, other, wanted_name) is appended to collisions."""
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

    log(f"  [{current_name}] JavStash id={jav_stash_id}, stash-box count={len(existing_stash_ids)}")

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
    log(f"  [{current_name}] JavStash name='{jav_name}', aliases={jav_aliases}")

    # ---- Build candidate list ----
    # Priority: JavStash Latin aliases (in order), then JavStash primary name,
    # then current local name. De-duplicated case-insensitively.
    candidates = []
    seen_terms = set()
    for alias in jav_aliases:
        a = alias.strip() if isinstance(alias, str) else ''
        if a and is_latin(a) and a.lower() not in seen_terms:
            candidates.append(a)
            seen_terms.add(a.lower())
        elif a and not is_latin(a):
            log(f"  [{current_name}] alias '{a}' is non-Latin, skipping as candidate")
    if jav_name and is_latin(jav_name) and jav_name.strip().lower() not in seen_terms:
        candidates.append(jav_name.strip())
        seen_terms.add(jav_name.strip().lower())
    if current_name and is_latin(current_name) and current_name.strip().lower() not in seen_terms:
        candidates.append(current_name.strip())
        seen_terms.add(current_name.strip().lower())

    log(f"  [{current_name}] candidates={candidates}")

    # Best available Latin name from JavStash (used for rename independent of StashDB)
    best_latin = candidates[0] if candidates else None

    if not best_latin:
        log(f"  {current_name}: no Latin name found on JavStash or locally, skipping")
        return 'skipped_no_alias'

    log(f"  [{current_name}] best_latin='{best_latin}', will search StashDB with {len(candidates)} candidate(s)")

    # ---- Loop all candidates against StashDB (fixes rerun dead-end) ----
    stashdb_name = stashdb_pid = stashdb_full = None
    if stashdb_box:
        stashdb_ep = stashdb_box['endpoint']
        stashdb_key = stashdb_box.get('api_key', '')
        for term in candidates:
            stashdb_name, stashdb_pid = search_stashdb(term, stashdb_ep, stashdb_key)
            if stashdb_name:
                log(f"  [{current_name}] StashDB matched '{stashdb_name}' (id={stashdb_pid}) on term '{term}'")
                break
        if not stashdb_name:
            log(f"  [{current_name}] StashDB: no match found after trying all {len(candidates)} candidate(s)")
        if stashdb_pid:
            stashdb_full = fetch_performer_full(stashdb_pid, stashdb_ep, stashdb_key)
    else:
        log(f"  [{current_name}] no StashDB box configured, skipping StashDB search")

    # ---- Determine new name (decoupled from StashDB match) ----
    # Rename to the best Latin alias regardless of whether StashDB matched.
    # StashDB canonical name overrides if available.
    if stashdb_name:
        new_name = stashdb_name
    else:
        new_name = best_latin

    name_changed = new_name.strip().lower() != current_name.strip().lower()
    if name_changed and name_index is not None:
        effective_dis = performer.get('disambiguation') or (
            (stashdb_full or {}).get('disambiguation') or '')
        other = find_rename_collision(p_id, new_name, effective_dis, name_index)
        if other is not None:
            log_warn(f"  {current_name}: rename to '{new_name}' skipped - performer "
                     f"'{other.get('name')}' (id={other.get('id')}) already has that name; "
                     f"tagging both '{REVIEW_TAG_NAME}'")
            if collisions is not None:
                collisions.append((performer, other, new_name))
            new_name = current_name
            name_changed = False
    if name_changed:
        log(f"  {current_name} -> {new_name} {'(StashDB)' if stashdb_name else '(JavStash alias)'}")
    elif stashdb_pid:
        log(f"  {current_name}: name unchanged, StashDB matched (id={stashdb_pid}), enriching")
    else:
        log(f"  {current_name}: no StashDB match found across {len(candidates)} candidate(s)")

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

    return update_payload  # caller logs (preview) or executes (live)


# ---------------------------------------------------------------------------
# Main processing loop
# ---------------------------------------------------------------------------
def fetch_all_performers(stash):
    result = stash.call_GQL(ALL_PERFORMERS_QUERY) or {}
    return result.get('findPerformers', {}).get('performers', [])


def get_review_tag_id(stash, dry_run=False):
    result = stash.call_GQL(FIND_TAGS_QUERY, {'filter': {'q': REVIEW_TAG_NAME, 'per_page': -1}}) or {}
    for t in (result.get('findTags') or {}).get('tags') or []:
        if normalize_name(t.get('name')) == normalize_name(REVIEW_TAG_NAME):
            return str(t['id'])
    if dry_run:
        log(f"  [DRY] Would create tag '{REVIEW_TAG_NAME}'")
        return 'new-tag'
    created = (stash.call_GQL(TAG_CREATE_MUTATION, {'input': {'name': REVIEW_TAG_NAME}}) or {}).get('tagCreate')
    if not created:
        raise RuntimeError(f"could not create tag '{REVIEW_TAG_NAME}'")
    return str(created['id'])


def tag_collisions(stash, collisions, dry_run=False):
    """Add the review tag to both sides of every skipped rename."""
    if not collisions:
        return 0
    tag_id = get_review_tag_id(stash, dry_run)
    targets = {}
    for performer, other, wanted in collisions:
        targets[str(performer['id'])] = performer
        targets[str(other['id'])] = other
    tagged = 0
    for pid, p in targets.items():
        current = [str(t['id']) for t in (p.get('tags') or [])]
        if tag_id in current:
            continue
        if dry_run:
            log(f"    [DRY] Would add tag '{REVIEW_TAG_NAME}' to '{p.get('name')}' (id={pid})")
        else:
            try:
                stash.call_GQL(PERFORMER_TAGS_UPDATE_MUTATION,
                               {'input': {'id': pid, 'tag_ids': current + [tag_id]}})
            except Exception as e:
                log_err(f"    Failed to tag '{p.get('name')}' (id={pid}): {e}")
                continue
        p['tags'] = (p.get('tags') or []) + [{'id': tag_id, 'name': REVIEW_TAG_NAME}]
        tagged += 1
    return tagged


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

    log("Fetching all performers from local Stash...")
    performers = fetch_all_performers(stash)
    log(f"  {len(performers)} performer(s) found")
    name_index = build_name_index(performers)

    javstash_ep = javstash_box['endpoint']
    candidates = []
    for p in performers:
        for sid in (p.get('stash_ids') or []):
            if endpoint_matches(sid.get('endpoint', ''), javstash_ep):
                candidates.append(p)
                break

    log(f"  {len(candidates)} performer(s) with JavStash stash-box IDs")
    log("=" * 50)
    log("Performer name sync & StashDB enrichment")
    log("=" * 50)
    stats = {'updated': 0, 'skipped_multi': 0, 'skipped_no_alias': 0,
             'skipped_no_change': 0, 'rename_collisions': 0, 'errors': 0}
    collisions = []
    total = len(candidates)

    for i, performer in enumerate(candidates):
        log_progress(i / max(total, 1))
        old_name = performer.get('name', '')

        try:
            result = process_performer(performer, javstash_box, stashdb_box, dry_run,
                                       name_index=name_index, collisions=collisions)
        except Exception as e:
            log_err(f"  {old_name}: processing failed: {e}")
            result = 'error'

        if isinstance(result, dict):
            if dry_run:
                log(f"    [DRY] Would update: {result}")
                ok = True
            else:
                try:
                    stash.update_performer(result)
                    ok = True
                except Exception as e:
                    log_err(f"  {old_name}: update failed: {e}")
                    ok = False
            if ok:
                stats['updated'] += 1
                new_name = result.get('name') or old_name
                if new_name != old_name:
                    reindex_rename(name_index, performer, new_name)
            else:
                stats['errors'] += 1
        elif result == 'error':
            stats['errors'] += 1
        elif result in stats:
            stats[result] += 1

    stats['rename_collisions'] = len(collisions)
    if collisions:
        log("=" * 50)
        log(f"Tagging {len(collisions)} rename collision(s) with '{REVIEW_TAG_NAME}'")
        tag_collisions(stash, collisions, dry_run)

    log_progress(1.0)
    log("=" * 50)
    log(f"Done!  Updated={stats['updated']}  "
        f"RenameCollisions={stats['rename_collisions']}  "
        f"SkippedMultiID={stats['skipped_multi']}  "
        f"SkippedNoAlias={stats['skipped_no_alias']}  "
        f"SkippedNoChange={stats['skipped_no_change']}  "
        f"Errors={stats['errors']}")
    if dry_run:
        log("(Dry run — no changes were made)")
    return stats


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
    api_key = api_key_from_input(plugin_input)

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
