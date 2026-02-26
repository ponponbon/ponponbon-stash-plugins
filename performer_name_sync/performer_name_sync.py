# Performer Name Sync
# Cross-references JavStash and StashDB to sync performer names and enrich metadata.

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
FIND_SCENES_BY_PERFORMER_QUERY = """
query FindScenesByPerformer($performer_id: [ID!]) {
    findScenes(
        scene_filter: { performers: { value: $performer_id, modifier: INCLUDES } }
        filter: { per_page: -1 }
    ) {
        scenes {
            id
            performers { id }
        }
    }
}
"""

FIND_GALLERIES_BY_PERFORMER_QUERY = """
query FindGalleriesByPerformer($performer_id: [ID!]) {
    findGalleries(
        gallery_filter: { performers: { value: $performer_id, modifier: INCLUDES } }
        filter: { per_page: -1 }
    ) {
        galleries {
            id
            performers { id }
        }
    }
}
"""

FIND_IMAGES_BY_PERFORMER_QUERY = """
query FindImagesByPerformer($performer_id: [ID!]) {
    findImages(
        image_filter: { performers: { value: $performer_id, modifier: INCLUDES } }
        filter: { per_page: -1 }
    ) {
        images {
            id
            performers { id }
        }
    }
}
"""

SCENE_UPDATE_QUERY = """
mutation SceneUpdate($id: ID!, $performer_ids: [ID!]) {
    sceneUpdate(input: { id: $id, performer_ids: $performer_ids }) { id }
}
"""

GALLERY_UPDATE_QUERY = """
mutation GalleryUpdate($id: ID!, $performer_ids: [ID!]) {
    galleryUpdate(input: { id: $id, performer_ids: $performer_ids }) { id }
}
"""

IMAGE_UPDATE_QUERY = """
mutation ImageUpdate($id: ID!, $performer_ids: [ID!]) {
    imageUpdate(input: { id: $id, performer_ids: $performer_ids }) { id }
}
"""

PERFORMER_DESTROY_QUERY = """
mutation PerformerDestroy($id: ID!) {
    performerDestroy(id: $id)
}
"""

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
                           {'term': term},
                           ensure_graphql(stashdb_ep), stashdb_key)
    if not data:
        log_warn(f"    StashDB search '{term}': no response data")
        return None, None
    results = data.get('searchPerformer', [])
    if not results:
        log(f"    StashDB search '{term}': 0 results")
        return None, None
    result_names = [p.get('name', '') for p in results]
    log(f"    StashDB search '{term}': {len(results)} result(s) -> {result_names}")
    target = term.strip().lower()
    for p in results:
        if p.get('name', '').strip().lower() == target:
            return p['name'], p['id']
        for alias in (p.get('aliases') or []):
            if alias.strip().lower() == target:
                return p['name'], p['id']
    log(f"    StashDB search '{term}': no exact name/alias match in results")
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
# Duplicate detection and merging
# ---------------------------------------------------------------------------
def _count_filled_fields(performer):
    """Score a performer by how much metadata they have filled in."""
    score = 0
    for key in ('name', 'disambiguation', 'gender', 'birthdate', 'ethnicity',
                'country', 'eye_color', 'hair_color', 'height_cm', 'weight',
                'measurements', 'fake_tits', 'career_length', 'tattoos',
                'piercings', 'url', 'details'):
        if not _is_empty(performer.get(key)):
            score += 1
    score += len(performer.get('alias_list') or [])
    score += len(performer.get('urls') or [])
    score += len(performer.get('stash_ids') or [])
    return score


def _pick_keeper(performers):
    """Pick the best performer to keep. Prefer the one with more metadata."""
    return max(performers, key=_count_filled_fields)


def _merge_performer_metadata(keeper, dupe):
    """Merge metadata from dupe into keeper, producing an update payload.
    Only fills empty fields on keeper; merges multi-value fields."""
    payload = {'id': keeper['id']}
    changed = False

    # Name: keep the keeper's name, but add dupe's name as alias
    new_aliases = list(keeper.get('alias_list') or [])
    dupe_name = (dupe.get('name') or '').strip()
    keeper_name = (keeper.get('name') or '').strip()

    # Add dupe's name as alias if different from keeper's name
    if dupe_name and dupe_name.lower() != keeper_name.lower():
        if dupe_name not in new_aliases:
            new_aliases.append(dupe_name)
            changed = True

    # Merge dupe's aliases
    existing_lower = {a.strip().lower() for a in new_aliases}
    existing_lower.add(keeper_name.lower())
    for alias in (dupe.get('alias_list') or []):
        a = alias.strip()
        if a and a.lower() not in existing_lower:
            existing_lower.add(a.lower())
            new_aliases.append(a)
            changed = True

    payload['alias_list'] = dedup_aliases(new_aliases)

    # Merge stash_ids (avoid duplicates by endpoint+stash_id)
    merged_sids = []
    seen_sids = set()
    for sid in (keeper.get('stash_ids') or []):
        key = (norm_endpoint(sid['endpoint']), sid['stash_id'])
        if key not in seen_sids:
            seen_sids.add(key)
            merged_sids.append({'endpoint': sid['endpoint'], 'stash_id': sid['stash_id']})
    for sid in (dupe.get('stash_ids') or []):
        key = (norm_endpoint(sid['endpoint']), sid['stash_id'])
        if key not in seen_sids:
            seen_sids.add(key)
            merged_sids.append({'endpoint': sid['endpoint'], 'stash_id': sid['stash_id']})
            changed = True
    payload['stash_ids'] = merged_sids

    # Merge scalar fields: fill empty on keeper from dupe
    for field in ('disambiguation', 'gender', 'birthdate', 'ethnicity', 'country',
                  'eye_color', 'hair_color', 'height_cm', 'weight', 'measurements',
                  'fake_tits', 'career_length', 'details', 'url'):
        if _is_empty(keeper.get(field)) and not _is_empty(dupe.get(field)):
            payload[field] = dupe[field]
            changed = True

    # Merge tattoos and piercings strings
    for field in ('tattoos', 'piercings'):
        keeper_val = (keeper.get(field) or '').strip()
        dupe_val = (dupe.get(field) or '').strip()
        if dupe_val and not keeper_val:
            payload[field] = dupe_val
            changed = True
        elif dupe_val and keeper_val:
            existing = {e.strip().lower() for e in keeper_val.split(',') if e.strip()}
            new_parts = []
            for part in dupe_val.split(','):
                p = part.strip()
                if p and p.lower() not in existing:
                    existing.add(p.lower())
                    new_parts.append(p)
            if new_parts:
                payload[field] = keeper_val.rstrip().rstrip(',') + ', ' + ', '.join(new_parts)
                changed = True

    # Merge URLs
    keeper_urls = list(keeper.get('urls') or [])
    keeper_url_set = {u.strip().rstrip('/').lower() for u in keeper_urls if isinstance(u, str)}
    new_urls = []
    for u in (dupe.get('urls') or []):
        if isinstance(u, str) and u.strip().rstrip('/').lower() not in keeper_url_set:
            keeper_url_set.add(u.strip().rstrip('/').lower())
            new_urls.append(u)
    if new_urls:
        payload['urls'] = keeper_urls + new_urls
        changed = True

    return payload, changed


def _reassign_content(stash, dupe_id, keeper_id, dry_run=False):
    """Reassign all scenes, galleries, and images from dupe to keeper."""
    reassigned = {'scenes': 0, 'galleries': 0, 'images': 0}

    # Scenes
    result = stash.call_GQL(FIND_SCENES_BY_PERFORMER_QUERY,
                            {'performer_id': [dupe_id]})
    scenes = (result or {}).get('findScenes', {}).get('scenes', [])
    for scene in scenes:
        perf_ids = [p['id'] for p in (scene.get('performers') or [])]
        # Replace dupe with keeper, avoiding duplicates
        new_ids = []
        for pid in perf_ids:
            if pid == dupe_id:
                if keeper_id not in new_ids:
                    new_ids.append(keeper_id)
            else:
                if pid not in new_ids:
                    new_ids.append(pid)
        if set(new_ids) != set(perf_ids):
            if not dry_run:
                stash.call_GQL(SCENE_UPDATE_QUERY,
                               {'id': scene['id'], 'performer_ids': new_ids})
            reassigned['scenes'] += 1

    # Galleries
    result = stash.call_GQL(FIND_GALLERIES_BY_PERFORMER_QUERY,
                            {'performer_id': [dupe_id]})
    galleries = (result or {}).get('findGalleries', {}).get('galleries', [])
    for gallery in galleries:
        perf_ids = [p['id'] for p in (gallery.get('performers') or [])]
        new_ids = []
        for pid in perf_ids:
            if pid == dupe_id:
                if keeper_id not in new_ids:
                    new_ids.append(keeper_id)
            else:
                if pid not in new_ids:
                    new_ids.append(pid)
        if set(new_ids) != set(perf_ids):
            if not dry_run:
                stash.call_GQL(GALLERY_UPDATE_QUERY,
                               {'id': gallery['id'], 'performer_ids': new_ids})
            reassigned['galleries'] += 1

    # Images
    result = stash.call_GQL(FIND_IMAGES_BY_PERFORMER_QUERY,
                            {'performer_id': [dupe_id]})
    images = (result or {}).get('findImages', {}).get('images', [])
    for image in images:
        perf_ids = [p['id'] for p in (image.get('performers') or [])]
        new_ids = []
        for pid in perf_ids:
            if pid == dupe_id:
                if keeper_id not in new_ids:
                    new_ids.append(keeper_id)
            else:
                if pid not in new_ids:
                    new_ids.append(pid)
        if set(new_ids) != set(perf_ids):
            if not dry_run:
                stash.call_GQL(IMAGE_UPDATE_QUERY,
                               {'id': image['id'], 'performer_ids': new_ids})
            reassigned['images'] += 1

    return reassigned


def _merge_duplicate_group(stash, group, dry_run=False):
    """Merge a group of duplicate performers into one. Returns (keeper_id, merged_count)."""
    if len(group) < 2:
        return None, 0

    keeper = _pick_keeper(group)
    dupes = [p for p in group if p['id'] != keeper['id']]

    log(f"  Merge group: keeping '{keeper['name']}' (id={keeper['id']}), "
        f"merging {len(dupes)} duplicate(s)")

    for dupe in dupes:
        log(f"    Merging '{dupe['name']}' (id={dupe['id']}) into '{keeper['name']}'")

        # Merge metadata
        payload, changed = _merge_performer_metadata(keeper, dupe)
        if changed and not dry_run:
            try:
                stash.update_performer(payload)
                # Update keeper in-memory with merged data for subsequent merges
                for k, v in payload.items():
                    if k != 'id':
                        keeper[k] = v
            except Exception as e:
                log_err(f"    Failed to update keeper '{keeper['name']}': {e}")
                continue

        # Reassign content
        reassigned = _reassign_content(stash, dupe['id'], keeper['id'], dry_run)
        if any(reassigned.values()):
            log(f"    Reassigned: {reassigned['scenes']} scene(s), "
                f"{reassigned['galleries']} gallery(ies), "
                f"{reassigned['images']} image(s)")

        # Delete the duplicate
        if not dry_run:
            try:
                stash.call_GQL(PERFORMER_DESTROY_QUERY, {'id': dupe['id']})
                log(f"    Deleted duplicate '{dupe['name']}' (id={dupe['id']})")
            except Exception as e:
                log_err(f"    Failed to delete duplicate '{dupe['name']}': {e}")
        else:
            log(f"    [DRY] Would delete duplicate '{dupe['name']}' (id={dupe['id']})")

    return keeper['id'], len(dupes)


def find_and_merge_duplicates(stash, performers, javstash_ep=None, dry_run=False):
    """Detect and merge duplicate performers.
    
    Duplicates are detected by:
    1. Same JavStash stash_id (two local performers linked to same JavStash entry)
    2. Same StashDB stash_id
    3. Same name (case-insensitive) after stripping whitespace
    
    Returns: dict with stats, and set of deleted performer IDs.
    """
    stats = {'groups_found': 0, 'duplicates_merged': 0}
    deleted_ids = set()

    # --- Phase 1: Group by stash_id (most reliable) ---
    log("Checking for duplicates by stash-box ID...")
    stashid_groups = {}  # key = (normalized_endpoint, stash_id) -> [performers]
    for p in performers:
        for sid in (p.get('stash_ids') or []):
            key = (norm_endpoint(sid.get('endpoint', '')), sid.get('stash_id', ''))
            if key[1]:  # has a stash_id
                stashid_groups.setdefault(key, []).append(p)

    for key, group in stashid_groups.items():
        # Filter out already-deleted performers
        group = [p for p in group if p['id'] not in deleted_ids]
        if len(group) < 2:
            continue
        ep_label = key[0].split('/')[-1] if '/' in key[0] else key[0]
        names = [p['name'] for p in group]
        log(f"  Duplicate group (stash_id {key[1]} @ {ep_label}): {names}")
        stats['groups_found'] += 1
        _, merged = _merge_duplicate_group(stash, group, dry_run)
        stats['duplicates_merged'] += merged
        # Track which IDs were deleted
        keeper = _pick_keeper(group)
        for p in group:
            if p['id'] != keeper['id']:
                deleted_ids.add(p['id'])

    # --- Phase 2: Group by name (case-insensitive) ---
    log("Checking for duplicates by name...")
    name_groups = {}
    for p in performers:
        if p['id'] in deleted_ids:
            continue
        name_key = (p.get('name') or '').strip().lower()
        if name_key:
            name_groups.setdefault(name_key, []).append(p)

    for name_key, group in name_groups.items():
        group = [p for p in group if p['id'] not in deleted_ids]
        if len(group) < 2:
            continue
        ids = [p['id'] for p in group]
        log(f"  Duplicate group (name '{group[0]['name']}'): IDs {ids}")
        stats['groups_found'] += 1
        _, merged = _merge_duplicate_group(stash, group, dry_run)
        stats['duplicates_merged'] += merged
        keeper = _pick_keeper(group)
        for p in group:
            if p['id'] != keeper['id']:
                deleted_ids.add(p['id'])

    # --- Phase 3: Cross-check aliases against names ---
    log("Checking for duplicates by alias-to-name match...")
    # Build name->performer index (only surviving performers)
    surviving = [p for p in performers if p['id'] not in deleted_ids]
    name_index = {}
    for p in surviving:
        nk = (p.get('name') or '').strip().lower()
        if nk:
            name_index.setdefault(nk, []).append(p)

    alias_merge_pairs = []  # (performer_with_alias, performer_with_matching_name)
    seen_pairs = set()
    for p in surviving:
        for alias in (p.get('alias_list') or []):
            ak = alias.strip().lower()
            if ak and ak in name_index:
                for match in name_index[ak]:
                    if match['id'] != p['id']:
                        pair_key = tuple(sorted([p['id'], match['id']]))
                        if pair_key not in seen_pairs:
                            seen_pairs.add(pair_key)
                            alias_merge_pairs.append((p, match))

    for p1, p2 in alias_merge_pairs:
        if p1['id'] in deleted_ids or p2['id'] in deleted_ids:
            continue
        group = [p1, p2]
        log(f"  Duplicate group (alias match): '{p1['name']}' <-> '{p2['name']}'")
        stats['groups_found'] += 1
        _, merged = _merge_duplicate_group(stash, group, dry_run)
        stats['duplicates_merged'] += merged
        keeper = _pick_keeper(group)
        for p in group:
            if p['id'] != keeper['id']:
                deleted_ids.add(p['id'])

    return stats, deleted_ids


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

    # ---- Phase 0: Pre-sync duplicate cleanup ----
    log("=" * 50)
    log("PHASE 1: Pre-sync duplicate cleanup")
    log("=" * 50)
    dedup_stats, deleted_ids = find_and_merge_duplicates(
        stash, performers, dry_run=dry_run
    )
    if dedup_stats['duplicates_merged'] > 0:
        log(f"  Pre-sync: found {dedup_stats['groups_found']} duplicate group(s), "
            f"merged {dedup_stats['duplicates_merged']} duplicate(s)")
    else:
        log("  Pre-sync: no duplicates found")

    # Filter to those with JavStash stash_ids (excluding deleted performers)
    javstash_ep = javstash_box['endpoint']
    candidates = []
    for p in performers:
        if p['id'] in deleted_ids:
            continue
        for sid in (p.get('stash_ids') or []):
            if endpoint_matches(sid.get('endpoint', ''), javstash_ep):
                candidates.append(p)
                break

    log(f"  {len(candidates)} performer(s) with JavStash stash-box IDs (after dedup)")

    # ---- Phase 1: Main sync loop ----
    log("=" * 50)
    log("PHASE 2: Performer name sync & enrichment")
    log("=" * 50)
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

    log_progress(0.95)

    # ---- Phase 2: Post-sync duplicate cleanup ----
    # After sync, performers may have been renamed to the same name or
    # resolved to the same StashDB ID, creating new duplicates.
    log("=" * 50)
    log("PHASE 3: Post-sync duplicate cleanup")
    log("=" * 50)
    log("Re-fetching performers after sync to check for new duplicates...")
    result = stash.call_GQL(ALL_PERFORMERS_QUERY)
    post_performers = result.get('findPerformers', {}).get('performers', [])

    post_dedup_stats, post_deleted = find_and_merge_duplicates(
        stash, post_performers, dry_run=dry_run
    )
    if post_dedup_stats['duplicates_merged'] > 0:
        log(f"  Post-sync: found {post_dedup_stats['groups_found']} duplicate group(s), "
            f"merged {post_dedup_stats['duplicates_merged']} duplicate(s)")
    else:
        log("  Post-sync: no new duplicates found")

    total_dupes = dedup_stats['duplicates_merged'] + post_dedup_stats['duplicates_merged']

    log_progress(1.0)
    log("=" * 50)
    log(f"Done!  Updated={stats['updated']}  "
        f"DuplicatesMerged={total_dupes}  "
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