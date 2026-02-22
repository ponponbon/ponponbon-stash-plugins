# testupdate - verify CI pipeline packages all plugins correctly
"""
Performer Name Sync - Stash Plugin
===================================
Cross-references JavStash and StashDB stash-box endpoints to update
performer names from Japanese to English.

For every performer with a JavStash stash-box ID whose primary name
contains non-Latin characters:
  1. Query JavStash for the performer's aliases
  2. Find the first Latin-character alias (English fallback)
  3. Search StashDB for a matching performer
  4. Update the local name (StashDB match preferred, else JavStash alias)
  5. Attach the StashDB stash_id to the performer if a match is found
  6. Preserve the original Japanese name in aliases

For performers already synced (Latin name), re-running will:
  - Attach a missing StashDB stash_id if a match is found
  - Update the name to StashDB's recommended name if it differs

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
        """Search performers by name (StashDB)."""
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

    # Fetch all performers
    log("Fetching all performers from local Stash...")
    result = stash.call_GQL("""
        query {
            findPerformers(filter: { per_page: -1 }) {
                performers {
                    id
                    name
                    alias_list
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
             'stashdb_match': 0, 'javstash_fallback': 0, 'stashdb_linked': 0, 'errors': 0}
    total = len(candidates)

    for i, (performer, jav_stash_id) in enumerate(candidates):
        log_progress(i / max(total, 1))
        p_id = performer['id']
        current_name = performer.get('name', '')
        current_aliases = performer.get('alias_list') or []

        # If already Latin, check if StashDB stash_id is missing or name
        # differs from StashDB's recommended name (handles re-runs)
        if is_latin(current_name):
            if not stashdb_client or not stashdb_box:
                stats['skipped_latin'] += 1
                continue

            # Check if performer already has a StashDB stash_id
            stashdb_ep = stashdb_box['endpoint']
            existing_stash_ids = performer.get('stash_ids') or []
            has_stashdb_id = any(
                endpoint_matches(s.get('endpoint', ''), stashdb_ep)
                for s in existing_stash_ids
            )

            # Search StashDB to check name and get stash_id
            try:
                stashdb_name, stashdb_pid = find_stashdb_match(stashdb_client, current_name)
            except Exception as e:
                log_warn(f"  [{i+1}/{total}] {current_name}: StashDB search failed: {e}")
                stats['skipped_latin'] += 1
                continue

            if not stashdb_name:
                stats['skipped_latin'] += 1
                continue

            needs_name_update = stashdb_name.strip().lower() != current_name.strip().lower()
            needs_stashdb_link = not has_stashdb_id and stashdb_pid

            if not needs_name_update and not needs_stashdb_link:
                stats['skipped_latin'] += 1
                continue

            # Build update payload for already-Latin performer
            update_data = {'id': p_id}

            if needs_name_update:
                log(f"  [{i+1}/{total}] {current_name} -> {stashdb_name} (StashDB recommended name)")
                # Preserve old name in aliases
                updated_aliases = list(current_aliases)
                if current_name and current_name not in updated_aliases:
                    updated_aliases.append(current_name)
                updated_aliases = [a for a in updated_aliases if a.strip().lower() != stashdb_name.strip().lower()]
                seen = set()
                deduped = []
                for a in updated_aliases:
                    key = a.strip().lower()
                    if key not in seen:
                        seen.add(key)
                        deduped.append(a)
                update_data['name'] = stashdb_name
                update_data['alias_list'] = deduped
                stats['stashdb_match'] += 1

            if needs_stashdb_link:
                updated_stash_ids = [
                    {'endpoint': s['endpoint'], 'stash_id': s['stash_id']}
                    for s in existing_stash_ids
                ]
                updated_stash_ids.append({
                    'endpoint': stashdb_ep,
                    'stash_id': stashdb_pid,
                })
                update_data['stash_ids'] = updated_stash_ids
                stats['stashdb_linked'] += 1
                log(f"    Attaching StashDB stash_id {stashdb_pid}")

            if dry_run:
                log(f"    [DRY] Would update (latin re-check): {update_data}")
                stats['updated'] += 1
                continue

            try:
                stash.update_performer(update_data)
                stats['updated'] += 1
            except Exception as e:
                log_err(f"  [{i+1}/{total}] {current_name}: update failed: {e}")
                stats['errors'] += 1
            continue

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
            # Maybe the JavStash primary name itself is Latin
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

        # Build updated aliases list: preserve existing + add original Japanese name
        updated_aliases = list(current_aliases)  # copy
        # Add the original Japanese name if not already present
        if current_name and current_name not in updated_aliases:
            updated_aliases.append(current_name)
        # Remove new_name from aliases if it happens to be there (it's now the primary)
        updated_aliases = [a for a in updated_aliases if a.strip().lower() != new_name.strip().lower()]
        # Deduplicate while preserving order
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
            # Only add if not already present
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

        if dry_run:
            log(f"    [DRY] Would update: name='{new_name}', aliases={updated_aliases}, stash_ids={updated_stash_ids}")
            stats['updated'] += 1
            continue

        # Update the performer
        try:
            stash.update_performer({
                'id': p_id,
                'name': new_name,
                'alias_list': updated_aliases,
                'stash_ids': updated_stash_ids,
            })
            stats['updated'] += 1
        except Exception as e:
            log_err(f"  [{i+1}/{total}] {current_name}: update failed: {e}")
            stats['errors'] += 1

    log_progress(1.0)
    log("=" * 50)
    log(f"Done!  Updated={stats['updated']}  "
        f"StashDB={stats['stashdb_match']}  StashDBLinked={stats['stashdb_linked']}  "
        f"JavFallback={stats['javstash_fallback']}  "
        f"SkippedLatin={stats['skipped_latin']}  SkippedNoAlias={stats['skipped_no_alias']}  "
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
