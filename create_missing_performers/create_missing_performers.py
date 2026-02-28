# Create Missing Performers
# Auto-creates missing performers from stash-box endpoints for identified scenes.

import sys
import json
import base64
import time
import requests

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
STASHBOX_DELAY = 1.5        # seconds between stash-box API calls (rate-limit)
BATCH_PAGE_SIZE = 100       # scenes per page when querying local Stash

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
# GraphQL helper with retries (for stash-box requests)
# ---------------------------------------------------------------------------
def graphql_request(query, variables, endpoint, api_key=None, retries=3):
    headers = {"Content-Type": "application/json"}
    if api_key:
        headers["ApiKey"] = api_key
    for attempt in range(retries):
        try:
            r = requests.post(
                endpoint,
                json={"query": query, "variables": variables},
                headers=headers,
                timeout=30,
            )
            r.raise_for_status()
            j = r.json()
            if "errors" in j:
                for e in j["errors"]:
                    log_err(f"GQL error ({endpoint}): {e.get('message', e)}")
                return None
            return j.get("data")
        except requests.exceptions.RequestException as e:
            status = getattr(getattr(e, "response", None), "status_code", None)
            log_err(f"GQL request to {endpoint} failed (attempt {attempt+1}/{retries}): {e}")
            if status == 422:
                return None  # semantic rejection, don't retry
            if attempt < retries - 1:
                time.sleep(2 ** attempt)
            else:
                return None

# ---------------------------------------------------------------------------
# Endpoint helpers
# ---------------------------------------------------------------------------
def norm_endpoint(u):
    u = (u or "").rstrip("/")
    if u.endswith("/graphql"):
        u = u[: -len("/graphql")]
    return u.rstrip("/").lower()

def endpoint_matches(a, b):
    return norm_endpoint(a) == norm_endpoint(b)

def ensure_graphql(ep):
    ep = ep.rstrip("/")
    if not ep.endswith("/graphql"):
        ep += "/graphql"
    return ep

# ---------------------------------------------------------------------------
# Local Stash queries
# ---------------------------------------------------------------------------
STASH_CONFIG_QUERY = """
query {
    configuration {
        general {
            stashBoxes {
                endpoint
                api_key
                name
            }
        }
    }
}
"""

FIND_IDENTIFIED_SCENES_QUERY = """
query FindScenes($filter: FindFilterType, $scene_filter: SceneFilterType) {
    findScenes(filter: $filter, scene_filter: $scene_filter) {
        count
        scenes {
            id
            title
            stash_ids {
                endpoint
                stash_id
            }
            performers {
                id
                name
                stash_ids {
                    endpoint
                    stash_id
                }
            }
        }
    }
}
"""

FIND_PERFORMER_BY_STASHID_QUERY = """
query FindPerformers($performer_filter: PerformerFilterType) {
    findPerformers(performer_filter: $performer_filter, filter: { per_page: -1 }) {
        count
        performers {
            id
            name
            stash_ids {
                endpoint
                stash_id
            }
        }
    }
}
"""

FIND_PERFORMER_BY_NAME_QUERY = """
query FindPerformers($performer_filter: PerformerFilterType) {
    findPerformers(performer_filter: $performer_filter, filter: { per_page: -1 }) {
        count
        performers {
            id
            name
            disambiguation
            stash_ids {
                endpoint
                stash_id
            }
        }
    }
}
"""

PERFORMER_CREATE_QUERY = """
mutation PerformerCreate($input: PerformerCreateInput!) {
    performerCreate(input: $input) {
        id
        name
    }
}
"""

PERFORMER_UPDATE_QUERY = """
mutation PerformerUpdate($input: PerformerUpdateInput!) {
    performerUpdate(input: $input) {
        id
        name
    }
}
"""

SCENE_UPDATE_QUERY = """
mutation SceneUpdate($input: SceneUpdateInput!) {
    sceneUpdate(input: $input) {
        id
        performers {
            id
            name
        }
    }
}
"""

# ---------------------------------------------------------------------------
# Stash-box queries
# ---------------------------------------------------------------------------
STASHBOX_FIND_SCENE_QUERY = """
query FindScene($id: ID!) {
    findScene(id: $id) {
        id
        title
        performers {
            as
            performer {
                id
                name
                disambiguation
                aliases
                gender
                birth_date
                death_date
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
                tattoos { location description }
                piercings { location description }
                images { id url }
                urls { url site { name } }
            }
        }
    }
}
"""

# ---------------------------------------------------------------------------
# Image download helper
# ---------------------------------------------------------------------------
def download_image_as_base64(url, max_size_mb=10):
    """Download an image and return as a base64 data URI, or None on failure."""
    try:
        r = requests.get(url, timeout=30, stream=True)
        r.raise_for_status()
        content_type = r.headers.get("Content-Type", "image/jpeg")
        data = r.content
        if len(data) > max_size_mb * 1024 * 1024:
            log_warn(f"  Image too large ({len(data)} bytes), skipping: {url}")
            return None
        b64 = base64.b64encode(data).decode("ascii")
        return f"data:{content_type};base64,{b64}"
    except Exception as e:
        log_warn(f"  Failed to download image {url}: {e}")
        return None

# ---------------------------------------------------------------------------
# Body-mod formatting (tattoos / piercings from stash-box)
# ---------------------------------------------------------------------------
def format_body_mods(mods):
    if not mods or not isinstance(mods, list):
        return ""
    parts = []
    for m in mods:
        if not isinstance(m, dict):
            continue
        loc = (m.get("location") or "").strip()
        desc = (m.get("description") or "").strip()
        if loc and desc:
            parts.append(f"{loc}: {desc}")
        elif loc:
            parts.append(loc)
        elif desc:
            parts.append(desc)
    return ", ".join(parts)

# ---------------------------------------------------------------------------
# Build PerformerCreateInput from stash-box performer data
# ---------------------------------------------------------------------------
def build_performer_create_input(stashbox_performer, endpoint):
    """Convert a stash-box performer object to a local PerformerCreateInput dict."""
    p = stashbox_performer
    inp = {}

    # Required field
    name = (p.get("name") or "").strip()
    if not name:
        return None
    inp["name"] = name

    # Disambiguation
    disambiguation = (p.get("disambiguation") or "").strip()
    if disambiguation:
        inp["disambiguation"] = disambiguation

    # Aliases
    aliases = p.get("aliases") or []
    if aliases:
        inp["alias_list"] = [a.strip() for a in aliases if a.strip()]

    # Gender mapping (stash-box uses MALE/FEMALE/etc., local uses same)
    gender = p.get("gender")
    if gender:
        inp["gender"] = gender

    # Dates
    birthdate = p.get("birth_date")
    if birthdate:
        inp["birthdate"] = str(birthdate)

    death_date = p.get("death_date")
    if death_date:
        inp["death_date"] = str(death_date)

    # Basic attributes
    for stashbox_field, local_field in [
        ("ethnicity", "ethnicity"),
        ("country", "country"),
        ("eye_color", "eye_color"),
        ("hair_color", "hair_color"),
    ]:
        val = p.get(stashbox_field)
        if val:
            inp[local_field] = val

    # Height (stash-box returns int in cm)
    height = p.get("height")
    if height:
        try:
            inp["height_cm"] = int(height)
        except (ValueError, TypeError):
            pass

    # Measurements (construct from cup, band, waist, hip)
    band = p.get("band_size")
    cup = (p.get("cup_size") or "").strip()
    waist = p.get("waist_size")
    hip = p.get("hip_size")
    if band and cup:
        parts = [f"{band}{cup}"]
        if waist:
            parts.append(str(waist))
        if hip:
            parts.append(str(hip))
        inp["measurements"] = "-".join(parts)

    # Breast type -> fake_tits
    breast_type = (str(p.get("breast_type") or "")).upper()
    if breast_type in ("FAKE", "AUGMENTED"):
        inp["fake_tits"] = "Yes"
    elif breast_type == "NATURAL":
        inp["fake_tits"] = "No"

    # Career length
    career_start = p.get("career_start_year")
    career_end = p.get("career_end_year")
    if career_start:
        if career_end:
            inp["career_length"] = f"{career_start}-{career_end}"
        else:
            inp["career_length"] = str(career_start)

    # Tattoos & piercings
    tattoos_str = format_body_mods(p.get("tattoos"))
    if tattoos_str:
        inp["tattoos"] = tattoos_str

    piercings_str = format_body_mods(p.get("piercings"))
    if piercings_str:
        inp["piercings"] = piercings_str

    # URLs
    urls = p.get("urls") or []
    url_list = []
    for u in urls:
        if isinstance(u, dict):
            url_str = (u.get("url") or "").strip()
            if url_str:
                url_list.append(url_str)
        elif isinstance(u, str) and u.strip():
            url_list.append(u.strip())
    if url_list:
        # Set the first URL as the primary url, rest as urls list
        inp["url"] = url_list[0]
        if len(url_list) > 1:
            inp["urls"] = url_list

    # Stash ID — link back to the stash-box
    stashbox_id = p.get("id")
    if stashbox_id and endpoint:
        inp["stash_ids"] = [{"endpoint": endpoint, "stash_id": stashbox_id}]

    # Image — download the first available image
    images = p.get("images") or []
    if images:
        img_url = None
        for img in images:
            if isinstance(img, dict) and img.get("url"):
                img_url = img["url"]
                break
        if img_url:
            b64_data = download_image_as_base64(img_url)
            if b64_data:
                inp["image"] = b64_data

    return inp

# ---------------------------------------------------------------------------
# Find or create a performer locally
# ---------------------------------------------------------------------------
def find_local_performer_by_stashid(stash, stashbox_id, endpoint):
    """Look up a local performer by stash-box stash_id. Returns performer dict or None."""
    result = stash.call_GQL(FIND_PERFORMER_BY_STASHID_QUERY, {
        "performer_filter": {
            "stash_id_endpoint": {
                "endpoint": endpoint,
                "stash_id": stashbox_id,
                "modifier": "EQUALS",
            }
        }
    })
    performers = (result or {}).get("findPerformers", {}).get("performers", [])
    if performers:
        return performers[0]
    return None


def find_local_performer_by_name(stash, name):
    """Look up local performers by exact name match. Returns list of matches."""
    result = stash.call_GQL(FIND_PERFORMER_BY_NAME_QUERY, {
        "performer_filter": {
            "name": {
                "value": name,
                "modifier": "EQUALS",
            }
        }
    })
    return (result or {}).get("findPerformers", {}).get("performers", [])


def add_stashid_to_existing_performer(stash, performer, stashbox_id, endpoint, dry_run=False):
    """Add a stash_id to an existing local performer that was matched by name."""
    existing_sids = performer.get("stash_ids") or []
    # Check if already has this stash_id
    for sid in existing_sids:
        if endpoint_matches(sid.get("endpoint", ""), endpoint) and sid.get("stash_id") == stashbox_id:
            return performer  # already linked

    new_sids = [
        {"endpoint": s["endpoint"], "stash_id": s["stash_id"]}
        for s in existing_sids
    ]
    new_sids.append({"endpoint": endpoint, "stash_id": stashbox_id})

    if dry_run:
        log(f"    [DRY] Would add stash_id {stashbox_id} to existing performer "
            f"'{performer['name']}' (id={performer['id']})")
        return performer

    result = stash.call_GQL(PERFORMER_UPDATE_QUERY, {
        "input": {
            "id": performer["id"],
            "stash_ids": new_sids,
        }
    })
    if result:
        log(f"    Added stash_id {stashbox_id} to existing performer "
            f"'{performer['name']}' (id={performer['id']})")
        return performer
    else:
        log_err(f"    Failed to add stash_id to performer '{performer['name']}'")
        return performer


# ---------------------------------------------------------------------------
# Core logic
# ---------------------------------------------------------------------------
def get_stashbox_configs(stash):
    """Retrieve all configured stash-box endpoints from local Stash."""
    result = stash.call_GQL(STASH_CONFIG_QUERY)
    boxes = (result or {}).get("configuration", {}).get("general", {}).get("stashBoxes", [])
    # Build dict keyed by normalized endpoint
    configs = {}
    for box in boxes:
        ep = box.get("endpoint", "")
        configs[norm_endpoint(ep)] = {
            "endpoint": ep,
            "api_key": box.get("api_key", ""),
            "name": box.get("name", ""),
        }
    return configs


def fetch_all_identified_scenes(stash):
    """Fetch all scenes that have at least one stash_id (identified scenes), paginated."""
    all_scenes = []
    page = 1
    while True:
        result = stash.call_GQL(FIND_IDENTIFIED_SCENES_QUERY, {
            "filter": {
                "page": page,
                "per_page": BATCH_PAGE_SIZE,
                "sort": "id",
                "direction": "ASC",
            },
            "scene_filter": {
                "stash_id_endpoint": {
                    "modifier": "NOT_NULL",
                }
            }
        })
        data = (result or {}).get("findScenes", {})
        scenes = data.get("scenes", [])
        total = data.get("count", 0)
        all_scenes.extend(scenes)
        log(f"  Fetched page {page}: {len(scenes)} scene(s) (total: {total})")
        if len(all_scenes) >= total or not scenes:
            break
        page += 1
    return all_scenes


def fetch_stashbox_scene(scene_stash_id, endpoint, api_key):
    """Fetch a scene's performer list from a stash-box endpoint."""
    data = graphql_request(
        STASHBOX_FIND_SCENE_QUERY,
        {"id": scene_stash_id},
        ensure_graphql(endpoint),
        api_key,
    )
    if data:
        return data.get("findScene")
    return None


def process_scenes(stash, stashbox_configs, dry_run=False):
    """Main processing loop: find identified scenes, create missing performers."""
    stats = {
        "scenes_checked": 0,
        "scenes_updated": 0,
        "scenes_skipped": 0,
        "scenes_error": 0,
        "performers_created": 0,
        "performers_linked": 0,  # existing performer got stash_id added
        "performers_already_exist": 0,
        "stashbox_errors": 0,
    }

    # Cache: stashbox_id -> local performer id (avoids redundant lookups)
    performer_cache = {}  # key = (norm_endpoint, stashbox_performer_id) -> local_id

    log("Fetching all identified scenes...")
    scenes = fetch_all_identified_scenes(stash)
    log(f"  Found {len(scenes)} identified scene(s)")

    if not scenes:
        log("No identified scenes found. Nothing to do.")
        return stats

    total = len(scenes)
    for idx, scene in enumerate(scenes):
        log_progress(idx / max(total, 1))
        scene_id = scene["id"]
        scene_title = scene.get("title") or f"(Scene {scene_id})"
        scene_stash_ids = scene.get("stash_ids") or []
        local_performers = scene.get("performers") or []

        stats["scenes_checked"] += 1

        # Build set of stash_ids already on local performers for this scene
        local_performer_stashids = set()
        for lp in local_performers:
            for sid in (lp.get("stash_ids") or []):
                local_performer_stashids.add(
                    (norm_endpoint(sid.get("endpoint", "")), sid.get("stash_id", ""))
                )

        # Process each stash-box endpoint this scene is identified on
        scene_updated = False
        new_performer_ids_for_scene = [p["id"] for p in local_performers]

        for scene_sid in scene_stash_ids:
            sb_endpoint = scene_sid.get("endpoint", "")
            sb_scene_id = scene_sid.get("stash_id", "")
            sb_norm = norm_endpoint(sb_endpoint)

            # Find the stash-box config for this endpoint
            sb_config = stashbox_configs.get(sb_norm)
            if not sb_config:
                log_warn(f"  [{scene_title}] No stash-box config found for endpoint: {sb_endpoint}")
                continue

            # Fetch the scene from stash-box to get its performers
            time.sleep(STASHBOX_DELAY)
            sb_scene = fetch_stashbox_scene(sb_scene_id, sb_config["endpoint"], sb_config["api_key"])
            if not sb_scene:
                log_warn(f"  [{scene_title}] Could not fetch scene {sb_scene_id} from {sb_endpoint}")
                stats["stashbox_errors"] += 1
                continue

            sb_performers = sb_scene.get("performers") or []
            if not sb_performers:
                continue

            log(f"  [{scene_title}] stash-box has {len(sb_performers)} performer(s), "
                f"local has {len(local_performers)} performer(s)")

            for sb_perf_entry in sb_performers:
                sb_performer = sb_perf_entry.get("performer", {})
                sb_perf_id = sb_performer.get("id", "")
                sb_perf_name = (sb_performer.get("name") or "").strip()
                performer_as = (sb_perf_entry.get("as") or "").strip()

                if not sb_perf_id or not sb_perf_name:
                    continue

                cache_key = (sb_norm, sb_perf_id)

                # Check if this performer is already linked to the scene
                if cache_key in local_performer_stashids:
                    stats["performers_already_exist"] += 1
                    continue

                # Check performer cache first
                if cache_key in performer_cache:
                    local_id = performer_cache[cache_key]
                    if local_id not in new_performer_ids_for_scene:
                        new_performer_ids_for_scene.append(local_id)
                        scene_updated = True
                        log(f"    Performer '{sb_perf_name}' already created (cached, id={local_id})")
                    stats["performers_already_exist"] += 1
                    continue

                # Step 1: Check if performer exists locally by stash_id
                local_perf = find_local_performer_by_stashid(stash, sb_perf_id, sb_config["endpoint"])

                if local_perf:
                    local_id = local_perf["id"]
                    performer_cache[cache_key] = local_id
                    if local_id not in new_performer_ids_for_scene:
                        new_performer_ids_for_scene.append(local_id)
                        scene_updated = True
                        log(f"    Performer '{sb_perf_name}' already exists locally "
                            f"(id={local_id}), adding to scene")
                    stats["performers_already_exist"] += 1
                    continue

                # Step 2: Check if performer exists locally by name (fallback)
                name_matches = find_local_performer_by_name(stash, sb_perf_name)
                if name_matches:
                    # Found by name — add stash_id to avoid future duplicates
                    local_perf = name_matches[0]
                    local_id = local_perf["id"]
                    add_stashid_to_existing_performer(
                        stash, local_perf, sb_perf_id, sb_config["endpoint"], dry_run
                    )
                    performer_cache[cache_key] = local_id
                    if local_id not in new_performer_ids_for_scene:
                        new_performer_ids_for_scene.append(local_id)
                        scene_updated = True
                        log(f"    Performer '{sb_perf_name}' found by name (id={local_id}), "
                            f"linked stash_id and adding to scene")
                    stats["performers_linked"] += 1
                    continue

                # Step 3: Performer doesn't exist — create them
                log(f"    Creating performer '{sb_perf_name}' from stash-box...")
                create_input = build_performer_create_input(sb_performer, sb_config["endpoint"])
                if not create_input:
                    log_err(f"    Failed to build create input for '{sb_perf_name}'")
                    continue

                # If the performer was listed with "as" (alternate scene name),
                # add the "as" name to aliases if not already present
                if performer_as and performer_as.lower() != sb_perf_name.lower():
                    alias_list = create_input.get("alias_list", [])
                    if performer_as not in alias_list:
                        alias_list.append(performer_as)
                        create_input["alias_list"] = alias_list

                if dry_run:
                    log(f"    [DRY] Would create performer: {create_input.get('name')} "
                        f"(stash_id={sb_perf_id})")
                    # Use a placeholder for tracking
                    placeholder_id = f"dry_{sb_perf_id}"
                    performer_cache[cache_key] = placeholder_id
                    scene_updated = True
                    stats["performers_created"] += 1
                    continue

                try:
                    result = stash.call_GQL(PERFORMER_CREATE_QUERY, {"input": create_input})
                    created = (result or {}).get("performerCreate")
                    if created:
                        local_id = created["id"]
                        performer_cache[cache_key] = local_id
                        new_performer_ids_for_scene.append(local_id)
                        scene_updated = True
                        stats["performers_created"] += 1
                        log(f"    ✓ Created performer '{created['name']}' (id={local_id})")
                    else:
                        log_err(f"    Failed to create performer '{sb_perf_name}': no result")
                except Exception as e:
                    log_err(f"    Failed to create performer '{sb_perf_name}': {e}")

        # Update scene with new performer list if changed
        if scene_updated and not dry_run:
            # Filter out any dry-run placeholders (shouldn't happen in live mode)
            real_ids = [pid for pid in new_performer_ids_for_scene
                        if not str(pid).startswith("dry_")]
            try:
                stash.call_GQL(SCENE_UPDATE_QUERY, {
                    "input": {
                        "id": scene_id,
                        "performer_ids": real_ids,
                    }
                })
                stats["scenes_updated"] += 1
                log(f"  ✓ Updated scene '{scene_title}' with {len(real_ids)} performer(s)")
            except Exception as e:
                log_err(f"  Failed to update scene '{scene_title}': {e}")
                stats["scenes_error"] += 1
        elif scene_updated and dry_run:
            stats["scenes_updated"] += 1
            log(f"  [DRY] Would update scene '{scene_title}'")
        else:
            stats["scenes_skipped"] += 1

    log_progress(1.0)
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

    server = plugin_input.get("server_connection", {})
    scheme = server.get("Scheme", "http")
    host = server.get("Host", "localhost")
    if host in ("0.0.0.0", ""):
        host = "localhost"
    port = server.get("Port", 9999)
    api_key = server.get("ApiKey", "")

    mode = plugin_input.get("args", {}).get("mode", "live")
    dry_run = mode == "dry_run"
    log(f"Mode: {'DRY RUN' if dry_run else 'LIVE'}")

    try:
        from stashapi.stashapp import StashInterface
    except ImportError:
        log_err("stashapp-tools is not installed. Run: pip install stashapp-tools")
        print(json.dumps({"output": "error"}))
        sys.exit(1)

    import logging
    logger = logging.getLogger("create_missing_performers")
    logger.setLevel(logging.DEBUG)
    handler = logging.StreamHandler(sys.stderr)
    handler.setLevel(logging.DEBUG)
    logger.addHandler(handler)

    stash = StashInterface({
        "scheme": scheme,
        "host": host,
        "port": port,
        "ApiKey": api_key,
        "logger": logger,
    })

    # Get stash-box configurations
    log("Reading stash-box configurations...")
    stashbox_configs = get_stashbox_configs(stash)
    if not stashbox_configs:
        log_err("No stash-box endpoints configured in Stash. "
                "Please add a stash-box (e.g., StashDB) in Settings > Metadata Providers.")
        print(json.dumps({"output": "error"}))
        return

    for nep, cfg in stashbox_configs.items():
        log(f"  Stash-box: {cfg.get('name') or nep} ({cfg['endpoint']})")

    log("=" * 60)
    log("Starting: Create Missing Performers")
    log("=" * 60)

    stats = process_scenes(stash, stashbox_configs, dry_run)

    log("=" * 60)
    log("Results:")
    log(f"  Scenes checked:           {stats['scenes_checked']}")
    log(f"  Scenes updated:           {stats['scenes_updated']}")
    log(f"  Scenes skipped (no change): {stats['scenes_skipped']}")
    log(f"  Scene errors:             {stats['scenes_error']}")
    log(f"  Performers created:       {stats['performers_created']}")
    log(f"  Performers linked (name): {stats['performers_linked']}")
    log(f"  Performers already exist: {stats['performers_already_exist']}")
    log(f"  Stash-box errors:         {stats['stashbox_errors']}")
    if dry_run:
        log("  (Dry run — no changes were made)")
    log("=" * 60)

    print(json.dumps({"output": "ok"}))


if __name__ == "__main__":
    main()
