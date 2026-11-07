# Performer Cleanup
# One-time cleanup of local performers so Stash's native Identify can keep the
# library clean afterwards:
#   1. merge performers that share a stash-box ID (native performerMerge)
#   2. link performers with no stash IDs to a unique exact stash-box match
#   3. add missing performers to identified scenes (match by stash ID only)
#   4. tag remaining possible duplicates for manual review
#   5. optionally delete orphan performers
#   6. optionally configure Identify defaults so it creates linked performers
#
# Requires Stash v0.31+ (performerMerge).

import sys
import json
import base64
import copy
import os
import re
import time
import unicodedata

try:
    import requests
except ImportError:  # pragma: no cover - reported at runtime by main()
    requests = None

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
PLUGIN_ID = "performer_cleanup"
STASHBOX_DELAY = 1.5        # seconds between stash-box API calls (rate-limit)
BATCH_PAGE_SIZE = 100       # scenes per page when querying local Stash
MIN_STASH_VERSION = (0, 31, 0)
DEFAULT_REVIEW_TAG = "Review: Possible Duplicate"


def first_nonempty(*values):
    for value in values:
        if value is None:
            continue
        text = str(value).strip()
        if text:
            return text
    return ""


def api_key_from_input(plugin_input):
    args = plugin_input.get("args", {})
    server = plugin_input.get("server_connection", {})
    return first_nonempty(
        args.get("api_key"),
        args.get("f_api_key"),
        server.get("ApiKey"),
        os.environ.get("STASH_API_KEY"),
    )


def setting_bool(value, default):
    if value is None or (isinstance(value, str) and not value.strip()):
        return default
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in ("1", "true", "yes", "on")


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

def log_step(title):
    log("=" * 60)
    log(title)
    log("=" * 60)


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
# Endpoint / name helpers
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

def normalize_name(name):
    """Case/width-insensitive comparison key: NFKC, casefold, collapse spaces."""
    if not isinstance(name, str):
        return ""
    text = unicodedata.normalize("NFKC", name).casefold()
    return " ".join(text.split())

def stash_id_keys(performer):
    return {
        (norm_endpoint(s.get("endpoint")), s.get("stash_id"))
        for s in (performer.get("stash_ids") or [])
        if s.get("stash_id")
    }

def id_sort_key(pid):
    text = str(pid)
    return (0, int(text)) if text.isdigit() else (1, text)


# ---------------------------------------------------------------------------
# Local Stash queries
# ---------------------------------------------------------------------------
VERSION_QUERY = "query { version { version } }"

MUTATION_FIELDS_QUERY = 'query { __type(name: "Mutation") { fields { name } } }'

PLUGIN_SETTINGS_QUERY = """
query PluginSettings($include: [ID!]) {
    configuration { plugins(include: $include) }
}
"""

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

IDENTIFY_OPTIONS_FIELDS = """
    fieldOptions { field strategy createMissing }
    setCoverImage
    setOrganized
    includeMalePerformers
    performerGenders
    skipMultipleMatches
    skipMultipleMatchTag
    skipSingleNamePerformers
    skipSingleNamePerformerTag
"""

IDENTIFY_DEFAULTS_QUERY = """
query {
    configuration {
        defaults {
            identify {
                sources {
                    source { stash_box_index stash_box_endpoint scraper_id }
                    options { %s }
                }
                options { %s }
            }
        }
    }
}
""" % (IDENTIFY_OPTIONS_FIELDS, IDENTIFY_OPTIONS_FIELDS)

CONFIGURE_DEFAULTS_MUTATION = """
mutation ConfigureDefaults($input: ConfigDefaultSettingsInput!) {
    configureDefaults(input: $input) {
        identify { sources { source { stash_box_endpoint scraper_id } } }
    }
}
"""

PERFORMER_FIELDS = """
    id
    name
    disambiguation
    alias_list
    gender
    birthdate
    death_date
    ethnicity
    country
    eye_color
    hair_color
    height_cm
    weight
    measurements
    fake_tits
    penis_length
    circumcised
    career_start
    career_end
    tattoos
    piercings
    urls
    details
    rating100
    tags { id name }
    stash_ids { endpoint stash_id }
    scene_count
    image_count
    gallery_count
"""

ALL_PERFORMERS_QUERY = """
query AllPerformers {
    findPerformers(filter: { per_page: -1, sort: "id", direction: ASC }) {
        count
        performers { %s }
    }
}
""" % PERFORMER_FIELDS

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

PERFORMER_MERGE_MUTATION = """
mutation PerformerMerge($input: PerformerMergeInput!) {
    performerMerge(input: $input) { id name }
}
"""

PERFORMER_DESTROY_MUTATION = """
mutation PerformerDestroy($input: PerformerDestroyInput!) {
    performerDestroy(input: $input)
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
STASHBOX_SEARCH_PERFORMER_QUERY = """
query SearchPerformer($term: String!) {
    searchPerformer(term: $term) {
        id
        name
        disambiguation
        aliases
    }
}
"""

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
# Version check
# ---------------------------------------------------------------------------
class StashVersionError(RuntimeError):
    pass


def parse_version(text):
    """'v0.31.2-12-gabc' -> (0, 31, 2). Returns None when unparseable."""
    if not text:
        return None
    m = re.match(r"^\s*v?(\d+)\.(\d+)(?:\.(\d+))?", str(text))
    if not m:
        return None
    return (int(m.group(1)), int(m.group(2)), int(m.group(3) or 0))


def check_stash_version(stash):
    """Stop with a clear error unless the server supports performerMerge."""
    need = ".".join(str(n) for n in MIN_STASH_VERSION)
    result = stash.call_GQL(VERSION_QUERY) or {}
    raw = ((result.get("version") or {}).get("version")) or ""
    parsed = parse_version(raw)
    if parsed is not None:
        if parsed < MIN_STASH_VERSION:
            raise StashVersionError(
                f"Performer Cleanup requires Stash v{need} or newer (native performerMerge). "
                f"This server reports {raw}. Please update Stash."
            )
        return raw
    # Development builds may not report a release version: fall back to
    # checking the schema for the mutation itself.
    fields = ((stash.call_GQL(MUTATION_FIELDS_QUERY) or {}).get("__type") or {}).get("fields") or []
    if not any(f.get("name") == "performerMerge" for f in fields):
        raise StashVersionError(
            f"Performer Cleanup requires Stash v{need} or newer (native performerMerge). "
            f"This server's version ({raw or 'unknown'}) does not provide performerMerge."
        )
    return raw or "unknown (performerMerge available)"


# ---------------------------------------------------------------------------
# Settings
# ---------------------------------------------------------------------------
def load_settings(stash, plugin_input):
    """Plugin settings come from configuration.plugins; task args override."""
    saved = {}
    try:
        result = stash.call_GQL(PLUGIN_SETTINGS_QUERY, {"include": [PLUGIN_ID]}) or {}
        saved = ((result.get("configuration") or {}).get("plugins") or {}).get(PLUGIN_ID) or {}
    except Exception as e:
        log_warn(f"Could not read plugin settings, using defaults: {e}")
    args = plugin_input.get("args", {}) or {}

    def pick(key):
        value = args.get(key)
        if value is None or (isinstance(value, str) and not value.strip()):
            value = saved.get(key)
        return value

    return {
        "delete_orphans": setting_bool(pick("delete_orphans"), False),
        "set_identify_defaults": setting_bool(pick("set_identify_defaults"), True),
        "review_tag": first_nonempty(pick("review_tag")) or DEFAULT_REVIEW_TAG,
    }


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
def build_performer_create_input(stashbox_performer, endpoint, download_image=True):
    """Convert a stash-box performer object to a local PerformerCreateInput dict."""
    p = stashbox_performer
    inp = {}

    name = (p.get("name") or "").strip()
    if not name:
        return None
    inp["name"] = name

    disambiguation = (p.get("disambiguation") or "").strip()
    if disambiguation:
        inp["disambiguation"] = disambiguation

    aliases = p.get("aliases") or []
    if aliases:
        inp["alias_list"] = [a.strip() for a in aliases if a.strip()]

    gender = p.get("gender")
    if gender:
        inp["gender"] = gender

    birthdate = p.get("birth_date")
    if birthdate:
        inp["birthdate"] = str(birthdate)

    death_date = p.get("death_date")
    if death_date:
        inp["death_date"] = str(death_date)

    for stashbox_field, local_field in [
        ("ethnicity", "ethnicity"),
        ("country", "country"),
        ("eye_color", "eye_color"),
        ("hair_color", "hair_color"),
    ]:
        val = p.get(stashbox_field)
        if val:
            inp[local_field] = val

    height = p.get("height")
    if height:
        try:
            inp["height_cm"] = int(height)
        except (ValueError, TypeError):
            pass

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

    breast_type = (str(p.get("breast_type") or "")).upper()
    if breast_type in ("FAKE", "AUGMENTED"):
        inp["fake_tits"] = "Yes"
    elif breast_type == "NATURAL":
        inp["fake_tits"] = "No"

    # career_length is deprecated but still parsed into career_start/end by Stash
    career_start = p.get("career_start_year")
    career_end = p.get("career_end_year")
    if career_start:
        if career_end:
            inp["career_length"] = f"{career_start}-{career_end}"
        else:
            inp["career_length"] = str(career_start)

    tattoos_str = format_body_mods(p.get("tattoos"))
    if tattoos_str:
        inp["tattoos"] = tattoos_str

    piercings_str = format_body_mods(p.get("piercings"))
    if piercings_str:
        inp["piercings"] = piercings_str

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
        inp["urls"] = url_list

    stashbox_id = p.get("id")
    if stashbox_id and endpoint:
        inp["stash_ids"] = [{"endpoint": endpoint, "stash_id": stashbox_id}]

    if download_image:
        for img in (p.get("images") or []):
            if isinstance(img, dict) and img.get("url"):
                b64_data = download_image_as_base64(img["url"])
                if b64_data:
                    inp["image"] = b64_data
                break

    return inp


# ---------------------------------------------------------------------------
# Shared local helpers
# ---------------------------------------------------------------------------
def get_stashbox_configs(stash):
    """Retrieve all configured stash-box endpoints from local Stash."""
    result = stash.call_GQL(STASH_CONFIG_QUERY)
    boxes = (result or {}).get("configuration", {}).get("general", {}).get("stashBoxes", [])
    configs = {}
    for box in boxes:
        ep = box.get("endpoint", "")
        configs[norm_endpoint(ep)] = {
            "endpoint": ep,
            "api_key": box.get("api_key", ""),
            "name": box.get("name", ""),
        }
    return configs


def identify_stashboxes(boxes):
    """Return (javstash, stashdb) config dicts, matched on endpoint or name."""
    javstash = stashdb = None
    for box in boxes:
        ep = (box.get("endpoint") or "").lower()
        name = (box.get("name") or "").lower()
        if "javstash" in ep or "javstash" in name:
            javstash = javstash or box
        elif "stashdb" in ep or "stashdb" in name:
            stashdb = stashdb or box
    return javstash, stashdb


def fetch_all_performers(stash):
    result = stash.call_GQL(ALL_PERFORMERS_QUERY) or {}
    return (result.get("findPerformers") or {}).get("performers") or []


def find_tag_id(stash, name):
    result = stash.call_GQL(FIND_TAGS_QUERY, {"filter": {"q": name, "per_page": -1}}) or {}
    for t in (result.get("findTags") or {}).get("tags") or []:
        if normalize_name(t.get("name")) == normalize_name(name):
            return str(t["id"])
    return None


def get_or_create_tag(stash, name, dry_run):
    existing = find_tag_id(stash, name)
    if existing:
        return existing
    if dry_run:
        log(f"  [DRY] Would create tag '{name}'")
        return f"new-tag:{name}"
    created = (stash.call_GQL(TAG_CREATE_MUTATION, {"input": {"name": name}}) or {}).get("tagCreate")
    if not created:
        raise RuntimeError(f"could not create tag '{name}'")
    log(f"  Created tag '{name}' (id={created['id']})")
    return str(created["id"])


def performer_merge(stash, source_ids, destination_id, values):
    return stash.call_GQL(PERFORMER_MERGE_MUTATION, {"input": {
        "source": [str(s) for s in source_ids],
        "destination": str(destination_id),
        "values": values,
    }})


def describe(p):
    dis = f" ({p['disambiguation']})" if p.get("disambiguation") else ""
    return f"'{p.get('name')}'{dis} [id={p.get('id')}]"


# ---------------------------------------------------------------------------
# Keeper selection and merge values (pure)
# ---------------------------------------------------------------------------
# Scalar PerformerUpdateInput fields filled on the keeper when it lacks them.
# Legacy url/twitter/instagram are excluded: performerMerge rejects them.
MERGE_SCALAR_FIELDS = (
    "disambiguation", "gender", "birthdate", "death_date", "ethnicity",
    "country", "eye_color", "hair_color", "height_cm", "weight",
    "measurements", "fake_tits", "penis_length", "circumcised",
    "career_start", "career_end", "tattoos", "piercings", "details",
    "rating100",
)


def _is_empty(value):
    if value is None:
        return True
    if isinstance(value, str) and value.strip() == "":
        return True
    if isinstance(value, (list, dict)) and not value:
        return True
    return False


def count_filled_fields(performer):
    """Score a performer by how much metadata is filled in."""
    score = 0
    for key in ("name",) + MERGE_SCALAR_FIELDS:
        if not _is_empty(performer.get(key)):
            score += 1
    score += len(performer.get("alias_list") or [])
    score += len(performer.get("urls") or [])
    score += len(performer.get("stash_ids") or [])
    return score


def pick_keeper(performers):
    """Most filled-in fields wins; ties go to more scenes, then the oldest id."""
    def key(p):
        pid = str(p["id"])
        older_first = -int(pid) if pid.isdigit() else 0
        return (count_filled_fields(p), p.get("scene_count") or 0, older_first)
    return max(performers, key=key)


def build_merge_values(keeper, others):
    """
    PerformerUpdateInput for performerMerge(values=...): the keeper gets
    combined aliases (deduplicated, never its own name), every stash ID, the
    union of URLs, and any scalar field it is missing that another has.
    """
    keeper_name_key = normalize_name(keeper.get("name"))
    ordered_others = sorted(others, key=count_filled_fields, reverse=True)

    aliases, seen = [], {keeper_name_key}
    for value in list(keeper.get("alias_list") or []) + [
        v for o in ordered_others for v in [o.get("name")] + list(o.get("alias_list") or [])
    ]:
        if not isinstance(value, str):
            continue
        key = normalize_name(value)
        if key and key not in seen:
            seen.add(key)
            aliases.append(value.strip())

    stash_ids, seen_sids = [], set()
    for p in [keeper] + ordered_others:
        for sid in (p.get("stash_ids") or []):
            key = (norm_endpoint(sid.get("endpoint")), sid.get("stash_id"))
            if sid.get("stash_id") and key not in seen_sids:
                seen_sids.add(key)
                stash_ids.append({"endpoint": sid["endpoint"], "stash_id": sid["stash_id"]})

    urls, seen_urls = [], set()
    for p in [keeper] + ordered_others:
        for u in (p.get("urls") or []):
            key = (u or "").strip().rstrip("/").lower()
            if key and key not in seen_urls:
                seen_urls.add(key)
                urls.append(u.strip())

    values = {
        "id": str(keeper["id"]),
        "alias_list": aliases,
        "stash_ids": stash_ids,
        "urls": urls,
    }
    for field in MERGE_SCALAR_FIELDS:
        if not _is_empty(keeper.get(field)):
            continue
        for o in ordered_others:
            if _is_empty(o.get(field)):
                continue
            # Stash has a unique index on (name, disambiguation) and updates the
            # keeper before deleting the sources, so never copy a disambiguation
            # from a source that has the same name as the keeper.
            if field == "disambiguation" and normalize_name(o.get("name")) == keeper_name_key:
                continue
            values[field] = o[field]
            break
    return values


def apply_values_in_memory(performer, values):
    for k, v in values.items():
        if k != "id":
            performer[k] = copy.deepcopy(v)


def group_by_stash_id(performers):
    """
    Groups of performers that share any (normalized endpoint, stash_id).
    Groups that overlap through a shared member are joined (union-find), so
    each performer ends up in at most one merge. Returns a list of
    (sorted keys, [performers]) for groups of 2+.
    """
    parent = {}

    def find(x):
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    by_id = {str(p["id"]): p for p in performers}
    for pid in by_id:
        parent[pid] = pid
    key_owner = {}
    for pid, p in by_id.items():
        for key in stash_id_keys(p):
            if key in key_owner:
                ra, rb = find(key_owner[key]), find(pid)
                if ra != rb:
                    parent[rb] = ra
            else:
                key_owner[key] = pid

    components = {}
    for pid in by_id:
        components.setdefault(find(pid), []).append(by_id[pid])

    groups = []
    for members in components.values():
        if len(members) < 2:
            continue
        members.sort(key=lambda p: id_sort_key(p["id"]))
        counts = {}
        for p in members:
            for key in stash_id_keys(p):
                counts[key] = counts.get(key, 0) + 1
        shared = sorted(k for k, c in counts.items() if c >= 2)
        groups.append((shared, members))
    groups.sort(key=lambda g: id_sort_key(g[1][0]["id"]))
    return groups


# ---------------------------------------------------------------------------
# Unique exact matching (pure)
# ---------------------------------------------------------------------------
def unique_exact_match(term, results):
    """
    Returns ("match", performer) when exactly one result's name or alias equals
    term (case-insensitive), ("ambiguous", [performers]) when several do,
    ("none", None) otherwise.
    """
    target = normalize_name(term)
    if not target:
        return "none", None
    matches = {}
    for p in results or []:
        names = [p.get("name")] + list(p.get("aliases") or [])
        if any(normalize_name(n) == target for n in names):
            matches[str(p.get("id"))] = p
    if len(matches) == 1:
        return "match", next(iter(matches.values()))
    if len(matches) > 1:
        return "ambiguous", list(matches.values())
    return "none", None


def search_terms(performer):
    terms, seen = [], set()
    for value in [performer.get("name")] + list(performer.get("alias_list") or []):
        key = normalize_name(value)
        if key and key not in seen:
            seen.add(key)
            terms.append(value.strip())
    return terms


# ---------------------------------------------------------------------------
# Possible-duplicate detection (pure)
# ---------------------------------------------------------------------------
def disambiguation_conflict(a, b):
    """Both have a disambiguation and they differ -> explicitly different people."""
    da = normalize_name(a.get("disambiguation"))
    db = normalize_name(b.get("disambiguation"))
    return bool(da and db and da != db)


def stash_id_conflict(a, b):
    """Different stash IDs on the same endpoint -> different people."""
    by_ep_a, by_ep_b = {}, {}
    for ep, sid in stash_id_keys(a):
        by_ep_a.setdefault(ep, set()).add(sid)
    for ep, sid in stash_id_keys(b):
        by_ep_b.setdefault(ep, set()).add(sid)
    for ep in set(by_ep_a) & set(by_ep_b):
        if not (by_ep_a[ep] & by_ep_b[ep]):
            return True
    return False


def find_possible_duplicate_pairs(performers):
    """
    Pairs (a, b, reason) that share a normalized name, or where one's name
    equals the other's alias. Pairs with conflicting disambiguations or
    conflicting stash IDs on the same endpoint are excluded.
    """
    by_name, by_alias = {}, {}
    for p in performers:
        nk = normalize_name(p.get("name"))
        if nk:
            by_name.setdefault(nk, []).append(p)
        for alias in (p.get("alias_list") or []):
            ak = normalize_name(alias)
            if ak:
                by_alias.setdefault(ak, []).append(p)

    pairs, seen = [], set()

    def consider(a, b, reason):
        if str(a["id"]) == str(b["id"]):
            return
        first, second = sorted([a, b], key=lambda p: id_sort_key(p["id"]))
        key = (str(first["id"]), str(second["id"]))
        if key in seen:
            return
        seen.add(key)
        if disambiguation_conflict(first, second) or stash_id_conflict(first, second):
            return
        pairs.append((first, second, reason))

    for p in performers:
        nk = normalize_name(p.get("name"))
        if not nk:
            continue
        for q in by_name.get(nk, []):
            consider(p, q, "same name")
        for q in by_alias.get(nk, []):
            consider(p, q, "name matches alias")
    return pairs


def is_orphan(performer):
    return (
        not (performer.get("stash_ids") or [])
        and not (performer.get("scene_count") or 0)
        and not (performer.get("image_count") or 0)
        and not (performer.get("gallery_count") or 0)
    )


# ---------------------------------------------------------------------------
# Identify defaults transformation (pure)
# ---------------------------------------------------------------------------
def _drop_none(value):
    if isinstance(value, dict):
        return {k: _drop_none(v) for k, v in value.items() if v is not None}
    if isinstance(value, list):
        return [_drop_none(v) for v in value]
    return value


def _source_box(source, stash_boxes):
    """Return the configured stash-box a source points to, or None for scrapers."""
    ep = source.get("stash_box_endpoint")
    if ep:
        for box in stash_boxes:
            if endpoint_matches(box.get("endpoint"), ep):
                return box
        return {"endpoint": ep, "name": ""}
    idx = source.get("stash_box_index")
    if idx is not None:
        if 0 <= idx < len(stash_boxes):
            return stash_boxes[idx]
        return {"endpoint": "", "name": f"index {idx}"}
    return None


def _is_stashbox_source(source):
    return bool(source.get("stash_box_endpoint")) or source.get("stash_box_index") is not None


def _set_performer_field(entry, strategy, create_missing):
    """Ensure entry.options.fieldOptions has performers=strategy/createMissing.
    Returns True when something changed."""
    options = entry.get("options")
    if options is None:
        options = {}
        entry["options"] = options
    field_options = options.get("fieldOptions")
    if field_options is None:
        field_options = []
        options["fieldOptions"] = field_options
    for fo in field_options:
        if fo.get("field") == "performers":
            changed = fo.get("strategy") != strategy or bool(fo.get("createMissing")) != create_missing
            fo["strategy"] = strategy
            fo["createMissing"] = create_missing
            return changed
    field_options.append({"field": "performers", "strategy": strategy, "createMissing": create_missing})
    return True


def transform_identify_defaults(identify, stash_boxes, javstash_box):
    """
    Given configuration.defaults.identify (may be None), return
    (IdentifyMetadataInput dict, [change descriptions]). Only these things
    change; every other option and source is carried over untouched:
      * JavStash source first (added at the front if missing)
      * stash-box sources: performers MERGE + createMissing
      * scraper sources: performers IGNORE
      * skipSingleNamePerformers false (globally, and on any source that
        explicitly overrides it to true)
    """
    changes = []
    current = _drop_none(copy.deepcopy(identify)) if identify else None

    if current is None:
        # No saved defaults yet: start from the configured stash-boxes.
        sources = [{"source": {"stash_box_endpoint": b["endpoint"]}} for b in stash_boxes]
        options = {}
        changes.append("no saved Identify defaults: created from configured stash-boxes")
    else:
        sources = current.get("sources") or []
        options = current.get("options")
        if options is None:
            options = {}

    jav_ep = (javstash_box or {}).get("endpoint")
    if jav_ep:
        jav_idx = None
        for i, entry in enumerate(sources):
            box = _source_box(entry.get("source") or {}, stash_boxes)
            if box is not None and endpoint_matches(box.get("endpoint"), jav_ep):
                jav_idx = i
                break
        if jav_idx is None:
            sources.insert(0, {"source": {"stash_box_endpoint": jav_ep}})
            changes.append("added JavStash source at the front")
        elif jav_idx != 0:
            sources.insert(0, sources.pop(jav_idx))
            changes.append(f"moved JavStash source from position {jav_idx + 1} to first")

    for entry in sources:
        src = entry.get("source") or {}
        label = src.get("stash_box_endpoint") or src.get("scraper_id") or f"stash_box_index={src.get('stash_box_index')}"
        if _is_stashbox_source(src):
            if _set_performer_field(entry, "MERGE", True):
                changes.append(f"{label}: performers -> MERGE, createMissing")
        else:
            if _set_performer_field(entry, "IGNORE", False):
                changes.append(f"{label}: performers -> IGNORE")
        src_opts = entry.get("options") or {}
        if src_opts.get("skipSingleNamePerformers") is True:
            src_opts["skipSingleNamePerformers"] = False
            changes.append(f"{label}: skipSingleNamePerformers -> false")

    if options.get("skipSingleNamePerformers") is not False:
        options["skipSingleNamePerformers"] = False
        changes.append("skipSingleNamePerformers -> false")

    new_identify = {"sources": sources, "options": options}
    return new_identify, changes


# ---------------------------------------------------------------------------
# Step 1: merge sure duplicates
# ---------------------------------------------------------------------------
def merge_sure_duplicates(stash, performers, dry_run=False):
    stats = {"groups": 0, "merged": 0, "errors": 0}
    groups = group_by_stash_id(performers)
    if not groups:
        log("  No performers share a stash ID")
        return stats
    for shared_keys, members in groups:
        stats["groups"] += 1
        keeper = pick_keeper(members)
        others = [p for p in members if p["id"] != keeper["id"]]
        values = build_merge_values(keeper, others)
        ids_text = ", ".join(f"{ep} {sid}" for ep, sid in shared_keys)
        log(f"  Shared stash ID ({ids_text}): keep {describe(keeper)}, "
            f"merge {', '.join(describe(o) for o in others)}")
        log(f"    values: {json.dumps(values, ensure_ascii=False)}")
        if dry_run:
            log("    [DRY] Would call performerMerge")
            stats["merged"] += len(others)
            continue
        try:
            performer_merge(stash, [o["id"] for o in others], keeper["id"], values)
            stats["merged"] += len(others)
            log(f"    ✓ Merged {len(others)} performer(s) into {describe(keeper)}")
        except Exception as e:
            stats["errors"] += 1
            log_err(f"    performerMerge failed for {describe(keeper)}: {e}")
    return stats


# ---------------------------------------------------------------------------
# Step 2: link unlinked performers
# ---------------------------------------------------------------------------
def make_stashbox_searcher(delay=STASHBOX_DELAY, sleep=time.sleep):
    cache = {}

    def search(term, box):
        key = (norm_endpoint(box["endpoint"]), normalize_name(term))
        if key in cache:
            return cache[key]
        sleep(delay)
        data = graphql_request(STASHBOX_SEARCH_PERFORMER_QUERY, {"term": term},
                               ensure_graphql(box["endpoint"]), box.get("api_key"))
        results = (data or {}).get("searchPerformer") or []
        cache[key] = results
        return results

    return search


def link_unlinked_performers(stash, performers, boxes, search, dry_run=False):
    """
    boxes: ordered list of (label, box) to try, e.g. [("JavStash", ...), ("StashDB", ...)].
    search(term, box) -> list of stash-box performers {id, name, aliases}.
    """
    stats = {"checked": 0, "linked": 0, "merged": 0, "ambiguous": 0,
             "no_match": 0, "errors": 0}
    by_key = {}
    for p in performers:
        for key in stash_id_keys(p):
            by_key.setdefault(key, p)

    unlinked = [p for p in performers if not (p.get("stash_ids") or [])]
    log(f"  {len(unlinked)} performer(s) without stash IDs")
    for i, p in enumerate(unlinked):
        log_progress(i / max(len(unlinked), 1))
        stats["checked"] += 1
        found = None
        for label, box in boxes:
            for term in search_terms(p):
                status, hit = unique_exact_match(term, search(term, box))
                if status == "ambiguous":
                    stats["ambiguous"] += 1
                    names = [f"{h.get('name')} ({h.get('id')})" for h in hit]
                    log_warn(f"  {describe(p)}: {label} '{term}' is ambiguous {names}, skipped")
                    continue
                if status == "match":
                    found = (label, box, hit, term)
                    break
            if found:
                break
        if not found:
            stats["no_match"] += 1
            continue

        label, box, hit, term = found
        key = (norm_endpoint(box["endpoint"]), hit["id"])
        existing = by_key.get(key)
        if existing is not None and str(existing["id"]) != str(p["id"]):
            values = build_merge_values(existing, [p])
            log(f"  {describe(p)}: {label} match '{hit.get('name')}' ({hit['id']}) on '{term}' "
                f"is already on {describe(existing)} -> merge into it")
            if dry_run:
                log(f"    [DRY] Would call performerMerge values={json.dumps(values, ensure_ascii=False)}")
            else:
                try:
                    performer_merge(stash, [p["id"]], existing["id"], values)
                except Exception as e:
                    stats["errors"] += 1
                    log_err(f"    performerMerge failed: {e}")
                    continue
            apply_values_in_memory(existing, values)
            p["_merged"] = True
            stats["merged"] += 1
            continue

        new_sids = [{"endpoint": box["endpoint"], "stash_id": hit["id"]}]
        log(f"  {describe(p)}: {label} match '{hit.get('name')}' ({hit['id']}) on '{term}' -> add stash ID")
        if dry_run:
            log("    [DRY] Would add stash ID")
        else:
            try:
                stash.call_GQL(PERFORMER_UPDATE_QUERY, {"input": {"id": p["id"], "stash_ids": new_sids}})
            except Exception as e:
                stats["errors"] += 1
                log_err(f"    performerUpdate failed: {e}")
                continue
        p["stash_ids"] = new_sids
        by_key[key] = p
        stats["linked"] += 1
    return stats


# ---------------------------------------------------------------------------
# Step 3: fill missing scene performers
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
    """Add stash-box performers missing from identified scenes. Performers are
    matched by stash ID only; unknown ones are created from stash-box."""
    stats = {
        "scenes_checked": 0,
        "scenes_updated": 0,
        "scenes_skipped": 0,
        "scenes_error": 0,
        "performers_created": 0,
        "performers_already_exist": 0,
        "stashbox_errors": 0,
    }

    performer_cache = {}  # (norm_endpoint, stashbox_performer_id) -> local_id

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

        local_performer_stashids = set()
        for lp in local_performers:
            for sid in (lp.get("stash_ids") or []):
                local_performer_stashids.add(
                    (norm_endpoint(sid.get("endpoint", "")), sid.get("stash_id", ""))
                )

        scene_updated = False
        new_performer_ids_for_scene = [p["id"] for p in local_performers]

        for scene_sid in scene_stash_ids:
            sb_endpoint = scene_sid.get("endpoint", "")
            sb_scene_id = scene_sid.get("stash_id", "")
            sb_norm = norm_endpoint(sb_endpoint)

            sb_config = stashbox_configs.get(sb_norm)
            if not sb_config:
                log_warn(f"  [{scene_title}] No stash-box config found for endpoint: {sb_endpoint}")
                continue

            time.sleep(STASHBOX_DELAY)
            sb_scene = fetch_stashbox_scene(sb_scene_id, sb_config["endpoint"], sb_config["api_key"])
            if not sb_scene:
                log_warn(f"  [{scene_title}] Could not fetch scene {sb_scene_id} from {sb_endpoint}")
                stats["stashbox_errors"] += 1
                continue

            sb_performers = sb_scene.get("performers") or []
            if not sb_performers:
                continue

            for sb_perf_entry in sb_performers:
                sb_performer = sb_perf_entry.get("performer", {}) or {}
                sb_perf_id = sb_performer.get("id", "")
                sb_perf_name = (sb_performer.get("name") or "").strip()
                performer_as = (sb_perf_entry.get("as") or "").strip()

                if not sb_perf_id or not sb_perf_name:
                    continue

                cache_key = (sb_norm, sb_perf_id)

                if cache_key in local_performer_stashids:
                    stats["performers_already_exist"] += 1
                    continue

                if cache_key in performer_cache:
                    local_id = performer_cache[cache_key]
                    if local_id not in new_performer_ids_for_scene:
                        new_performer_ids_for_scene.append(local_id)
                        scene_updated = True
                        log(f"    Performer '{sb_perf_name}' already resolved (id={local_id}), adding to scene")
                    stats["performers_already_exist"] += 1
                    continue

                # Match by stash ID only — never by name.
                local_perf = find_local_performer_by_stashid(stash, sb_perf_id, sb_config["endpoint"])
                if local_perf:
                    local_id = local_perf["id"]
                    performer_cache[cache_key] = local_id
                    if local_id not in new_performer_ids_for_scene:
                        new_performer_ids_for_scene.append(local_id)
                        scene_updated = True
                        log(f"    Performer '{sb_perf_name}' exists locally "
                            f"(id={local_id}), adding to scene")
                    stats["performers_already_exist"] += 1
                    continue

                create_input = build_performer_create_input(
                    sb_performer, sb_config["endpoint"], download_image=not dry_run
                )
                if not create_input:
                    log_err(f"    Failed to build create input for '{sb_perf_name}'")
                    continue

                # Keep the scene-specific "as" name as an alias
                if performer_as and performer_as.lower() != sb_perf_name.lower():
                    alias_list = create_input.get("alias_list", [])
                    if performer_as not in alias_list:
                        alias_list.append(performer_as)
                        create_input["alias_list"] = alias_list

                if dry_run:
                    log(f"    [DRY] Would create performer '{create_input.get('name')}' "
                        f"(stash_id={sb_perf_id}) and add to scene '{scene_title}'")
                    placeholder_id = f"dry_{sb_perf_id}"
                    performer_cache[cache_key] = placeholder_id
                    new_performer_ids_for_scene.append(placeholder_id)
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

        if scene_updated and not dry_run:
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
            log(f"  [DRY] Would update performers on scene '{scene_title}'")
        else:
            stats["scenes_skipped"] += 1

    return stats


# ---------------------------------------------------------------------------
# Step 4: flag possible duplicates
# ---------------------------------------------------------------------------
def flag_possible_duplicates(stash, performers, review_tag, dry_run=False):
    stats = {"pairs": 0, "tagged": 0, "errors": 0}
    pairs = find_possible_duplicate_pairs(performers)
    if not pairs:
        log("  No possible duplicates found")
        return stats

    to_tag = {}
    for a, b, reason in pairs:
        stats["pairs"] += 1
        log(f"  Possible duplicate ({reason}): {describe(a)} <-> {describe(b)}")
        to_tag[str(a["id"])] = a
        to_tag[str(b["id"])] = b

    tag_id = get_or_create_tag(stash, review_tag, dry_run)
    for pid in sorted(to_tag, key=id_sort_key):
        p = to_tag[pid]
        current = [str(t["id"]) for t in (p.get("tags") or [])]
        if tag_id in current:
            continue
        if dry_run:
            log(f"    [DRY] Would add tag '{review_tag}' to {describe(p)}")
            stats["tagged"] += 1
            continue
        try:
            stash.call_GQL(PERFORMER_UPDATE_QUERY, {"input": {"id": pid, "tag_ids": current + [tag_id]}})
            stats["tagged"] += 1
        except Exception as e:
            stats["errors"] += 1
            log_err(f"    Failed to tag {describe(p)}: {e}")
    return stats


# ---------------------------------------------------------------------------
# Step 5: delete orphans
# ---------------------------------------------------------------------------
def delete_orphans(stash, performers, dry_run=False):
    stats = {"deleted": 0, "errors": 0}
    for p in performers:
        if not is_orphan(p):
            continue
        if dry_run:
            log(f"  [DRY] Would delete orphan {describe(p)}")
            stats["deleted"] += 1
            continue
        try:
            stash.call_GQL(PERFORMER_DESTROY_MUTATION, {"input": {"id": p["id"]}})
            log(f"  Deleted orphan {describe(p)}")
            stats["deleted"] += 1
        except Exception as e:
            stats["errors"] += 1
            log_err(f"  Failed to delete {describe(p)}: {e}")
    if not stats["deleted"] and not stats["errors"]:
        log("  No orphan performers")
    return stats


# ---------------------------------------------------------------------------
# Step 6: Identify defaults
# ---------------------------------------------------------------------------
def configure_identify_defaults(stash, stash_boxes, javstash_box, dry_run=False):
    stats = {"changes": 0, "saved": 0}
    if not javstash_box:
        log_warn("  No JavStash stash-box configured; JavStash will not be put first")
    result = stash.call_GQL(IDENTIFY_DEFAULTS_QUERY) or {}
    identify = ((result.get("configuration") or {}).get("defaults") or {}).get("identify")
    new_identify, changes = transform_identify_defaults(identify, stash_boxes, javstash_box)
    stats["changes"] = len(changes)
    if not changes:
        log("  Identify defaults already configured, nothing to change")
        return stats
    for c in changes:
        log(f"  - {c}")
    log(f"  Before: {json.dumps(identify, ensure_ascii=False)}")
    log(f"  After:  {json.dumps(new_identify, ensure_ascii=False)}")
    if dry_run:
        log("  [DRY] Would save Identify defaults with configureDefaults")
        return stats
    stash.call_GQL(CONFIGURE_DEFAULTS_MUTATION, {"input": {"identify": new_identify}})
    stats["saved"] = 1
    log("  ✓ Saved Identify defaults")
    return stats


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------
def run_cleanup(stash, settings, dry_run=False, search=None):
    summary = {}

    log("Reading stash-box configurations...")
    stashbox_configs = get_stashbox_configs(stash)
    stash_boxes = list(stashbox_configs.values())
    for nep, cfg in stashbox_configs.items():
        log(f"  Stash-box: {cfg.get('name') or nep} ({cfg['endpoint']})")
    javstash_box, stashdb_box = identify_stashboxes(stash_boxes)
    if not stash_boxes:
        log_warn("No stash-box endpoints configured; linking and scene steps will be skipped.")

    log_step("Step 1/6: Merge performers that share a stash ID")
    performers = fetch_all_performers(stash)
    log(f"  {len(performers)} performer(s) in library")
    summary["merge_duplicates"] = merge_sure_duplicates(stash, performers, dry_run)

    log_step("Step 2/6: Link performers without stash IDs")
    if not dry_run:
        performers = fetch_all_performers(stash)
    else:
        merged_away = set()
        for _, members in group_by_stash_id(performers):
            keeper = pick_keeper(members)
            values = build_merge_values(keeper, [m for m in members if m["id"] != keeper["id"]])
            apply_values_in_memory(keeper, values)
            merged_away.update(str(m["id"]) for m in members if m["id"] != keeper["id"])
        performers = [p for p in performers if str(p["id"]) not in merged_away]
    boxes = [(label, box) for label, box in (("JavStash", javstash_box), ("StashDB", stashdb_box)) if box]
    if boxes:
        summary["link_unlinked"] = link_unlinked_performers(
            stash, performers, boxes, search or make_stashbox_searcher(), dry_run)
    else:
        log_warn("  Neither JavStash nor StashDB is configured; skipping")
        summary["link_unlinked"] = {"skipped": 1}

    log_step("Step 3/6: Add missing performers to identified scenes")
    if stashbox_configs:
        summary["fill_scenes"] = process_scenes(stash, stashbox_configs, dry_run)
    else:
        summary["fill_scenes"] = {"skipped": 1}

    log_step("Step 4/6: Flag possible duplicates for review")
    if not dry_run:
        performers = fetch_all_performers(stash)
    else:
        performers = [p for p in performers if not p.get("_merged")]
    summary["flag_duplicates"] = flag_possible_duplicates(
        stash, performers, settings["review_tag"], dry_run)

    log_step("Step 5/6: Delete orphan performers")
    if settings["delete_orphans"]:
        if not dry_run:
            performers = fetch_all_performers(stash)
        summary["delete_orphans"] = delete_orphans(stash, performers, dry_run)
    else:
        log("  Disabled (setting 'Delete performers with no scenes, images or galleries' is off)")
        summary["delete_orphans"] = {"skipped": 1}

    log_step("Step 6/6: Configure Identify defaults")
    if settings["set_identify_defaults"]:
        summary["identify_defaults"] = configure_identify_defaults(
            stash, stash_boxes, javstash_box, dry_run)
    else:
        log("  Disabled (setting 'Configure Identify defaults for performers' is off)")
        summary["identify_defaults"] = {"skipped": 1}

    log_progress(1.0)
    log_step("Summary" + (" (PREVIEW — nothing was written)" if dry_run else ""))
    for step, stats in summary.items():
        log(f"  {step}: " + ", ".join(f"{k}={v}" for k, v in stats.items()))
    return summary


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
    api_key = api_key_from_input(plugin_input)

    mode = plugin_input.get("args", {}).get("mode", "preview")
    dry_run = mode != "cleanup"
    log(f"Mode: {'PREVIEW (no writes)' if dry_run else 'LIVE'}")

    if requests is None:
        log_err("The 'requests' module is not installed. Run: pip install requests")
        print(json.dumps({"error": "requests not installed"}))
        sys.exit(1)
    try:
        from stashapi.stashapp import StashInterface
    except ImportError:
        log_err("stashapp-tools is not installed. Run: pip install stashapp-tools")
        print(json.dumps({"error": "stashapp-tools not installed"}))
        sys.exit(1)

    import logging
    logger = logging.getLogger("performer_cleanup")
    logger.setLevel(logging.DEBUG)
    handler = logging.StreamHandler(sys.stderr)
    handler.setLevel(logging.DEBUG)
    logger.addHandler(handler)

    conn = {"scheme": scheme, "host": host, "port": port, "ApiKey": api_key, "logger": logger}
    if server.get("SessionCookie"):
        conn["SessionCookie"] = server["SessionCookie"]
    stash = StashInterface(conn)

    try:
        version = check_stash_version(stash)
    except StashVersionError as e:
        log_err(str(e))
        print(json.dumps({"error": str(e)}))
        sys.exit(1)
    log(f"Stash version: {version}")

    settings = load_settings(stash, plugin_input)
    log(f"Settings: delete_orphans={settings['delete_orphans']}, "
        f"set_identify_defaults={settings['set_identify_defaults']}, "
        f"review_tag='{settings['review_tag']}'")

    summary = run_cleanup(stash, settings, dry_run)
    print(json.dumps({"output": {"dry_run": dry_run, "summary": summary}}))


if __name__ == "__main__":
    main()
