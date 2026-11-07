#!/usr/bin/env python3
"""
Multipart Scene Groups - Stash Plugin
=====================================
Detects multipart scenes (pt1/pt2, cd1/cd2, disc I/II, A/B ...) that live in
the same directory and share a base name, and links them together with a
native Stash Group instead of merging them into a single scene.

Why Groups and not sceneMerge: Stash's player only plays a scene's first
file (ScenePlayer uses scene.files[0]), so merged parts 2+ become unplayable
from the UI. A Group keeps every part as its own playable scene, ordered by
scene_index (= part number).

For each detected group the plugin:
  * finds or creates a Group titled with the cleaned base title
  * adds each part scene to it with scene_index = part number (existing group
    memberships on the scene are kept)
  * adds the Multipart tag (existing tags are kept)
  * adds the VR tag only if the scene already has it or the opt-in
    "Tag multipart scenes as VR" setting is enabled
Re-running is idempotent: existing groups/memberships are reused.
"""
import os
import re
import sys
import time
import json
import math
import pathlib
from typing import Dict, List, Any, Tuple, Optional

try:
    import requests
except ImportError:  # pragma: no cover - only hit when requests is missing
    requests = None


# ---------------------------------------------------------------------------
# Plugin input / config helpers
# ---------------------------------------------------------------------------

def get_plugin_input():
    try:
        return json.loads(sys.stdin.read())
    except Exception:
        return {"server_connection": {"Scheme": "http", "Port": 9999}, "args": {}}


def normalize_graphql_url(url: str) -> str:
    if not url:
        return url
    url = url.rstrip("/")
    if not url.endswith("/graphql"):
        url += "/graphql"
    return url


def first_nonempty(*values) -> str:
    for value in values:
        if value is None:
            continue
        text = str(value).strip()
        if text:
            return text
    return ""


def get_stash_url(plugin_input, server_connection):
    args = plugin_input.get("args", {})
    url = first_nonempty(args.get("stash_url"), args.get("e_stash_url"), os.environ.get("STASH_URL"))
    if url:
        return normalize_graphql_url(url), "setting"
    if server_connection:
        scheme = server_connection.get("Scheme", "http")
        host = server_connection.get("Host", "localhost")
        if host in ("0.0.0.0", ""):
            host = "localhost"
        port = server_connection.get("Port", 9999)
        return f"{scheme}://{host}:{port}/graphql", "server_connection"
    return "http://localhost:9999/graphql", "localhost fallback"


def get_plugin_setting(plugin_input, setting_names, default_value):
    args = plugin_input.get("args", {})
    if isinstance(setting_names, str):
        setting_names = [setting_names]
    for setting_name in setting_names:
        value = args.get(setting_name)
        if first_nonempty(value):
            return value
    env_value = os.environ.get(setting_names[0].upper())
    if first_nonempty(env_value):
        return env_value
    return default_value


def setting_bool(value) -> bool:
    return str(value).strip().lower() in ("1", "true", "yes", "on")


def output_result(error=None, output=None):
    result = {}
    if error:
        result["error"] = str(error)
    if output:
        result["output"] = output
    print(json.dumps(result))


# ---------------------------------------------------------------------------
# Runtime configuration (populated by configure() from the plugin input)
# ---------------------------------------------------------------------------

PLUGIN_ID = "merge_multipart_vr"   # Stash derives it from the .yml filename
STASH_URL = "http://localhost:9999/graphql"
URL_SOURCE = "default"
API_KEY = ""
VR_TAG_NAME = "VR"
MULTIPART_TAG_NAME = "Multipart"
VR_ONLY = False
TAG_AS_VR = False
DRY_RUN = True
UPDATE_DELAY_S = 0.2
SESSION = None

LOG_MESSAGES: List[str] = []


def configure(plugin_input: Dict[str, Any]):
    """Read settings from the plugin input and build the HTTP session."""
    global STASH_URL, URL_SOURCE, API_KEY, VR_TAG_NAME, MULTIPART_TAG_NAME
    global VR_ONLY, TAG_AS_VR, DRY_RUN, UPDATE_DELAY_S, SESSION

    server_connection = plugin_input.get("server_connection", {}) or {}
    args = plugin_input.get("args", {}) or {}

    STASH_URL, URL_SOURCE = get_stash_url(plugin_input, server_connection)
    API_KEY = get_plugin_setting(
        plugin_input,
        ["api_key", "f_api_key"],
        first_nonempty(server_connection.get("ApiKey"), os.environ.get("STASH_API_KEY")),
    )
    VR_TAG_NAME = first_nonempty(get_plugin_setting(plugin_input, ["vr_tag", "a_vr_tag"], "VR"))
    MULTIPART_TAG_NAME = first_nonempty(
        get_plugin_setting(plugin_input, ["multipart_tag", "b_multipart_tag"], "Multipart")
    )
    # "vr_only" restricts grouping to scenes that already carry the VR tag
    VR_ONLY = setting_bool(get_plugin_setting(plugin_input, ["vr_only", "c_vr_only"], "false"))
    # Opt-in: also add the VR tag to every multipart scene (default off)
    TAG_AS_VR = setting_bool(get_plugin_setting(plugin_input, ["tag_as_vr"], "false"))

    mode = args.get("mode", "group")
    DRY_RUN = (mode == "preview") or setting_bool(get_plugin_setting(plugin_input, "dry_run", "false"))

    # Milliseconds to sleep between scene updates. "merge_delay_ms" is the
    # pre-Groups setting name and is still honoured for existing installs.
    UPDATE_DELAY_S = float(get_plugin_setting(
        plugin_input,
        ["update_delay_ms", "merge_delay_ms", "d_merge_delay", "f_merge_delay"],
        "200",
    )) / 1000.0

    SESSION = requests.Session()
    SESSION.headers.update({"Content-Type": "application/json"})
    if API_KEY:
        SESSION.headers.update({"ApiKey": API_KEY})
    else:
        cookie = server_connection.get("SessionCookie")
        if cookie:
            SESSION.cookies.set(
                cookie.get("Name", "session"),
                cookie.get("Value", ""),
                domain=cookie.get("Domain", "localhost"),
                path=cookie.get("Path", "/"),
            )


# Stash raw-plugin log-level prefixes (written to stderr)
def stash_log(level: str, message: str):
    print(f"\x01{level}\x02{message}", file=sys.stderr, flush=True)

def log_info(message: str):
    stash_log("i", message)
    LOG_MESSAGES.append(message)

def log_warn(message: str):
    stash_log("w", message)
    LOG_MESSAGES.append(message)

def log_err(message: str):
    stash_log("e", message)
    LOG_MESSAGES.append(message)

def log_progress(pct: float):
    stash_log("p", f"{pct:.2f}")

def log_section(title: str):
    log_info(f"\n{'─' * 50}")
    log_info(f"  {title}")
    log_info('─' * 50)


# ---------------------------------------------------------------------------
# Part-number detection
# ---------------------------------------------------------------------------

# Matches: pt1, part-02, cd2, disc iii  (surrounded by word/path separators)
PART_TOKEN = re.compile(
    r"(?ix)"
    r"(?:^|[ _.\-\(\)\[\]])"
    r"(?:pt|part|cd|disc)"
    r"[ _.\-]*"
    r"(?P<num>(?:\d{1,2}|[ivx]{1,6}))"
    r"(?=$|[ _.\-\(\)\[\]])"   # lookahead — don't consume trailing separator
)

# Matches trailing A/B split only when the letter is a standalone token
AB_TOKEN = re.compile(
    r"(?i)(?:^|[ _.\-\(\)\[\]])(?P<ab>[AB])(?=$|[ _.\-\(\)\[\]])"
)


def roman_to_int(s: str) -> Optional[int]:
    numerals = {"I": 1, "V": 5, "X": 10}
    prev, total = 0, 0
    for ch in reversed(s.upper()):
        val = numerals.get(ch, 0)
        total += val if val >= prev else -val
        prev = val
    return total if total > 0 else None


def normalize_basename(name: str) -> Tuple[str, Optional[int]]:
    """
    Return (base_without_part_token, part_number_or_None).
    Handles numeric parts, roman numerals, and A/B suffixes.
    """
    stem = pathlib.Path(name).stem

    m = PART_TOKEN.search(stem)
    if m:
        raw = m.group("num")
        try:
            part = int(raw)
        except ValueError:
            part = roman_to_int(raw)
        # Strip the matched token (replace with a space then normalise whitespace)
        base = stem[: m.start()] + " " + stem[m.end() :]
        base = re.sub(r"\s{2,}", " ", base).strip()
        return base, part

    m2 = AB_TOKEN.search(stem)
    if m2:
        letter = m2.group("ab").upper()
        part = 1 if letter == "A" else 2
        base = stem[: m2.start()] + " " + stem[m2.end() :]
        base = re.sub(r"\s{2,}", " ", base).strip()
        return base, part

    return stem, None


# ---------------------------------------------------------------------------
# Title cleanup — strip part tokens safely
# ---------------------------------------------------------------------------

# Only strip explicit part tokens; won't touch words like "Discount" or "Disc Jockey"
_PART_STRIP = re.compile(
    r"(?i)"
    r"(?:^|(?<=[ _.\-]))"          # start or preceded by separator
    r"(?:pt|part|cd|disc)"
    r"[ _.\-]*"
    r"(?:\d{1,2}|[ivx]{1,6})"
    r"(?=$|[ _.\-])"               # end or followed by separator
)


def clean_title(title: str) -> str:
    cleaned = _PART_STRIP.sub("", title)
    cleaned = re.sub(r"[ _.\-]{2,}", " ", cleaned).strip(" _.-")
    return cleaned if cleaned else title


# ---------------------------------------------------------------------------
# Pure planning logic (no I/O — unit tested)
# ---------------------------------------------------------------------------

def scene_has_tag(scene: Dict, tag_id: Optional[str]) -> bool:
    if not tag_id:
        return False
    return any(str(t["id"]) == str(tag_id) for t in (scene.get("tags") or []))


def detect_groups(scenes: List[Dict], vr_only: bool = False,
                  vr_tag_id: Optional[str] = None) -> Tuple[List[Dict], List[Dict]]:
    """
    Group scenes by (directory, normalised base name) using the first file.
    Returns (valid_groups, skipped_groups). Each group is
    {"dir", "base", "items": [{"scene", "part", "basename"}]} with items
    sorted by part number. Groups with duplicate part numbers are skipped.
    """
    groups: Dict[Tuple[str, str], List[Dict]] = {}
    for sc in scenes:
        if vr_only and not scene_has_tag(sc, vr_tag_id):
            continue
        if not sc.get("files"):
            continue
        f = sc["files"][0]
        dirpath = str(pathlib.Path(f["path"]).parent)
        base, part = normalize_basename(f["basename"])
        if part is None:
            continue
        key = (dirpath, base.lower())
        groups.setdefault(key, []).append({"scene": sc, "part": part, "basename": f["basename"]})

    valid, skipped = [], []
    for (dirpath, base_lower), items in groups.items():
        if len(items) < 2:
            continue
        # Keep the original-case base from the lowest part for display/title use
        items.sort(key=lambda x: x["part"])
        display_base, _ = normalize_basename(items[0]["basename"])
        entry = {"dir": dirpath, "base": display_base, "items": items}
        part_nums = [it["part"] for it in items]
        if len(part_nums) != len(set(part_nums)):
            skipped.append(entry)
        else:
            valid.append(entry)
    return valid, skipped


def group_title_for(items: List[Dict], base: str) -> str:
    """Cleaned base title: part 1's scene title with part tokens stripped,
    falling back to the file base name when the scene has no title."""
    first_title = (items[0]["scene"].get("title") or "").strip()
    if first_title:
        return clean_title(first_title)
    return clean_title(base)


def select_existing_group(title: str, part_scene_ids: List[str],
                          scene_groups: List[Dict],
                          name_matches: List[Dict]) -> Optional[str]:
    """
    Pick an existing Group to reuse for this multipart set, or None to create.

    1. A group one of the part scenes already belongs to whose name equals the
       title (case-insensitive) — this is what makes re-runs idempotent.
    2. Otherwise a group with that exact name whose scenes are all part of
       this set (or that has no scenes yet).
    A same-named group containing unrelated scenes is never reused.
    """
    want = title.strip().lower()
    ids = {str(s) for s in part_scene_ids}

    def id_key(g):
        gid = str(g["id"])
        return (0, int(gid)) if gid.isdigit() else (1, gid)

    member = [g for g in scene_groups if (g.get("name") or "").strip().lower() == want]
    if member:
        return str(sorted(member, key=id_key)[0]["id"])

    reusable = []
    for g in name_matches:
        if (g.get("name") or "").strip().lower() != want:
            continue
        g_scene_ids = {str(s["id"]) for s in (g.get("scenes") or [])}
        if g_scene_ids.issubset(ids):
            reusable.append(g)
    if reusable:
        return str(sorted(reusable, key=id_key)[0]["id"])
    return None


def plan_scene_update(scene: Dict, group_id: str, part: int,
                      add_tag_ids: List[str]) -> Optional[Dict]:
    """
    Build a SceneUpdateInput that puts the scene in group_id at scene_index=part
    and adds add_tag_ids, keeping all existing group memberships and tags.
    Returns None when nothing would change.
    """
    changed = False

    groups_in: List[Dict[str, Any]] = []
    found = False
    for sg in (scene.get("groups") or []):
        gid = str(sg["group"]["id"])
        idx = sg.get("scene_index")
        if gid == str(group_id):
            if found:
                changed = True  # drop a duplicate membership row
                continue
            found = True
            if idx != part:
                idx = part
                changed = True
        entry = {"group_id": gid}
        if idx is not None:
            entry["scene_index"] = idx
        groups_in.append(entry)
    if not found:
        groups_in.append({"group_id": str(group_id), "scene_index": part})
        changed = True

    tag_ids = [str(t["id"]) for t in (scene.get("tags") or [])]
    for tid in add_tag_ids:
        if tid and str(tid) not in tag_ids:
            tag_ids.append(str(tid))
            changed = True

    if not changed:
        return None
    return {"id": str(scene["id"]), "groups": groups_in, "tag_ids": tag_ids}


# ---------------------------------------------------------------------------
# GraphQL helpers
# ---------------------------------------------------------------------------

def gql(query: str, variables: Dict[str, Any] = None) -> Dict[str, Any]:
    payload = {"query": query, "variables": variables or {}}
    resp = SESSION.post(STASH_URL, data=json.dumps(payload), timeout=30)
    resp.raise_for_status()
    out = resp.json()
    if "errors" in out:
        raise RuntimeError(out["errors"])
    return out["data"]


def test_connection() -> Tuple[bool, str]:
    try:
        data = gql("query { version { version build_time } }")
        v = data.get("version", {})
        return True, f"Stash v{v.get('version','?')} (built: {v.get('build_time','?')})"
    except requests.exceptions.Timeout:
        return False, "Connection timeout"
    except requests.exceptions.ConnectionError:
        return False, "Connection refused — check URL/network"
    except Exception as e:
        return False, str(e)


def find_tag_id(name: str) -> Optional[str]:
    data = gql(
        """query FindTags($filter: FindFilterType!) {
          findTags(filter: $filter) { tags { id name } }
        }""",
        {"filter": {"q": name, "per_page": 100}},
    )
    for t in data["findTags"]["tags"]:
        if t["name"].lower() == name.lower():
            return str(t["id"])
    return None


def get_or_create_tag(name: str) -> str:
    """Return the tag id, creating the tag in live mode. In preview a
    placeholder id is returned and nothing is written."""
    existing = find_tag_id(name)
    if existing:
        return existing
    if DRY_RUN:
        log_info(f"  [DRY] would create tag '{name}'")
        return f"new-tag:{name}"
    created = gql(
        "mutation CreateTag($input: TagCreateInput!) { tagCreate(input: $input) { id } }",
        {"input": {"name": name}},
    )["tagCreate"]
    log_info(f"  Created tag '{name}' (id={created['id']})")
    return str(created["id"])


def fetch_scenes_page(page: int, per_page: int = 200) -> Tuple[int, List[Dict]]:
    data = gql(
        """query($page: Int!, $per_page: Int!) {
          findScenes(filter: {per_page: $per_page, page: $page, sort: "id", direction: ASC}) {
            count
            scenes {
              id title
              files { id path basename }
              tags { id name }
              groups { group { id name } scene_index }
            }
          }
        }""",
        {"page": page, "per_page": per_page},
    )["findScenes"]
    return data["count"], data["scenes"]


def find_groups_by_name(name: str) -> List[Dict]:
    data = gql(
        """query($gf: GroupFilterType, $f: FindFilterType) {
          findGroups(group_filter: $gf, filter: $f) {
            groups { id name scenes { id } }
          }
        }""",
        {"gf": {"name": {"value": name, "modifier": "EQUALS"}}, "f": {"per_page": -1}},
    )
    return data["findGroups"]["groups"]


def group_create(name: str) -> str:
    created = gql(
        "mutation($input: GroupCreateInput!) { groupCreate(input: $input) { id name } }",
        {"input": {"name": name}},
    )["groupCreate"]
    return str(created["id"])


def scene_update(update_input: Dict[str, Any]):
    gql(
        "mutation($input: SceneUpdateInput!) { sceneUpdate(input: $input) { id } }",
        {"input": update_input},
    )


# ---------------------------------------------------------------------------
# Main logic
# ---------------------------------------------------------------------------

def scan_library() -> List[Dict]:
    page, per_page = 1, 200
    total, scenes = fetch_scenes_page(page, per_page)
    pages = max(1, math.ceil(total / per_page))
    all_scenes = list(scenes)
    while page < pages:
        page += 1
        _, scenes = fetch_scenes_page(page, per_page)
        all_scenes.extend(scenes)
    return all_scenes


def with_saved_settings(plugin_input: Dict[str, Any], saved: Dict[str, Any]) -> Dict[str, Any]:
    """Stash does not pass saved plugin settings in args, so merge them in
    from configuration.plugins. Non-empty task args take precedence."""
    merged = dict(plugin_input)
    args = dict(saved or {})
    for key, value in (plugin_input.get("args") or {}).items():
        if first_nonempty(value) or isinstance(value, bool):
            args[key] = value
    merged["args"] = args
    return merged


def load_saved_settings() -> Dict[str, Any]:
    try:
        data = gql("query($ids: [ID!]) { configuration { plugins(include: $ids) } }",
                   {"ids": [PLUGIN_ID]})
        return ((data.get("configuration") or {}).get("plugins") or {}).get(PLUGIN_ID) or {}
    except Exception as e:
        log_warn(f"  Could not read plugin settings, using defaults: {e}")
        return {}


def main():
    plugin_input = get_plugin_input()
    configure(plugin_input)

    ok, msg = test_connection()
    if not ok:
        output_result(error=f"Cannot reach Stash GraphQL: {msg}")
        sys.exit(2)
    configure(with_saved_settings(plugin_input, load_saved_settings()))

    log_section("Multipart Scene Groups")
    log_info(f"  Endpoint : {STASH_URL}  (source: {URL_SOURCE})")
    log_info(f"  Stash    : {msg}")
    log_info(f"  Mode     : {'PREVIEW — no changes will be made' if DRY_RUN else 'LIVE — changes will be applied'}")
    log_info(f"  VR Only  : {VR_ONLY}")
    log_info(f"  Tag as VR: {TAG_AS_VR}")

    vr_tag_id = find_tag_id(VR_TAG_NAME) if VR_TAG_NAME else None
    if VR_ONLY and not vr_tag_id:
        log_warn(f"  VR Scenes Only is on but tag '{VR_TAG_NAME}' does not exist — nothing will match")
    if TAG_AS_VR and VR_TAG_NAME and not vr_tag_id:
        vr_tag_id = get_or_create_tag(VR_TAG_NAME)
    mp_tag_id = get_or_create_tag(MULTIPART_TAG_NAME)
    log_info(f"  Tags     : '{MULTIPART_TAG_NAME}'" + (f" + '{VR_TAG_NAME}' (opt-in)" if TAG_AS_VR else ""))

    # ---- Scan --------------------------------------------------------------
    log_section("Scanning Library")
    all_scenes = scan_library()
    log_info(f"  Found {len(all_scenes)} scenes")

    valid, skipped = detect_groups(all_scenes, VR_ONLY, vr_tag_id)
    log_info(f"  Detected {len(valid)} multipart group(s), {len(skipped)} skipped")

    for g in skipped:
        log_warn(f"  ⚠  SKIP  {g['base']}  ({g['dir']})")
        log_warn(f"         Duplicate part numbers detected: {[it['part'] for it in g['items']]}")

    # ---- Process -----------------------------------------------------------
    log_section("Processing Groups")
    stats = {"groups_created": 0, "groups_reused": 0, "scenes_updated": 0,
             "scenes_unchanged": 0, "skipped": len(skipped), "errors": 0}
    summary = []
    # Groups planned (preview) or created (live) during this run, so two
    # detections with the same title see each other.
    run_groups: List[Dict] = []

    for gi, g in enumerate(valid):
        log_progress(gi / max(len(valid), 1))
        items = g["items"]
        part_nums = [it["part"] for it in items]
        title = group_title_for(items, g["base"])
        scene_ids = [str(it["scene"]["id"]) for it in items]

        log_info(f"\n  ▶  [{' + '.join(str(p) for p in part_nums)}]  {title}")
        log_info(f"     Folder  : {g['dir']}")

        scene_groups = []
        for it in items:
            for sg in (it["scene"].get("groups") or []):
                scene_groups.append(sg["group"])

        try:
            name_matches = find_groups_by_name(title) + [
                rg for rg in run_groups if rg["name"].lower() == title.lower()
            ]
            group_id = select_existing_group(title, scene_ids, scene_groups, name_matches)
            if group_id:
                log_info(f"     Group   : reuse #{group_id} '{title}'")
                stats["groups_reused"] += 1
            elif DRY_RUN:
                group_id = f"new-group:{len(run_groups) + 1}"
                log_info(f"     Group   : [DRY] would create group '{title}'")
                stats["groups_created"] += 1
                run_groups.append({"id": group_id, "name": title,
                                   "scenes": [{"id": s} for s in scene_ids]})
            else:
                group_id = group_create(title)
                log_info(f"     Group   : created #{group_id} '{title}'")
                stats["groups_created"] += 1
                run_groups.append({"id": group_id, "name": title,
                                   "scenes": [{"id": s} for s in scene_ids]})
        except Exception as e:
            log_err(f"     Group lookup/create failed: {e}")
            stats["errors"] += 1
            continue

        for it in items:
            sc = it["scene"]
            add_tags = [mp_tag_id]
            if TAG_AS_VR and vr_tag_id:
                add_tags.append(vr_tag_id)
            upd = plan_scene_update(sc, group_id, it["part"], add_tags)
            label = f"scene {sc['id']} ({it['basename']})"
            if upd is None:
                log_info(f"     Part {it['part']}: {label} already up to date")
                stats["scenes_unchanged"] += 1
                continue
            if DRY_RUN:
                log_info(f"     Part {it['part']}: [DRY] would update {label}: "
                         f"groups={upd['groups']} tag_ids={upd['tag_ids']}")
                stats["scenes_updated"] += 1
                continue
            try:
                scene_update(upd)
                log_info(f"     Part {it['part']}: updated {label}")
                stats["scenes_updated"] += 1
            except Exception as e:
                log_err(f"     Part {it['part']}: update failed for {label}: {e}")
                stats["errors"] += 1
            if UPDATE_DELAY_S > 0:
                time.sleep(UPDATE_DELAY_S)

        summary.append({"group": f"{g['dir']} :: {g['base']}", "title": title,
                        "group_id": group_id, "parts": part_nums, "scene_ids": scene_ids})

    log_progress(1.0)

    # ---- Summary -----------------------------------------------------------
    log_section("Summary")
    result_message = (f"Groups created: {stats['groups_created']}  |  reused: {stats['groups_reused']}  |  "
                      f"Scenes updated: {stats['scenes_updated']}  |  unchanged: {stats['scenes_unchanged']}  |  "
                      f"Skipped: {stats['skipped']}  |  Errors: {stats['errors']}")
    log_info(f"  {result_message}")
    if DRY_RUN:
        log_info("  PREVIEW — no changes were written. Run 'Group Multipart Scenes' to apply.")

    output_result(output={
        "message": result_message,
        "stats": stats,
        "dry_run": DRY_RUN,
        "summary": summary,
        "log_messages": LOG_MESSAGES,
    })


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        if requests is not None and isinstance(e, requests.HTTPError):
            msg = f"HTTP error: {e}"
        else:
            msg = f"Unexpected error: {e}"
        log_err(msg)
        output_result(error=msg)
        sys.exit(1)
