#!/usr/bin/env python3
# testupdate - verify CI pipeline packages all plugins correctly
import os
import re
import sys
import time
import json
import math
import pathlib
import requests
from typing import Dict, List, Any, Tuple, Optional


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


def get_stash_url(plugin_input, server_connection):
    args = plugin_input.get("args", {})
    if args.get("e_stash_url"):
        return normalize_graphql_url(args["stash_url"]), "plugin arg"
    env_url = os.environ.get("STASH_URL")
    if env_url:
        return normalize_graphql_url(env_url), "environment variable"
    if server_connection:
        scheme = server_connection.get("Scheme", "http")
        host = server_connection.get("Host", "localhost")
        port = server_connection.get("Port", 9999)
        return f"{scheme}://{host}:{port}/graphql", "server_connection"
    return "http://localhost:9999/graphql", "localhost fallback"


def get_plugin_setting(plugin_input, setting_name, default_value):
    args = plugin_input.get("args", {})
    if setting_name in args and args[setting_name] is not None:
        return args[setting_name]
    env_value = os.environ.get(setting_name.upper())
    if env_value is not None:
        return env_value
    return default_value


def output_result(error=None, output=None):
    result = {}
    if error:
        result["error"] = str(error)
    if output:
        result["output"] = output
    print(json.dumps(result))


# ---------------------------------------------------------------------------
# Bootstrap
# ---------------------------------------------------------------------------

plugin_input = get_plugin_input()
server_connection = plugin_input.get("server_connection", {})
args = plugin_input.get("args", {})

STASH_URL, URL_SOURCE = get_stash_url(plugin_input, server_connection)
API_KEY = get_plugin_setting(plugin_input, "f_api_key", os.environ.get("STASH_API_KEY", ""))
VR_TAG_NAME = get_plugin_setting(plugin_input, "a_vr_tag", "VR")
MULTIPART_TAG_NAME = get_plugin_setting(plugin_input, "b_multipart_tag", "Multipart")

# "vr_only" restricts merging to scenes that already carry the VR tag
VR_ONLY = str(get_plugin_setting(plugin_input, "c_vr_only", "false")).lower() == "true"

mode = args.get("mode", "merge")
DRY_RUN = (mode == "preview") or (
    str(get_plugin_setting(plugin_input, "dry_run", "false")).lower() == "true"
)

# Milliseconds to sleep between merge mutations — avoids hammering Stash on large libraries
MERGE_DELAY_S = float(get_plugin_setting(plugin_input, "f_merge_delay", "200")) / 1000.0

LOG_MESSAGES: List[str] = []


# Stash raw-plugin log-level prefixes (written to stderr)
# \x01=trace  \x02=debug  \x03=info  \x04=warning  \x05=error  \x06=progress
def log_info(message: str):
    print(f"\x03{message}", file=sys.stderr, flush=True)
    LOG_MESSAGES.append(message)

def log_warn(message: str):
    print(f"\x04{message}", file=sys.stderr, flush=True)
    LOG_MESSAGES.append(message)

def log_err(message: str):
    print(f"\x05{message}", file=sys.stderr, flush=True)
    LOG_MESSAGES.append(message)

def log_progress(pct: float):
    print(f"\x06{pct:.2f}", file=sys.stderr, flush=True)

def log_section(title: str):
    log_info(f"\n{'─' * 50}")
    log_info(f"  {title}")
    log_info('─' * 50)


# ---------------------------------------------------------------------------
# HTTP session
# ---------------------------------------------------------------------------

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


def get_or_create_tag(name: str) -> str:
    data = gql(
        """query FindTags($filter: FindFilterType!) {
          findTags(filter: $filter) { tags { id name } }
        }""",
        {"filter": {"q": name, "per_page": 100}},
    )
    for t in data["findTags"]["tags"]:
        if t["name"].lower() == name.lower():
            return t["id"]
    created = gql(
        "mutation CreateTag($input: TagCreateInput!) { tagCreate(input: $input) { id } }",
        {"input": {"name": name}},
    )["tagCreate"]
    return created["id"]


def fetch_scenes_page(page: int, per_page: int = 200) -> Tuple[int, List[Dict]]:
    data = gql(
        """query($page: Int!, $per_page: Int!) {
          findScenes(filter: {per_page: $per_page, page: $page}) {
            count
            scenes {
              id title
              files { id path basename }
              tags { id name }
            }
          }
        }""",
        {"page": page, "per_page": per_page},
    )["findScenes"]
    return data["count"], data["scenes"]


def scene_update_tags(scene_id: str, tag_ids: List[str]):
    if DRY_RUN:
        log_info(f"  [DRY] would set tags {tag_ids} on scene {scene_id}")
        return
    gql(
        "mutation($input: SceneUpdateInput!) { sceneUpdate(input: $input) { id } }",
        {"input": {"id": scene_id, "tag_ids": tag_ids}},
    )


def scene_update_title(scene_id: str, title: str):
    if DRY_RUN:
        log_info(f"  [DRY] would set title {title!r} on scene {scene_id}")
        return
    gql(
        "mutation($input: SceneUpdateInput!) { sceneUpdate(input: $input) { id } }",
        {"input": {"id": scene_id, "title": title}},
    )


def scene_merge(target_id: str, source_ids: List[str]):
    if not source_ids:
        return
    if DRY_RUN:
        log_info(f"  [DRY] would merge scenes {source_ids} → target {target_id}")
        return
    gql(
        "mutation($input: SceneMergeInput!) { sceneMerge(input: $input) { id } }",
        {"input": {"destination": target_id, "source": source_ids}},
    )


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
# Main logic
# ---------------------------------------------------------------------------

def main():
    log_section("Merge Multipart VR Scenes")
    log_info(f"  Endpoint : {STASH_URL}  (source: {URL_SOURCE})")
    log_info(f"  Mode     : {'DRY RUN — no changes will be made' if DRY_RUN else 'LIVE — changes will be applied'}")
    log_info(f"  VR Only  : {VR_ONLY}")

    ok, msg = test_connection()
    if not ok:
        output_result(error=f"Cannot reach Stash GraphQL: {msg}")
        sys.exit(2)
    log_info(f"  Stash    : {msg}")

    vr_tag_id = get_or_create_tag(VR_TAG_NAME) if VR_TAG_NAME else None
    mp_tag_id = get_or_create_tag(MULTIPART_TAG_NAME)
    log_info(f"  Tags     : '{MULTIPART_TAG_NAME}' + '{VR_TAG_NAME}'")

    # ---- Paginate all scenes ------------------------------------------------
    log_section("Scanning Library")
    page, per_page = 1, 200
    total, scenes = fetch_scenes_page(page, per_page)
    pages = max(1, math.ceil(total / per_page))
    all_scenes = list(scenes)
    while page < pages:
        page += 1
        _, scenes = fetch_scenes_page(page, per_page)
        all_scenes.extend(scenes)
    log_info(f"  Found {len(all_scenes)} scenes")

    # ---- Group by (directory, normalised base) ------------------------------
    groups: Dict[Tuple[str, str], List[Dict]] = {}

    for sc in all_scenes:
        if VR_ONLY and vr_tag_id:
            if not any(t["id"] == vr_tag_id for t in sc["tags"]):
                continue

        if not sc["files"]:
            continue

        f = sc["files"][0]
        dirpath = str(pathlib.Path(f["path"]).parent)
        base, part = normalize_basename(f["basename"])

        if part is None:
            continue

        key = (dirpath, base.lower())
        groups.setdefault(key, []).append({"scene": sc, "part": part, "basename": f["basename"]})

    multipart_groups = {k: v for k, v in groups.items() if len(v) >= 2}
    log_info(f"  Detected {len(multipart_groups)} multipart group(s)")

    # ---- Plan and execute merges --------------------------------------------
    log_section("Processing Groups")
    merged_count = 0
    skipped_count = 0
    merge_summary = []

    for key, items in groups.items():
        if len(items) < 2:
            continue

        dirpath, base = key
        part_nums = [it["part"] for it in items]

        if len(part_nums) != len(set(part_nums)):
            log_warn(f"  ⚠  SKIP  {base}")
            log_warn(f"         Duplicate part numbers detected: {part_nums}")
            skipped_count += 1
            continue

        items.sort(key=lambda x: x["part"])
        scene_ids = [it["scene"]["id"] for it in items]
        target = items[0]["scene"]
        sources = [it["scene"]["id"] for it in items[1:]]

        log_info(f"\n  ✂  Merging  [{' + '.join(str(p) for p in part_nums)}]  {base}")
        log_info(f"     Folder  : {dirpath}")
        log_info(f"     Target  : {target['id']}  ({target['title'] or 'untitled'})")
        log_info(f"     Sources : {sources}")

        scene_merge(target["id"], sources)
        merged_count += 1

        tag_ids = {t["id"] for t in target["tags"]}
        tag_ids.add(mp_tag_id)
        if vr_tag_id:
            tag_ids.add(vr_tag_id)
        scene_update_tags(target["id"], list(tag_ids))
        log_info(f"     Tags    : applied '{MULTIPART_TAG_NAME}'" + (f" + '{VR_TAG_NAME}'" if vr_tag_id else ""))

        new_title = clean_title(target["title"] or "")
        if new_title and new_title != target["title"]:
            log_info(f"     Title   : {target['title']!r}  →  {new_title!r}")
            scene_update_title(target["id"], new_title)
        else:
            log_info(f"     Title   : no change")

        merge_summary.append({
            "group": f"{dirpath} :: {base}",
            "parts": part_nums,
            "target_id": target["id"],
            "source_ids": sources,
        })

        if MERGE_DELAY_S > 0 and not DRY_RUN:
            time.sleep(MERGE_DELAY_S)

    # ---- Summary ------------------------------------------------------------
    log_section("Summary")
    result_message = f"Merged: {merged_count}  |  Skipped: {skipped_count}"
    log_info(f"  {result_message}")
    if DRY_RUN:
        log_info("  DRY RUN — no changes were written. Run 'Merge Multipart Scenes' to apply.")

    output_result(output={
        "message": result_message,
        "merged_count": merged_count,
        "skipped_count": skipped_count,
        "dry_run": DRY_RUN,
        "merge_summary": merge_summary,
        "log_messages": LOG_MESSAGES,
    })


if __name__ == "__main__":
    try:
        main()
    except requests.HTTPError as e:
        msg = f"HTTP error: {e}"
        log_err(msg)
        output_result(error=msg)
        sys.exit(2)
    except Exception as e:
        msg = f"Unexpected error: {e}"
        log_err(msg)
        output_result(error=msg)
        sys.exit(1)
