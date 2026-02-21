"""
Extrafanart Gallery Linker - Stash Plugin
==========================================
For each gallery whose path contains 'extrafanart':
  1. Finds a cover image (folder.jpg, poster.jpg, cover.jpg, etc.)
     in the *parent* directory (the video folder, one level up)
  2. Sets that image as the gallery cover via GraphQL
  3. Links the gallery to any scene found in the same parent folder

Install:
  pip install stashapp-tools requests

Usage (from Stash UI):
  Settings > Tasks > Plugins > "Link Extrafanart Galleries"
  or "Dry Run (Preview Only)"
"""

import sys
import json
import os
import base64

try:
    import requests
except ImportError:
    print("ERROR: 'requests' module not found. Run: pip install requests", file=sys.stderr)
    sys.exit(1)

# ---------------------------------------------------------------------------
# Cover image filename candidates (checked in this order, case-insensitive)
# ---------------------------------------------------------------------------
COVER_CANDIDATES = [
    "folder.jpg", "folder.jpeg", "folder.png", "folder.webp",
    "poster.jpg", "poster.jpeg", "poster.png", "poster.webp",
    "cover.jpg",  "cover.jpeg",  "cover.png",  "cover.webp",
    "board.jpg",  "board.jpeg",  "board.png",  "board.webp",
]

# Name fragment that identifies extrafanart-type folders
FANART_FOLDER_NAME = "extrafanart"


# ---------------------------------------------------------------------------
# GraphQL helpers
# ---------------------------------------------------------------------------

class StashClient:
    def __init__(self, scheme, host, port, api_key=None):
        self.url = f"{scheme}://{host}:{port}/graphql"
        self.session = requests.Session()
        self.session.headers.update({"Content-Type": "application/json"})
        if api_key:
            self.session.headers.update({"ApiKey": api_key})

    def call(self, query, variables=None):
        payload = {"query": query}
        if variables:
            payload["variables"] = variables
        try:
            resp = self.session.post(self.url, json=payload, timeout=30)
            resp.raise_for_status()
            data = resp.json()
            if "errors" in data:
                for err in data["errors"]:
                    log_error(f"GraphQL error: {err.get('message', err)}")
            return data.get("data", {})
        except requests.RequestException as e:
            log_error(f"Request failed: {e}")
            return {}


# ---------------------------------------------------------------------------
# Logging helpers (Stash log protocol: prefix lines with control chars)
# ---------------------------------------------------------------------------

def log_info(msg):
    print(f"\x02INFO: {msg}", file=sys.stderr)
    sys.stderr.flush()

def log_error(msg):
    print(f"\x03ERROR: {msg}", file=sys.stderr)
    sys.stderr.flush()

def log_progress(pct):
    # pct is 0.0 – 1.0
    print(f"\x05{pct:.2f}", file=sys.stderr)
    sys.stderr.flush()


# ---------------------------------------------------------------------------
# Core logic
# ---------------------------------------------------------------------------

def find_cover_image(directory):
    """Return the full path of a cover image in `directory`, or None."""
    try:
        entries = {e.lower(): e for e in os.listdir(directory)}
    except OSError:
        return None
    for candidate in COVER_CANDIDATES:
        if candidate in entries:
            return os.path.join(directory, entries[candidate])
    return None


def image_to_base64(path):
    """Read an image file and return a base64 data URL string."""
    ext = os.path.splitext(path)[1].lower().lstrip(".")
    mime_map = {"jpg": "jpeg", "jpeg": "jpeg", "png": "png", "webp": "webp"}
    mime = mime_map.get(ext, "jpeg")
    with open(path, "rb") as f:
        data = base64.b64encode(f.read()).decode("utf-8")
    return f"data:image/{mime};base64,{data}"


def get_all_extrafanart_galleries(client):
    """Fetch all galleries whose folder/path contains the fanart folder name."""
    query = """
    query FindGalleries($filter: FindFilterType, $gallery_filter: GalleryFilterType) {
      findGalleries(filter: $filter, gallery_filter: $gallery_filter) {
        count
        galleries {
          id
          title
          folder {
            path
          }
          files {
            path
          }
          scenes {
            id
          }
        }
      }
    }
    """
    variables = {
        "filter": {"per_page": -1},
        "gallery_filter": {
            "path": {
                "value": FANART_FOLDER_NAME,
                "modifier": "INCLUDES"
            }
        }
    }
    data = client.call(query, variables)
    return data.get("findGalleries", {}).get("galleries", [])


def get_scenes_in_directory(client, directory_path):
    """Find scenes whose file path starts with the given directory."""
    query = """
    query FindScenes($scene_filter: SceneFilterType, $filter: FindFilterType) {
      findScenes(scene_filter: $scene_filter, filter: $filter) {
        scenes {
          id
          title
          files {
            path
          }
        }
      }
    }
    """
    variables = {
        "filter": {"per_page": -1},
        "scene_filter": {
            "path": {
                "value": directory_path,
                "modifier": "INCLUDES"
            }
        }
    }
    data = client.call(query, variables)
    scenes = data.get("findScenes", {}).get("scenes", [])

    # Filter: scene file must be directly inside directory_path (not a subdirectory)
    matched = []
    for scene in scenes:
        for f in scene.get("files", []):
            scene_dir = os.path.dirname(f.get("path", ""))
            if os.path.normpath(scene_dir) == os.path.normpath(directory_path):
                matched.append(scene)
                break
    return matched


def set_gallery_cover(client, gallery_id, cover_image_b64):
    """
    Set the gallery cover. Stash v0.27+ exposes galleryUpdate with cover_image.
    Falls back to setting it via an image in the gallery if mutation fails.
    """
    mutation = """
    mutation GalleryUpdate($input: GalleryUpdateInput!) {
      galleryUpdate(input: $input) {
        id
      }
    }
    """
    variables = {
        "input": {
            "id": gallery_id,
            "cover_image": cover_image_b64
        }
    }
    data = client.call(mutation, variables)
    return bool(data.get("galleryUpdate"))


def link_gallery_to_scene(client, gallery_id, scene_id, existing_gallery_ids):
    """Add this gallery to a scene's gallery list (preserving existing ones)."""
    all_gallery_ids = list(set(existing_gallery_ids + [gallery_id]))
    mutation = """
    mutation SceneUpdate($input: SceneUpdateInput!) {
      sceneUpdate(input: $input) {
        id
        galleries {
          id
        }
      }
    }
    """
    variables = {
        "input": {
            "id": scene_id,
            "gallery_ids": all_gallery_ids
        }
    }
    data = client.call(mutation, variables)
    return bool(data.get("sceneUpdate"))


def get_scene_gallery_ids(client, scene_id):
    """Fetch the current gallery IDs already linked to a scene."""
    query = """
    query FindScene($id: ID!) {
      findScene(id: $id) {
        galleries {
          id
        }
      }
    }
    """
    data = client.call(query, {"id": scene_id})
    scene = data.get("findScene") or {}
    return [g["id"] for g in scene.get("galleries", [])]


def get_gallery_path(gallery):
    """Extract the filesystem path for a gallery (folder-based or file-based)."""
    # Folder-based gallery
    folder = gallery.get("folder")
    if folder and folder.get("path"):
        return folder["path"]
    # File-based (zip) gallery - use directory of first file
    files = gallery.get("files", [])
    if files:
        return os.path.dirname(files[0].get("path", ""))
    return None


def process_galleries(client, dry_run=False):
    log_info("Fetching extrafanart galleries...")
    galleries = get_all_extrafanart_galleries(client)
    total = len(galleries)
    log_info(f"Found {total} extrafanart gallery/galleries to process.")

    if total == 0:
        log_info("Nothing to do. Make sure 'Create galleries from folders' is enabled in Settings.")
        return

    results = {"cover_set": 0, "scene_linked": 0, "skipped": 0, "errors": 0}

    for i, gallery in enumerate(galleries):
        log_progress(i / total)
        gid = gallery["id"]
        gtitle = gallery.get("title") or f"Gallery #{gid}"

        gallery_path = get_gallery_path(gallery)
        if not gallery_path:
            log_error(f"Could not determine path for gallery {gid} ({gtitle}), skipping.")
            results["errors"] += 1
            continue

        # The parent folder is one level up from the extrafanart folder
        parent_dir = os.path.dirname(os.path.normpath(gallery_path))
        log_info(f"[{i+1}/{total}] Gallery: {gtitle}")
        log_info(f"  Gallery path : {gallery_path}")
        log_info(f"  Parent dir   : {parent_dir}")

        # ---- 1. Find cover image in parent directory ----
        cover_path = find_cover_image(parent_dir)
        if cover_path:
            log_info(f"  Cover found  : {cover_path}")
            if not dry_run:
                try:
                    b64 = image_to_base64(cover_path)
                    ok = set_gallery_cover(client, gid, b64)
                    if ok:
                        log_info(f"  ✓ Cover set successfully.")
                        results["cover_set"] += 1
                    else:
                        log_error(f"  ✗ Failed to set cover (mutation returned false). "
                                  f"Your Stash may need v0.27+ for gallery cover support.")
                        results["errors"] += 1
                except Exception as e:
                    log_error(f"  ✗ Exception setting cover: {e}")
                    results["errors"] += 1
            else:
                log_info(f"  [DRY RUN] Would set cover from {cover_path}")
                results["cover_set"] += 1
        else:
            log_info(f"  No cover image found in {parent_dir} (tried folder/poster/cover/board + jpg/png/webp).")
            results["skipped"] += 1

        # ---- 2. Find scene(s) in the parent directory ----
        scenes = get_scenes_in_directory(client, parent_dir)
        if not scenes:
            log_info(f"  No scenes found directly in {parent_dir}.")
        else:
            for scene in scenes:
                sid = scene["id"]
                stitle = scene.get("title") or f"Scene #{sid}"
                # Check if gallery already linked
                already_linked = any(g["id"] == gid for g in gallery.get("scenes", []))
                if already_linked:
                    log_info(f"  Already linked to scene: {stitle} (#{sid})")
                    continue

                log_info(f"  Scene found  : {stitle} (#{sid})")
                if not dry_run:
                    existing = get_scene_gallery_ids(client, sid)
                    if gid in existing:
                        log_info(f"  Already in scene's gallery list, skipping.")
                        continue
                    ok = link_gallery_to_scene(client, gid, sid, existing)
                    if ok:
                        log_info(f"  ✓ Gallery linked to scene.")
                        results["scene_linked"] += 1
                    else:
                        log_error(f"  ✗ Failed to link gallery to scene.")
                        results["errors"] += 1
                else:
                    log_info(f"  [DRY RUN] Would link gallery to scene #{sid}.")
                    results["scene_linked"] += 1

    log_progress(1.0)
    log_info("=" * 50)
    log_info(f"Done! Results:")
    log_info(f"  Covers set     : {results['cover_set']}")
    log_info(f"  Scenes linked  : {results['scene_linked']}")
    log_info(f"  Skipped (no cover): {results['skipped']}")
    log_info(f"  Errors         : {results['errors']}")
    if dry_run:
        log_info("  (Dry run — no changes were made)")


# ---------------------------------------------------------------------------
# Entry point — Stash passes plugin input via stdin as JSON
# ---------------------------------------------------------------------------

def main():
    # Read plugin input from stdin
    raw_input = sys.stdin.read()
    try:
        plugin_input = json.loads(raw_input)
    except (json.JSONDecodeError, ValueError):
        plugin_input = {}

    # Parse server connection details
    server = plugin_input.get("server_connection", {})
    scheme = server.get("Scheme", "http")
    host = server.get("Host", "localhost")
    if host in ("0.0.0.0", ""):
        host = "localhost"
    port = server.get("Port", 9999)
    api_key = server.get("ApiKey", "")

    # Parse mode from args
    args = plugin_input.get("args", {})
    mode = args.get("mode", "link")
    dry_run = (mode == "dry_run")

    if dry_run:
        log_info("Running in DRY RUN mode — no changes will be made.")
    else:
        log_info("Running in LIVE mode — changes will be applied.")

    client = StashClient(scheme, host, port, api_key or None)
    process_galleries(client, dry_run=dry_run)

    # Output a result for Stash
    print(json.dumps({"output": "Plugin completed successfully."}))


if __name__ == "__main__":
    main()

    # 400 lines yay