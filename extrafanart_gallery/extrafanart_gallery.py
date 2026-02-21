"""
Extrafanart Gallery Linker – Stash Plugin
==========================================
Scans scene directories for a subfolder named extrafanart.
If found, the images inside are imported as a gallery associated with
that scene's parent folder. The gallery cover is set to the folder's
existing cover image (such as folder.jpg or poster.png), and the gallery
is linked to any scene files located in the same directory.

Install:  pip install requests
"""

import sys
import json
import os
import base64

try:
    import requests
except ImportError:
    print("ERROR: 'requests' module not found. Run: pip install requests",
          file=sys.stderr)
    sys.exit(1)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
COVER_CANDIDATES = [
    "folder.jpg", "folder.jpeg", "folder.png", "folder.webp",
    "poster.jpg", "poster.jpeg", "poster.png", "poster.webp",
    "cover.jpg",  "cover.jpeg",  "cover.png",  "cover.webp",
    "board.jpg",  "board.jpeg",  "board.png",  "board.webp",
]
FANART_FOLDER_NAME = "extrafanart"


# ---------------------------------------------------------------------------
# Logging (Stash log protocol)
# ---------------------------------------------------------------------------
def log_info(msg):
    print(f"\x02INFO: {msg}", file=sys.stderr); sys.stderr.flush()

def log_error(msg):
    print(f"\x03ERROR: {msg}", file=sys.stderr); sys.stderr.flush()

def log_progress(pct):
    print(f"\x05{pct:.2f}", file=sys.stderr); sys.stderr.flush()


# ---------------------------------------------------------------------------
# GraphQL client
# ---------------------------------------------------------------------------
class StashClient:
    def __init__(self, scheme, host, port, api_key=None):
        self.url = f"{scheme}://{host}:{port}/graphql"
        self.session = requests.Session()
        self.session.headers["Content-Type"] = "application/json"
        if api_key:
            self.session.headers["ApiKey"] = api_key

    def call(self, query, variables=None):
        payload = {"query": query}
        if variables:
            payload["variables"] = variables
        try:
            resp = self.session.post(self.url, json=payload, timeout=30)
            resp.raise_for_status()
            body = resp.json()
            if "errors" in body:
                for e in body["errors"]:
                    log_error(f"GraphQL: {e.get('message', e)}")
            return body.get("data", {})
        except requests.RequestException as exc:
            log_error(f"Request failed: {exc}")
            return {}

    # ---- queries ----------------------------------------------------------

    def get_all_scenes(self):
        """Return every scene with its id, title, file paths, and linked gallery ids."""
        query = """
        query AllScenes($filter: FindFilterType) {
          findScenes(filter: $filter) {
            count
            scenes {
              id
              title
              files { path }
              galleries { id }
            }
          }
        }"""
        data = self.call(query, {"filter": {"per_page": -1}})
        return data.get("findScenes", {}).get("scenes", [])

    def find_gallery_by_folder(self, folder_path):
        """Find a gallery whose folder path matches exactly."""
        query = """
        query FindGalleries($filter: FindFilterType, $gallery_filter: GalleryFilterType) {
          findGalleries(filter: $filter, gallery_filter: $gallery_filter) {
            galleries {
              id
              title
              folder { path }
              scenes { id }
            }
          }
        }"""
        # Use the folder name as a broad filter, then exact-match client-side
        variables = {
            "filter": {"per_page": -1},
            "gallery_filter": {
                "path": {"value": folder_path, "modifier": "EQUALS"}
            }
        }
        data = self.call(query, variables)
        galleries = data.get("findGalleries", {}).get("galleries", [])
        norm = os.path.normpath(folder_path)
        for g in galleries:
            folder = g.get("folder")
            if folder and os.path.normpath(folder.get("path", "")) == norm:
                return g
        return None

    def find_galleries_by_paths(self, folder_paths):
        """Return a dict {normalised_path: gallery} for a batch of paths."""
        if not folder_paths:
            return {}
        query = """
        query FindGalleries($filter: FindFilterType, $gallery_filter: GalleryFilterType) {
          findGalleries(filter: $filter, gallery_filter: $gallery_filter) {
            galleries {
              id
              title
              folder { path }
              scenes { id }
            }
          }
        }"""
        variables = {
            "filter": {"per_page": -1},
            "gallery_filter": {
                "path": {"value": FANART_FOLDER_NAME, "modifier": "INCLUDES"}
            }
        }
        data = self.call(query, variables)
        galleries = data.get("findGalleries", {}).get("galleries", [])
        norm_set = {os.path.normpath(p) for p in folder_paths}
        result = {}
        for g in galleries:
            folder = g.get("folder")
            if folder:
                gp = os.path.normpath(folder.get("path", ""))
                if gp in norm_set:
                    result[gp] = g
        return result

    # ---- mutations --------------------------------------------------------

    def trigger_scan(self, paths):
        """Start a metadata scan for specific paths (fire-and-forget)."""
        mutation = """
        mutation MetadataScan($input: ScanMetadataInput!) {
          metadataScan(input: $input)
        }"""
        variables = {"input": {"paths": paths}}
        data = self.call(mutation, variables)
        return data.get("metadataScan")

    def set_gallery_cover(self, gallery_id, cover_b64):
        mutation = """
        mutation GalleryUpdate($input: GalleryUpdateInput!) {
          galleryUpdate(input: $input) { id }
        }"""
        data = self.call(mutation, {
            "input": {"id": gallery_id, "cover_image": cover_b64}
        })
        return bool(data.get("galleryUpdate"))

    def link_gallery_to_scene(self, scene_id, gallery_id, existing_gallery_ids):
        """Add gallery to a scene's gallery list (preserving existing)."""
        all_ids = list(set(existing_gallery_ids + [gallery_id]))
        mutation = """
        mutation SceneUpdate($input: SceneUpdateInput!) {
          sceneUpdate(input: $input) { id }
        }"""
        data = self.call(mutation, {
            "input": {"id": scene_id, "gallery_ids": all_ids}
        })
        return bool(data.get("sceneUpdate"))


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def find_cover_image(directory):
    """Return path of a cover image in *directory*, or None."""
    try:
        entries = {e.lower(): e for e in os.listdir(directory)}
    except OSError:
        return None
    for candidate in COVER_CANDIDATES:
        if candidate in entries:
            return os.path.join(directory, entries[candidate])
    return None


def image_to_base64(path):
    ext = os.path.splitext(path)[1].lower().lstrip(".")
    mime = {"jpg": "jpeg", "jpeg": "jpeg", "png": "png", "webp": "webp"}.get(ext, "jpeg")
    with open(path, "rb") as f:
        data = base64.b64encode(f.read()).decode()
    return f"data:image/{mime};base64,{data}"


# ---------------------------------------------------------------------------
# Core logic
# ---------------------------------------------------------------------------

def process(client, dry_run=False):
    """
    Phase 1 – Discover extrafanart folders by walking scene directories.
    Phase 2 – Scan/import any folders that don't yet have a gallery.
    Phase 3 – Set covers and link galleries to their parent scenes.
    """

    # ── Phase 1: discover ─────────────────────────────────────────────────
    log_info("Phase 1: Fetching all scenes and scanning for extrafanart folders...")
    scenes = client.get_all_scenes()
    log_info(f"  Found {len(scenes)} scene(s) in Stash.")

    # Map scene-directory → list of scene dicts
    dir_scenes = {}   # {normalised_dir: [scene, ...]}
    for scene in scenes:
        for f in scene.get("files", []):
            d = os.path.normpath(os.path.dirname(f.get("path", "")))
            dir_scenes.setdefault(d, []).append(scene)

    # Check each scene directory for an extrafanart subfolder
    # targets: [(parent_dir, extrafanart_path, [scene, ...])]
    targets = []
    for d, scene_list in dir_scenes.items():
        ef = os.path.join(d, FANART_FOLDER_NAME)
        if os.path.isdir(ef):
            targets.append((d, ef, scene_list))

    log_info(f"  Found {len(targets)} scene director(y/ies) with an '{FANART_FOLDER_NAME}' subfolder.")

    if not targets:
        log_info("Nothing to do.")
        return

    # ── Phase 2: ensure galleries exist ───────────────────────────────────
    log_info("Phase 2: Checking for existing galleries and scanning missing ones...")

    ef_paths = [ef for _, ef, _ in targets]
    existing = client.find_galleries_by_paths(ef_paths)
    paths_to_scan = [ef for ef in ef_paths if os.path.normpath(ef) not in existing]

    if paths_to_scan:
        log_info(f"  {len(paths_to_scan)} extrafanart folder(s) need scanning:")
        for p in paths_to_scan:
            log_info(f"    • {p}")
        if not dry_run:
            log_info("  Triggering metadata scan (runs in background)...")
            job_id = client.trigger_scan(paths_to_scan)
            if job_id:
                log_info(f"  Scan job queued (ID: {job_id}).")
                log_info(f"  >> The scan runs in the background. Run this task again")
                log_info(f"  >> after the scan finishes to set covers and link galleries.")
            else:
                log_error("  Failed to start scan job.")
        else:
            log_info("  [DRY RUN] Would scan these paths to create galleries.")
    else:
        log_info("  All extrafanart folders already have galleries.")

    # ── Phase 3: set covers & link ────────────────────────────────────────
    log_info("Phase 3: Setting covers and linking galleries to scenes...")
    results = {"cover_set": 0, "linked": 0, "skipped": 0, "errors": 0}
    total = len(targets)

    for i, (parent_dir, ef_path, scene_list) in enumerate(targets):
        log_progress(i / total)
        norm_ef = os.path.normpath(ef_path)
        gallery = existing.get(norm_ef)

        if not gallery:
            if dry_run:
                log_info(f"[{i+1}/{total}] {ef_path}")
                log_info(f"  [DRY RUN] Gallery would be created by scan.")
                cover = find_cover_image(parent_dir)
                if cover:
                    log_info(f"  [DRY RUN] Would set cover from {cover}")
                    results["cover_set"] += 1
                for s in scene_list:
                    log_info(f"  [DRY RUN] Would link to scene: {s.get('title') or s['id']}")
                    results["linked"] += 1
            else:
                log_info(f"  Skipping {ef_path} (gallery not yet created — waiting for scan)")
                results["skipped"] += 1
            continue

        gid = gallery["id"]
        gtitle = gallery.get("title") or f"Gallery #{gid}"
        log_info(f"[{i+1}/{total}] {gtitle}  ({ef_path})")

        # ── Cover ──
        cover_path = find_cover_image(parent_dir)
        if cover_path:
            if not dry_run:
                try:
                    b64 = image_to_base64(cover_path)
                    if client.set_gallery_cover(gid, b64):
                        log_info(f"  Cover set from {os.path.basename(cover_path)}")
                        results["cover_set"] += 1
                    else:
                        log_error(f"  Failed to set cover (needs Stash v0.27+?).")
                        results["errors"] += 1
                except Exception as exc:
                    log_error(f"  Exception setting cover: {exc}")
                    results["errors"] += 1
            else:
                log_info(f"  [DRY RUN] Would set cover from {cover_path}")
                results["cover_set"] += 1
        else:
            log_info(f"  No cover image found in {parent_dir}")
            results["skipped"] += 1

        # ── Link to scenes ──
        linked_scene_ids = {s["id"] for s in gallery.get("scenes", [])}
        for scene in scene_list:
            sid = scene["id"]
            stitle = scene.get("title") or f"Scene #{sid}"
            if sid in linked_scene_ids:
                log_info(f"  Already linked to: {stitle}")
                continue
            if not dry_run:
                existing_gids = [g["id"] for g in scene.get("galleries", [])]
                if client.link_gallery_to_scene(sid, gid, existing_gids):
                    log_info(f"  Linked to scene: {stitle}")
                    results["linked"] += 1
                else:
                    log_error(f"  Failed to link to scene: {stitle}")
                    results["errors"] += 1
            else:
                log_info(f"  [DRY RUN] Would link to scene: {stitle}")
                results["linked"] += 1

    log_progress(1.0)
    log_info("=" * 50)
    log_info("Done!")
    log_info(f"  Covers set      : {results['cover_set']}")
    log_info(f"  Scenes linked   : {results['linked']}")
    log_info(f"  Skipped (no cov): {results['skipped']}")
    log_info(f"  Errors          : {results['errors']}")
    if dry_run:
        log_info("  (Dry run — no changes were made)")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    raw = sys.stdin.read()
    try:
        plugin_input = json.loads(raw)
    except (json.JSONDecodeError, ValueError):
        plugin_input = {}

    server = plugin_input.get("server_connection", {})
    scheme = server.get("Scheme", "http")
    host   = server.get("Host", "localhost")
    if host in ("0.0.0.0", ""):
        host = "localhost"
    port    = server.get("Port", 9999)
    api_key = server.get("ApiKey", "")

    mode    = plugin_input.get("args", {}).get("mode", "link")
    dry_run = mode == "dry_run"

    if dry_run:
        log_info("=== DRY RUN — no changes will be made ===")
    else:
        log_info("=== LIVE MODE — changes will be applied ===")

    client = StashClient(scheme, host, port, api_key or None)
    process(client, dry_run)

    print(json.dumps({"output": "Plugin completed successfully."}))


if __name__ == "__main__":
    main()
