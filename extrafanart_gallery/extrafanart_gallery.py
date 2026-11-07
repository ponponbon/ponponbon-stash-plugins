"""
Extrafanart Gallery Linker - Stash Plugin
==========================================
Scans scene directories for a subfolder named extrafanart.
If found, creates a gallery from the images inside (via GraphQL, not
metadataScan), adds the parent folder's cover/fanart images, sets the
gallery cover (setGalleryCover) to the first of folder/poster/cover/board
found, and links the gallery to any scene files in the same parent
directory. On later runs, extrafanart images that are not yet in the linked
gallery are added, whether the gallery came from the folder or the API.

Install:  pip install requests
"""
import json
import os
import sys

try:
    import requests
except ImportError:
    print("ERROR: 'requests' module not found.", file=sys.stderr)
    sys.exit(1)

COVER_CANDIDATES = [
    "folder.jpg", "folder.jpeg", "folder.png", "folder.webp",
    "poster.jpg", "poster.jpeg", "poster.png", "poster.webp",
    "cover.jpg",  "cover.jpeg",  "cover.png",  "cover.webp",
    "board.jpg",  "board.jpeg",  "board.png",  "board.webp",
]
# Extra images in the parent dir to add to the gallery (cover + fanart)
PARENT_IMAGE_CANDIDATES = COVER_CANDIDATES + [
    "fanart.jpg", "fanart.jpeg", "fanart.png", "fanart.webp",
]
FANART_FOLDER = "extrafanart"
PLUGIN_ID = "extrafanart_gallery"
PAGE_SIZE = 500

def path_key(path):
    if not path:
        return ""
    return os.path.normcase(os.path.normpath(path))

# --- Logging (Stash raw plugin protocol on stderr) ---
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

def normalize_graphql_url(url):
    url = (url or "").strip().rstrip("/")
    if not url:
        return ""
    if not url.endswith("/graphql"):
        url += "/graphql"
    return url

def graphql_url_from_input(plugin_input):
    args = plugin_input.get("args", {})
    url = first_nonempty(args.get("stash_url"), args.get("e_stash_url"), os.environ.get("STASH_URL"))
    if url:
        return normalize_graphql_url(url), "setting"

    srv = plugin_input.get("server_connection", {})
    scheme = srv.get("Scheme", "http")
    host = srv.get("Host", "localhost")
    if host in ("0.0.0.0", ""):
        host = "localhost"
    port = srv.get("Port", 9999)
    return f"{scheme}://{host}:{port}/graphql", "server_connection"

def api_key_from_input(plugin_input):
    args = plugin_input.get("args", {})
    srv = plugin_input.get("server_connection", {})
    return first_nonempty(
        args.get("api_key"),
        args.get("f_api_key"),
        srv.get("ApiKey"),
        os.environ.get("STASH_API_KEY"),
    )

def truthy(value):
    return str(value).strip().lower() in ("1", "true", "yes", "on")

def apply_session_cookie(session, cookie):
    if not cookie:
        return
    session.cookies.set(
        cookie.get("Name", "session"),
        cookie.get("Value", ""),
        domain=cookie.get("Domain", "localhost"),
        path=cookie.get("Path", "/"),
    )

# --- GraphQL client ---
class GQL:
    def __init__(self, url, api_key=None, session_cookie=None):
        self.url = normalize_graphql_url(url)
        self.s = requests.Session()
        self.s.headers["Content-Type"] = "application/json"
        if api_key:
            self.s.headers["ApiKey"] = api_key
        else:
            apply_session_cookie(self.s, session_cookie)
        self.gallery_cache = {}

    def q(self, query, variables=None):
        body = {"query": query}
        if variables:
            body["variables"] = variables
        r = self.s.post(self.url, json=body, timeout=30)
        r.raise_for_status()
        j = r.json()
        if "errors" in j:
            for e in j["errors"]:
                log_err(f"GQL: {e.get('message', e)}")
        return j.get("data", {})

    def all_scenes(self):
        scenes = []
        page = 1
        while True:
            d = self.q("""query($f:FindFilterType!){
                findScenes(filter:$f){
                    count
                    scenes{id title files{path} galleries{id}}
                }}""", {"f": {"page": page, "per_page": PAGE_SIZE, "sort": "id", "direction": "ASC"}})
            result = d.get("findScenes", {})
            batch = result.get("scenes", [])
            scenes.extend(batch)
            count = result.get("count", len(scenes))
            if len(scenes) >= count or not batch:
                break
            page += 1
        return scenes

    def find_galleries_for_paths(self, folder_paths):
        """Return {norm_path: gallery} for extrafanart galleries."""
        norms = {path_key(p) for p in folder_paths}
        out = {}
        page = 1
        while True:
            d = self.q("""query($f:FindFilterType,$gf:GalleryFilterType){
                findGalleries(filter:$f,gallery_filter:$gf){
                    count
                    galleries{id title folder{path} scenes{id} cover{id}}
                }}""", {
                "f": {"page": page, "per_page": PAGE_SIZE},
                "gf": {"path": {"value": FANART_FOLDER, "modifier": "INCLUDES"}},
            })
            result = d.get("findGalleries", {})
            gals = result.get("galleries", [])
            for g in gals:
                fp = g.get("folder") or {}
                gp = path_key(fp.get("path", ""))
                if gp in norms:
                    out[gp] = g
                    self.gallery_cache[str(g["id"])] = g
            count = result.get("count", page * PAGE_SIZE)
            if page * PAGE_SIZE >= count or not gals:
                break
            page += 1
        return out

    def find_gallery(self, gallery_id):
        cache_key = str(gallery_id)
        if cache_key in self.gallery_cache:
            return self.gallery_cache[cache_key]
        d = self.q("""query($id:ID!){
            findGallery(id:$id){id title scenes{id} cover{id}}}""",
            {"id": gallery_id})
        gallery = d.get("findGallery")
        if gallery:
            self.gallery_cache[cache_key] = gallery
        return gallery

    def find_images_for_dirs(self, folder_paths, path_fragment):
        """Return {norm_dir: [image, ...]} for images in the requested directories."""
        norms = {path_key(p) for p in folder_paths}
        out = {norm: [] for norm in norms}
        seen = {norm: set() for norm in norms}
        page = 1
        while True:
            d = self.q("""query($f:FindFilterType,$if:ImageFilterType){
                findImages(filter:$f,image_filter:$if){
                    count
                    images{id files{path} galleries{id}}
                }}""", {
                "f": {"page": page, "per_page": PAGE_SIZE},
                "if": {"path": {"value": path_fragment, "modifier": "INCLUDES"}},
            })
            result = d.get("findImages", {})
            imgs = result.get("images", [])
            for img in imgs:
                image_id = str(img.get("id", ""))
                for f in img.get("files", []):
                    dirname = path_key(os.path.dirname(f.get("path", "")))
                    if dirname in out and image_id not in seen[dirname]:
                        out[dirname].append(img)
                        seen[dirname].add(image_id)
                        break
            count = result.get("count", page * PAGE_SIZE)
            if page * PAGE_SIZE >= count or not imgs:
                break
            page += 1
        return out

    def find_images_for_exact_paths(self, file_paths):
        """Return {norm_path: image} for a list of exact image paths."""
        wanted = {path_key(p) for p in file_paths}
        out = {}
        if not wanted:
            return out

        basenames = sorted({os.path.basename(p) for p in file_paths if os.path.basename(p)})
        for basename in basenames:
            page = 1
            while True:
                d = self.q("""query($f:FindFilterType,$if:ImageFilterType){
                    findImages(filter:$f,image_filter:$if){
                        count
                        images{id files{path} galleries{id}}
                    }}""", {
                    "f": {"page": page, "per_page": PAGE_SIZE},
                    "if": {"path": {"value": basename, "modifier": "INCLUDES"}},
                })
                result = d.get("findImages", {})
                imgs = result.get("images", [])
                for img in imgs:
                    for f in img.get("files", []):
                        norm = path_key(f.get("path", ""))
                        if norm in wanted and norm not in out:
                            out[norm] = img
                count = result.get("count", page * PAGE_SIZE)
                if page * PAGE_SIZE >= count or not imgs:
                    break
                page += 1
        return out

    def create_gallery(self, title, scene_ids=None):
        inp = {"title": title}
        if scene_ids:
            inp["scene_ids"] = scene_ids
        d = self.q("""mutation($i:GalleryCreateInput!){
            galleryCreate(input:$i){id}}""", {"i": inp})
        return (d.get("galleryCreate") or {}).get("id")

    def add_images(self, gallery_id, image_ids):
        self.q("""mutation($i:GalleryAddInput!){
            addGalleryImages(input:$i)}""",
            {"i":{"gallery_id":gallery_id,"image_ids":image_ids}})

    def find_image_by_path(self, file_path):
        """Find a single image in Stash by exact file path."""
        d = self.q("""query($f:FindFilterType,$if:ImageFilterType){
            findImages(filter:$f,image_filter:$if){images{id
            files{path} galleries{id}}}}""",
            {"f":{"per_page":5},"if":{"path":{"value":file_path,"modifier":"EQUALS"}}})
        imgs = d.get("findImages",{}).get("images",[])
        norm = path_key(file_path)
        for img in imgs:
            for f in img.get("files",[]):
                if path_key(f.get("path","")) == norm:
                    return img
        return None

    def set_cover(self, gallery_id, image_id):
        self.q("""mutation($i:GallerySetCoverInput!){
            setGalleryCover(input:$i)}""",
            {"i": {"gallery_id": gallery_id, "cover_image_id": image_id}})

    def update_gallery_title(self, gallery_id, title):
        self.q("""mutation($i:GalleryUpdateInput!){
            galleryUpdate(input:$i){id}}""",
            {"i":{"id":gallery_id,"title":title}})

    def link_scene(self, scene_id, gallery_id, existing_gids):
        all_ids = list(set(existing_gids + [gallery_id]))
        self.q("""mutation($i:SceneUpdateInput!){
            sceneUpdate(input:$i){id}}""",
            {"i":{"id":scene_id,"gallery_ids":all_ids}})

# --- Helpers ---
def find_parent_images(d):
    """Return list of full paths for cover/fanart images in directory."""
    try:
        entries = {e.lower(): e for e in os.listdir(d)}
    except OSError:
        return []
    found = []
    seen = set()
    for c in PARENT_IMAGE_CANDIDATES:
        if c in entries and c not in seen:
            found.append(os.path.join(d, entries[c]))
            seen.add(c)
    return found

def gallery_title_from_dir(parent_name):
    """Extract code/ID from directory name. 'SNOS-094 - (2026-02-09)' -> 'SNOS-094'"""
    return parent_name.split(" - ")[0].strip()

def gallery_ids_for_image(image):
    return {g["id"] for g in image.get("galleries", [])}

def pick_existing_gallery_id(images):
    counts = {}
    for img in images:
        for gid in gallery_ids_for_image(img):
            counts[gid] = counts.get(gid, 0) + 1
    if not counts:
        return None

    def sort_key(item):
        gid, count = item
        gid_text = str(gid)
        gid_order = (0, int(gid_text)) if gid_text.isdigit() else (1, gid_text)
        return (-count, gid_order)

    return sorted(counts.items(), key=sort_key)[0][0]

def gallery_linked_to_scenes(gallery, scenes):
    linked_sids = {s["id"] for s in gallery.get("scenes", [])}
    target_sids = {s["id"] for s in scenes}
    return bool(target_sids) and target_sids.issubset(linked_sids)


def first_cover_image(parent_imgs, parent_image_map):
    """First parent cover candidate (folder/poster/cover/board, in that order)
    that exists in Stash. fanart.* is never used as the cover."""
    for pimg in parent_imgs:
        if os.path.basename(pimg).lower() not in COVER_CANDIDATES:
            continue
        img = parent_image_map.get(path_key(pimg))
        if img:
            return img
    return None


def plan_gallery_actions(gallery, ef_images, parent_imgs, parent_image_map, scenes,
                         refresh_existing=False, just_created=False):
    """
    Work out what to change on one gallery. Pure: no I/O.
    Returns a dict with:
      add_ids / add_names  - extrafanart and parent images not yet in the gallery
      missing_parents      - True when parent images exist on disk but not in Stash
      cover_id             - image to set as cover, or None
      link_scenes          - scenes not yet linked to the gallery
    """
    gid = str(gallery["id"])
    add_ids, add_names, seen = [], [], set()

    # Extrafanart images added since the gallery was made (folder or API).
    ef_new = 0
    for img in ef_images:
        iid = str(img["id"])
        if gid in {str(g) for g in gallery_ids_for_image(img)} or iid in seen:
            continue
        seen.add(iid)
        add_ids.append(iid)
        ef_new += 1
    if ef_new:
        add_names.append(f"{ef_new} extrafanart image(s)")

    found_parent = False
    for pimg in parent_imgs:
        img = parent_image_map.get(path_key(pimg))
        if not img:
            continue
        found_parent = True
        iid = str(img["id"])
        if gid in {str(g) for g in gallery_ids_for_image(img)} or iid in seen:
            continue
        seen.add(iid)
        add_ids.append(iid)
        add_names.append(os.path.basename(pimg))

    cover_id = None
    cover = first_cover_image(parent_imgs, parent_image_map)
    if cover:
        cid = str(cover["id"])
        current = str((gallery.get("cover") or {}).get("id") or "")
        # Set it on new galleries and when the cover image was just added;
        # otherwise only re-assert it with Refresh Existing, so a cover the
        # user picked by hand is not overwritten on every run.
        if current != cid and (just_created or cid in seen or refresh_existing):
            cover_id = cid

    linked = {str(s["id"]) for s in gallery.get("scenes", [])}
    link_scenes = []
    for sc in scenes:
        sid = str(sc["id"])
        scene_gids = {str(g["id"]) for g in sc.get("galleries", [])}
        if sid in linked or gid in scene_gids:
            continue
        link_scenes.append(sc)
        linked.add(sid)

    return {
        "add_ids": add_ids,
        "add_names": add_names,
        "missing_parents": bool(parent_imgs) and not found_parent,
        "cover_id": cover_id,
        "link_scenes": link_scenes,
    }


# --- Core ---
def process(gql, dry_run=False, refresh_existing=False):
    # Phase 1: Find scene directories with extrafanart subfolders
    log("Phase 1: Finding scenes with extrafanart folders...")
    scenes = gql.all_scenes()
    log(f"  {len(scenes)} scene(s) in Stash")

    dir_scenes = {}
    for sc in scenes:
        for f in sc.get("files", []):
            d = os.path.normpath(os.path.dirname(f.get("path", "")))
            dir_scenes.setdefault(d, []).append(sc)

    targets = []
    for d, slist in dir_scenes.items():
        ef = os.path.join(d, FANART_FOLDER)
        if os.path.isdir(ef):
            targets.append((d, ef, slist))

    log(f"  {len(targets)} dir(s) with '{FANART_FOLDER}' subfolder")
    if not targets:
        log("Nothing to do.")
        return

    # Phase 2: Existing folder-based galleries and image matches. Images are
    # loaded for every target (not just new ones) so extrafanart images added
    # after the gallery was created are picked up without Refresh Existing.
    ef_paths = [ef for _, ef, _ in targets]
    existing_gals = gql.find_galleries_for_paths(ef_paths)
    log(f"  {len(existing_gals)} existing folder-based gallery/galleries found")

    log("Phase 2: Loading image matches...")
    images_by_ef = gql.find_images_for_dirs(ef_paths, FANART_FOLDER)
    ef_image_count = sum(len(images) for images in images_by_ef.values())
    log(f"  {ef_image_count} extrafanart image(s) found in Stash")

    parent_imgs_by_parent = {}
    parent_image_paths = []
    for parent, _, _ in targets:
        parent_imgs = find_parent_images(parent)
        parent_imgs_by_parent[parent] = parent_imgs
        parent_image_paths.extend(parent_imgs)

    parent_image_map = gql.find_images_for_exact_paths(parent_image_paths)
    if parent_image_paths:
        log(f"  {len(parent_image_map)} parent cover/fanart image(s) found in Stash")

    # Phase 3: Process each target
    log("Phase 3: Processing galleries...")
    stats = {"created": 0, "images_added": 0, "covers": 0, "linked": 0,
             "skipped": 0, "errors": 0}
    total = len(targets)
    galleries_loaded = 0

    for i, (parent, ef_path, slist) in enumerate(targets):
        log_progress(i / total)
        norm_ef = path_key(ef_path)
        parent_name = os.path.basename(parent)
        prefix = f"  [{i+1}/{total}] {parent_name}:"
        gallery = existing_gals.get(norm_ef)
        imgs = images_by_ef.get(norm_ef, [])
        parent_imgs = parent_imgs_by_parent.get(parent, [])
        just_created = False

        if not gallery:
            if not imgs:
                log(f"{prefix} no images in Stash for {ef_path}")
                log(f"    Ensure path is in library and run a Scan first.")
                stats["skipped"] += 1
                continue

            gal_id = pick_existing_gallery_id(imgs)
            if gal_id:
                gallery = gql.find_gallery(gal_id) or {"id": gal_id, "scenes": []}
                galleries_loaded += 1
            else:
                title = gallery_title_from_dir(parent_name)
                scene_ids = list({s["id"] for s in slist})
                if dry_run:
                    log(f"{prefix} [DRY] would create '{title}' with {len(imgs)} images "
                        f"linked to {len(scene_ids)} scene(s)")
                    plan = plan_gallery_actions({"id": "new", "scenes": []}, [], parent_imgs,
                                                parent_image_map, [], just_created=True)
                    if plan["add_names"]:
                        log(f"{prefix} [DRY] would add {', '.join(plan['add_names'])}")
                    if plan["cover_id"]:
                        log(f"{prefix} [DRY] would set cover to image #{plan['cover_id']}")
                    stats["created"] += 1
                    continue
                gal_id = gql.create_gallery(title, scene_ids)
                if not gal_id:
                    log_err(f"{prefix} failed to create gallery")
                    stats["errors"] += 1
                    continue
                gql.add_images(gal_id, [img["id"] for img in imgs])
                log(f"{prefix} created gallery #{gal_id} ({len(imgs)} images)")
                stats["created"] += 1
                just_created = True
                gallery = {"id": gal_id, "title": title, "scenes": [{"id": s} for s in scene_ids]}
                imgs = [dict(img, galleries=list(img.get("galleries", [])) + [{"id": gal_id}])
                        for img in imgs]

        gid = gallery["id"]
        plan = plan_gallery_actions(gallery, imgs, parent_imgs, parent_image_map, slist,
                                    refresh_existing=refresh_existing, just_created=just_created)
        expected_title = gallery_title_from_dir(parent_name)
        title_change = gallery.get("title", "") != expected_title

        if not (plan["add_ids"] or plan["cover_id"] or plan["link_scenes"] or title_change):
            if plan["missing_parents"]:
                log(f"{prefix} parent images not in Stash (run Scan)")
            log(f"{prefix} gallery #{gid} already up to date")
            stats["skipped"] += 1
            continue

        # Sync gallery title
        if title_change:
            if dry_run:
                log(f"{prefix} [DRY] would set title -> '{expected_title}'")
            else:
                gql.update_gallery_title(gid, expected_title)
                log(f"{prefix} title updated -> '{expected_title}'")

        # Add new extrafanart images and parent images (folder.jpg, fanart.jpg ...)
        if plan["add_ids"]:
            names = ", ".join(plan["add_names"])
            if dry_run:
                log(f"{prefix} [DRY] would add {names} to gallery #{gid}")
            else:
                gql.add_images(gid, plan["add_ids"])
                log(f"{prefix} added {names} to gallery #{gid}")
            stats["images_added"] += len(plan["add_ids"])
        elif plan["missing_parents"]:
            log(f"{prefix} parent images not in Stash (run Scan)")
        elif not parent_imgs:
            log(f"{prefix} no parent images found")

        # Cover image
        if plan["cover_id"]:
            if dry_run:
                log(f"{prefix} [DRY] would set cover to image #{plan['cover_id']}")
            else:
                gql.set_cover(gid, plan["cover_id"])
                log(f"{prefix} cover set to image #{plan['cover_id']}")
            stats["covers"] += 1

        # Link to scenes
        for sc in plan["link_scenes"]:
            sid = sc["id"]
            if dry_run:
                log(f"    [DRY] would link to scene {sc.get('title') or sid}")
            else:
                eg = [g["id"] for g in sc.get("galleries", [])]
                gql.link_scene(sid, gid, eg)
                log(f"    linked to scene: {sc.get('title') or sid}")
            stats["linked"] += 1

    log_progress(1.0)
    log("=" * 40)
    log(f"Done! Created={stats['created']} ImagesAdded={stats['images_added']} "
        f"Covers={stats['covers']} Linked={stats['linked']} "
        f"Skipped={stats['skipped']} Errors={stats['errors']}")
    if galleries_loaded:
        log(f"Loaded {galleries_loaded} existing image-backed gallery/galleries")
    if dry_run:
        log("(Dry run - no changes made)")
    return stats


# --- Entry ---
def main():
    raw = sys.stdin.read()
    try:
        pi = json.loads(raw)
    except Exception:
        pi = {}
    srv = pi.get("server_connection", {})
    gql_url, url_source = graphql_url_from_input(pi)
    api_key = api_key_from_input(pi)
    mode = pi.get("args", {}).get("mode", "link")
    dry_run = mode == "dry_run"
    log(f"Mode: {'DRY RUN' if dry_run else 'LIVE'}")
    log(f"Endpoint: {gql_url} ({url_source})")
    g = GQL(gql_url, api_key or None, srv.get("SessionCookie"))
    # Stash does not pass saved plugin settings in args; read them from the
    # plugin configuration (a task arg still takes precedence).
    refresh_arg = pi.get("args", {}).get("refresh_existing")
    if refresh_arg is None:
        saved = (g.q("""query($ids:[ID!]){configuration{plugins(include:$ids)}}""",
                     {"ids": [PLUGIN_ID]}).get("configuration") or {}).get("plugins") or {}
        refresh_arg = (saved.get(PLUGIN_ID) or {}).get("refresh_existing", False)
    refresh_existing = truthy(refresh_arg)
    log(f"Refresh existing: {'yes' if refresh_existing else 'no'}")
    process(g, dry_run, refresh_existing)
    print(json.dumps({"output": "ok"}))

if __name__ == "__main__":
    main()
