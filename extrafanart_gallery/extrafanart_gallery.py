# testupdate - verify CI pipeline packages all plugins correctly
"""
Extrafanart Gallery Linker - Stash Plugin
==========================================
Scans scene directories for a subfolder named extrafanart.
If found, creates a gallery from the images inside (via GraphQL, not
metadataScan), sets the cover image, and links the gallery to any
scene files in the same parent directory.

Install:  pip install requests
"""
import sys, json, os
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

# --- Logging (Stash raw plugin protocol on stderr) ---
def log(msg):
    # \x01=trace \x02=debug \x03=info \x04=warning \x05=error \x06=progress
    print(f"\x03{msg}", file=sys.stderr, flush=True)

def log_warn(msg):
    print(f"\x04{msg}", file=sys.stderr, flush=True)

def log_err(msg):
    print(f"\x05{msg}", file=sys.stderr, flush=True)

def log_progress(pct):
    print(f"\x06{pct:.2f}", file=sys.stderr, flush=True)

# --- GraphQL client ---
class GQL:
    def __init__(self, scheme, host, port, api_key=None):
        self.url = f"{scheme}://{host}:{port}/graphql"
        self.s = requests.Session()
        self.s.headers["Content-Type"] = "application/json"
        if api_key:
            self.s.headers["ApiKey"] = api_key

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
        d = self.q("""query{findScenes(filter:{per_page:-1}){
            scenes{id title files{path} galleries{id}}}}""")
        return d.get("findScenes",{}).get("scenes",[])

    def find_galleries_for_paths(self, folder_paths):
        """Return {norm_path: gallery} for extrafanart galleries."""
        d = self.q("""query($f:FindFilterType,$gf:GalleryFilterType){
            findGalleries(filter:$f,gallery_filter:$gf){galleries{
            id title folder{path} scenes{id}}}}""",
            {"f":{"per_page":-1},"gf":{"path":{"value":FANART_FOLDER,"modifier":"INCLUDES"}}})
        gals = d.get("findGalleries",{}).get("galleries",[])
        norms = {os.path.normpath(p) for p in folder_paths}
        out = {}
        for g in gals:
            fp = g.get("folder") or {}
            gp = os.path.normpath(fp.get("path",""))
            if gp in norms:
                out[gp] = g
        return out

    def find_images_in_path(self, folder_path):
        """Find images whose file path starts with folder_path."""
        d = self.q("""query($f:FindFilterType,$if:ImageFilterType){
            findImages(filter:$f,image_filter:$if){images{id
            files{path} galleries{id}}}}""",
            {"f":{"per_page":-1},"if":{"path":{"value":folder_path,"modifier":"INCLUDES"}}})
        imgs = d.get("findImages",{}).get("images",[])
        norm = os.path.normpath(folder_path)
        return [i for i in imgs if any(
            os.path.normpath(os.path.dirname(f.get("path",""))) == norm
            for f in i.get("files",[]))]

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
            files{path}}}}""",
            {"f":{"per_page":5},"if":{"path":{"value":file_path,"modifier":"EQUALS"}}})
        imgs = d.get("findImages",{}).get("images",[])
        norm = os.path.normpath(file_path)
        for img in imgs:
            for f in img.get("files",[]):
                if os.path.normpath(f.get("path","")) == norm:
                    return img["id"]
        return None

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


# --- Core ---
def process(gql, dry_run=False):
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

    # Phase 2: Check for existing folder-based galleries
    ef_paths = [ef for _, ef, _ in targets]
    existing_gals = gql.find_galleries_for_paths(ef_paths)
    log(f"  {len(existing_gals)} existing folder-based gallery/galleries found")

    # Phase 3: Process each target
    log("Phase 2: Processing galleries...")
    stats = {"created": 0, "covers": 0, "linked": 0, "skipped": 0, "errors": 0}
    total = len(targets)

    for i, (parent, ef_path, slist) in enumerate(targets):
        log_progress(i / total)
        norm_ef = os.path.normpath(ef_path)
        parent_name = os.path.basename(parent)
        gallery = existing_gals.get(norm_ef)

        # If no folder gallery, check for images and create a virtual gallery
        if not gallery:
            imgs = gql.find_images_in_path(ef_path)
            if not imgs:
                log(f"  [{i+1}/{total}] {parent_name}: no images in Stash for {ef_path}")
                log(f"    Ensure path is in library and run a Scan first.")
                stats["skipped"] += 1
                continue

            # Check if images are already in a gallery
            existing_gal_ids = set()
            for img in imgs:
                for g in img.get("galleries", []):
                    existing_gal_ids.add(g["id"])
            if existing_gal_ids:
                # Images already belong to a gallery - use the first one
                gal_id = list(existing_gal_ids)[0]
                log(f"  [{i+1}/{total}] {parent_name}: images already in gallery #{gal_id}")
                gallery = {"id": gal_id, "scenes": []}
            else:
                # Create new gallery and add images
                title = gallery_title_from_dir(parent_name)
                scene_ids = list({s["id"] for s in slist})
                if dry_run:
                    log(f"  [{i+1}/{total}] {parent_name}: [DRY] would create '{title}' with {len(imgs)} images")
                    stats["created"] += 1
                    continue
                gal_id = gql.create_gallery(title, scene_ids)
                if not gal_id:
                    log_err(f"  [{i+1}/{total}] {parent_name}: failed to create gallery")
                    stats["errors"] += 1
                    continue
                image_ids = [img["id"] for img in imgs]
                gql.add_images(gal_id, image_ids)
                log(f"  [{i+1}/{total}] {parent_name}: created gallery #{gal_id} ({len(imgs)} images)")
                stats["created"] += 1
                gallery = {"id": gal_id, "scenes": [{"id": s} for s in scene_ids]}

        gid = gallery["id"]

        # Sync gallery title
        expected_title = gallery_title_from_dir(parent_name)
        cur_title = gallery.get("title", "")
        if cur_title != expected_title and not dry_run:
            gql.update_gallery_title(gid, expected_title)
            log(f"  [{i+1}/{total}] {parent_name}: title updated -> '{expected_title}'")

        # Add parent images (folder.jpg, fanart.jpg, etc) to gallery
        parent_imgs = find_parent_images(parent)
        if parent_imgs:
            added_names = []
            ids_to_add = []
            for pimg in parent_imgs:
                if dry_run:
                    added_names.append(os.path.basename(pimg))
                    continue
                img_id = gql.find_image_by_path(pimg)
                if img_id:
                    ids_to_add.append(img_id)
                    added_names.append(os.path.basename(pimg))
            if dry_run:
                log(f"  [{i+1}/{total}] {parent_name}: [DRY] would add {', '.join(added_names)}")
                stats["covers"] += 1
            elif ids_to_add:
                gql.add_images(gid, ids_to_add)
                log(f"  [{i+1}/{total}] {parent_name}: added {', '.join(added_names)}")
                stats["covers"] += 1
            else:
                log(f"  [{i+1}/{total}] {parent_name}: parent images not in Stash (run Scan)")
        else:
            log(f"  [{i+1}/{total}] {parent_name}: no parent images found")

        # Link to scenes
        linked_sids = {s["id"] for s in gallery.get("scenes", [])}
        for sc in slist:
            sid = sc["id"]
            if sid in linked_sids:
                continue
            if dry_run:
                log(f"    [DRY] would link to scene {sc.get('title') or sid}")
            else:
                eg = [g["id"] for g in sc.get("galleries", [])]
                gql.link_scene(sid, gid, eg)
                log(f"    linked to scene: {sc.get('title') or sid}")
                stats["linked"] += 1

    log_progress(1.0)
    log("=" * 40)
    log(f"Done! Created={stats['created']} Covers={stats['covers']} "
        f"Linked={stats['linked']} Skipped={stats['skipped']} Errors={stats['errors']}")
    if dry_run:
        log("(Dry run - no changes made)")

# --- Entry ---
def main():
    raw = sys.stdin.read()
    try:
        pi = json.loads(raw)
    except Exception:
        pi = {}
    srv = pi.get("server_connection", {})
    scheme = srv.get("Scheme", "http")
    host = srv.get("Host", "localhost")
    if host in ("0.0.0.0", ""):
        host = "localhost"
    port = srv.get("Port", 9999)
    api_key = srv.get("ApiKey", "")
    mode = pi.get("args", {}).get("mode", "link")
    dry_run = mode == "dry_run"
    log(f"Mode: {'DRY RUN' if dry_run else 'LIVE'}")
    g = GQL(scheme, host, port, api_key or None)
    process(g, dry_run)
    print(json.dumps({"output": "ok"}))

if __name__ == "__main__":
    main()
