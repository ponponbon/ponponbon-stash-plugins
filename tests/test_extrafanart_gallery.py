import os
import tempfile
import unittest

from _loader import load_plugin

eg = load_plugin("extrafanart_gallery")


def img(iid, path, gids=()):
    return {"id": str(iid), "files": [{"path": path}], "galleries": [{"id": g} for g in gids]}


class PlanTests(unittest.TestCase):
    def setUp(self):
        self.parent = os.path.join("lib", "ABC-123 - (2024)")
        self.folder = os.path.join(self.parent, "folder.jpg")
        self.poster = os.path.join(self.parent, "poster.jpg")
        self.fanart = os.path.join(self.parent, "fanart.jpg")

    def test_new_extrafanart_images_added_to_existing_gallery(self):
        gallery = {"id": "9", "title": "ABC-123", "scenes": [{"id": "1"}], "cover": {"id": "50"}}
        ef = [img(1, "a.jpg", ["9"]), img(2, "b.jpg")]
        plan = eg.plan_gallery_actions(gallery, ef, [], {}, [{"id": "1", "galleries": [{"id": "9"}]}])
        self.assertEqual(plan["add_ids"], ["2"])
        self.assertEqual(plan["link_scenes"], [])
        self.assertIsNone(plan["cover_id"])

    def test_cover_is_first_cover_candidate_not_fanart(self):
        pmap = {eg.path_key(self.fanart): img(70, self.fanart),
                eg.path_key(self.poster): img(60, self.poster)}
        gallery = {"id": "9", "scenes": []}
        plan = eg.plan_gallery_actions(gallery, [], [self.folder, self.poster, self.fanart], pmap, [],
                                       just_created=True)
        # folder.jpg is not in Stash, so poster.jpg is the first candidate found
        self.assertEqual(plan["cover_id"], "60")
        self.assertEqual(sorted(plan["add_ids"]), ["60", "70"])

    def test_cover_not_overridden_on_existing_gallery_without_refresh(self):
        pmap = {eg.path_key(self.folder): img(60, self.folder, ["9"])}
        gallery = {"id": "9", "scenes": [], "cover": {"id": "1"}}
        plan = eg.plan_gallery_actions(gallery, [], [self.folder], pmap, [])
        self.assertIsNone(plan["cover_id"])
        plan = eg.plan_gallery_actions(gallery, [], [self.folder], pmap, [], refresh_existing=True)
        self.assertEqual(plan["cover_id"], "60")

    def test_cover_set_when_cover_image_newly_added(self):
        pmap = {eg.path_key(self.folder): img(60, self.folder)}
        gallery = {"id": "9", "scenes": [], "cover": {"id": "1"}}
        plan = eg.plan_gallery_actions(gallery, [], [self.folder], pmap, [])
        self.assertEqual((plan["add_ids"], plan["cover_id"]), (["60"], "60"))

    def test_nothing_to_do(self):
        pmap = {eg.path_key(self.folder): img(60, self.folder, ["9"])}
        gallery = {"id": "9", "scenes": [{"id": "1"}], "cover": {"id": "60"}}
        plan = eg.plan_gallery_actions(gallery, [img(1, "a.jpg", ["9"])], [self.folder], pmap,
                                       [{"id": "1", "galleries": []}], refresh_existing=True)
        self.assertEqual((plan["add_ids"], plan["cover_id"], plan["link_scenes"]), ([], None, []))


class FakeGQL:
    def __init__(self, scenes, galleries, ef_images, parent_images):
        self.scenes, self.galleries = scenes, galleries
        self.ef_images, self.parent_images = ef_images, parent_images
        self.writes = []
        self.gallery_cache = {}

    def all_scenes(self):
        return self.scenes

    def find_galleries_for_paths(self, paths):
        return self.galleries

    def find_images_for_dirs(self, paths, frag):
        return self.ef_images

    def find_images_for_exact_paths(self, paths):
        return self.parent_images

    def find_gallery(self, gid):
        return {"id": gid, "title": "ABC-123", "scenes": [{"id": "1"}], "cover": {"id": "60"}}

    def create_gallery(self, title, scene_ids):
        self.writes.append(("create", title))
        return "100"

    def add_images(self, gid, ids):
        self.writes.append(("add", gid, sorted(ids)))

    def set_cover(self, gid, iid):
        self.writes.append(("cover", gid, iid))

    def update_gallery_title(self, gid, title):
        self.writes.append(("title", gid, title))

    def link_scene(self, sid, gid, existing):
        self.writes.append(("link", sid, gid))


class ProcessTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.parent = os.path.join(self.tmp.name, "ABC-123 - (2024)")
        self.ef = os.path.join(self.parent, "extrafanart")
        os.makedirs(self.ef)
        for name in ("folder.jpg", "fanart.jpg"):
            open(os.path.join(self.parent, name), "w").close()
        self.scene = {"id": "1", "title": "ABC-123",
                      "files": [{"path": os.path.join(self.parent, "ABC-123.mp4")}],
                      "galleries": [{"id": "9"}]}
        self.folder = os.path.join(self.parent, "folder.jpg")
        self.fanart = os.path.join(self.parent, "fanart.jpg")

    def tearDown(self):
        self.tmp.cleanup()

    def existing_linked(self):
        """API-created gallery #9 already linked, with a new image #3 on disk."""
        ef_imgs = [img(1, os.path.join(self.ef, "1.jpg"), ["9"]),
                   img(3, os.path.join(self.ef, "3.jpg"))]
        parents = {eg.path_key(self.folder): img(60, self.folder, ["9"]),
                   eg.path_key(self.fanart): img(70, self.fanart, ["9"])}
        return FakeGQL([self.scene], {}, {eg.path_key(self.ef): ef_imgs}, parents)

    def test_existing_linked_gallery_picks_up_new_images(self):
        gql = self.existing_linked()
        eg.process(gql, dry_run=False, refresh_existing=False)
        self.assertEqual(gql.writes, [("add", "9", ["3"])])

    def test_folder_gallery_also_gets_missing_images(self):
        ef_imgs = [img(1, os.path.join(self.ef, "1.jpg"), ["9"]), img(3, os.path.join(self.ef, "3.jpg"))]
        folder_gallery = {"id": "9", "title": "ABC-123", "scenes": [{"id": "1"}], "cover": {"id": "60"},
                          "folder": {"path": self.ef}}
        gql = FakeGQL([self.scene], {eg.path_key(self.ef): folder_gallery},
                      {eg.path_key(self.ef): ef_imgs}, {})
        eg.process(gql, dry_run=False)
        self.assertEqual(gql.writes, [("add", "9", ["3"])])

    def test_new_gallery_gets_cover(self):
        ef_imgs = [img(1, os.path.join(self.ef, "1.jpg"))]
        parents = {eg.path_key(self.folder): img(60, self.folder),
                   eg.path_key(self.fanart): img(70, self.fanart)}
        scene = dict(self.scene, galleries=[])
        gql = FakeGQL([scene], {}, {eg.path_key(self.ef): ef_imgs}, parents)
        eg.process(gql, dry_run=False)
        self.assertEqual(gql.writes, [("create", "ABC-123"), ("add", "100", ["1"]),
                                      ("add", "100", ["60", "70"]), ("cover", "100", "60")])

    def test_preview_writes_nothing(self):
        gql = self.existing_linked()
        eg.process(gql, dry_run=True)
        self.assertEqual(gql.writes, [])
        ef_imgs = [img(1, os.path.join(self.ef, "1.jpg"))]
        gql = FakeGQL([dict(self.scene, galleries=[])], {}, {eg.path_key(self.ef): ef_imgs},
                      {eg.path_key(self.folder): img(60, self.folder)})
        eg.process(gql, dry_run=True)
        self.assertEqual(gql.writes, [])

    def test_rerun_is_noop(self):
        ef_imgs = [img(1, os.path.join(self.ef, "1.jpg"), ["9"])]
        parents = {eg.path_key(self.folder): img(60, self.folder, ["9"])}
        gql = FakeGQL([self.scene], {}, {eg.path_key(self.ef): ef_imgs}, parents)
        stats = eg.process(gql, dry_run=False, refresh_existing=True)
        self.assertEqual(gql.writes, [])
        self.assertEqual(stats["skipped"], 1)


if __name__ == "__main__":
    unittest.main()
