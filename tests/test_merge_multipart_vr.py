import unittest

from _loader import load_plugin

mm = load_plugin("merge_multipart_vr")


def scene(sid, path, title="", tags=None, groups=None):
    basename = path.replace("\\", "/").rsplit("/", 1)[-1]
    return {
        "id": str(sid), "title": title,
        "files": [{"id": f"f{sid}", "path": path, "basename": basename}],
        "tags": [{"id": t, "name": t} for t in (tags or [])],
        "groups": groups or [],
    }


class PartParsingTests(unittest.TestCase):
    def test_numeric_tokens(self):
        self.assertEqual(mm.normalize_basename("Movie pt1.mp4"), ("Movie", 1))
        self.assertEqual(mm.normalize_basename("Movie_part-02.mkv"), ("Movie", 2))
        self.assertEqual(mm.normalize_basename("Movie.cd2.mp4"), ("Movie", 2))

    def test_roman_numerals(self):
        self.assertEqual(mm.normalize_basename("Movie disc III.mp4"), ("Movie", 3))
        self.assertEqual(mm.normalize_basename("Movie Part IV.mp4"), ("Movie", 4))

    def test_ab_suffix(self):
        self.assertEqual(mm.normalize_basename("ABC-123-A.mp4"), ("ABC-123", 1))
        self.assertEqual(mm.normalize_basename("ABC-123 B.mp4"), ("ABC-123", 2))

    def test_no_part(self):
        self.assertEqual(mm.normalize_basename("Discount Deals.mp4"), ("Discount Deals", None))
        self.assertEqual(mm.normalize_basename("Partners.mp4"), ("Partners", None))

    def test_clean_title(self):
        self.assertEqual(mm.clean_title("Great Scene Part 1"), "Great Scene")
        self.assertEqual(mm.clean_title("Great Scene - pt.2"), "Great Scene")
        self.assertEqual(mm.clean_title("Disc Jockey Night"), "Disc Jockey Night")
        self.assertEqual(mm.clean_title("Discount"), "Discount")
        self.assertEqual(mm.clean_title("cd1"), "cd1")  # never returns empty


class DetectGroupsTests(unittest.TestCase):
    def test_groups_by_dir_and_base_and_sorts(self):
        scenes = [
            scene(2, "/lib/a/Movie pt2.mp4"),
            scene(1, "/lib/a/Movie pt1.mp4"),
            scene(3, "/lib/b/Movie pt1.mp4"),  # other dir, alone -> ignored
            scene(4, "/lib/a/Other.mp4"),
        ]
        valid, skipped = mm.detect_groups(scenes)
        self.assertEqual(len(valid), 1)
        self.assertEqual(skipped, [])
        self.assertEqual([it["part"] for it in valid[0]["items"]], [1, 2])
        self.assertEqual(valid[0]["base"], "Movie")

    def test_duplicate_part_numbers_skipped(self):
        scenes = [
            scene(1, "/lib/a/Movie pt1.mp4"),
            scene(2, "/lib/a/Movie part1.mkv"),
            scene(3, "/lib/a/Movie pt2.mp4"),
        ]
        valid, skipped = mm.detect_groups(scenes)
        self.assertEqual(valid, [])
        self.assertEqual(len(skipped), 1)

    def test_vr_only(self):
        scenes = [
            scene(1, "/lib/a/Movie pt1.mp4", tags=["9"]),
            scene(2, "/lib/a/Movie pt2.mp4"),
        ]
        valid, _ = mm.detect_groups(scenes, vr_only=True, vr_tag_id="9")
        self.assertEqual(valid, [])
        valid, _ = mm.detect_groups(scenes, vr_only=False, vr_tag_id="9")
        self.assertEqual(len(valid), 1)

    def test_group_title(self):
        items = [{"scene": scene(1, "/x/M pt1.mp4", title="My Film Part 1"), "part": 1}]
        self.assertEqual(mm.group_title_for(items, "M"), "My Film")
        items = [{"scene": scene(1, "/x/M pt1.mp4", title=""), "part": 1}]
        self.assertEqual(mm.group_title_for(items, "My_File"), "My_File")


class GroupIdempotencyTests(unittest.TestCase):
    def test_reuses_group_scene_already_belongs_to(self):
        gid = mm.select_existing_group(
            "Movie", ["1", "2"],
            scene_groups=[{"id": "7", "name": "movie"}, {"id": "3", "name": "Unrelated"}],
            name_matches=[{"id": "5", "name": "Movie", "scenes": []}],
        )
        self.assertEqual(gid, "7")

    def test_reuses_empty_or_subset_named_group(self):
        gid = mm.select_existing_group(
            "Movie", ["1", "2"], scene_groups=[],
            name_matches=[{"id": "12", "name": "Movie", "scenes": [{"id": "1"}]},
                          {"id": "11", "name": "Movie", "scenes": [{"id": "99"}]}],
        )
        self.assertEqual(gid, "12")

    def test_does_not_reuse_group_with_unrelated_scenes(self):
        gid = mm.select_existing_group(
            "Movie", ["1", "2"], scene_groups=[],
            name_matches=[{"id": "11", "name": "Movie", "scenes": [{"id": "99"}]}],
        )
        self.assertIsNone(gid)

    def test_plan_adds_membership_and_keeps_existing(self):
        sc = scene(1, "/a/M pt1.mp4", tags=["5"],
                   groups=[{"group": {"id": "3", "name": "Series"}, "scene_index": 4}])
        upd = mm.plan_scene_update(sc, "7", 1, ["mp"])
        self.assertEqual(upd["groups"], [{"group_id": "3", "scene_index": 4},
                                         {"group_id": "7", "scene_index": 1}])
        self.assertEqual(upd["tag_ids"], ["5", "mp"])

    def test_plan_is_noop_when_already_applied(self):
        sc = scene(1, "/a/M pt1.mp4", tags=["5", "mp"],
                   groups=[{"group": {"id": "7", "name": "M"}, "scene_index": 1}])
        self.assertIsNone(mm.plan_scene_update(sc, "7", 1, ["mp"]))

    def test_plan_fixes_index_without_duplicating(self):
        sc = scene(1, "/a/M pt2.mp4", tags=["mp"],
                   groups=[{"group": {"id": "7", "name": "M"}, "scene_index": None}])
        upd = mm.plan_scene_update(sc, "7", 2, ["mp"])
        self.assertEqual(upd["groups"], [{"group_id": "7", "scene_index": 2}])

    def test_rerun_round_trip_is_idempotent(self):
        sc = scene(1, "/a/M pt1.mp4", tags=["5"])
        upd = mm.plan_scene_update(sc, "7", 1, ["mp"])
        # Simulate Stash applying the update, then re-plan.
        sc["tags"] = [{"id": t} for t in upd["tag_ids"]]
        sc["groups"] = [{"group": {"id": g["group_id"]}, "scene_index": g.get("scene_index")}
                        for g in upd["groups"]]
        self.assertIsNone(mm.plan_scene_update(sc, "7", 1, ["mp"]))


class VrTagTests(unittest.TestCase):
    def test_vr_tag_not_added_by_default(self):
        mm.TAG_AS_VR = False
        sc = scene(1, "/a/M pt1.mp4")
        add = ["mp"] + (["vr"] if mm.TAG_AS_VR else [])
        upd = mm.plan_scene_update(sc, "7", 1, add)
        self.assertNotIn("vr", upd["tag_ids"])

    def test_existing_vr_tag_kept(self):
        sc = scene(1, "/a/M pt1.mp4", tags=["vr"])
        upd = mm.plan_scene_update(sc, "7", 1, ["mp"])
        self.assertIn("vr", upd["tag_ids"])


class SettingsTests(unittest.TestCase):
    def test_saved_settings_merged_and_args_win(self):
        pi = {"server_connection": {"Port": 9999}, "args": {"mode": "preview", "vr_tag": ""}}
        merged = mm.with_saved_settings(pi, {"tag_as_vr": True, "vr_tag": "VR180", "mode": "group"})
        self.assertEqual(merged["args"], {"tag_as_vr": True, "vr_tag": "VR180", "mode": "preview"})
        mm.configure(merged)
        self.assertTrue(mm.TAG_AS_VR)
        self.assertTrue(mm.DRY_RUN)
        self.assertEqual(mm.VR_TAG_NAME, "VR180")

    def test_defaults(self):
        mm.configure(mm.with_saved_settings({"args": {"mode": "group"}}, {}))
        self.assertFalse(mm.TAG_AS_VR)
        self.assertFalse(mm.DRY_RUN)
        self.assertEqual((mm.VR_TAG_NAME, mm.MULTIPART_TAG_NAME), ("VR", "Multipart"))


if __name__ == "__main__":
    unittest.main()
