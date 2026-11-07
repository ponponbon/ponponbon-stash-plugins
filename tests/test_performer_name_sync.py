import unittest

from _loader import load_plugin, RecordingStash

ns = load_plugin("performer_name_sync")

JAV = "https://javstash.org/graphql"
SDB = "https://stashdb.org/graphql"


def perf(pid, name, dis=None, aliases=None, sids=None, tags=None):
    return {"id": str(pid), "name": name, "disambiguation": dis, "alias_list": list(aliases or []),
            "stash_ids": [{"endpoint": e, "stash_id": s} for e, s in (sids or [])],
            "tags": [{"id": t, "name": t} for t in (tags or [])], "urls": []}


class RemovedPhasesTests(unittest.TestCase):
    def test_phase_one_and_two_are_gone(self):
        for name in ("find_and_merge_duplicates", "_merge_duplicate_group", "_reassign_content",
                     "_merge_performer_metadata", "_pick_keeper", "repair_unlinked_performers",
                     "process_unlinked_performer", "_destroy_unlinked_performer"):
            self.assertFalse(hasattr(ns, name), name)


class RenameCollisionTests(unittest.TestCase):
    def test_collision_with_other_performer(self):
        other = perf(2, "Yua Mikami")
        idx = ns.build_name_index([perf(1, "三上悠亜"), other])
        self.assertIs(ns.find_rename_collision("1", "yua  mikami", None, idx), other)

    def test_own_name_is_not_a_collision(self):
        idx = ns.build_name_index([perf(1, "yua mikami")])
        self.assertIsNone(ns.find_rename_collision("1", "Yua Mikami", None, idx))

    def test_distinct_disambiguations_are_not_a_collision(self):
        idx = ns.build_name_index([perf(2, "Yui", dis="gravure")])
        self.assertIsNone(ns.find_rename_collision("1", "Yui", "AV", idx))
        self.assertIsNotNone(ns.find_rename_collision("1", "Yui", None, idx))

    def test_reindex_tracks_planned_renames(self):
        a, b = perf(1, "あ"), perf(2, "い")
        idx = ns.build_name_index([a, b])
        ns.reindex_rename(idx, a, "Anna")
        self.assertIs(ns.find_rename_collision("2", "anna", None, idx), a)
        self.assertEqual(idx[ns.normalize_name("あ")], [])


class SearchStashdbTests(unittest.TestCase):
    def setUp(self):
        self.orig = ns.graphql_request

    def tearDown(self):
        ns.graphql_request = self.orig

    def test_ambiguous_exact_match_is_not_linked(self):
        ns.graphql_request = lambda *a, **k: {"searchPerformer": [
            {"id": "1", "name": "Yui", "aliases": []},
            {"id": "2", "name": "Yui Hatano", "aliases": ["Yui"]},
        ]}
        self.assertEqual(ns.search_stashdb("Yui", SDB, ""), (None, None))

    def test_unique_match_is_linked(self):
        ns.graphql_request = lambda *a, **k: {"searchPerformer": [
            {"id": "1", "name": "Yui", "aliases": []},
            {"id": "2", "name": "Yui Hatano", "aliases": []},
        ]}
        self.assertEqual(ns.search_stashdb("Yui", SDB, ""), ("Yui", "1"))


class ProcessTests(unittest.TestCase):
    """process() with stash-box calls mocked: one performer wants a name that
    another local performer already has."""

    def setUp(self):
        self.orig = (ns.fetch_performer, ns.search_stashbox, ns.fetch_performer_full)
        ns.fetch_performer = lambda pid, ep, key: {"id": pid, "name": "三上悠亜",
                                                   "aliases": ["Yua Mikami"]}
        ns.search_stashbox = lambda *a, **k: (None, None)
        ns.fetch_performer_full = lambda *a, **k: None

    def tearDown(self):
        ns.fetch_performer, ns.search_stashbox, ns.fetch_performer_full = self.orig

    def stash(self, performers, tags=()):
        s = RecordingStash([
            ("stashBoxes", {"configuration": {"general": {"stashBoxes": [
                {"endpoint": JAV, "api_key": "", "name": "JavStash"},
                {"endpoint": SDB, "api_key": "", "name": "StashDB"}]}}}),
            ("findPerformers", {"findPerformers": {"performers": performers}}),
            ("findTags", {"findTags": {"tags": [{"id": t, "name": ns.REVIEW_TAG_NAME} for t in tags]}}),
            ("tagCreate", {"tagCreate": {"id": "77", "name": ns.REVIEW_TAG_NAME}}),
        ])
        s.updates = []
        s.update_performer = lambda payload: s.updates.append(payload)
        return s

    def test_collision_skips_rename_and_tags_both(self):
        target = perf(1, "三上悠亜", sids=[(JAV, "j1")], tags=["5"])
        other = perf(2, "Yua Mikami")
        stash = self.stash([target, other])
        stats = ns.process(stash, dry_run=False)
        self.assertEqual(stats["rename_collisions"], 1)
        # the rename was skipped: no payload renames performer 1
        for payload in stash.updates:
            self.assertEqual(payload["name"], "三上悠亜")
        tag_writes = {m[1]["input"]["id"]: m[1]["input"]["tag_ids"]
                      for m in stash.mutations if "performerUpdate" in m[0]}
        self.assertEqual(tag_writes, {"1": ["5", "77"], "2": ["77"]})

    def test_preview_writes_nothing(self):
        target = perf(1, "三上悠亜", sids=[(JAV, "j1")])
        other = perf(2, "Yua Mikami")
        stash = self.stash([target, other])
        ns.process(stash, dry_run=True)
        self.assertEqual(stash.mutations, [])
        self.assertEqual(stash.updates, [])

    def test_rename_without_collision(self):
        target = perf(1, "三上悠亜", sids=[(JAV, "j1")])
        stash = self.stash([target, perf(2, "Someone Else")])
        stats = ns.process(stash, dry_run=False)
        self.assertEqual(stats["rename_collisions"], 0)
        self.assertEqual(stash.updates[0]["name"], "Yua Mikami")
        self.assertIn("三上悠亜", stash.updates[0]["alias_list"])


if __name__ == "__main__":
    unittest.main()
