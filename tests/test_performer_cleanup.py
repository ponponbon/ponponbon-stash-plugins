import copy
import unittest

from _loader import load_plugin, RecordingStash

pc = load_plugin("performer_cleanup")

JAV = "https://javstash.org/graphql"
SDB = "https://stashdb.org/graphql"


def perf(pid, name, aliases=None, sids=None, **extra):
    p = {"id": str(pid), "name": name, "alias_list": list(aliases or []),
         "stash_ids": [{"endpoint": ep, "stash_id": sid} for ep, sid in (sids or [])],
         "tags": [], "urls": [], "scene_count": 0, "image_count": 0, "gallery_count": 0}
    p.update(extra)
    return p


class VersionTests(unittest.TestCase):
    def test_parse(self):
        self.assertEqual(pc.parse_version("v0.31.0"), (0, 31, 0))
        self.assertEqual(pc.parse_version("v0.31.2-12-gabcdef"), (0, 31, 2))
        self.assertEqual(pc.parse_version("0.32"), (0, 32, 0))
        self.assertIsNone(pc.parse_version(""))
        self.assertIsNone(pc.parse_version(None))

    def test_old_server_rejected(self):
        stash = RecordingStash([("version", {"version": {"version": "v0.30.1"}})])
        with self.assertRaises(pc.StashVersionError) as ctx:
            pc.check_stash_version(stash)
        self.assertIn("v0.31.0", str(ctx.exception))

    def test_new_server_ok(self):
        stash = RecordingStash([("version", {"version": {"version": "v0.31.0"}})])
        self.assertEqual(pc.check_stash_version(stash), "v0.31.0")

    def test_dev_build_falls_back_to_schema(self):
        stash = RecordingStash([
            ("__type", {"__type": {"fields": [{"name": "performerMerge"}]}}),
            ("version", {"version": {"version": None}}),
        ])
        pc.check_stash_version(stash)
        stash = RecordingStash([
            ("__type", {"__type": {"fields": [{"name": "sceneMerge"}]}}),
            ("version", {"version": {"version": None}}),
        ])
        with self.assertRaises(pc.StashVersionError):
            pc.check_stash_version(stash)


class StashIdGroupingTests(unittest.TestCase):
    def test_groups_by_normalized_endpoint(self):
        a = perf(1, "A", sids=[(JAV, "x")])
        b = perf(2, "B", sids=[("https://JAVSTASH.org/graphql/", "x")])
        c = perf(3, "C", sids=[(JAV, "y")])
        groups = pc.group_by_stash_id([a, b, c])
        self.assertEqual(len(groups), 1)
        keys, members = groups[0]
        self.assertEqual([m["id"] for m in members], ["1", "2"])
        self.assertEqual(keys, [("https://javstash.org", "x")])

    def test_overlapping_groups_are_joined(self):
        a = perf(1, "A", sids=[(JAV, "x")])
        b = perf(2, "B", sids=[(JAV, "x"), (SDB, "s")])
        c = perf(3, "C", sids=[(SDB, "s")])
        groups = pc.group_by_stash_id([a, b, c])
        self.assertEqual(len(groups), 1)
        self.assertEqual([m["id"] for m in groups[0][1]], ["1", "2", "3"])

    def test_same_id_different_endpoint_not_grouped(self):
        a = perf(1, "A", sids=[(JAV, "x")])
        b = perf(2, "B", sids=[(SDB, "x")])
        self.assertEqual(pc.group_by_stash_id([a, b]), [])


class KeeperAndValuesTests(unittest.TestCase):
    def test_keeper_has_most_fields(self):
        a = perf(1, "A")
        b = perf(2, "B", country="JP", birthdate="1990-01-01")
        self.assertEqual(pc.pick_keeper([a, b])["id"], "2")

    def test_keeper_tie_breaks_on_scenes_then_oldest(self):
        a = perf(5, "A", scene_count=1)
        b = perf(3, "B", scene_count=4)
        self.assertEqual(pc.pick_keeper([a, b])["id"], "3")
        a = perf(5, "A")
        b = perf(3, "B")
        self.assertEqual(pc.pick_keeper([a, b])["id"], "3")

    def test_merge_values(self):
        keeper = perf(1, "Yua Mikami", aliases=["Mikami Yua", "yua mikami"], sids=[(JAV, "x")],
                      country="JP", urls=["https://a.example/"])
        other = perf(2, "三上悠亜", aliases=["Mikami Yua", "Yua"], sids=[(JAV, "x"), (SDB, "s")],
                     country="Japan", birthdate="1993-08-16", urls=["https://a.example", "https://b.example"],
                     disambiguation="idol")
        values = pc.build_merge_values(keeper, [other])
        self.assertEqual(values["id"], "1")
        # combined, deduplicated, without the keeper's own name
        self.assertEqual(values["alias_list"], ["Mikami Yua", "三上悠亜", "Yua"])
        self.assertEqual(values["stash_ids"], [{"endpoint": JAV, "stash_id": "x"},
                                               {"endpoint": SDB, "stash_id": "s"}])
        self.assertEqual(values["urls"], ["https://a.example/", "https://b.example"])
        # missing fields filled, existing ones left alone
        self.assertEqual(values["birthdate"], "1993-08-16")
        self.assertEqual(values["disambiguation"], "idol")
        self.assertNotIn("country", values)
        # performerMerge rejects legacy url fields
        self.assertNotIn("url", values)

    def test_disambiguation_not_copied_from_same_name_source(self):
        keeper = perf(1, "Yui", country="JP", sids=[(JAV, "x")])
        other = perf(2, "yui", disambiguation="JAV", sids=[(JAV, "x")])
        values = pc.build_merge_values(keeper, [other])
        self.assertNotIn("disambiguation", values)

    def test_merge_step_preview_makes_no_writes(self):
        stash = RecordingStash()
        a = perf(1, "A", sids=[(JAV, "x")])
        b = perf(2, "B", sids=[(JAV, "x")], country="JP")
        stats = pc.merge_sure_duplicates(stash, [a, b], dry_run=True)
        self.assertEqual(stats["merged"], 1)
        self.assertEqual(stash.mutations, [])

    def test_merge_step_live_calls_performer_merge(self):
        stash = RecordingStash([("performerMerge", {"performerMerge": {"id": "2", "name": "B"}})])
        a = perf(1, "A", sids=[(JAV, "x")])
        b = perf(2, "B", sids=[(JAV, "x")], country="JP")
        pc.merge_sure_duplicates(stash, [a, b], dry_run=False)
        self.assertEqual(len(stash.mutations), 1)
        inp = stash.mutations[0][1]["input"]
        self.assertEqual(inp["destination"], "2")
        self.assertEqual(inp["source"], ["1"])
        self.assertEqual(inp["values"]["alias_list"], ["A"])


class UniqueMatchTests(unittest.TestCase):
    def test_unique_name_match(self):
        status, hit = pc.unique_exact_match("yua mikami", [
            {"id": "1", "name": "Yua Mikami", "aliases": []},
            {"id": "2", "name": "Yua Mikamii", "aliases": []},
        ])
        self.assertEqual(status, "match")
        self.assertEqual(hit["id"], "1")

    def test_alias_match(self):
        status, hit = pc.unique_exact_match("Mikami Yua", [
            {"id": "1", "name": "Yua Mikami", "aliases": ["mikami yua"]},
        ])
        self.assertEqual((status, hit["id"]), ("match", "1"))

    def test_ambiguous(self):
        status, hits = pc.unique_exact_match("Yui", [
            {"id": "1", "name": "Yui", "aliases": []},
            {"id": "2", "name": "Someone", "aliases": ["YUI"]},
        ])
        self.assertEqual(status, "ambiguous")
        self.assertEqual(len(hits), 2)

    def test_same_performer_twice_is_unique(self):
        status, _ = pc.unique_exact_match("Yui", [
            {"id": "1", "name": "Yui", "aliases": ["yui"]},
            {"id": "1", "name": "Yui", "aliases": []},
        ])
        self.assertEqual(status, "match")

    def test_partial_is_not_a_match(self):
        self.assertEqual(pc.unique_exact_match("Yui", [{"id": "1", "name": "Yui Hatano"}]),
                         ("none", None))


class LinkUnlinkedTests(unittest.TestCase):
    def boxes(self):
        return [("JavStash", {"endpoint": JAV, "api_key": ""}),
                ("StashDB", {"endpoint": SDB, "api_key": ""})]

    def test_links_unique_match_and_skips_ambiguous(self):
        searches = []

        def search(term, box):
            searches.append((term, box["endpoint"]))
            if box["endpoint"] == JAV and term == "Yui":
                return [{"id": "j1", "name": "Yui"}, {"id": "j2", "name": "Yui", "aliases": []}]
            if box["endpoint"] == JAV and term == "Yui H":
                return [{"id": "j3", "name": "Yui Hatano", "aliases": ["Yui H"]}]
            return []

        stash = RecordingStash()
        p = perf(1, "Yui", aliases=["Yui H"])
        stats = pc.link_unlinked_performers(stash, [p], self.boxes(), search, dry_run=False)
        self.assertEqual(stats["ambiguous"], 1)
        self.assertEqual(stats["linked"], 1)
        self.assertEqual(searches, [("Yui", JAV), ("Yui H", JAV)])  # stops after JavStash match
        self.assertEqual(stash.mutations[0][1]["input"],
                         {"id": "1", "stash_ids": [{"endpoint": JAV, "stash_id": "j3"}]})

    def test_falls_back_to_stashdb(self):
        def search(term, box):
            return [{"id": "s1", "name": "Anna"}] if box["endpoint"] == SDB else []

        stash = RecordingStash()
        p = perf(1, "Anna")
        pc.link_unlinked_performers(stash, [p], self.boxes(), search, dry_run=False)
        self.assertEqual(p["stash_ids"], [{"endpoint": SDB, "stash_id": "s1"}])

    def test_existing_owner_gets_merge(self):
        def search(term, box):
            return [{"id": "j1", "name": "Anna"}] if box["endpoint"] == JAV else []

        owner = perf(5, "Anna Official", sids=[(JAV, "j1")])
        p = perf(1, "Anna", country="JP")
        stash = RecordingStash([("performerMerge", {"performerMerge": {"id": "5"}})])
        stats = pc.link_unlinked_performers(stash, [owner, p], self.boxes(), search, dry_run=False)
        self.assertEqual(stats["merged"], 1)
        inp = stash.mutations[0][1]["input"]
        self.assertEqual((inp["source"], inp["destination"]), (["1"], "5"))
        self.assertIn("Anna", inp["values"]["alias_list"])
        self.assertEqual(inp["values"]["country"], "JP")
        self.assertNotIn("name", inp["values"])  # never renames

    def test_preview_writes_nothing(self):
        def search(term, box):
            return [{"id": "j1", "name": "Anna"}]

        stash = RecordingStash()
        pc.link_unlinked_performers(stash, [perf(1, "Anna"), perf(2, "anna", disambiguation="b")],
                                    self.boxes(), search, dry_run=True)
        self.assertEqual(stash.mutations, [])

    def test_searcher_rate_limits_and_caches(self):
        sleeps = []
        calls = []
        orig = pc.graphql_request
        pc.graphql_request = lambda q, v, ep, key: calls.append(v) or {"searchPerformer": []}
        try:
            search = pc.make_stashbox_searcher(delay=1.5, sleep=sleeps.append)
            box = {"endpoint": JAV, "api_key": ""}
            search("Yui", box)
            search("yui", box)
            search("Anna", box)
        finally:
            pc.graphql_request = orig
        self.assertEqual(sleeps, [1.5, 1.5])
        self.assertEqual(len(calls), 2)


class DuplicatePairTests(unittest.TestCase):
    def ids(self, pairs):
        return sorted((a["id"], b["id"], r) for a, b, r in pairs)

    def test_same_normalized_name(self):
        pairs = pc.find_possible_duplicate_pairs([
            perf(1, "Yua  Mikami"), perf(2, "yua mikami", disambiguation="x"), perf(3, "Other")])
        self.assertEqual(self.ids(pairs), [("1", "2", "same name")])

    def test_name_equals_alias(self):
        pairs = pc.find_possible_duplicate_pairs([
            perf(1, "Mikami Yua"), perf(2, "Yua Mikami", aliases=["mikami yua"])])
        self.assertEqual(self.ids(pairs), [("1", "2", "name matches alias")])

    def test_different_disambiguations_excluded(self):
        pairs = pc.find_possible_duplicate_pairs([
            perf(1, "Yui", disambiguation="gravure"), perf(2, "Yui", disambiguation="AV")])
        self.assertEqual(pairs, [])

    def test_one_blank_disambiguation_still_flagged(self):
        pairs = pc.find_possible_duplicate_pairs([perf(1, "Yui"), perf(2, "Yui", disambiguation="AV")])
        self.assertEqual(len(pairs), 1)

    def test_conflicting_stash_ids_excluded(self):
        pairs = pc.find_possible_duplicate_pairs([
            perf(1, "Yui", sids=[(JAV, "a")]), perf(2, "Yui", sids=[(JAV + "/", "b")])])
        self.assertEqual(pairs, [])

    def test_ids_on_different_endpoints_not_a_conflict(self):
        pairs = pc.find_possible_duplicate_pairs([
            perf(1, "Yui", sids=[(JAV, "a")]), perf(2, "Yui", sids=[(SDB, "b")])])
        self.assertEqual(len(pairs), 1)

    def test_flag_keeps_existing_tags_and_preview_is_read_only(self):
        a = perf(1, "Yui", tags=[{"id": "7", "name": "Fav"}])
        b = perf(2, "yui", disambiguation="AV", tags=[{"id": "99", "name": "Review"}])
        stash = RecordingStash([("findTags", {"findTags": {"tags": [{"id": "99", "name": "Review"}]}})])
        pc.flag_possible_duplicates(stash, [a, b], "Review", dry_run=True)
        self.assertEqual(stash.mutations, [])
        pc.flag_possible_duplicates(stash, [a, b], "Review", dry_run=False)
        self.assertEqual(len(stash.mutations), 1)  # b already tagged
        self.assertEqual(stash.mutations[0][1]["input"], {"id": "1", "tag_ids": ["7", "99"]})

    def test_tag_not_created_in_preview(self):
        stash = RecordingStash([("findTags", {"findTags": {"tags": []}})])
        pc.flag_possible_duplicates(stash, [perf(1, "Yui"), perf(2, "Yui", disambiguation="x")],
                                    "Review: Possible Duplicate", dry_run=True)
        self.assertEqual(stash.mutations, [])


class OrphanTests(unittest.TestCase):
    def test_is_orphan(self):
        self.assertTrue(pc.is_orphan(perf(1, "A")))
        self.assertFalse(pc.is_orphan(perf(1, "A", scene_count=1)))
        self.assertFalse(pc.is_orphan(perf(1, "A", image_count=1)))
        self.assertFalse(pc.is_orphan(perf(1, "A", gallery_count=1)))
        self.assertFalse(pc.is_orphan(perf(1, "A", sids=[(JAV, "x")])))

    def test_delete_preview_and_live(self):
        stash = RecordingStash()
        pc.delete_orphans(stash, [perf(1, "A"), perf(2, "B", scene_count=3)], dry_run=True)
        self.assertEqual(stash.mutations, [])
        pc.delete_orphans(stash, [perf(1, "A"), perf(2, "B", scene_count=3)], dry_run=False)
        self.assertEqual([m[1] for m in stash.mutations], [{"input": {"id": "1"}}])


BOXES = [{"endpoint": SDB, "name": "StashDB"}, {"endpoint": JAV, "name": "JavStash"}]


class IdentifyDefaultsTests(unittest.TestCase):
    def current(self):
        return {
            "sources": [
                {"source": {"stash_box_index": None, "stash_box_endpoint": SDB, "scraper_id": None},
                 "options": None},
                {"source": {"stash_box_index": None, "stash_box_endpoint": None, "scraper_id": "r18"},
                 "options": {"fieldOptions": [{"field": "title", "strategy": "OVERWRITE", "createMissing": None}],
                             "setCoverImage": False, "setOrganized": None, "includeMalePerformers": None,
                             "performerGenders": None, "skipMultipleMatches": None,
                             "skipMultipleMatchTag": None, "skipSingleNamePerformers": True,
                             "skipSingleNamePerformerTag": None}},
                {"source": {"stash_box_index": None, "stash_box_endpoint": JAV + "/", "scraper_id": None},
                 "options": {"fieldOptions": [{"field": "performers", "strategy": "OVERWRITE", "createMissing": False}],
                             "setOrganized": True}},
            ],
            "options": {
                "fieldOptions": [{"field": "tags", "strategy": "MERGE", "createMissing": True}],
                "setCoverImage": True, "setOrganized": False, "includeMalePerformers": None,
                "performerGenders": ["FEMALE"], "skipMultipleMatches": True,
                "skipMultipleMatchTag": "Multi", "skipSingleNamePerformers": True,
                "skipSingleNamePerformerTag": "Single",
            },
        }

    def test_transformation(self):
        cur = self.current()
        before = copy.deepcopy(cur)
        new, changes = pc.transform_identify_defaults(cur, BOXES, BOXES[1])
        self.assertEqual(cur, before, "input must not be mutated")

        src = new["sources"]
        # JavStash moved first; the others keep their relative order
        self.assertEqual([s["source"].get("stash_box_endpoint") or s["source"].get("scraper_id") for s in src],
                         [JAV + "/", SDB, "r18"])
        jav, sdb, scraper = src
        self.assertEqual(jav["options"]["fieldOptions"],
                         [{"field": "performers", "strategy": "MERGE", "createMissing": True}])
        self.assertEqual(jav["options"]["setOrganized"], True)  # untouched
        self.assertEqual(sdb["options"]["fieldOptions"],
                         [{"field": "performers", "strategy": "MERGE", "createMissing": True}])
        self.assertEqual(scraper["options"]["fieldOptions"], [
            {"field": "title", "strategy": "OVERWRITE"},
            {"field": "performers", "strategy": "IGNORE", "createMissing": False}])
        self.assertEqual(scraper["options"]["setCoverImage"], False)
        self.assertEqual(scraper["options"]["skipSingleNamePerformers"], False)

        opts = new["options"]
        self.assertEqual(opts["skipSingleNamePerformers"], False)
        # unrelated global options preserved exactly
        expected = {k: v for k, v in before["options"].items() if v is not None}
        expected["skipSingleNamePerformers"] = False
        self.assertEqual(opts, expected)
        self.assertTrue(changes)

    def test_adds_javstash_when_missing(self):
        cur = {"sources": [{"source": {"stash_box_endpoint": SDB}, "options": None}], "options": None}
        new, changes = pc.transform_identify_defaults(cur, BOXES, BOXES[1])
        self.assertEqual(new["sources"][0]["source"], {"stash_box_endpoint": JAV})
        self.assertIn("added JavStash source at the front", changes)

    def test_index_based_source_recognised(self):
        cur = {"sources": [{"source": {"stash_box_endpoint": SDB}},
                           {"source": {"stash_box_index": 1}}], "options": {}}
        new, _ = pc.transform_identify_defaults(cur, BOXES, BOXES[1])
        self.assertEqual(new["sources"][0]["source"], {"stash_box_index": 1})
        self.assertEqual(len(new["sources"]), 2)

    def test_idempotent(self):
        new, _ = pc.transform_identify_defaults(self.current(), BOXES, BOXES[1])
        again, changes = pc.transform_identify_defaults(new, BOXES, BOXES[1])
        self.assertEqual(changes, [])
        self.assertEqual(again, new)

    def test_no_saved_defaults(self):
        new, changes = pc.transform_identify_defaults(None, BOXES, BOXES[1])
        self.assertEqual([s["source"]["stash_box_endpoint"] for s in new["sources"]], [JAV, SDB])
        self.assertEqual(new["options"], {"skipSingleNamePerformers": False})

    def test_preview_logs_and_does_not_save(self):
        stash = RecordingStash([("defaults", {"configuration": {"defaults": {"identify": self.current()}}})])
        stats = pc.configure_identify_defaults(stash, BOXES, BOXES[1], dry_run=True)
        self.assertGreater(stats["changes"], 0)
        self.assertEqual(stash.mutations, [])
        pc.configure_identify_defaults(stash, BOXES, BOXES[1], dry_run=False)
        self.assertEqual(len(stash.mutations), 1)
        self.assertIn("identify", stash.mutations[0][1]["input"])
        self.assertEqual(list(stash.mutations[0][1]["input"]), ["identify"])


class SettingsTests(unittest.TestCase):
    def test_defaults_and_overrides(self):
        stash = RecordingStash([("plugins", {"configuration": {"plugins": {}}})])
        s = pc.load_settings(stash, {"args": {}})
        self.assertEqual(s, {"delete_orphans": False, "set_identify_defaults": True,
                             "review_tag": "Review: Possible Duplicate"})
        stash = RecordingStash([("plugins", {"configuration": {"plugins": {"performer_cleanup": {
            "delete_orphans": True, "set_identify_defaults": False, "review_tag": "Dupe?"}}}})])
        s = pc.load_settings(stash, {"args": {}})
        self.assertEqual(s, {"delete_orphans": True, "set_identify_defaults": False, "review_tag": "Dupe?"})


class ProcessScenesTests(unittest.TestCase):
    def test_no_name_fallback_and_as_alias(self):
        scene = {"id": "10", "title": "S", "stash_ids": [{"endpoint": JAV, "stash_id": "sc1"}],
                 "performers": [{"id": "1", "name": "Anna", "stash_ids": []}]}
        sb_scene = {"performers": [{"as": "Annie", "performer": {"id": "p1", "name": "Anna"}}]}

        stash = RecordingStash([
            ("findScenes", {"findScenes": {"count": 1, "scenes": [scene]}}),
            ("findPerformers", {"findPerformers": {"performers": []}}),
            ("performerCreate", {"performerCreate": {"id": "50", "name": "Anna"}}),
        ])
        orig_fetch, orig_delay = pc.fetch_stashbox_scene, pc.STASHBOX_DELAY
        pc.fetch_stashbox_scene = lambda *a: sb_scene
        pc.STASHBOX_DELAY = 0
        try:
            configs = {pc.norm_endpoint(JAV): {"endpoint": JAV, "api_key": "", "name": "JavStash"}}
            preview = RecordingStash(stash.rules)
            pc.process_scenes(preview, configs, dry_run=True)
            self.assertEqual(preview.mutations, [])
            stats = pc.process_scenes(stash, configs, dry_run=False)
        finally:
            pc.fetch_stashbox_scene, pc.STASHBOX_DELAY = orig_fetch, orig_delay
        # Same-named local performer "Anna" (no stash id) is NOT reused
        self.assertEqual(stats["performers_created"], 1)
        create = [m for m in stash.mutations if "performerCreate" in m[0]][0][1]["input"]
        self.assertIn("Annie", create["alias_list"])
        self.assertNotIn("image", create)
        update = [m for m in stash.mutations if "sceneUpdate" in m[0]][0][1]["input"]
        self.assertEqual(update, {"id": "10", "performer_ids": ["1", "50"]})


if __name__ == "__main__":
    unittest.main()
