"""Тесты beats_db — pure функции парсинга и in-memory операций."""
import beats_db


class TestParseTagsFromText:
    def test_simple(self):
        assert beats_db.parse_tags_from_text("#hard #memphis") == ["hard", "memphis"]

    def test_mixed_with_text(self):
        assert beats_db.parse_tags_from_text("bit #memphis drop #hard 808") == ["memphis", "hard"]

    def test_empty(self):
        assert beats_db.parse_tags_from_text("") == []

    def test_none(self):
        assert beats_db.parse_tags_from_text(None) == []

    def test_no_hashtags(self):
        assert beats_db.parse_tags_from_text("просто текст без тегов") == []

    def test_lowercased(self):
        assert beats_db.parse_tags_from_text("#HARD #Memphis") == ["hard", "memphis"]


class TestParseBpmFromText:
    def test_with_space(self):
        assert beats_db.parse_bpm_from_text("140 bpm hard beat") == 140

    def test_no_space(self):
        assert beats_db.parse_bpm_from_text("140bpm") == 140

    def test_three_digit(self):
        assert beats_db.parse_bpm_from_text("track at 160 BPM memphis") == 160

    def test_none_if_absent(self):
        assert beats_db.parse_bpm_from_text("track") is None

    def test_empty(self):
        assert beats_db.parse_bpm_from_text("") is None
        assert beats_db.parse_bpm_from_text(None) is None


class TestParseKeyFromText:
    def test_minor(self):
        assert beats_db.parse_key_from_text("in Am minor")

    def test_sharp_minor(self):
        result = beats_db.parse_key_from_text("C#m key")
        assert result and result.startswith("C")

    def test_none_if_absent(self):
        assert beats_db.parse_key_from_text("") is None
        assert beats_db.parse_key_from_text(None) is None


class TestInMemoryOps:
    def setup_method(self):
        # Изолируем BEATS_CACHE — тесты не должны влиять на реальные данные.
        self._saved_cache = beats_db.BEATS_CACHE.copy()
        self._saved_index = beats_db.BEATS_BY_ID.copy()
        beats_db.BEATS_CACHE.clear()
        beats_db.BEATS_BY_ID.clear()

    def teardown_method(self):
        beats_db.BEATS_CACHE.clear()
        beats_db.BEATS_CACHE.extend(self._saved_cache)
        beats_db.BEATS_BY_ID.clear()
        beats_db.BEATS_BY_ID.update(self._saved_index)

    def test_get_beat_by_id_none(self):
        assert beats_db.get_beat_by_id(999) is None

    def test_get_beat_by_id_after_index_rebuild(self):
        beats_db.BEATS_CACHE.append({"id": 42, "name": "X", "tags": []})
        beats_db._rebuild_index()
        assert beats_db.get_beat_by_id(42)["name"] == "X"

    def test_get_random_beat_empty(self):
        assert beats_db.get_random_beat() is None

    def test_get_beats_by_tag(self):
        beats_db.BEATS_CACHE.extend([
            {"id": 1, "name": "a", "tags": ["memphis", "hard"]},
            {"id": 2, "name": "b", "tags": ["detroit"]},
            {"id": 3, "name": "c", "tags": ["memphis"]},
        ])
        beats_db._rebuild_index()
        hits = beats_db.get_beats_by_tag("memphis")
        assert len(hits) == 2
        assert {b["id"] for b in hits} == {1, 3}

    def test_get_all_tags_sorted_unique(self):
        beats_db.BEATS_CACHE.extend([
            {"id": 1, "tags": ["hard", "memphis"]},
            {"id": 2, "tags": ["memphis", "detroit"]},
        ])
        assert beats_db.get_all_tags() == ["detroit", "hard", "memphis"]

    def test_similar_by_tags(self):
        cur = {"id": 1, "tags": ["memphis", "hard"], "bpm": 140, "content_type": "beat"}
        beats_db.BEATS_CACHE.extend([
            cur,
            {"id": 2, "tags": ["memphis"], "bpm": 150, "content_type": "beat"},
            {"id": 3, "tags": ["detroit"], "bpm": 145, "content_type": "beat"},
        ])
        beats_db._rebuild_index()
        result = beats_db.get_similar_beats(cur)
        assert any(b["id"] == 2 for b in result)
