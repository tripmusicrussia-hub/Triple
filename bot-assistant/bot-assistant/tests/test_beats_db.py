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


class TestPersistenceAndRecovery:
    """Тесты 3-уровневого recovery в load_beats + atomic save."""

    def setup_method(self, tmp_path=None):
        import tempfile
        self._saved_cache = beats_db.BEATS_CACHE.copy()
        self._saved_index = beats_db.BEATS_BY_ID.copy()
        self._saved_path = beats_db.BEATS_FILE
        # Используем временный путь чтоб не трогать реальные данные
        self._tmp_dir = tempfile.mkdtemp()
        beats_db.BEATS_FILE = f"{self._tmp_dir}/test_beats.json"

    def teardown_method(self):
        import shutil
        beats_db.BEATS_FILE = self._saved_path
        beats_db.BEATS_CACHE.clear()
        beats_db.BEATS_CACHE.extend(self._saved_cache)
        beats_db.BEATS_BY_ID.clear()
        beats_db.BEATS_BY_ID.update(self._saved_index)
        shutil.rmtree(self._tmp_dir, ignore_errors=True)

    def test_save_round_trip(self):
        beats_db.BEATS_CACHE.extend([{"id": 1, "name": "a", "tags": []}])
        beats_db._rebuild_index()
        beats_db.save_beats()

        # Симулируем рестарт
        beats_db.BEATS_CACHE.clear()
        beats_db.BEATS_BY_ID.clear()
        beats_db.load_beats()
        assert len(beats_db.BEATS_CACHE) == 1
        assert beats_db.get_beat_by_id(1)["name"] == "a"

    def test_save_creates_bak_after_second_save(self):
        # Первый save: .bak ещё не создаётся (target не существовал до)
        beats_db.BEATS_CACHE.append({"id": 1, "name": "v1", "tags": []})
        beats_db.save_beats()

        # Второй save: теперь target существует → .bak должен быть создан
        beats_db.BEATS_CACHE.append({"id": 2, "name": "v2", "tags": []})
        beats_db.save_beats()

        import os
        bak = beats_db.BEATS_FILE + ".bak"
        assert os.path.exists(bak), f"{bak} not created"
        import json
        with open(bak, encoding="utf-8") as f:
            bak_data = json.load(f)
        # .bak содержит состояние ДО второго save — только 1 запись
        assert len(bak_data) == 1
        assert bak_data[0]["name"] == "v1"

    def test_recovery_from_bak_when_main_corrupted(self):
        # Сначала создаём валидный каталог + его .bak
        beats_db.BEATS_CACHE.extend([{"id": 1, "name": "a", "tags": []}])
        beats_db.save_beats()
        beats_db.BEATS_CACHE.append({"id": 2, "name": "b", "tags": []})
        beats_db.save_beats()  # теперь target и .bak оба валидны

        # Портим main-файл
        with open(beats_db.BEATS_FILE, "w", encoding="utf-8") as f:
            f.write("{broken json}")

        # load_beats должен восстановиться из .bak
        beats_db.BEATS_CACHE.clear()
        beats_db.BEATS_BY_ID.clear()
        beats_db.load_beats()
        assert len(beats_db.BEATS_CACHE) >= 1  # из .bak (может быть 1 или 2 в зависимости от timing)

    def test_both_corrupted_empty_cache_synced_index(self):
        import os
        # Битый main + битый .bak + git-checkout недоступен (tmp не git)
        with open(beats_db.BEATS_FILE, "w") as f:
            f.write("garbage")
        with open(beats_db.BEATS_FILE + ".bak", "w") as f:
            f.write("also garbage")

        beats_db.BEATS_CACHE.append({"id": 999, "name": "stale"})
        beats_db.BEATS_BY_ID[999] = {"id": 999}
        beats_db.load_beats()

        assert beats_db.BEATS_CACHE == []
        assert beats_db.get_beat_by_id(999) is None, "stale index not cleared"

    def test_try_load_rejects_non_list(self):
        import json
        with open(beats_db.BEATS_FILE, "w", encoding="utf-8") as f:
            json.dump({"not": "a list"}, f)
        assert beats_db._try_load_file(beats_db.BEATS_FILE) is False

    def test_try_load_returns_false_for_missing(self):
        assert beats_db._try_load_file("/nonexistent/path") is False
