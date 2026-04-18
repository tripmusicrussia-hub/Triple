"""Тесты парсера имени mp3-файла в BeatMeta."""
import pytest

from beat_upload import parse_filename


class TestValidNames:
    def test_simple_artist(self):
        m = parse_filename("kenny muney type beat HEAT 160 Am.mp3")
        assert m.artist_raw == "kenny muney"
        assert m.artist_display == "Kenny Muney"
        assert m.name == "HEAT"
        assert m.bpm == 160
        assert m.key_short == "Am"
        assert "minor" in m.key

    def test_single_word_artist(self):
        m = parse_filename("future type beat HOOK 140 Am.mp3")
        assert m.artist_display == "Future"
        assert m.bpm == 140

    def test_collab_with_x(self):
        m = parse_filename("future x don toliver type beat HOOK 140 Am.mp3")
        assert "Future x Don Toliver" == m.artist_display
        assert m.name == "HOOK"

    def test_casing_override_nardowick(self):
        m = parse_filename("nardowick type beat FRIK 153 G#m.mp3")
        assert m.artist_display == "NardoWick"
        assert m.key_short == "G#m"

    def test_casing_override_key_glock(self):
        m = parse_filename("keyglock type beat X 140 Am.mp3")
        assert m.artist_display == "Key Glock"

    def test_multi_word_name(self):
        m = parse_filename("rob49 x bossman dlow type beat BIG FLIPPA 152 Dm.mp3")
        assert m.name == "BIG FLIPPA"
        assert m.bpm == 152
        assert m.key_short == "Dm"

    def test_major_key(self):
        m = parse_filename("artist type beat NAME 140 C.mp3")
        assert m.key_short == "C"
        assert "major" in m.key

    def test_sharp_key(self):
        m = parse_filename("artist type beat NAME 150 C#m.mp3")
        assert m.key_short == "C#m"

    def test_flat_key_b(self):
        # 'Bb' допустим — нота B-flat
        m = parse_filename("artist type beat NAME 140 Bbm.mp3")
        assert m.key_short.startswith("B")


class TestInvalidNames:
    def test_no_type_beat(self):
        with pytest.raises(ValueError, match="type beat"):
            parse_filename("random_name.mp3")

    def test_no_bpm_no_key(self):
        # Нет BPM + key в конце → парсер должен отказать
        with pytest.raises(ValueError):
            parse_filename("artist type beat NAME.mp3")
