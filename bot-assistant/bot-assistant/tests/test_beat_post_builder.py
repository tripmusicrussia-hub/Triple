"""Тесты YT/TG текст-билдеров."""
from beat_upload import parse_filename
from beat_post_builder import (
    build_yt_title, build_shorts_title, build_shorts_tags,
    build_tiktok_caption,
    _hashtag_nav, _fmt_ts,
)


def _beat():
    return parse_filename("kenny muney type beat HEAT 160 Am.mp3")


class TestYtTitle:
    def test_format_matches_rule_r1(self):
        title = build_yt_title(_beat())
        # Формат: (FREE) Artist Type Beat YEAR - "NAME"
        assert title.startswith("(FREE)")
        assert "Kenny Muney" in title
        assert "Type Beat" in title
        assert '"HEAT"' in title
        assert " - " in title  # тире с пробелами

    def test_no_emoji(self):
        # Rule R4 — ноль эмодзи в title
        title = build_yt_title(_beat())
        for ch in title:
            assert ord(ch) < 0x2600 or ord(ch) > 0x27BF, f"emoji found: {ch!r}"


class TestShortsTitle:
    def test_contains_shorts_tag(self):
        title = build_shorts_title(_beat())
        assert "#Shorts" in title

    def test_within_yt_limit(self):
        title = build_shorts_title(_beat())
        assert len(title) <= 100  # YT title limit

    def test_long_name_gets_truncated(self):
        # Искусственно длинное название → должно обрезаться до 95 симв
        from beat_upload import BeatMeta
        b = BeatMeta(
            artist_raw="artist", artist_display="Very Long Artist Name Here",
            artist_line="X", name="VERY LONG BEAT NAME WITH MANY WORDS",
            bpm=140, key="A minor", key_short="Am",
        )
        title = build_shorts_title(b)
        assert len(title) <= 100
        assert "#Shorts" in title


class TestShortsTags:
    def test_first_is_shorts(self):
        tags = build_shorts_tags(_beat())
        assert tags[0] == "shorts"

    def test_max_15(self):
        tags = build_shorts_tags(_beat())
        assert len(tags) <= 15

    def test_unique(self):
        tags = build_shorts_tags(_beat())
        assert len(set(tags)) == len(tags)


class TestTiktokCaption:
    def test_contains_fyp_tag(self):
        # #fyp обязателен для TikTok discovery
        caption = build_tiktok_caption(_beat())
        assert "#fyp" in caption

    def test_contains_foryou_tag(self):
        caption = build_tiktok_caption(_beat())
        assert "#foryou" in caption

    def test_contains_beat_name(self):
        caption = build_tiktok_caption(_beat())
        assert "HEAT" in caption

    def test_contains_artist_slug(self):
        caption = build_tiktok_caption(_beat())
        assert "#kennymuneytypebeat" in caption

    def test_contains_bpm_bucket(self):
        caption = build_tiktok_caption(_beat())
        assert "#bpm160" in caption

    def test_within_length_limit(self):
        # Не более 500 chars — TikTok показывает ~150 в первом экране
        caption = build_tiktok_caption(_beat())
        assert len(caption) <= 500

    def test_hashtags_unique(self):
        caption = build_tiktok_caption(_beat())
        tags = [w for w in caption.split() if w.startswith("#")]
        assert len(set(tags)) == len(tags), f"дубликаты в {tags}"


class TestHashtagNav:
    def test_produces_4_tags(self):
        nav = _hashtag_nav(_beat())
        tags = nav.split()
        assert len(tags) == 4
        assert "#typebeat" in tags
        assert "#bpm160" in tags

    def test_contains_artist_and_scene(self):
        nav = _hashtag_nav(_beat())
        assert "#kennymuney" in nav
        assert "#memphis" in nav


class TestFmtTs:
    def test_zero(self):
        assert _fmt_ts(0) == "0:00"

    def test_round_minute(self):
        assert _fmt_ts(60) == "1:00"

    def test_mixed(self):
        assert _fmt_ts(196.4) == "3:16"

    def test_long(self):
        assert _fmt_ts(3725) == "62:05"
