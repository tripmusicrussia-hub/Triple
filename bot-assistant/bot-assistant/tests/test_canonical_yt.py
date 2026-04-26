"""Tests для canonical YT title/description (юзер-approved шаблон 2026-04-26).

Шаблон A:
  [FREE] {ARTIST} Type Beat {YEAR} - "{NAME}" | {SCENE} | {BPM} BPM {KEY}
"""
from __future__ import annotations

import pytest

import beat_post_builder as bpb
from beat_upload import BeatMeta


def _meta(
    *,
    artist_raw: str = "kenny muney",
    artist_display: str | None = None,
    name: str = "HEAT",
    bpm: int = 145,
    key: str = "A minor",
    key_short: str = "Am",
) -> BeatMeta:
    if artist_display is None:
        artist_display = artist_raw.title()
    return BeatMeta(
        artist_raw=artist_raw,
        artist_display=artist_display,
        artist_line=f"{artist_display} Type Beat",
        name=name,
        bpm=bpm,
        key=key,
        key_short=key_short,
    )


class TestCanonicalTitleBasic:
    def test_full_format_kenny_muney(self):
        m = _meta(artist_raw="kenny muney", artist_display="Kenny Muney",
                  name="HEAT", bpm=145, key_short="Am")
        title = bpb.canonical_yt_title(m)
        assert title == '[FREE] Kenny Muney Type Beat 2026 - "HEAT" | Memphis | 145 BPM Am'

    def test_full_format_obladaet_ru(self):
        m = _meta(artist_raw="obladaet", artist_display="Obladaet",
                  name="DARK", bpm=152, key_short="C#m")
        title = bpb.canonical_yt_title(m)
        # Obladaet → RU Hard override (не Detroit как в ARTIST_PROFILE)
        assert title == '[FREE] Obladaet Type Beat 2026 - "DARK" | RU Hard | 152 BPM C#m'

    def test_full_format_rob49_nola(self):
        m = _meta(artist_raw="rob49", artist_display="Rob49",
                  name="JUNGLE", bpm=162, key_short="Dm")
        title = bpb.canonical_yt_title(m)
        assert 'NOLA' in title
        assert title.startswith('[FREE] Rob49 Type Beat 2026 - "JUNGLE"')


class TestCanonicalTitleNameRule:
    def test_short_name_uppercased(self):
        m = _meta(name="heat")  # lowercase входит → должен стать HEAT
        assert '"HEAT"' in bpb.canonical_yt_title(m)

    def test_long_name_preserved_as_is(self):
        # >16 символов → не upper'им (выглядит как кричащая каша)
        long_name = "Some Long Beat Title For Test"
        m = _meta(name=long_name)
        assert f'"{long_name}"' in bpb.canonical_yt_title(m)

    def test_name_at_threshold_16_chars_upper(self):
        m = _meta(name="ExactlySixteen!!")  # 16 chars
        assert '"EXACTLYSIXTEEN!!"' in bpb.canonical_yt_title(m)

    def test_name_17_chars_preserved(self):
        m = _meta(name="SeventeenChrsHere")  # 17 chars
        assert '"SeventeenChrsHere"' in bpb.canonical_yt_title(m)

    def test_empty_name_fallback(self):
        m = _meta(name="")
        assert '"?"' in bpb.canonical_yt_title(m)


class TestCanonicalTitleKey:
    def test_key_present_shown(self):
        m = _meta(key_short="C#m")
        assert "145 BPM C#m" in bpb.canonical_yt_title(m)

    def test_key_missing_skipped(self):
        m = _meta(key_short="")
        title = bpb.canonical_yt_title(m)
        assert "145 BPM" in title
        assert "??" not in title  # не должны показывать placeholder

    def test_key_only_no_bpm(self):
        m = _meta(bpm=0, key_short="Am")
        title = bpb.canonical_yt_title(m)
        # Без BPM — показываем хотя бы key
        assert "Am" in title


class TestCanonicalTitleScene:
    def test_memphis_artists(self):
        for raw in ["kenny muney", "key glock", "young dolph", "glorilla"]:
            m = _meta(artist_raw=raw, artist_display=raw.title())
            assert "| Memphis |" in bpb.canonical_yt_title(m)

    def test_detroit_artists(self):
        for raw in ["nardowick", "babytron", "tee grizzley"]:
            m = _meta(artist_raw=raw, artist_display=raw.title())
            assert "| Detroit |" in bpb.canonical_yt_title(m)

    def test_nola(self):
        m = _meta(artist_raw="rob49", artist_display="Rob49")
        assert "| NOLA |" in bpb.canonical_yt_title(m)

    def test_florida(self):
        m = _meta(artist_raw="bossman dlow", artist_display="Bossman Dlow")
        assert "| FL Hard |" in bpb.canonical_yt_title(m)

    def test_ru_override_for_obladaet(self):
        m = _meta(artist_raw="obladaet", artist_display="Obladaet")
        assert "| RU Hard |" in bpb.canonical_yt_title(m)

    def test_ru_override_for_kizaru(self):
        m = _meta(artist_raw="kizaru", artist_display="Kizaru")
        assert "| RU Hard |" in bpb.canonical_yt_title(m)

    def test_unknown_artist_fallback_hard_trap(self):
        m = _meta(artist_raw="unknown_artist_xyz",
                  artist_display="Unknown Artist Xyz")
        assert "| Hard Trap |" in bpb.canonical_yt_title(m)


class TestCanonicalTitleArtist:
    def test_empty_artist_fallback_to_hard_trap_phrase(self):
        m = _meta(artist_raw="", artist_display="")
        title = bpb.canonical_yt_title(m)
        # Без артиста — phrase должна быть `Hard Trap Type Beat`
        assert "Hard Trap Type Beat" in title

    def test_collab_artist_kept(self):
        m = _meta(artist_raw="kenny muney x key glock",
                  artist_display="Kenny Muney x Key Glock")
        title = bpb.canonical_yt_title(m)
        assert "[FREE] Kenny Muney x Key Glock Type Beat 2026" in title


class TestCanonicalTitleLength:
    def test_under_100_chars(self):
        m = _meta(artist_display="Kenny Muney", name="HEAT")
        assert len(bpb.canonical_yt_title(m)) <= 100

    def test_truncates_when_too_long(self):
        # Создаём bit с очень длинным name → суммарно >100
        very_long = "X" * 150
        m = _meta(artist_display="Some Artist", name=very_long)
        title = bpb.canonical_yt_title(m)
        assert len(title) <= 100
        # Brand-часть должна сохраниться
        assert title.startswith("[FREE]")


class TestDisclaimer:
    def test_disclaimer_contains_required_phrases(self):
        text = bpb.canonical_yt_description_disclaimer(beat_id=42)
        assert "FREE FOR NON-PROFIT" in text
        assert "MP3 lease" in text
        assert "1700₽" in text
        assert "100k streams" in text
        assert "TRIPLE FILL" in text

    def test_disclaimer_includes_beat_id_in_buy_link(self):
        text = bpb.canonical_yt_description_disclaimer(beat_id=9117196)
        assert "9117196" in text

    def test_disclaimer_without_beat_id_uses_default_link(self):
        text = bpb.canonical_yt_description_disclaimer(beat_id=None)
        assert "triplekillpost_bot" in text
