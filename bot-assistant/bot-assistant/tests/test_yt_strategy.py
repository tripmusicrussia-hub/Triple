"""Tests для yt_strategy module — pure functions без yt-dlp/LLM mocks."""
from __future__ import annotations

import yt_strategy


class TestBuildPrompt:
    def test_includes_current_title(self):
        p = yt_strategy.build_title_optimizer_prompt(
            artist_display="Kenny Muney",
            current_title="kenny muney type beat heat 145bpm Am.mp3",
            top10=[],
            bpm=145, key_short="Am", scene="Memphis",
        )
        assert "kenny muney type beat heat 145bpm Am.mp3" in p

    def test_includes_artist(self):
        p = yt_strategy.build_title_optimizer_prompt(
            artist_display="Obladaet", current_title="x", top10=[],
        )
        assert "Obladaet" in p

    def test_top10_block_when_provided(self):
        top10 = [
            {"title": "Kenny Muney Type Beat 2026 - HEAT", "views": 12500,
             "channel": "Producer X", "url": "u1", "duration": 180},
            {"title": "Memphis Type Beat", "views": 8000,
             "channel": "Y", "url": "u2", "duration": 200},
        ]
        p = yt_strategy.build_title_optimizer_prompt(
            artist_display="Kenny Muney", current_title="old", top10=top10,
        )
        assert "Kenny Muney Type Beat 2026 - HEAT" in p
        assert "12,500 views" in p

    def test_empty_top10_includes_fallback_note(self):
        p = yt_strategy.build_title_optimizer_prompt(
            artist_display="X", current_title="y", top10=[],
        )
        assert "нет данных" in p

    def test_meta_block_partial(self):
        # Только bpm, без key/scene
        p = yt_strategy.build_title_optimizer_prompt(
            artist_display="X", current_title="y", top10=[], bpm=145,
        )
        assert "BPM: 145" in p
        assert "Key:" not in p

    def test_meta_block_full(self):
        p = yt_strategy.build_title_optimizer_prompt(
            artist_display="X", current_title="y", top10=[],
            bpm=160, key_short="C#m", scene="Memphis",
        )
        assert "BPM: 160" in p
        assert "Key: C#m" in p
        assert "Scene: Memphis" in p

    def test_includes_template_anchor(self):
        p = yt_strategy.build_title_optimizer_prompt(
            artist_display="Kenny Muney", current_title="x", top10=[],
        )
        # Template должен включать `[FREE] Kenny Muney Type Beat 2026 - "<NAME>"...`
        assert "[FREE] Kenny Muney Type Beat 2026" in p

    def test_includes_json_output_instruction(self):
        p = yt_strategy.build_title_optimizer_prompt(
            artist_display="X", current_title="y", top10=[],
        )
        assert "JSON" in p
        assert "variants" in p


class TestParseLlmResponse:
    def test_clean_json(self):
        text = '''
{
  "variants": [
    {"title": "[FREE] Kenny Muney Type Beat 2026 - \\"HEAT\\" | Memphis", "rationale": "Memphis pattern"},
    {"title": "[FREE] Kenny Muney x Key Glock - \\"DARK\\"", "rationale": "Collab boost"}
  ]
}
'''
        out = yt_strategy.parse_llm_titles_response(text)
        assert len(out) == 2
        assert out[0]["title"].startswith("[FREE]")
        assert out[0]["rationale"] == "Memphis pattern"

    def test_markdown_fenced_json(self):
        text = '''Here are 3 alternative titles:

```json
{"variants": [{"title": "X1", "rationale": "r1"}, {"title": "X2", "rationale": "r2"}]}
```
'''
        out = yt_strategy.parse_llm_titles_response(text)
        assert len(out) == 2
        assert out[0]["title"] == "X1"

    def test_no_json_returns_empty(self):
        text = "Sorry, I can't generate titles right now."
        out = yt_strategy.parse_llm_titles_response(text)
        assert out == []

    def test_invalid_json_returns_empty(self):
        text = '{"variants": [{"title": "X1", "rationale":'  # truncated
        out = yt_strategy.parse_llm_titles_response(text)
        assert out == []

    def test_no_variants_key_returns_empty(self):
        text = '{"foo": "bar"}'
        out = yt_strategy.parse_llm_titles_response(text)
        assert out == []

    def test_max_3_variants(self):
        # LLM может вернуть больше 3 — truncate
        text = '{"variants":' + ','.join(
            ['{"title":"T1","rationale":"r"}'] * 5
        ).join(['[', ']']) + '}'
        # Манипулируем чтобы получить валидный 5-вариантный JSON
        text = '''
        {"variants": [
            {"title": "T1", "rationale": "r1"},
            {"title": "T2", "rationale": "r2"},
            {"title": "T3", "rationale": "r3"},
            {"title": "T4", "rationale": "r4"},
            {"title": "T5", "rationale": "r5"}
        ]}
        '''
        out = yt_strategy.parse_llm_titles_response(text)
        assert len(out) == 3

    def test_truncates_long_title(self):
        long_title = "[FREE] " + "X" * 200
        text = f'{{"variants": [{{"title": "{long_title}", "rationale": "r"}}]}}'
        out = yt_strategy.parse_llm_titles_response(text)
        assert len(out) == 1
        assert len(out[0]["title"]) <= 100

    def test_empty_title_skipped(self):
        text = '''
        {"variants": [
            {"title": "", "rationale": "r1"},
            {"title": "Valid", "rationale": "r2"}
        ]}
        '''
        out = yt_strategy.parse_llm_titles_response(text)
        assert len(out) == 1
        assert out[0]["title"] == "Valid"

    def test_truncates_long_rationale(self):
        text = '{"variants": [{"title": "T", "rationale": "' + ("X" * 500) + '"}]}'
        out = yt_strategy.parse_llm_titles_response(text)
        assert len(out[0]["rationale"]) <= 200

    def test_empty_text(self):
        assert yt_strategy.parse_llm_titles_response("") == []
        assert yt_strategy.parse_llm_titles_response("   ") == []
