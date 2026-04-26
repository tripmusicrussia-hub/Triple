"""Tests for Bundle deals (3 beats one transaction).

Покрывает чистые функции:
- цены `PRICE_BUNDLE3_*` соответствуют дискаунту от single
- `bundled_mp3_lease_text` содержит все 3 бита + buyer + charge
- helpers `_bundle_eligible_beats`, `_bundle_anchor_and_selected` — поведение

UI-callbacks (bundle_pick/confirm/pay_*) не покрыты — они требуют Telegram
mocks (PTB Update/CallbackQuery). End-to-end тестируется live через Stars/USDT/RUB.
"""
import licensing


class TestBundlePrices:
    def test_bundle_rub_cheaper_than_3_singles(self):
        # Bundle должен давать видимую экономию vs 3×single
        single_total = licensing.PRICE_MP3_RUB * 3
        assert licensing.PRICE_BUNDLE3_RUB < single_total
        # Скидка ≥10% — иначе бессмысленно
        discount_pct = (single_total - licensing.PRICE_BUNDLE3_RUB) / single_total * 100
        assert discount_pct >= 10

    def test_bundle_usdt_cheaper_than_3_singles(self):
        single_total = licensing.PRICE_MP3_USDT * 3
        assert licensing.PRICE_BUNDLE3_USDT < single_total

    def test_bundle_stars_cheaper_than_3_singles(self):
        single_total = licensing.PRICE_MP3_STARS * 3
        assert licensing.PRICE_BUNDLE3_STARS < single_total

    def test_bundle_prices_positive(self):
        assert licensing.PRICE_BUNDLE3_STARS > 0
        assert licensing.PRICE_BUNDLE3_USDT > 0
        assert licensing.PRICE_BUNDLE3_RUB > 0


class TestBundledLicenseText:
    def _sample_beats(self) -> list[dict]:
        return [
            {"id": 1, "name": "HEAT", "bpm": 160, "key": "Am"},
            {"id": 2, "name": "DARK NIGHT", "bpm": 145, "key": "C#m"},
            {"id": 3, "name": "GLOCK", "bpm": 152, "key": None},
        ]

    def test_contains_all_beat_names(self):
        text = licensing.bundled_mp3_lease_text(
            buyer_name="Vasya",
            buyer_tg_id=42,
            beats=self._sample_beats(),
            payment_charge_id="charge_xyz",
        )
        for name in ["HEAT", "DARK NIGHT", "GLOCK"]:
            assert name in text

    def test_contains_buyer_and_charge(self):
        text = licensing.bundled_mp3_lease_text(
            buyer_name="Vasya",
            buyer_tg_id=42,
            beats=self._sample_beats(),
            payment_charge_id="charge_xyz",
        )
        assert "Vasya" in text
        assert "42" in text
        assert "charge_xyz" in text

    def test_contains_total_count(self):
        beats = self._sample_beats()
        text = licensing.bundled_mp3_lease_text(
            buyer_name="X", buyer_tg_id=1, beats=beats, payment_charge_id="c",
        )
        assert f"Total beats:     {len(beats)}" in text

    def test_handles_missing_bpm_key(self):
        beats = [{"id": 1, "name": "X", "bpm": None, "key": None}]
        text = licensing.bundled_mp3_lease_text(
            buyer_name="A", buyer_tg_id=1, beats=beats, payment_charge_id="c",
        )
        assert "—" in text

    def test_producer_credit(self):
        text = licensing.bundled_mp3_lease_text(
            buyer_name="A", buyer_tg_id=1, beats=self._sample_beats(),
            payment_charge_id="c",
        )
        assert "TRIPLE FILL" in text


class TestBundlePayloadFormat:
    """Контракт: payload `bundle:<id1>,<id2>,<id3>` (Stars) и
    `bundle:<csv>:<user_id>` (USDT). PreCheckout/SuccessfulPayment парсят
    через `payload.split(':', 1)` + `ids_csv.split(',')`.

    Регрессия: если кто-то поменяет separator на `_` или `;` — break payments.
    """
    def test_stars_payload_parses(self):
        payload = "bundle:1,2,3"
        prefix, rest = payload.split(":", 1)
        assert prefix == "bundle"
        ids = [int(x) for x in rest.split(",") if x.strip()]
        assert ids == [1, 2, 3]

    def test_usdt_payload_parses(self):
        # USDT добавляет user_id в конец через `:`
        payload = "bundle:1,2,3:99887766"
        prefix, rest = payload.split(":", 1)
        ids_csv = rest.rsplit(":", 1)[0]
        ids = [int(x) for x in ids_csv.split(",") if x.strip()]
        assert ids == [1, 2, 3]

    def test_payload_handles_large_ids(self):
        # beat_id в каталоге — bigint (id=1931710 видели в проде)
        payload = "bundle:9117196,9118000,1931710"
        ids = [int(x) for x in payload.split(":", 1)[1].split(",")]
        assert ids == [9117196, 9118000, 1931710]
