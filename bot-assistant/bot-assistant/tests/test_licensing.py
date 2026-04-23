"""Тесты lookup-таблиц и генерации license-текстов."""
import licensing


class TestPrices:
    def test_mp3_stars_price(self):
        assert licensing.PRICE_MP3_STARS == 1500

    def test_mp3_usdt_price(self):
        assert licensing.PRICE_MP3_USDT == 20.0

    def test_mp3_rub_price(self):
        assert licensing.PRICE_MP3_RUB == 1700

    def test_mix_stars_price(self):
        assert licensing.PRICE_MIX_STARS == 4500

    def test_mix_usdt_price(self):
        assert licensing.PRICE_MIX_USDT == 60.0

    def test_mix_rub_price(self):
        assert licensing.PRICE_MIX_RUB == 5000

    def test_kit_stars_price(self):
        assert licensing.PRICE_KIT_STARS == 1500

    def test_pack_stars_price(self):
        assert licensing.PRICE_PACK_STARS == 1000


class TestProductTypes:
    def test_all_types_have_labels(self):
        for ctype in ["drumkit", "samplepack", "looppack"]:
            assert ctype in licensing.PRODUCT_TYPE_LABELS
            assert licensing.PRODUCT_TYPE_LABELS[ctype]  # non-empty

    def test_all_types_have_default_prices(self):
        for ctype in ["drumkit", "samplepack", "looppack"]:
            stars, usdt = licensing.DEFAULT_PRICES[ctype]
            assert stars > 0
            assert usdt > 0

    def test_pack_and_loop_same_price(self):
        # Бизнес-решение: loops = packs по цене
        assert licensing.DEFAULT_PRICES["samplepack"] == licensing.DEFAULT_PRICES["looppack"]

    def test_kit_more_expensive_than_pack(self):
        kit_stars, _ = licensing.DEFAULT_PRICES["drumkit"]
        pack_stars, _ = licensing.DEFAULT_PRICES["samplepack"]
        assert kit_stars > pack_stars

    def test_aliases_map_to_valid_types(self):
        for alias, ctype in licensing.PRODUCT_TYPE_ALIASES.items():
            assert ctype in licensing.PRODUCT_TYPE_LABELS


class TestMp3LeaseText:
    def test_contains_key_fields(self):
        text = licensing.mp3_lease_text(
            buyer_name="Vasya Pupkin",
            buyer_tg_id=12345,
            beat_name="HEAT",
            bpm=160,
            key="A minor",
            payment_charge_id="charge_abc",
        )
        assert "Vasya Pupkin" in text
        assert "12345" in text
        assert "HEAT" in text
        assert "160" in text
        assert "charge_abc" in text
        assert "TRIPLE FILL" in text
        assert "MP3 LEASE" in text.upper()

    def test_no_missing_fallback(self):
        # bpm/key не переданы → текст всё равно строится
        text = licensing.mp3_lease_text(
            buyer_name="X", buyer_tg_id=1, beat_name="B",
            bpm=None, key=None, payment_charge_id="c",
        )
        assert "—" in text  # fallback dash


class TestProductLicenseText:
    def test_drumkit_license_contains_label(self):
        text = licensing.product_license_text(
            buyer_name="A", buyer_tg_id=1,
            product_type="drumkit", product_name="Kit V1",
            payment_charge_id="c",
        )
        assert "DRUM KIT" in text.upper()
        assert "Kit V1" in text
        assert "non-exclusive" in text.lower()
