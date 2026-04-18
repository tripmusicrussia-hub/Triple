"""Тесты валидации zip-файла продукта."""
import pytest

import product_upload


class TestValidateFile:
    def test_valid_zip(self):
        product_upload.validate_file("kit.zip", 10_000_000)  # не кидает

    def test_valid_rar(self):
        product_upload.validate_file("pack.rar", 40_000_000)

    def test_valid_7z(self):
        product_upload.validate_file("loops.7z", 20_000_000)

    def test_reject_too_large(self):
        with pytest.raises(product_upload.CaptionError, match="50MB"):
            product_upload.validate_file("big.zip", 60 * 1024 * 1024)

    def test_reject_wrong_extension(self):
        with pytest.raises(product_upload.CaptionError, match="zip/rar/7z"):
            product_upload.validate_file("doc.pdf", 5_000_000)

    def test_reject_no_size(self):
        with pytest.raises(product_upload.CaptionError):
            product_upload.validate_file("kit.zip", None)


class TestProductMeta:
    def test_all_fields_present(self):
        m = product_upload.ProductMeta(
            content_type="drumkit",
            name="Test Kit",
            price_stars=1500,
            price_usdt=15.0,
            description="desc",
        )
        assert m.content_type == "drumkit"
        assert m.price_stars == 1500
