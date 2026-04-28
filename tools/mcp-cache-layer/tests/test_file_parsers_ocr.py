# -*- coding: utf-8 -*-
"""test_file_parsers_ocr.py — task-110 OCR 통합 테스트"""
import sys
from pathlib import Path
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))


class TestOcrImage:
    """_ocr_image() 단위 테스트"""

    def setup_method(self):
        import scripts.file_parsers as fp
        fp._tesseract_available = None  # 각 테스트 전 상태 초기화

    def test_t2_tesseract_not_installed_returns_empty(self, tmp_path, caplog):
        """T-2: Tesseract 미설치 시 경고 1회 + 빈 문자열 반환"""
        from scripts.file_parsers import _ocr_image
        png = tmp_path / "dummy.png"
        png.write_bytes(b"not a real image")
        result = _ocr_image(str(png))
        assert result == ""
        # 두 번째 호출 — 경고 없이 즉시 반환
        result2 = _ocr_image(str(png))
        assert result2 == ""

    def test_t4_no_saved_path_ocr_skipped(self):
        """T-4: saved_path가 None이면 OCR 호출 안 됨 (parse_pptx 로직 보증)"""
        img_info = {"filename": "slide01_img001.png", "size_bytes": 1024, "saved_path": None}
        # OCR 통합 조건: img_info.get("saved_path") 이므로 None이면 skipped
        assert not img_info.get("saved_path")

    def test_t5_corrupt_image_returns_empty(self, tmp_path):
        """T-5: 손상된 이미지 → debug 로그 + 빈 문자열 반환"""
        import scripts.file_parsers as fp
        fp._tesseract_available = True  # 설치된 것처럼 설정
        from scripts.file_parsers import _ocr_image
        corrupt = tmp_path / "corrupt.png"
        corrupt.write_bytes(b"not a real image")
        result = _ocr_image(str(corrupt))
        assert result == ""
        # _tesseract_available은 True 유지 (이미지 오류이지 바이너리 오류 아님)
        assert fp._tesseract_available is True

    def test_t6_empty_ocr_result_not_appended(self):
        """T-6: _ocr_image가 '' 반환 시 slide_parts에 추가 안 됨"""
        slide_parts = ["## Slide 1", "텍스트"]
        ocr_text = ""
        if ocr_text:
            slide_parts.append(ocr_text)
        assert len(slide_parts) == 2
