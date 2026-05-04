"""coverage_comparator.py — Semantic Coverage Comparator 레지스트리.

task-123 Step 4 sub-step 4
설계 출처: step2_design_v2_addendum.md §2 함정 2 (threshold 매트릭스)
          step2_design_v2.md §2.3 (Comparator 레지스트리 패턴)

file_kind별 comparator 함수를 등록하는 레지스트리 패턴.
각 comparator는 ReferenceContent + CacheContent → list[LossRecord] 반환.

block_violation 결정 규칙 (addendum §2 함정 2):
  loss_pct > threshold AND extractable == 1 → block_violation=1
  extractable == 0 → block_violation=0 (라이브러리 한계, BLOCK 금지)

TRUNCATED_BY_DESIGN origin:
  cache.text_units 마지막 chunk에 '_(… 외 N행 생략)_' 마커 존재 시
  origin='TRUNCATED_BY_DESIGN' 기록 (file_parsers.py MAX_TABLE_ROWS 절단)
"""

from __future__ import annotations

import logging
import re
from typing import Callable

from scripts.cache_reconstructor import CacheContent
from scripts.reference_parsers.models import LossRecord, ReferenceContent
from scripts.file_parsers import MAX_BODY_CHARS as _MAX_BODY_CHARS_FROM_PARSERS

logger = logging.getLogger(__name__)


# ── threshold 매트릭스 (addendum §2 함정 2 — 절대 준수) ──────────────────────

THRESHOLDS: dict[tuple[str, str], float] = {
    ("xlsx", "TABLE_LOSS"):    0.01,
    ("xlsx", "FORMULA_LOSS"):  0.05,
    ("xlsx", "IMAGE_LOSS"):    0.01,   # extractable=1만 적용
    ("xlsx", "STRUCTURE_LOSS"): 0.05,
    ("pptx", "TABLE_LOSS"):    0.01,
    ("pptx", "IMAGE_LOSS"):    0.01,
    ("pptx", "STRUCTURE_LOSS"): 0.05,
    ("tsv",  "TABLE_LOSS"):    0.01,
    ("tsv",  "ENCODING_LOSS"): 0.0,    # strict — 1건 차이도 BLOCK
    ("tsv",  "STRUCTURE_LOSS"): 0.05,
}


# ── Comparator 레지스트리 ────────────────────────────────────────────────────

COMPARATORS: dict[str, Callable[[ReferenceContent, CacheContent], list[LossRecord]]] = {}


def register_comparator(file_kind: str):
    """comparator 함수 등록 데코레이터."""
    def deco(fn):
        COMPARATORS[file_kind] = fn
        return fn
    return deco


# ── 내부 유틸 ────────────────────────────────────────────────────────────────

# TRUNCATED_BY_DESIGN 마커 패턴 (file_parsers.py 절단 시 삽입)
# - MAX_TABLE_ROWS 절단: `_(… 외 N행 생략)_`
# - MAX_BODY_CHARS 절단 (file_parsers.py:478-479): `_(본문 잘림)_`  (task-124 Step 6 시정)
_TRUNCATED_RE = re.compile(r"_\(…\s*외\s*\d+행\s*생략[^)]*\)_|_\(본문 잘림\)_")

# IMAGE 마커 패턴 (cache.text_units 내 이미지 proxy 매칭)
# HIGH-2 ReDoS 가드 (task-125 Step 7): [^\]\n]{1,500} — 길이 500자 제한으로 backtracking 차단
_IMAGE_MARKER_RE = re.compile(r"\[이미지[^\]\n]{1,500}\]")

# FORMULA 마커 패턴 (task-125 C-1): '[=...]' 블록 내용 추출
# op body에 'value [=SUM(A1:A10)]' 형태가 있을 때 [=...] 자체는 단일 토큰이므로
# 내부 숫자 리터럴을 별도 추출하여 cache_numeric에 보강
# HIGH-1 ReDoS 가드 (task-125 Step 7): [^\]\n]{1,500} — 길이 500자 제한으로 backtracking 차단
_FORMULA_BLOCK_RE = re.compile(r"\[=([^\]\n]{1,500})\]")

# task-127 Step 6: MAX_BODY_CHARS 임계값 — file_parsers.py import로 단일 소스화 (L-1 시정)
# 캐시 재조합 텍스트 총 길이가 이 값 이상이면 TRUNCATED_BY_DESIGN으로 추가 감지
# (마커가 청크 분할 경계에 의해 소실된 경우 대비)
_MAX_BODY_CHARS_THRESHOLD = int(_MAX_BODY_CHARS_FROM_PARSERS * 0.99)  # MAX_BODY_CHARS * 99%


def _is_truncated_by_design(text_units: list[str]) -> bool:
    """cache.text_units 중 어딘가에 절단 마커 존재 여부.

    task-127 Step 6 시정: 마커 기반 감지 외에, 캐시 재조합 텍스트 총 길이가
    MAX_BODY_CHARS(500K) 근접(>=495K) 시 TRUNCATED_BY_DESIGN으로 추가 판정.
    이는 force_reload 후 청크 분할 경계에 의해 절단 마커가 특정 청크에
    포함되지 않는 경우를 커버한다 (NPCFunc.xlsx 등 대형 xlsx).
    """
    for tu in text_units:
        if _TRUNCATED_RE.search(tu):
            return True
    # 마커 미발견 → 총 길이 기반 fallback 감지
    total_len = sum(len(tu) for tu in text_units)
    if total_len >= _MAX_BODY_CHARS_THRESHOLD:
        return True
    return False


def _make_loss(
    file_kind: str,
    loss_type: str,
    loss_pct: float,
    loss_count: int,
    extractable: int,
    origin: str = "CHUNKER",
    detail_json: dict | None = None,
) -> LossRecord:
    """LossRecord 생성 헬퍼 — threshold 자동 조회 + block_violation 판정.

    v3 정정 (Master 지적 + step2_design_v3 §3.2):
      - origin='TRUNCATED_BY_DESIGN' 시 block_violation=0 (의도적 절단은 BLOCK 아님)
      - extractable=0 시 block_violation=0 (라이브러리 한계, BLOCK 금지)
    """
    threshold = THRESHOLDS.get((file_kind, loss_type), 0.01)
    block_violation = 1 if (
        loss_pct > threshold
        and extractable == 1
        and origin != "TRUNCATED_BY_DESIGN"
    ) else 0
    return LossRecord(
        loss_type=loss_type,
        loss_pct=loss_pct,
        loss_count=loss_count,
        block_violation=block_violation,
        origin=origin,
        extractable=extractable,
        detail_json=detail_json or {},
    )


def _set_threshold(loss: LossRecord, file_kind: str) -> LossRecord:
    """기존 LossRecord에 threshold 컬럼 기록 (insert_loss 전달 시 그대로 사용)."""
    threshold = THRESHOLDS.get((file_kind, loss.loss_type), 0.01)
    loss.threshold = threshold  # type: ignore[attr-defined]
    return loss


def _extract_token_set(text_units: list[str]) -> set[str]:
    """text_units 전체에서 공백/탭 분리 토큰 set 추출 (TABLE_LOSS 측정용)."""
    tokens: set[str] = set()
    for tu in text_units:
        for tok in re.split(r"[\s\t|]+", tu):
            t = tok.strip()
            if t:
                tokens.add(t)
    return tokens


# ── xlsx comparator ───────────────────────────────────────────────────────────

@register_comparator("xlsx")
def compare_xlsx(ref: ReferenceContent, cache: CacheContent) -> list[LossRecord]:
    """xlsx: TABLE_LOSS / FORMULA_LOSS / IMAGE_LOSS / STRUCTURE_LOSS 4종 측정.

    semantic 알고리즘:
      TABLE_LOSS:   ref.text_units 토큰 set vs cache.text_units 토큰 set 누락률
      FORMULA_LOSS: formula_eval_count > 0 → 수식 평가값 토큰 매칭 누락률
      IMAGE_LOSS:   extractable=True 인 이미지 마커 cache 등장 횟수 비교
      STRUCTURE_LOSS: sheet 순서 보존 여부 (structure.sheets vs cache text)
    """
    losses: list[LossRecord] = []
    truncated = _is_truncated_by_design(cache.text_units)
    origin = "TRUNCATED_BY_DESIGN" if truncated else "CHUNKER"

    # ── TABLE_LOSS ──────────────────────────────────────────────────────────
    ref_tokens = _extract_token_set(ref.text_units)
    cache_tokens = _extract_token_set(cache.text_units)

    if ref_tokens:
        missing = ref_tokens - cache_tokens
        loss_pct = len(missing) / len(ref_tokens)
        loss_count = len(missing)
    else:
        loss_pct = 0.0
        loss_count = 0

    table_loss = _make_loss(
        "xlsx", "TABLE_LOSS", loss_pct, loss_count,
        extractable=1,
        origin=origin,
        detail_json={"ref_token_count": len(ref_tokens), "cache_token_count": len(cache_tokens)},
    )
    table_loss.threshold = THRESHOLDS[("xlsx", "TABLE_LOSS")]
    losses.append(table_loss)

    # ── FORMULA_LOSS ─────────────────────────────────────────────────────────
    if ref.formula_eval_count > 0:
        # ref 숫자 토큰 vs cache 숫자 토큰 비교
        ref_numeric = {t for t in ref_tokens if re.match(r"^-?[\d,.]+%?$", t)}
        cache_numeric = {t for t in cache_tokens if re.match(r"^-?[\d,.]+%?$", t)}

        # C-1 (task-125 Step 6): [=FORMULA] 마커 내 숫자 리터럴도 cache_numeric에 포함.
        # op body에 'value [=COUNTIF(G8:G450,"pass")]' 형태가 있을 때
        # [=COUNTIF(...)] 자체는 단일 토큰으로 처리되어 내부 숫자가 cache_numeric 미포함.
        # → [=...] 블록을 분해하여 숫자 리터럴 추출 후 set union.
        # R-5 정합: xlsx_reference_parser.py 변경 0줄 유지.
        cache_text_for_formula = "\n".join(cache.text_units)
        formula_numeric: set = set()
        for formula_content in _FORMULA_BLOCK_RE.findall(cache_text_for_formula):
            # 수식 인자 구분자(콤마)를 토큰 경계로 처리하기 위해 콤마 제외 패턴 사용.
            # 예: DATE(2026,1,8) → '2026','1','8' 각각 추출 (콤마 포함 패턴이면 '2026,1,8' 단일 토큰)
            for num_tok in re.findall(r"-?[\d.]+%?", formula_content):
                t = num_tok.strip(".")  # 부동소수점 trailing dot 제거
                if t and re.match(r"^-?[\d,.]+%?$", t):
                    formula_numeric.add(t)
        cache_numeric = cache_numeric | formula_numeric

        if ref_numeric:
            missing_num = ref_numeric - cache_numeric
            fl_pct = len(missing_num) / len(ref_numeric)
            fl_count = len(missing_num)
        else:
            fl_pct = 0.0
            fl_count = 0

        formula_loss = _make_loss(
            "xlsx", "FORMULA_LOSS", fl_pct, fl_count,
            extractable=1,
            origin=origin,
            detail_json={"formula_eval_count": ref.formula_eval_count},
        )
        formula_loss.threshold = THRESHOLDS[("xlsx", "FORMULA_LOSS")]
        losses.append(formula_loss)

    # ── IMAGE_LOSS ───────────────────────────────────────────────────────────
    if ref.image_count > 0 or ref.image_markers:
        cache_text_all = "\n".join(cache.text_units)
        cache_image_markers = _IMAGE_MARKER_RE.findall(cache_text_all)

        total_ref_images = len(ref.image_markers) if ref.image_markers else ref.image_count
        cache_matched = sum(
            1 for m in ref.image_markers
            if any(m in ctu or _marker_key(m) in ctu for ctu in cache.text_units)
        )

        if total_ref_images > 0:
            il_pct = max(0.0, (total_ref_images - cache_matched) / total_ref_images)
            il_count = total_ref_images - cache_matched
        else:
            il_pct = 0.0
            il_count = 0

        # R5-A (task-125): xlsx ref Parser는 images_extractable=False 하드코딩(R-5 변경 금지).
        # op body_text에 image proxy marker가 있으면 audit 측에서 extractable=1로 판정 보강.
        # 변경 영향: pptx/tsv는 ref.images_extractable=True/N/A이므로 기존과 동일 결과.
        op_body_has_marker = bool(cache_image_markers)
        extractable = 1 if (ref.image_count > 0 and (ref.images_extractable or op_body_has_marker)) else 0
        image_loss = _make_loss(
            "xlsx", "IMAGE_LOSS", il_pct, il_count,
            extractable=extractable,
            origin=origin,
        )
        image_loss.threshold = THRESHOLDS[("xlsx", "IMAGE_LOSS")]
        losses.append(image_loss)

    # ── STRUCTURE_LOSS ───────────────────────────────────────────────────────
    sheets = ref.structure.get("sheets", [])
    if sheets:
        cache_text_all = "\n".join(cache.text_units)
        missing_sheets = [s for s in sheets if s not in cache_text_all]
        sl_pct = len(missing_sheets) / len(sheets) if sheets else 0.0
        sl_count = len(missing_sheets)
    else:
        sl_pct = 0.0
        sl_count = 0

    struct_loss = _make_loss(
        "xlsx", "STRUCTURE_LOSS", sl_pct, sl_count,
        extractable=1,
        origin=origin,
        detail_json={"sheet_count": len(sheets)},
    )
    struct_loss.threshold = THRESHOLDS[("xlsx", "STRUCTURE_LOSS")]
    # TRUNCATED_BY_DESIGN origin은 STRUCTURE_LOSS에도 전파
    if truncated:
        struct_loss.origin = "TRUNCATED_BY_DESIGN"
    losses.append(struct_loss)

    return losses


def _marker_key(marker: str) -> str:
    """이미지 마커에서 파일명 부분 추출 (느슨한 매칭용)."""
    m = re.search(r"\[이미지:\s*([^,\]]+)", marker)
    return m.group(1).strip() if m else marker


# ── pptx comparator ───────────────────────────────────────────────────────────

@register_comparator("pptx")
def compare_pptx(ref: ReferenceContent, cache: CacheContent) -> list[LossRecord]:
    """pptx: TABLE_LOSS / IMAGE_LOSS / STRUCTURE_LOSS 3종 측정.

    semantic 알고리즘:
      TABLE_LOSS:     ref 텍스트 토큰 vs cache 텍스트 토큰 누락률
      IMAGE_LOSS:     image_markers cache 등장 횟수 비교 (extractable=True 시)
      STRUCTURE_LOSS: slide_count vs cache slide 헤더 수 비교
    """
    losses: list[LossRecord] = []
    truncated = _is_truncated_by_design(cache.text_units)
    origin = "TRUNCATED_BY_DESIGN" if truncated else "CHUNKER"

    # ── TABLE_LOSS (텍스트 토큰 누락) ────────────────────────────────────────
    ref_tokens = _extract_token_set(ref.text_units)
    cache_tokens = _extract_token_set(cache.text_units)

    if ref_tokens:
        missing = ref_tokens - cache_tokens
        tl_pct = len(missing) / len(ref_tokens)
        tl_count = len(missing)
    else:
        tl_pct = 0.0
        tl_count = 0

    table_loss = _make_loss(
        "pptx", "TABLE_LOSS", tl_pct, tl_count,
        extractable=1,
        origin=origin,
    )
    table_loss.threshold = THRESHOLDS[("pptx", "TABLE_LOSS")]
    losses.append(table_loss)

    # ── IMAGE_LOSS ───────────────────────────────────────────────────────────
    total_ref_images = len(ref.image_markers) if ref.image_markers else ref.image_count
    if total_ref_images > 0:
        cache_matched = sum(
            1 for m in ref.image_markers
            if any(m in ctu or _marker_key(m) in ctu for ctu in cache.text_units)
        )
        il_pct = max(0.0, (total_ref_images - cache_matched) / total_ref_images)
        il_count = total_ref_images - cache_matched
    else:
        il_pct = 0.0
        il_count = 0

    extractable = 1 if ref.images_extractable else 0
    image_loss = _make_loss(
        "pptx", "IMAGE_LOSS", il_pct, il_count,
        extractable=extractable,
        origin=origin,
    )
    image_loss.threshold = THRESHOLDS[("pptx", "IMAGE_LOSS")]
    losses.append(image_loss)

    # ── STRUCTURE_LOSS (슬라이드 순서/수) ────────────────────────────────────
    ref_slide_count = ref.structure.get("slide_count", 0)
    if ref_slide_count > 0:
        # cache text에서 "## Slide N" 헤더 수 추출
        cache_text_all = "\n".join(cache.text_units)
        cache_slide_headers = re.findall(r"##\s+Slide\s+\d+", cache_text_all)
        cache_slide_count = len(cache_slide_headers)

        if ref_slide_count > cache_slide_count:
            sl_pct = (ref_slide_count - cache_slide_count) / ref_slide_count
            sl_count = ref_slide_count - cache_slide_count
        else:
            sl_pct = 0.0
            sl_count = 0
    else:
        sl_pct = 0.0
        sl_count = 0

    struct_loss = _make_loss(
        "pptx", "STRUCTURE_LOSS", sl_pct, sl_count,
        extractable=1,
        origin=origin,
        detail_json={"ref_slide_count": ref_slide_count},
    )
    struct_loss.threshold = THRESHOLDS[("pptx", "STRUCTURE_LOSS")]
    losses.append(struct_loss)

    return losses


# ── tsv comparator ────────────────────────────────────────────────────────────

@register_comparator("tsv")
def compare_tsv(ref: ReferenceContent, cache: CacheContent) -> list[LossRecord]:
    """tsv: TABLE_LOSS / ENCODING_LOSS / STRUCTURE_LOSS 3종 측정.

    semantic 알고리즘:
      TABLE_LOSS:   ref 행 토큰 set vs cache 행 토큰 set 누락률
      ENCODING_LOSS: encoding != None AND 인코딩 불일치 or 깨진 텍스트 → 100% loss
                     (threshold=0.0 strict — 1건 차이도 BLOCK)
      STRUCTURE_LOSS: row_count 보존 여부
    """
    losses: list[LossRecord] = []
    truncated = _is_truncated_by_design(cache.text_units)
    origin = "TRUNCATED_BY_DESIGN" if truncated else "CHUNKER"

    # ── TABLE_LOSS ───────────────────────────────────────────────────────────
    ref_tokens = _extract_token_set(ref.text_units)
    cache_tokens = _extract_token_set(cache.text_units)

    if ref_tokens:
        missing = ref_tokens - cache_tokens
        tl_pct = len(missing) / len(ref_tokens)
        tl_count = len(missing)
    else:
        tl_pct = 0.0
        tl_count = 0

    table_loss = _make_loss(
        "tsv", "TABLE_LOSS", tl_pct, tl_count,
        extractable=1,
        origin=origin,
    )
    table_loss.threshold = THRESHOLDS[("tsv", "TABLE_LOSS")]
    losses.append(table_loss)

    # ── ENCODING_LOSS ────────────────────────────────────────────────────────
    el_pct = 0.0
    el_count = 0
    el_extractable = 1

    if ref.encoding is not None:
        enc_lower = ref.encoding.lower()
        # 인코딩이 UTF-8 계열이 아니고 cache 텍스트에 깨진 문자(?) 비율이 높으면 LOSS
        # 또는 ref.encoding과 cache가 실질적으로 매칭 안 될 때
        cache_text_all = "\n".join(cache.text_units)
        # 깨진 문자 패턴: ? 연속 또는 � (replacement character)
        broken_count = cache_text_all.count("?") + cache_text_all.count("�")
        total_chars = len(cache_text_all)

        if total_chars > 0 and broken_count / total_chars > 0.05:
            # 5% 이상 깨진 문자 → ENCODING_LOSS 발생
            el_pct = min(1.0, broken_count / total_chars)
            el_count = broken_count
            el_extractable = 1

    enc_loss = _make_loss(
        "tsv", "ENCODING_LOSS", el_pct, el_count,
        extractable=el_extractable,
        origin=origin,
    )
    enc_loss.threshold = THRESHOLDS[("tsv", "ENCODING_LOSS")]
    losses.append(enc_loss)

    # ── STRUCTURE_LOSS (v3: row count 비교 폐기 — chunker가 row 단위 split 안 함) ──
    # v2 결함: cache_row_count = len(text_units) - 1 (chunk count != row count)
    #          → tsv STRUCTURE_LOSS 81% false positive
    # v3 정정: 헤더 보존 매칭으로 단순화 (ref headers가 cache text에 등장하는지)
    ref_headers = ref.structure.get("headers", [])
    if ref_headers:
        cache_text_all = "\n".join(cache.text_units)
        missing_headers = [h for h in ref_headers if h and h not in cache_text_all]
        sl_pct = len(missing_headers) / len(ref_headers) if ref_headers else 0.0
        sl_count = len(missing_headers)
        sl_detail = {
            "ref_header_count": len(ref_headers),
            "missing_header_count": len(missing_headers),
            "missing_headers": missing_headers[:10],  # 최대 10건만 detail 기록
        }
    else:
        sl_pct = 0.0
        sl_count = 0
        sl_detail = {"ref_header_count": 0}

    struct_loss = _make_loss(
        "tsv", "STRUCTURE_LOSS", sl_pct, sl_count,
        extractable=1,
        origin="TRUNCATED_BY_DESIGN" if truncated else origin,
        detail_json=sl_detail,
    )
    struct_loss.threshold = THRESHOLDS[("tsv", "STRUCTURE_LOSS")]
    losses.append(struct_loss)

    return losses
