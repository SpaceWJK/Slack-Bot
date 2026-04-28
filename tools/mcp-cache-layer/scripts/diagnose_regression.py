"""regression 상위 10건 진단 — 새 파서가 누락하는 텍스트 식별."""
import sys
import os
import re
import html as _html
import json
from pathlib import Path
from html.parser import HTMLParser as _HTMLParser

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, "D:/Vibe Dev/Slack Bot/Slack Bot")

from src.models import init_db, get_connection

# ── 기존 파서 (regex) ─────────────────────────────────
def old_strip_html(html_text: str) -> str:
    text = html_text or ''
    text = re.sub(r'<(script|style)[^>]*>.*?</\1>', '', text, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r'<ac:parameter[^>]*>.*?</ac:parameter>', '', text, flags=re.DOTALL)
    text = re.sub(r'<!\[CDATA\[(.*?)\]\]>', r'\1', text, flags=re.DOTALL)
    text = re.sub(r'<ac:emoticon[^/]*/>', '', text)
    text = re.sub(r'</t[dh]>\s*<t[dh][^>]*>', ' | ', text, flags=re.IGNORECASE)
    text = re.sub(r'</?tr[^>]*>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'<[^>]+>', ' ', text)
    text = _html.unescape(text)
    text = re.sub(r'[ \t]+', ' ', text)
    text = re.sub(r'\n[ \t]+', '\n', text)
    text = re.sub(r'\n{3,}', '\n\n', text)
    return text.strip()


# ── 새 파서 (HTMLParser) ─────────────────────────────────
class _ConfluenceHTMLExtractor(_HTMLParser):
    _SKIP_TAGS = frozenset([
        "script", "style", "ac:parameter", "ac:emoticon",
        "select", "option", "input", "button", "form", "canvas",
    ])
    _SKIP_CLASSES = frozenset([
        "chart-controls", "chart-menu-buttons", "aui-dropdown2",
        "aui-toolbar2", "tf-chart-message", "chart-settings",
        "tfac-menu",
        "table-filter-menu", "table-filter-controls",
        "tableFilterCbStyle", "lockEnabled", "lockDisabled",
        "no-table-message", "waiting-for-table",
        "empty-message", "show-n-rows-only-message",
        "tf-hider-wrapper", "tf-shower-wrapper",
        "tf-body-storage",
    ])
    _BLOCK_TAGS = frozenset([
        "p", "div", "br", "hr",
        "h1", "h2", "h3", "h4", "h5", "h6",
        "blockquote", "pre",
        "table", "thead", "tbody",
    ])

    def __init__(self):
        super().__init__(convert_charrefs=True)
        self._parts = []
        self._skip_stack = []
        self._skipped_parts = []  # 진단용: 스킵된 텍스트 기록
        self._skip_reasons = []   # 진단용: 스킵 이유

    @property
    def _skipping(self):
        return len(self._skip_stack) > 0

    def _should_skip(self, tag_lower, attrs):
        if tag_lower in self._SKIP_TAGS:
            return True, f"SKIP_TAG:{tag_lower}"
        attr_dict = dict(attrs)
        style = attr_dict.get("style", "")
        if "display:" in style and "none" in style:
            return True, f"display:none"
        classes = set(attr_dict.get("class", "").split())
        match = classes & self._SKIP_CLASSES
        if match:
            return True, f"SKIP_CLASS:{match}"
        return False, ""

    def handle_starttag(self, tag, attrs):
        tag_lower = tag.lower()
        if self._skipping:
            self._skip_stack.append(tag_lower)
            return
        should, reason = self._should_skip(tag_lower, attrs)
        if should:
            self._skip_stack.append(tag_lower)
            self._skip_reasons.append(reason)
            return
        if tag_lower in self._BLOCK_TAGS:
            self._parts.append("\n")
        elif tag_lower == "tr":
            self._parts.append("\n")
        elif tag_lower in ("td", "th"):
            if self._parts and not self._parts[-1].endswith("\n"):
                self._parts.append(" | ")
        elif tag_lower == "li":
            self._parts.append("\n- ")

    def handle_endtag(self, tag):
        tag_lower = tag.lower()
        if self._skipping:
            if self._skip_stack and self._skip_stack[-1] == tag_lower:
                self._skip_stack.pop()
            elif self._skip_stack:
                for i in range(len(self._skip_stack) - 1, -1, -1):
                    if self._skip_stack[i] == tag_lower:
                        self._skip_stack.pop(i)
                        break
            return
        if tag_lower in self._BLOCK_TAGS or tag_lower == "tr":
            self._parts.append("\n")

    def handle_data(self, data):
        if self._skipping:
            self._skipped_parts.append(data)
            return
        self._parts.append(data)

    def unknown_decl(self, data):
        if self._skipping:
            if data.startswith("CDATA[") and data.endswith("]"):
                self._skipped_parts.append(data[6:-1])
            return
        if data.startswith("CDATA[") and data.endswith("]"):
            content = data[6:-1]
            if content.strip():
                self._parts.append(content)


def new_strip_html(html_text):
    ext = _ConfluenceHTMLExtractor()
    try:
        ext.feed(html_text or "")
    except Exception:
        pass
    text = "".join(ext._parts)
    text = re.sub(r'[ \t]+', ' ', text)
    text = re.sub(r'\n[ \t]+', '\n', text)
    text = re.sub(r'\n{3,}', '\n\n', text)
    skipped_text = "".join(ext._skipped_parts)
    return text.strip(), skipped_text.strip(), ext._skip_reasons


# ── 메인 ─────────────────────────────────
def main():
    init_db()
    conn = get_connection()

    report_path = PROJECT_ROOT / "cache" / "parser_results" / "_report.json"
    with open(report_path, "r", encoding="utf-8") as f:
        report = json.load(f)

    regressions = report["regressions"]

    print("=" * 80)
    print("REGRESSION 진단 — 새 파서가 누락하는 텍스트 분석")
    print("=" * 80)

    for i, reg in enumerate(regressions):
        page_id = reg["page_id"]
        title = reg["title"]
        old_len = reg["old"]
        new_len = reg["new"]
        diff = reg["diff"]

        # DB에서 원본 HTML 가져오기 (nodes → doc_content 조인)
        row = conn.execute(
            """SELECT dc.body_raw FROM doc_content dc
               JOIN nodes n ON dc.node_id = n.id
               WHERE n.source_id = ? AND n.source_type = 'wiki'""",
            (page_id,)
        ).fetchone()

        if not row or not row[0]:
            print(f"\n[{i+1}] {page_id} '{title}' — DB에 body_raw 없음")
            continue

        html = row[0]

        # 양쪽 파서 실행
        old_text = old_strip_html(html)
        new_text, skipped_text, skip_reasons = new_strip_html(html)

        # 스킵된 텍스트에서 실제 의미 있는 내용 추출
        skipped_meaningful = [
            line.strip() for line in skipped_text.split("\n")
            if line.strip() and len(line.strip()) > 3
        ]

        print(f"\n{'─'*80}")
        print(f"[{i+1}/10] page_id={page_id}")
        print(f"  제목: {title}")
        print(f"  기존: {len(old_text)}자 / 신규: {len(new_text)}자 / 차이: {len(new_text)-len(old_text)}자")
        print(f"  HTML 길이: {len(html)}자")
        print(f"  스킵 이유 횟수: {len(skip_reasons)}")

        # 스킵 이유 빈도 분석
        reason_counts = {}
        for r in skip_reasons:
            reason_counts[r] = reason_counts.get(r, 0) + 1
        if reason_counts:
            print(f"  스킵 이유 분포:")
            for reason, count in sorted(reason_counts.items(), key=lambda x: -x[1]):
                print(f"    {reason}: {count}회")

        print(f"  스킵된 텍스트 총 길이: {len(skipped_text)}자")
        if skipped_meaningful:
            print(f"  스킵된 의미있는 라인 수: {len(skipped_meaningful)}개")
            # 상위 5개만 표시
            print(f"  스킵된 텍스트 샘플 (상위 5):")
            for j, line in enumerate(skipped_meaningful[:5]):
                preview = line[:100] + ("..." if len(line) > 100 else "")
                print(f"    [{j+1}] {preview}")

        # 기존에만 있는 텍스트 찾기 (간단한 diff)
        old_words = set(old_text.split())
        new_words = set(new_text.split())
        only_old = old_words - new_words
        only_new = new_words - old_words

        print(f"  기존에만 있는 단어 수: {len(only_old)}")
        print(f"  신규에만 있는 단어 수: {len(only_new)}")

    conn.close()
    print(f"\n{'='*80}")
    print("진단 완료")


if __name__ == "__main__":
    main()
