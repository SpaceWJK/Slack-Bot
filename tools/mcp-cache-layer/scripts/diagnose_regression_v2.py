"""regression 상위 10건 정밀 진단 — wiki_client의 실제 파서 사용."""
import sys
import os
import re
import html as _html
import json
import difflib
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, "D:/Vibe Dev/Slack Bot/Slack Bot")

from src.models import init_db, get_connection
from wiki_client import _strip_html as new_strip_html  # 실제 파서


def old_strip_html(html_text):
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


def find_lost_content(old_text, new_text):
    """기존에만 있고 신규에 없는 텍스트 블록 추출."""
    old_lines = old_text.split('\n')
    new_lines = new_text.split('\n')

    diff = difflib.unified_diff(new_lines, old_lines, n=0)

    lost_blocks = []
    for line in diff:
        if line.startswith('+') and not line.startswith('+++'):
            content = line[1:].strip()
            if content and len(content) > 5:
                lost_blocks.append(content)
    return lost_blocks


def main():
    init_db()
    conn = get_connection()

    report_path = PROJECT_ROOT / "cache" / "parser_results" / "_report.json"
    with open(report_path, "r", encoding="utf-8") as f:
        report = json.load(f)

    regressions = report["regressions"]
    output_lines = []

    def pr(s=""):
        print(s)
        output_lines.append(s)

    pr("=" * 80)
    pr("REGRESSION 정밀 진단 (wiki_client 실제 파서)")
    pr("=" * 80)

    for i, reg in enumerate(regressions):
        page_id = reg["page_id"]
        title = reg["title"]

        row = conn.execute(
            """SELECT dc.body_raw FROM doc_content dc
               JOIN nodes n ON dc.node_id = n.id
               WHERE n.source_id = ? AND n.source_type = 'wiki'""",
            (page_id,)
        ).fetchone()

        if not row or not row[0]:
            pr(f"\n[{i+1}] {page_id} '{title}' -- DB body_raw 없음")
            continue

        html = row[0]
        old_text = old_strip_html(html)
        new_text = new_strip_html(html)

        pr(f"\n{'='*80}")
        pr(f"[{i+1}/10] page_id={page_id}")
        pr(f"  제목: {title}")
        pr(f"  HTML: {len(html)}자")
        pr(f"  기존: {len(old_text)}자 | 신규: {len(new_text)}자 | 차이: {len(new_text)-len(old_text)}")

        # 누락 콘텐츠 추출
        lost = find_lost_content(old_text, new_text)

        if not lost:
            pr(f"  누락 블록: 없음 (공백/포맷 차이만)")
            continue

        # 누락 내용 분류
        noise_keywords = [
            'false', 'true', 'Sparkline', 'Filtration', 'panel',
            'Point', 'Comma', 'noneEnabled', 'Disabled',
            'chart', 'filter', 'menu', 'dropdown',
        ]

        meaningful = []
        noise = []
        for block in lost:
            is_noise = any(kw.lower() in block.lower() for kw in noise_keywords)
            if is_noise:
                noise.append(block)
            else:
                meaningful.append(block)

        total_lost_chars = sum(len(b) for b in lost)
        noise_chars = sum(len(b) for b in noise)
        meaningful_chars = sum(len(b) for b in meaningful)

        pr(f"  누락 블록 수: {len(lost)}개 (총 {total_lost_chars}자)")
        pr(f"    노이즈: {len(noise)}개 ({noise_chars}자)")
        pr(f"    유의미: {len(meaningful)}개 ({meaningful_chars}자)")

        if noise:
            pr(f"  [노이즈 샘플] (상위 3):")
            for j, block in enumerate(noise[:3]):
                preview = block[:120] + ("..." if len(block) > 120 else "")
                pr(f"    N{j+1}. {preview}")

        if meaningful:
            pr(f"  [유의미 콘텐츠 샘플] (상위 5):")
            for j, block in enumerate(meaningful[:5]):
                preview = block[:120] + ("..." if len(block) > 120 else "")
                pr(f"    M{j+1}. {preview}")

        # 특이 HTML 패턴 분석
        ac_param_count = len(re.findall(r'<ac:parameter', html))
        select_count = len(re.findall(r'<select', html, re.IGNORECASE))
        form_count = len(re.findall(r'<form', html, re.IGNORECASE))
        display_none = len(re.findall(r'display:\s*none', html))
        skip_class_count = sum(
            len(re.findall(cls, html))
            for cls in ['table-filter', 'chart-controls', 'tf-body-storage',
                        'aui-dropdown', 'tfac-menu']
        )

        pr(f"  [HTML 패턴]")
        pr(f"    ac:parameter: {ac_param_count} | select: {select_count} | form: {form_count}")
        pr(f"    display:none: {display_none} | skip-class 매칭: {skip_class_count}")

    conn.close()

    # 결과 파일 저장
    out_path = PROJECT_ROOT / "cache" / "parser_results" / "_regression_diagnosis.txt"
    out_path.write_text("\n".join(output_lines), encoding="utf-8")
    pr(f"\n결과 저장: {out_path}")


if __name__ == "__main__":
    main()
