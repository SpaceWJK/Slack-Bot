"""Regression guard — s3_server.py must NEVER call webbrowser.open().

ISS-012 v3 + task-132 cycle (2026-05-12):
  pm2 restart loop x webbrowser.open = Chrome tabs x 4029 -> PC mabi.
  Surface fix attempted: --silent flag (still triggers if flag missing).
  Root cause fix: webbrowser.open call removed entirely.

This test enforces the root cause fix at source level — preventing future
contributors from re-introducing the call accidentally.
"""

from pathlib import Path

S3_SERVER_PATH = Path(__file__).resolve().parents[1] / "tools" / "s3_server.py"


def _strip_inline_comments(src: str) -> str:
    """Best-effort strip of `# ...` inline comments to avoid false positives in
    explanatory text. Does not handle triple-quoted strings — acceptable for
    this specific regression guard (the target pattern doesn't appear in any
    legitimate string literal in the codebase).
    """
    out = []
    for line in src.split("\n"):
        if "#" in line:
            line = line.split("#", 1)[0]
        out.append(line)
    return "\n".join(out)


def test_s3_server_does_not_call_webbrowser_open():
    """The literal call `webbrowser.open(` must not appear in active code."""
    assert S3_SERVER_PATH.exists(), f"missing file: {S3_SERVER_PATH}"
    src = S3_SERVER_PATH.read_text(encoding="utf-8")
    code = _strip_inline_comments(src)

    assert "webbrowser.open(" not in code, (
        "webbrowser.open() detected in s3_server.py — 4029-tab incident regression risk. "
        "Root cause fix (ISS-012 v3 + task-132): the auto-open call must remain removed."
    )


def test_s3_server_does_not_import_webbrowser():
    """Defense-in-depth: even importing `webbrowser` should not be needed once
    the call is removed. If a future change re-adds the import, this catches it.
    """
    src = S3_SERVER_PATH.read_text(encoding="utf-8")
    code = _strip_inline_comments(src)

    assert "import webbrowser" not in code, (
        "`import webbrowser` detected in s3_server.py — likely re-introducing "
        "the auto-open behavior. Keep this module free of webbrowser usage."
    )
