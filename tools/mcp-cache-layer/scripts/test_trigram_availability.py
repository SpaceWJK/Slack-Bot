"""test_trigram_availability.py — in-memory SQLite trigram 지원 확인 (task-102)

SQLite 3.35+ 에서 FTS5 trigram 토크나이저를 사용할 수 있는지 확인한다.
migrate_v4_to_v5.py --apply 실행 전 반드시 exit 0 확인 필요.

사용법:
    python scripts/test_trigram_availability.py
"""
import sqlite3
import sys


def check_trigram() -> bool:
    """in-memory DB에서 trigram FTS5 테이블 생성 + MATCH 검증."""
    try:
        conn = sqlite3.connect(":memory:")
        conn.execute("""
            CREATE VIRTUAL TABLE _t USING fts5(body, tokenize='trigram')
        """)
        conn.execute("INSERT INTO _t VALUES ('기획서 v2.docx')")
        # trigram은 3자 이상만 MATCH 가능 (2자 이하는 LIKE 폴백 사용)
        rows = conn.execute("SELECT * FROM _t WHERE _t MATCH '기획서'").fetchall()
        conn.close()
        if rows:
            print("OK: trigram 지원 확인")
            return True
        print("WARN: MATCH 결과 없음 - trigram 인덱싱 이상 (SQLite 버전 확인 필요)")
        return False
    except Exception as e:
        print(f"FAIL: {e}")
        return False


if __name__ == "__main__":
    sys.exit(0 if check_trigram() else 1)
