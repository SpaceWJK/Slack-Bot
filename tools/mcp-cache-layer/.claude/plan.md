# Wiki 파서 검증 시스템 구현 계획

## 목표
Confluence 페이지의 스크린샷(렌더된 이미지)과 파서 출력(텍스트)을 비교하여
파서가 핵심 텍스트 정보를 온전히 추출하는지 검증하고 고도화한다.

## 아키텍처

```
[REST API body.view] → [로컬 HTML 저장] → [Playwright 렌더+스크린샷]
                                                    ↓
[캐시 DB body_raw] → [파서 실행] → [파싱 텍스트]  ←→ [Claude Code 비교 검증]
                                                    ↓
                                            [정합성 리포트]
```

## 인증 방식
- Confluence 페이지 URL은 SAML SSO → Playwright 직접 접근 불가
- REST API `body.view`는 Bearer 토큰으로 접근 가능 (검증 완료)
- **방식**: REST API로 렌더 HTML 가져와 로컬에서 Playwright 렌더링

## 구현 파일

### 1. `scripts/capture_screenshots.py` — 스크린샷 캡처 스크립트

**역할**: Wiki 페이지별 body.view HTML을 가져와 로컬 렌더링 후 스크린샷

**동작**:
1. 캐시 DB에서 body_raw 있는 wiki 페이지 목록 조회
2. 각 페이지: REST API body.view → Confluence CSS 포함 HTML 템플릿 래핑
3. Playwright (Chromium headless)로 렌더링 → PNG 스크린샷
4. `cache/screenshots/{page_id}.png` 저장
5. 진행 상황 로깅

**주요 설계**:
- Confluence AUI CSS CDN 링크 포함 (테이블/매크로 기본 렌더링)
- expand 매크로: CSS로 기본 펼침 처리
- viewport: 1280px 폭, 전체 높이 스크롤 캡처 (full_page=True)
- 동시성: 순차 처리 (서버 부하 방지, 페이지당 ~1초 예상)
- 재시도: 실패 시 1회 재시도
- 이미 스크린샷 있으면 스킵 (--force로 덮어쓰기)

**예상 소요**: 2,842페이지 × ~1초 = ~50분

### 2. `scripts/validate_parser.py` — 파서 검증 스크립트

**역할**: 캐시 DB의 body_raw에 wiki_client.py의 새 파서를 적용하고 결과를 저장

**동작**:
1. 캐시 DB에서 body_raw 로드
2. wiki_client.py의 `_ConfluenceHTMLExtractor` 파서 실행
3. 기존 body_text (sync_engine의 regex 파서)와 비교
4. 결과를 `cache/parser_results/{page_id}.json`에 저장:
   ```json
   {
     "page_id": "552936235",
     "title": "서비스 장애 리포트",
     "raw_len": 43589,
     "old_parser_len": 1200,
     "new_parser_len": 1913,
     "new_parser_text": "...",
     "screenshot_path": "cache/screenshots/552936235.png"
   }
   ```

### 3. Claude Code 수동 검증 워크플로우

스크린샷 이미지(Read 도구)와 파싱 텍스트를 나란히 비교:
- 이미지에 보이는 텍스트가 파싱 결과에 있는지
- 파싱 결과에 불필요한 노이즈가 있는지
- 누락된 핵심 정보 식별 → 파서 수정

## 파일 구조 (추가)

```
mcp-cache-layer/
├── cache/
│   ├── screenshots/          # 스크린샷 PNG (page_id.png)
│   └── parser_results/       # 파서 검증 결과 JSON
├── scripts/
│   ├── capture_screenshots.py
│   └── validate_parser.py
└── templates/
    └── confluence_viewer.html  # body.view 래핑용 HTML 템플릿
```

## 의존성 추가
- `playwright` (pip install playwright && playwright install chromium)
- requests는 이미 설치됨

## 실행 순서

1. Playwright 설치
2. capture_screenshots.py 실행 (전체 2,842페이지)
3. validate_parser.py 실행 (전체 페이지 파싱)
4. Claude Code에서 페이지별 이미지+텍스트 비교 검증
5. 파서 수정 → 재검증 반복
6. 최종 정합성 리포트 작성
