# 공통 시간 표현 변환 규칙 (wiki/gdi 공유)

## 절대 날짜 변환 (today 기준)

| 표현 | date_from | date_to |
|------|-----------|---------|
| 오늘 | today | today |
| 어제 | yesterday | yesterday |
| 이번 주 | 이번주_월요일 | today |
| 이번 달 | 이번달_1일 | today |
| 지난 주 | 지난주_월요일 | 지난주_일요일 |
| 지난 달 | 지난달_1일 | 지난달_말일 |
| 최근 N일 | today-N일 | today |
| N일 전 | today-N일 | today-N일 |
| N월 M일 | {year}-N-M | {year}-N-M |
| N월 | {year}-N-01 | {year}-N-말일 |

## 상한 exclusive 처리 (MAJOR-NEW-5 v4)

Query builder에서 date_to → `_next_day(date_to)` 로 변환하여 exclusive 상한 적용.
예: date_to='2026-04-29' → SQL params에 '2026-04-30' 전달.
`dm.last_modified < '2026-04-30'` → 4월 29일 종일 포함.

## 시간 표현 segment 제거 (MAJOR-NEW-2 v4 — gdi 한정)

gdi path_segments에 시간 표현이 포함된 경우:
- Claude는 split 결과 그대로 path_segments에 채움
- Query builder(_strip_time_expressions)가 시간 표현 segment를 자동 제거
- 제거된 segment는 ref_date_from/to로 이미 변환되어 있어야 함
