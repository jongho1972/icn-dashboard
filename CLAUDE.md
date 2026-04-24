# ICN_Dashboard

인천공항 국제선 출발편 현황 월간 비교 대시보드 (FastAPI + Plotly.js → Render)

## 구성

| 파일 | 역할 |
|------|------|
| `main.py` | FastAPI 앱 — `/` 라우트, 1시간 TTL 인메모리 캐시, HTML 테이블 생성 |
| `templates/index.html` | Jinja2 템플릿 — CSS·Plotly 차트 2개 (T1·T2 분리) |
| `icn_utils/data_loader.py` | 인천공항 API 호출 + Daily/Final pkl 로드 + 가공 |
| `icn_utils/aggregator.py` | 월간 비교 집계 (전체·일자별·항공사별·도착지별·게이트별) |
| `icn_utils/__init__.py` | 패키지 초기화 (Python 3.14 import 이슈 회피용) |
| `backfill.py` | cron용 일별 API 수집 스크립트 |
| `항공편목적지.txt` | 공항코드 → 국가·지역 매핑 |
| `Daily_Data/` | 일별 원본 pkl (매일 누적) |
| `Final_Data/` | 완료된 월의 가공된 cum pkl |
| `render.yaml` | Render 배포 설정 (python runtime, uvicorn) |
| `requirements.txt` | fastapi, uvicorn, jinja2, pandas, requests, holidays 등 |
| `.env` | `INCHEON_API_KEY` (gitignore) |

## 로컬 실행

```bash
uvicorn main:app --reload --port 8000
```

`.env`에 `INCHEON_API_KEY=...` 필요.

## 데이터 수급 흐름

- **이번달**: 최근 10일치(D-3~D+6) API 실시간 + `Daily_Data/` 과거 일별 pkl 병합 → 가공
- **지난달**: `Final_Data/flight_schedule_YYYYMM_cum.pkl` 우선, 없으면 `Daily_Data` 재가공
- **매일 자동 수집**: Claude Code 스케줄 트리거 → `backfill.py` 실행 → `Daily_Data/` 갱신 + git push → Render 자동 재배포
- **캐싱**: 1시간 TTL 인메모리 캐시 (모든 요청 공유). 새로고침 시 즉시 표시, 1시간 경과 첫 요청에서만 API 재호출.

## 집계 규칙

- `CODESHARE == "Master"`만 카운트 (공동운항 편 제외)
- 국내선 제외
- 지난달·이번달 동일기간 비교 (양쪽 모두 `DD <= 이번달_max_day` 필터)

**게이트 분류 (T1·T2 공용):**
- **동편**: 1 ~ 24 또는 251 ~ 299
- **중앙**: 25 ~ 28
- **서편**: 29 ~ 99 또는 200 ~ 250
- **탑승동**: 100 ~ 199

**항공사 그룹:**
- `대한항공` · `아시아나`(아시아나항공, T2로 이전) · `진에어` · `제주항공` · `티웨이`(티웨이항공)
- `국내기타`: 이스타 · 에어부산 · 에어서울 · 에어프레미아 · 플라이강원 · 하이에어 · 파라타 · 에어인천 · 플라이나스
- `외국항공`: 그 외

**도착지(지역) 그룹 (7개):**
- 일본 · 동남아 · 중국 · 미주 · 동북아 · 유럽 · 기타 (중동·대양주는 '기타'로 통합 — `REGION_MERGE`)

## 배포

- **Render (무료 플랜)**: GitHub 푸시 시 자동 재빌드
- URL: <https://jhawk-flight-schedule.onrender.com>
- Env: `INCHEON_API_KEY` (Render Dashboard → Environment)

## 자동 수집

- **Claude Code 스케줄 트리거** `trig_01KXfKu4nJ4A1asgvekGCiBN`
  - 스케줄: `0 8 * * *` UTC = 매일 17:00 KST
  - 동작: 원격 에이전트가 레포 clone → `backfill.py` 실행 → `Daily_Data/` 갱신 → 변경 있으면 `git push origin main`
  - 관리: <https://claude.ai/code/scheduled/trig_01KXfKu4nJ4A1asgvekGCiBN>

## 참고

- 인천공항 API: <https://www.data.go.kr/data/15112968/openapi.do>
- D-3 ~ D+6 10일치만 반환. 그 외 기간은 누적 데이터 필요.
- 2024/12 대한항공-아시아나 합병 후 **아시아나는 T2**, 에어부산·에어서울도 T2.
- Plotly.js CDN (`https://cdn.plot.ly/plotly-2.35.3.min.js`) 로드 후 client-side 렌더.
