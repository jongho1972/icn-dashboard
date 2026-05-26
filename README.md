# ICN Dashboard — 인천공항 국제선 항공편수 월간 비교

인천국제공항 [공식 OpenAPI](https://www.data.go.kr/data/15112968/openapi.do)로 운항 정보를 수집해 **이번달 vs 전월 동일기간** 항공편수를 비교하는 사내 대시보드.

- 라이브: <https://flight.jhawk.kr> (비번 `0708`)
- 백엔드: FastAPI + Plotly.js (메모리 + 디스크 pickle 이중 캐시, TTL 48h)
- 배포: j-hawk VPS (Hetzner CAX11 ARM · Docker Compose + Caddy)

## 화면 구성

1. **일자별** 섹션
   - D+1일 예정 편수 요약 + T1·T2 분리 라인 차트 (이번달 실선·전월 점선·전월 평균선·Today 수직선·주말/공휴일 빨강)
   - 일자별 표: 날짜·요일·T1[전월·이번달·전월동요일비]·T2[전월·이번달·전월동요일비]
2. **월누적**: T1+T2 합계 · 전월비 ±N% · 일평균
3. **항공사별** 표 (T1·T2 그룹)
4. **도착지별** 표 (7권역: 일본·동남아·중국·미주·동북아·유럽·기타)
5. **탑승구별** 표 (동편/중앙/서편/탑승동)
6. **Raw 데이터 CSV 다운로드**: 최대 1년 범위, 18개 컬럼 (`/api/export-raw`)

## 데이터 수급

- **이번달**: API 실시간(D-3 ~ D+6) + `Daily_Data/` 과거 일별 pkl 병합
- **지난달**: `Final_Data/flight_schedule_YYYYMM_cum.pkl` 우선, 없으면 `Daily_Data` 재가공
- API는 D-3 ~ D+6 10일치만 반환 → 매일 백필 누적 필수

## 집계 규칙

- `CODESHARE == "Master"`만 카운트 (빈값/결측은 Master로 정규화 — 2025-10 단독 운항편 누락 사고 방지)
- 국내선 제외 — `typeOfFlight == "I"` 우선, fallback `지역 != "국내선"`
- 결항·회항 제외 (`remark` 기준)
- dedup 키: `fid` 우선, 누락 시 `Flight_Key`(편명+일자) — 자정 넘기는 편 중복 방지

**탑승구 분류** (T1·T2 공용)

| 그룹 | 게이트 번호 |
|---|---|
| 동편 | 1 ~ 24 또는 251 ~ 299 |
| 중앙 | 25 ~ 28 |
| 서편 | 29 ~ 99 또는 200 ~ 250 |
| 탑승동 | 100 ~ 199 |

**항공사 그룹**: 대한항공 · 아시아나 · 진에어 · 제주항공 · 티웨이 · 국내기타 · 중국(본토 14사) · 중국외

> 2024/12 대한항공-아시아나 합병 후 **아시아나는 T2**, 에어부산·에어서울도 T2.

## 자동화

> GH Actions cron 큐 지연(+1~3h) 보정을 위해 모든 스케줄은 의도한 KST 도착 시각보다 2~3시간 일찍 예약. 메일러 시각이 어긋나면 `gh run list`로 지연 재측정.

| 워크플로우 | 의도한 도착 KST | 동작 |
|---|---|---|
| `daily-backfill.yml` | 17:00 | cron-job.org 외부 트리거 → `backfill.py` → `Daily_Data/` push → VPS 자동 재배포 |
| `refresh-cache.yml` | 10:00 · 17:00 | `POST /api/refresh` (X-Refresh-Token) |
| `monthly-cum.yml` | 매월 3일 05:00 | 직전 월 Daily_Data → `Final_Data/*_cum.pkl` push (부분 cum 방지 exit 1 + SMTP 통지) |
| `daily-mailer.yml` | 17:30 | Playwright 캡처 → `send_daily_email.py` SMTP 발송 |
| cron-job.org | 14분 간격 | `GET /healthz` 캐시 워밍 |

## 로컬 실행

```bash
pip install -r requirements.txt
uvicorn main:app --reload --port 8000
```

`.env`에 `INCHEON_API_KEY` 필요.

## 환경변수

| 키 | 용도 |
|---|---|
| `INCHEON_API_KEY` | 인천공항 OpenAPI 키 |
| `REFRESH_TOKEN` | `/api/refresh` 요청 인증 |
| `GITHUB_TOKEN` | (옵션) 워크플로우 디스패치용 |

## Final_Data cum pkl 재생성

```bash
python3 build_final_cum.py 202604
# 또는
gh workflow run monthly-cum.yml -f yyyymm=202604
```

`process_raw`가 `typeOfFlight`·`fid` 컬럼을 보존해 강한 필터·강한 dedup이 자동 적용된다.

## 라이선스

내부용 (private). 데이터 출처: 인천국제공항공사 OpenAPI(data.go.kr).
