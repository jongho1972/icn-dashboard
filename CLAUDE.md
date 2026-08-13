# ICN_Dashboard

인천공항 국제선 항공편수 월간 비교 대시보드 (FastAPI + Plotly.js → j-hawk VPS)

## 접근 제어
- `templates/index.html` 상단 인라인 비번 게이트 (`<style id="auth-gate">`)
- 비번: `0708`, sessionStorage 키: `icn_dashboard_auth_ok` (신라 사이트 `shilla_auth_ok`와 키 분리)
- 게이트는 `visibility:hidden` 방식 — 레이아웃 유지로 Plotly 차트가 컨테이너 너비 0이 아닌 정상 너비(1120px)로 렌더 보장
- 세션 복원 감지(`navType==='back_forward' && !sameOriginRef`) 시 인증 무효화

## 이미지 캡처(클립보드 복사)
- html2canvas `scale: 4` + 다단 다운샘플링 → `TARGET_WIDTH: 1200px` PNG
- 1200px가 메일 붙여넣기 시 적당한 크기 (이전 1600px는 메일에서 다소 큼)

## 구성

| 파일 | 역할 |
|------|------|
| `main.py` | FastAPI 앱 — `/` 라우트, 1시간 TTL 인메모리 캐시, HTML 테이블 생성 |
| `templates/index.html` | Jinja2 템플릿 — CSS·Plotly 차트 2개 (T1·T2 분리) |
| `icn_utils/data_loader.py` | 인천공항 API 호출 + Daily/Final pkl 로드 + 가공 |
| `icn_utils/aggregator.py` | 월간 비교 집계 (전체·일자별·항공사별·도착지별·게이트별) |
| `icn_utils/__init__.py` | 패키지 초기화 (Python 3.14 import 이슈 회피용) |
| `backfill.py` | cron용 일별 API 수집 스크립트 |
| `backfill_web.py` | **폴백** 일별 수집 — data.go.kr API가 막혔을 때 airport.kr 출발 시간표 JSON에서 동일 스키마 수집 (인증키 불필요) |
| `항공편목적지.txt` | 공항코드 → 국가·지역 매핑 |
| `Daily_Data/` | 일별 원본 pkl (매일 누적) |
| `Final_Data/` | 완료된 월의 가공된 cum pkl |
| `Dockerfile` | VPS Docker Compose 빌드 (python:3.11-slim · uvicorn) |
| `render.yaml` | (deprecated) 구 Render 배포 설정. VPS 전환 후 미사용 |
| `requirements.txt` | fastapi, uvicorn, jinja2, pandas, requests, holidays 등 |
| `Raw_Data_Format.txt` | `/api/export-raw` CSV 컬럼 순서·샘플 레퍼런스 (18개 컬럼) |
| `.env` | `INCHEON_API_KEY` (gitignore) |

## 로컬 실행

```bash
uvicorn main:app --reload --port 8000
```

`.env`에 `INCHEON_API_KEY=...` 필요.

## 데이터 수급 흐름

- **이번달**: 최근 10일치(D-3~D+6) API 실시간 + `Daily_Data/` 과거 일별 pkl 병합 → 가공
- **지난달**: `Final_Data/flight_schedule_YYYYMM_cum.pkl` 우선, 없으면 `Daily_Data` 재가공
- **매일 자동 수집**: cron-job.org 외부 트리거 → GH Actions `backfill.py` 실행 → `Daily_Data/` 갱신 + git push → VPS 자동 재배포
- **수집 폴백 (2026-07-29~)**: `backfill.py`가 실패하면 워크플로우가 `backfill_web.py`(airport.kr)로 자동 전환. 상세는 아래 "데이터 소스 폴백" 참조
- **캐싱**: 메모리 + 디스크 pickle 이중 캐시 (`/tmp/icn_dashboard_cache.pkl`). TTL 48시간(cron 누락 안전 마진). 모든 요청 공유, 재시작 시 디스크에서 즉시 로드.
- **캐시 갱신**: 매일 10:00 / 17:00 KST에 GitHub Actions cron이 `/api/refresh` 호출. 그 외 시간은 디스크 캐시로 즉시 응답(캐시 히트 ~4ms).

## 집계 규칙

- `CODESHARE == "Master"`만 카운트 (공동운항 편 제외)
  - **process_raw가 빈값/결측 CODESHARE를 `Master`로 정규화**(`Slave`만 보존)한다. API가 코드셰어 없는 단독 운항편의 codeshare를 빈값으로 주는 월(예: 2025-10)이 있어, 정규화 없이는 단독편이 통째로 누락돼 과소집계됨(2026-05-24 수정). 빈값 편은 Master_Flight도 비어 슬레이브가 아니라 단독 운항편임이 확인됨
- 국내선 제외 — `typeOfFlight == "I"` (API 명세 공식 필드) 우선, 컬럼 누락 시 `지역 != "국내선"` fallback
- **결항·회항 제외** (`remark` 값이 "결항" 또는 "회항"인 건 제외)
- 지난달·이번달 동일기간 비교 (양쪽 모두 `DD <= 이번달_max_day` 필터)
- **dedup 키**: process_raw 내부에서 `fid`(API 명세상 unique) 우선, 누락 시 `Flight_Key`(편명+일자) fallback. 자정 넘기는 편의 estimatedDateTime 변경으로 같은 운항이 두 Flight_Key로 분리되는 케이스를 방지

> **Final_Data cum pkl 재생성**: `build_final_cum.py YYYYMM` 또는 monthly-cum 워크플로우(매월 1·2일 자동) 사용. process_raw가 `typeOfFlight`·`fid` 컬럼을 보존해 강한 필터·강한 dedup이 자동 적용된다. 외부 노트북 수동 작업 불필요.

**게이트 분류 (T1·T2 공용):**
- **동편**: 1 ~ 24 또는 251 ~ 299
- **중앙**: 25 ~ 28
- **서편**: 29 ~ 99 또는 200 ~ 250
- **탑승동**: 100 ~ 199

**항공사 그룹:**
- `대한항공` · `아시아나`(아시아나항공, T2로 이전) · `진에어` · `제주항공` · `티웨이`(티웨이항공)
- `국내기타`: 이스타 · 에어부산 · 에어서울 · 에어프레미아 · 플라이강원 · 하이에어 · 파라타 · 에어인천 · 에어로케이
- `중국`: 중국 본토 국적 14사 (중국국제·남방·동방·해남, 산동·상하이·샤먼·심천·사천·북경수도·천진·청도·춘추·길상). 홍콩·마카오·대만은 제외
- `중국외`: 그 외 (홍콩·대만·마카오·일본·동남아·중동·구미주 등)

**도착지(지역) 그룹 (7개):**
- 일본 · 동남아 · 중국 · 미주 · 동북아 · 유럽 · 기타 (중동·대양주는 '기타'로 통합 — `REGION_MERGE`)

## 화면 구성 (위→아래 순서)

1. **일자별** 섹션
   - **D+1일 예정 편수 요약** (`.summary` 텍스트): `M/D(요일) 항공편수` + T1/T2 편수 + 전월 동요일 평균 대비 `+N편(+N%)` 색상 span (파랑=증가·빨강=감소)
   - **차트 2개** (T1 파랑, T2 주황, 세로 분리): 이번달 실선+마커 + 전월 점선 + 전월 평균 가로선(라벨은 y축 **바깥** 좌측에 배치) + Today 수직선(슬레이트) + 주말·공휴일 x축 빨강
   - **일자별 표**: 행=일자, 컬럼=`날짜·요일·T1[전월·이번달·전월동요일비]·T2[전월·이번달·전월동요일비]`. **D+1 ~ 월말** 노란 배경(`future-row`, 오늘은 하이라이트 제외), 토·일·공휴일 빨강
     - **전월동요일비**: 같은 요일 평균 대비 비율 (예: 4월 7일=월 → 3월 모든 월요일 평균과 비교)
2. **월누적** 섹션
   - 요약 텍스트: `T1+T2 기준 N 편 (전월비 ±N.N%) · 일평균 N 편`
   - 전체 표 (T1·T2 × 월누적·일평균)
3. **항공사별** 표 (T1·T2 그룹)
4. **도착지별** 표 (T1·T2 그룹)
5. **탑승구별** 표 (T1·T2 그룹) + section-note
6. **각주**: 데이터 출처 / 집계 제외 기준 / 탑승구 분류 기준

## Raw 데이터 CSV 다운로드 (`/api/export-raw`)

- 모달 UI: 날짜 범위 선택(최대 1년) → 진행 상태바(서버 생성 중 indeterminate → 전송 중 바이트 %) → 완료 시 900ms 후 자동 닫힘
- 서버: `StreamingResponse` + `Content-Length` 헤더(클라이언트 % 계산 근거)
- 컬럼 순서: `Raw_Data_Format.txt` 기준 18개 (YYYYMMDD · 출발시간 · 목적지 · 항공사 · 운항편명 · 터미널 · 체크인 카운터 · 탑승구 · remark · CODESHARE · 항공사 구분 · 국가 · 도착지 구분 · 게이트 구분 · Master_Flight · scheduleDateTime · estimatedDateTime · Flight_Key)
- 출발시간은 `="HH:MM"` 수식 형태로 저장 — Excel이 시간 타입으로 자동 변환하지 않고 `06:00`처럼 0 패딩 유지. 단, Google Sheets·pandas 등 다른 파서는 문자열 그대로 읽음

## 배포

- **j-hawk VPS** (Hetzner CAX11 ARM · Docker Compose + Caddy): GitHub `main` 푸시 시 `deploy.yml`이 VPS SSH → `git reset --hard` → `docker compose build/up flight` → `/healthz` 체크
- URL: <https://flight.jhawk.kr>
- Env: `INCHEON_API_KEY`, `GITHUB_TOKEN`, `REFRESH_TOKEN` (VPS `/opt/j-hawk/deploy/.env.flight`)
- 공통 인프라·롤백·트러블슈팅: 워크스페이스 루트 `deploy/README.md` 또는 `vps-deploy` 스킬

## 자동화

> **GH Actions cron 큐 지연 보정**: 무료/private 환경에서 cron 예약이 평균 +2h(오전 refresh는 +3h) 지연 발사됨. 모든 스케줄은 의도한 KST 도착 시각보다 2~3시간 일찍 예약되어 있음. 실제 도착 시각이 어긋나면 최근 `gh run list`로 평균 지연을 재측정해 cron을 재조정할 것.

- **GitHub Actions** `.github/workflows/daily-backfill.yml` (Daily_Data 수집)
  - 트리거: **cron-job.org 외부 트리거** (workflow_dispatch) — 17:00 KST 정시 발사
  - GH Actions schedule는 큐 지연(+1~3h)으로 17:30 메일러 시각을 못 맞출 위험. icn-pax-congestion 5/12 stale 사고 같은 방식의 재발 가능성 사전 차단 → cron-job.org 이전 (2026-05-13)
  - 동작: GH-hosted runner가 `backfill.py` 실행 → `Daily_Data/` 갱신 → 변경 있으면 `git push origin main` → VPS 자동 재배포 (~1-2분)
  - **폴백 분기**: `backfill.py` 스텝은 `continue-on-error`. 실패하면 `backfill_web.py`(airport.kr)가 실행되고, 커밋 메시지에 `(web fallback)`이 붙는다. **폴백 성공은 메일 알림 없음**(2026-08-13~, 해외 IP 차단이 구조적이라 상시 경로로 확정돼 매일 알림이 노이즈였음). 양쪽 다 실패해야 `[실패]` 메일 + 워크플로우 실패
  - Secret: `INCHEON_API_KEY` (GitHub repo secret)
  - 이전 Claude Code 라우틴 `trig_01KXfKu4nJ4A1asgvekGCiBN`은 Anthropic CCR이 `apis.data.go.kr`을 host_not_allowed로 차단해 GH Actions로 마이그레이션 (2026-04-29)
- **외부 cron** cron-job.org (페이로드 캐시 워밍 유지용 — VPS 전환 후 슬립 방지 목적은 없음)
  - 14분 간격 `GET /healthz`
- **GitHub Actions** `.github/workflows/refresh-cache.yml` (캐시 갱신)
  - cron 예약: `0 22,6 * * *` UTC = 07:00 KST(다음날) / 15:00 KST → 큐 지연 흡수 후 실제 ~10:00 / ~17:00 KST 도착
  - 동작: `POST /api/refresh` (헤더 `X-Refresh-Token: ${{ secrets.REFRESH_TOKEN }}`)
- **GitHub Actions** `.github/workflows/monthly-cum.yml` (Final_Data cum pkl 자동 생성)
  - cron 예약: `0 18 2 * *` UTC = 3일 03:00 KST → 큐 지연 후 실제 ~05:00 KST 도착
  - 동작: `build_final_cum.py` 실행 → 직전 월 Daily_Data를 process_raw로 가공 → `Final_Data/flight_schedule_YYYYMM_cum.pkl` 저장 → push
  - 부분 cum 방지: 말일까지 DD가 채워지지 않으면 exit 1 + SMTP 통지
  - 수동 재생성: `gh workflow run monthly-cum.yml -f yyyymm=202604` 또는 로컬에서 `python3 build_final_cum.py 202604`
- **GitHub Actions** `.github/workflows/daily-mailer.yml` (대시보드 일일 메일링)
  - cron 예약: `30 6 * * *` UTC = 15:30 KST → 큐 지연 흡수 후 실제 ~17:30 KST 도착
  - 동작: Playwright로 대시보드 캡처 → `send_daily_email.py`로 `mailing_list.txt` 수신자에게 SMTP 발송
  - `workflow_dispatch` 입력 `test_recipient` 지원 (입력 시 해당 1명에게만 발송)
  - 실패 시(`if: failure()`) `jongho1972@gmail.com`로 자동 통지 (Gmail SMTP)
  - Secrets: `GMAIL_USER`, `GMAIL_APP_PASSWORD`, `MAIL_RECIPIENTS`, `DASHBOARD_PASSWORD`

## 데이터 소스 폴백 (2026-07-29~)

**사건**: 2026-07-29 17:00 KST 수집분부터 data.go.kr `B551177`(인천공항공사) 전 엔드포인트 접근 불가.

**최종 진단 (2026-08-13 재확인)**: 원인은 활용신청 만료가 아니라 **해외 서버 IP 구조적 차단**으로 확정.
- 활용신청은 이미 재승인 완료(활용기간 2026-08-07~2028-08-07, 처리상태 승인)됐는데도 GH Actions에서는 여전히 매일 실패 → 활용신청 문제가 아님을 재확인
- 로컬(한국 IP): 동일 서비스키로 HTTP·HTTPS 모두 `200 NORMAL SERVICE`
- GitHub Actions(Azure 데이터센터 IP): `ConnectTimeout`(TCP 연결 자체가 30초간 무응답) — 방화벽 레벨에서 패킷 드롭
- j-hawk VPS(Hetzner, 핀란드 IP): TCP 연결은 되지만 동일 유효 키로 `403 SERVICE_KEY_IS_NOT_REGISTERED_ERROR`(등록되지 않은 서비스키) — 실제로는 키가 유효한데도 이 에러가 뜨는 건 WAF/CDN이 해외 IP를 걸러내며 가짜 인증 에러를 돌려주는 지오블록 패턴
- **결론**: 국내 IP가 아니면 GH Actions든 VPS든 어떤 해외 클라우드에서도 우회 불가능. `activation(활용신청) 재연장은 무의미`하며, **airport.kr 폴백이 사실상 상시 정식 경로**다.
- 폴백 사용 자체는 정상 동작이라 매일 메일 알림은 발송하지 않음(2026-08-13~). 양쪽 다 실패했을 때만 `[실패]` 메일 발송.

**타 프로젝트 교차검증 (2026-08-13)**: 해외 IP 차단이 `apis.data.go.kr`(인천공항공사 `B551177`) 고유 문제인지, data.go.kr 플랫폼 전반의 문제인지 확인하기 위해 신라 산하 자매 프로젝트 2개를 동일 방식(VPS·GH Actions에서 실제 키로 직접 호출)으로 점검함.
- `관광통계조회`(pax2, 한국문화관광연구원 `openapi.tour.go.kr`): VPS에서 직접 호출 → `200 OK` 정상. 영향 없음
- `출국장이용객수조회`(pax, airport.kr 기반이라 애초에 무관): GH Actions 로그 정상 완료 확인. 영향 없음
- **결론**: 해외 IP 차단은 `apis.data.go.kr` B551177(인천공항공사) 엔드포인트에 한정된 문제. 같은 data.go.kr 플랫폼이라도 제공 기관별 방화벽/WAF 정책이 달라 일반화할 수 없음 — 향후 data.go.kr 신규 연동 시 기관마다 별도 검증 필요.

**폴백 소스**: airport.kr 여객 출발 시간표의 내부 JSON 엔드포인트.

```
POST https://www.airport.kr/dep/ap_ko/getDepPasSchList.do
siteId=ap_ko&langSe=ko&daySel=YYYYMMDD&curDate=YYYYMMDD
&fromTime=0000&toTime=2359&startTime=0000&endTime=2359
```

- 인증키·쿠키·layout 파라미터 불필요. **하루치 1회 요청**으로 전량(1,050~1,130행) 반환
- 조회 범위 **D-14 ~ D+6** (D+7은 0행) — API의 D-3~D+6보다 넓어 결손 복구 여력이 있음
- 해외 IP 정상 (GH Actions·VPS 확인)
- 필드 매핑: `airlineNameKo→airline` `fnumber→flightId` `sdate+stime→scheduleDateTime` `btime→estimatedDateTime` `airportName1Ko→airport` `p1code→airportCode` `terminalId→terminalid` `stattxt→remark` `standPosition→fstandposition` `afsId→fid`
- `codeshare`가 API와 **같은 어휘**(`Master`/`Slave`/빈값)를 쓰고 **`afsId` 값이 API `fid`와 동일** → 기존 정규화·dedup 로직이 수정 없이 성립하고 기존 Daily_Data와 섞여도 안전
- `masterflight`는 단독편에 자기 편명이 들어와 `to_row()`에서 빈값으로 정규화 (API 규약에 맞춤)

**검증 (2026-07-29, 10일치 대조)**: `fid` 교집합 기준 집계 핵심 컬럼(airline·flightId·scheduleDateTime·airport·airportCode·terminalid·typeOfFlight·codeshare·masterflightid) **불일치 0건**. Master·국제·결항제외 편수도 일자별 동일(예: 7/26 552=552, 7/31 565=565). 차이 나는 건 `remark`·`estimatedDateTime`·게이트뿐이고 전부 "17:00 스냅샷 vs 현재 최종값"의 시점 차 — 폴백 쪽이 오히려 최신이다. `terminalid` P01↔P02 재배정은 `터미널` 매핑상 둘 다 T1이라 T1/T2 집계에 영향 없음.

**주의**
- 비공식 내부 AJAX라 사이트 개편 시 예고 없이 바뀔 수 있다. **API 복구 후에는 API 경로로 자동 복귀**(폴백은 `backfill.py` 실패 시에만 실행)
- robots.txt는 `/dep/`를 크롤러 허용 목록에 두지 않는다(같은 사이트를 쓰는 icn-pax-congestion도 동일 조건). 하루 10회 수준의 자체 지표 수집 용도
- 런타임(`data_loader.fetch_recent`)은 여전히 API만 호출한다. 실패 시 빈 DataFrame으로 degrade되어 화면은 `Daily_Data` 기준으로 정상 동작하며, 당일 중 실시간 갱신만 빠진다

## 참고

- 인천공항 API: <https://www.data.go.kr/data/15112968/openapi.do>
- D-3 ~ D+6 10일치만 반환. 그 외 기간은 누적 데이터 필요.
- 2024/12 대한항공-아시아나 합병 후 **아시아나는 T2**, 에어부산·에어서울도 T2.
- Plotly.js basic CDN (`https://cdn.plot.ly/plotly-basic-2.35.3.min.js`) 로드 후 client-side 렌더. html2canvas·flatpickr는 사용 시점 lazy load.
