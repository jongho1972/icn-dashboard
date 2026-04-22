# ICN_Dashboard

인천공항 출발편 현황 월간 비교 대시보드 (Streamlit + Streamlit Cloud)

## 구성

| 파일 | 역할 |
|------|------|
| `streamlit_app.py` | 대시보드 UI (접속 시 API 호출 → 가공 → 테이블·차트 렌더) |
| `data_loader.py` | 인천공항 API 호출 + Daily/Final pkl 로드 + 가공 로직 |
| `aggregator.py` | 월간 비교 집계 (전체·일자별·항공사별·지역별·게이트별) |
| `항공편목적지.txt` | 공항코드 → 국가·지역 매핑 |
| `Daily_Data/` | 일별 원본 pkl (매일 누적) |
| `Final_Data/` | 완료된 월의 가공된 cum pkl |
| `항공편 자료수집 코드.ipynb` | 수동 백필·재수집용 노트북 |
| `requirements.txt` | streamlit, pandas, numpy, requests |

## 로컬 실행

```bash
streamlit run streamlit_app.py
```

로컬 개발에는 `.streamlit/secrets.toml`에 `INCHEON_API_KEY`가 있어야 한다 (git에 제외).

## 데이터 수급 흐름

- **접속 시**: 이번달 = 최근 10일치(D-3~D+6) API 실시간 + `Daily_Data/` 과거 일별 pkl 병합 → 가공
- **지난달**: `Final_Data/flight_schedule_YYYYMM_cum.pkl` 우선, 없으면 `Daily_Data` 재가공
- **매일 자동 수집**: cron으로 `backfill.py` 실행 → `Daily_Data/` 갱신 + git push (GitHub 저장소 최신 유지)

## 집계 규칙

- `CODESHARE == "Master"`만 카운트 (공동운항 편 제외)
- 국내선 제외
- 3월·4월 동일기간 비교 = 양쪽 모두 `DD <= 이번달_max_day` 필터

**게이트 분류 (T1·T2 공용):**
- ≤25 또는 251~299: **동편**
- 26~28: **중앙**
- 29~50 또는 200~250: **서편**
- 51~199: **탑승동**

**항공사 그룹:**
- `대한항공`, `아시아나`(아시아나항공), `진에어`, `제주항공`, `티웨이`(티웨이항공)
- `국내기타`: 이스타·에어부산·에어서울·에어프레미아·플라이강원·하이에어·파라타·에어인천·플라이나스
- `외국항공`: 그 외

## 배포

- **Streamlit Cloud**: 이 레포 연결, secrets에 `INCHEON_API_KEY` 등록
- URL: <https://jhawk-icn-dashboard.streamlit.app>
- **매일 자동 수집**: Claude Code 스케줄 트리거 `trig_01KXfKu4nJ4A1asgvekGCiBN`
  - 스케줄: `0 8 * * *` UTC = 매일 17:00 KST
  - 동작: `backfill.py` 실행 → `Daily_Data/` 갱신 → 변경 있으면 `git push origin main`
  - 관리: <https://claude.ai/code/scheduled/trig_01KXfKu4nJ4A1asgvekGCiBN>

## 참고

- 인천공항 API: <https://www.data.go.kr/data/15112968/openapi.do>
- D-3 ~ D+6 10일치만 반환. 그 외 기간은 누적 데이터 필요.
