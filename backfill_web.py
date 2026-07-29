"""일별 수집 폴백 스크립트 — airport.kr 출발 시간표 JSON 사용.

data.go.kr 인천공항공사 오픈API(backfill.py)가 막혔을 때만 쓰는 대체 경로다.
(2026-07-29 활용신청 승인 만료로 B551177 전 엔드포인트가 403 Forbidden)

인증키 불필요. 하루치를 1회 POST로 전량 반환하며 D-14 ~ D+6 조회 가능.
출력 스키마는 backfill.py(API)와 완전히 동일한 15개 컬럼이고, afsId 값이
API의 fid와 같아 기존 Daily_Data와 섞여도 fid dedup이 그대로 성립한다.

실행: python3 backfill_web.py
결과: Daily_Data/flight_schedule_YYYYMMDD.pkl (D-3 ~ D+6 10일치)
"""
import os
import sys
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

KST = ZoneInfo("Asia/Seoul")

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

LIST_URL = "https://www.airport.kr/dep/ap_ko/getDepPasSchList.do"
UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
)

# backfill.py(API) 산출 pkl과 동일한 컬럼·순서
COLUMNS = [
    "airline", "flightId", "scheduleDateTime", "estimatedDateTime", "airport",
    "chkinrange", "gatenumber", "codeshare", "masterflightid", "remark",
    "airportCode", "terminalid", "typeOfFlight", "fid", "fstandposition",
]


def _make_session() -> requests.Session:
    s = requests.Session()
    retry = Retry(
        total=3, backoff_factor=1.5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["POST"],
    )
    s.mount("https://", HTTPAdapter(max_retries=retry))
    s.headers.update({"User-Agent": UA, "Referer": "https://www.airport.kr/ap_ko/869/subview.do"})
    return s


def fetch_day(session: requests.Session, ymd: str) -> list[dict]:
    r = session.post(
        LIST_URL,
        data={
            "siteId": "ap_ko", "langSe": "ko",
            "daySel": ymd, "curDate": ymd,
            "fromTime": "0000", "toTime": "2359",
            "startTime": "0000", "endTime": "2359",
        },
        timeout=60,
    )
    r.raise_for_status()
    return r.json().get("scheduleList") or []


def to_row(x: dict) -> dict:
    """airport.kr 응답 1건 → API 스키마 1행."""
    sdate = x.get("sdate") or ""
    stime = (x.get("stime") or "").replace(":", "")
    etime = (x.get("etime") or "").replace(":", "")
    master = x.get("masterflight") or ""
    flight = x.get("fnumber") or ""
    return {
        "airline": x.get("airlineNameKo") or "",
        "flightId": flight,
        "scheduleDateTime": sdate + stime,
        # btime = 실제/변경 출발시각. API estimatedDateTime과 같은 자리.
        "estimatedDateTime": x.get("btime") or (sdate + etime),
        "airport": x.get("airportName1Ko") or "",
        "chkinrange": x.get("chkinrange") or "",
        "gatenumber": x.get("gatenumber") or "",
        # 빈값 = 단독 운항편. process_raw가 Master로 정규화하므로 API와 동일하게 처리된다.
        "codeshare": x.get("codeshare") or "",
        # API는 마스터편의 masterflightid를 비워둔다. airport.kr은 자기 편명을 넣으므로 맞춰준다.
        "masterflightid": "" if master == flight else master,
        "remark": x.get("stattxt") or "",
        "airportCode": x.get("p1code") or "",
        "terminalid": x.get("terminalId") or "",
        "typeOfFlight": x.get("typeOfFlight") or "",
        "fid": x.get("afsId") or "",
        "fstandposition": x.get("standPosition") or "",
    }


def main():
    base = os.path.dirname(os.path.abspath(__file__))
    daily_dir = os.path.join(base, "Daily_Data")
    os.makedirs(daily_dir, exist_ok=True)

    start = datetime.now(KST).date() - timedelta(days=3)
    dates = pd.date_range(start, periods=10).strftime("%Y%m%d").tolist()
    print(f"[폴백] airport.kr 수집 대상: {dates[0]} ~ {dates[-1]}")

    session = _make_session()
    saved = 0
    errors: list[tuple[str, str]] = []
    for d in dates:
        try:
            items = fetch_day(session, d)
            if items:
                df = pd.DataFrame([to_row(x) for x in items], columns=COLUMNS)
                df.to_pickle(os.path.join(daily_dir, f"flight_schedule_{d}.pkl"))
                saved += 1
                print(f"  {d}: {len(df):,}건 저장")
            else:
                print(f"  {d}: 데이터 없음")
        except Exception as e:
            errors.append((d, repr(e)))
            sys.stderr.write(f"  {d} 오류: {e!r}\n")

    print(f"[폴백] 완료 (saved={saved}/{len(dates)}, errors={len(errors)})")
    if saved == 0:
        sys.stderr.write("FATAL: 폴백 수집도 모든 일자 실패\n")
        sys.exit(1)


if __name__ == "__main__":
    main()
