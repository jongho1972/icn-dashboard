"""일별 API 수집 스크립트 (cron 자동 실행용).

환경변수 INCHEON_API_KEY 필요.
실행: python3 backfill.py
결과: Daily_Data/flight_schedule_YYYYMMDD.pkl (D-3 ~ D+6 10일치)
"""
import os
import sys
from datetime import date, timedelta

import pandas as pd
import requests

API_URL = "http://apis.data.go.kr/B551177/StatusOfPassengerFlightsDeOdp/getPassengerDeparturesDeOdp"


def main():
    service_key = os.environ.get("INCHEON_API_KEY")
    if not service_key:
        sys.stderr.write("환경변수 INCHEON_API_KEY 가 필요합니다.\n")
        sys.exit(1)

    base = os.path.dirname(os.path.abspath(__file__))
    daily_dir = os.path.join(base, "Daily_Data")
    os.makedirs(daily_dir, exist_ok=True)

    start = date.today() - timedelta(days=3)
    dates = pd.date_range(start, periods=10).strftime("%Y%m%d").tolist()
    print(f"수집 대상: {dates[0]} ~ {dates[-1]}")

    for d in dates:
        params = {
            "serviceKey": service_key, "pageNo": "1", "numOfRows": "2000",
            "type": "json", "from_time": "0000", "to_time": "2400", "searchday": d,
        }
        try:
            r = requests.get(API_URL, params=params, timeout=30)
            items = r.json()["response"]["body"]["items"]
            if items:
                df = pd.DataFrame(items)
                df.to_pickle(os.path.join(daily_dir, f"flight_schedule_{d}.pkl"))
                print(f"  {d}: {len(df):,}건 저장")
            else:
                print(f"  {d}: 데이터 없음")
        except Exception as e:
            print(f"  {d} 오류: {e}")

    print("완료")


if __name__ == "__main__":
    main()
