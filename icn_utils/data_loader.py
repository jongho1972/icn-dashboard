"""인천공항 API 호출 + 누적 pkl 로드 + 가공."""
import os
import requests
import pandas as pd
from datetime import date, datetime, timedelta

API_URL = "http://apis.data.go.kr/B551177/StatusOfPassengerFlightsDeOdp/getPassengerDeparturesDeOdp"


def fetch_api_day(yyyymmdd, service_key):
    """하루치 API 호출. 실패 시 빈 DataFrame."""
    params = {
        "serviceKey": service_key, "pageNo": "1", "numOfRows": "2000",
        "type": "json", "from_time": "0000", "to_time": "2400", "searchday": yyyymmdd,
    }
    try:
        r = requests.get(API_URL, params=params, timeout=20)
        items = r.json()["response"]["body"]["items"]
        return pd.DataFrame(items) if items else pd.DataFrame()
    except Exception:
        return pd.DataFrame()


def fetch_recent(service_key, days_back=3, days_forward=6):
    """오늘 기준 D-3 ~ D+6 (10일) API 호출 결과 통합."""
    base = date.today() - timedelta(days=days_back)
    days = pd.date_range(base, periods=days_back + 1 + days_forward).strftime("%Y%m%d").tolist()
    dfs = [fetch_api_day(d, service_key) for d in days]
    dfs = [d for d in dfs if len(d) > 0]
    return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()


def load_daily_month(daily_dir, yyyymm):
    """Daily_Data의 해당 월(YYYYMM) 일별 pkl 전체 로드."""
    if not os.path.isdir(daily_dir):
        return pd.DataFrame()
    pkls = sorted(f for f in os.listdir(daily_dir) if f.startswith(f"flight_schedule_{yyyymm}") and f.endswith(".pkl"))
    if not pkls:
        return pd.DataFrame()
    dfs = [pd.read_pickle(os.path.join(daily_dir, f)) for f in pkls]
    return pd.concat(dfs, ignore_index=True)


def load_final_month(final_dir, yyyymm):
    """Final_Data의 해당 월 cum pkl 로드 (이미 가공된 상태)."""
    path = os.path.join(final_dir, f"flight_schedule_{yyyymm}_cum.pkl")
    if os.path.exists(path):
        return pd.read_pickle(path)
    return pd.DataFrame()


def process_raw(raw_df, dest_df):
    """Raw API 데이터 → 집계 가능한 가공 DataFrame (노트북 2셀 로직)."""
    if len(raw_df) == 0:
        return pd.DataFrame()
    df = raw_df.copy()
    df["scheduleDateTime"] = df["scheduleDateTime"].map(lambda x: datetime.strptime(x, "%Y%m%d%H%M"))
    df["estimatedDateTime"] = df["estimatedDateTime"].map(lambda x: datetime.strptime(x, "%Y%m%d%H%M"))
    df["YYYYMMDD"] = df["estimatedDateTime"].map(lambda x: datetime.strptime(x.strftime("%Y-%m-%d"), "%Y-%m-%d"))
    df["YYYY"] = df["estimatedDateTime"].dt.year
    df["MM"] = df["estimatedDateTime"].dt.month
    df["DD"] = df["estimatedDateTime"].dt.day
    df["출발시각"] = df["estimatedDateTime"].dt.hour
    df["출발분"] = df["estimatedDateTime"].dt.minute
    df["Flight_Key"] = df["flightId"] + df["YYYYMMDD"].apply(lambda x: x.strftime("%Y-%m-%d"))
    df["터미널"] = df["terminalid"].apply(lambda x: "T1" if x in ["P01", "P02"] else ("T2" if x == "P03" else x))
    df["목적지"] = (df["airport"] + "(" + df["airportCode"] + ")").str.replace(" ", "")
    df = df.rename(columns={
        "flightId": "운항편명", "airline": "항공사", "chkinrange": "체크인 카운터",
        "gatenumber": "탑승구", "codeshare": "CODESHARE", "masterflightid": "Master_Flight",
    })

    def _prior(x):
        if x["remark"] == "출발": return 1
        if x["탑승구"] != "": return 2
        return 3

    df["priority"] = df.apply(_prior, axis=1)
    df = df.sort_values(["Flight_Key", "priority"]).drop_duplicates("Flight_Key", keep="first").drop(columns="priority")
    df = pd.merge(df, dest_df, on="목적지", how="left")
    return df


def build_current_month(daily_dir, dest_df, service_key, year, month):
    """이번달 데이터 = Daily_Data 과거 pkl + 최신 API (D-3~D+6) 병합 → 가공."""
    yyyymm = f"{year:04d}{month:02d}"
    raw_daily = load_daily_month(daily_dir, yyyymm)
    raw_api = fetch_recent(service_key)
    raw = pd.concat([raw_daily, raw_api], ignore_index=True) if len(raw_daily) and len(raw_api) else (raw_daily if len(raw_daily) else raw_api)
    if len(raw) == 0:
        return pd.DataFrame()
    df = process_raw(raw, dest_df)
    df = df[(df["YYYY"] == year) & (df["MM"] == month)]
    df = df.drop_duplicates("Flight_Key")
    return df


def build_previous_month(final_dir, daily_dir, dest_df, year, month):
    """지난달 데이터 = Final_Data cum pkl 우선, 없으면 Daily_Data 재가공."""
    yyyymm = f"{year:04d}{month:02d}"
    cum = load_final_month(final_dir, yyyymm)
    if len(cum) > 0:
        df = cum.drop_duplicates("Flight_Key")
        return df[(df["YYYY"] == year) & (df["MM"] == month)]
    raw_daily = load_daily_month(daily_dir, yyyymm)
    if len(raw_daily) == 0:
        return pd.DataFrame()
    df = process_raw(raw_daily, dest_df).drop_duplicates("Flight_Key")
    return df[(df["YYYY"] == year) & (df["MM"] == month)]
