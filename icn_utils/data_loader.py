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


def load_daily_range(daily_dir, start_ymd: str, end_ymd: str):
    """Daily_Data에서 start_ymd~end_ymd(YYYYMMDD, 포함) 범위 일별 pkl을 로드해 합쳐 반환.

    실제 운항 일자(estimatedDateTime)가 범위 안에 드는 행만 남깁니다.
    """
    if not os.path.isdir(daily_dir):
        return pd.DataFrame()
    start = datetime.strptime(start_ymd, "%Y%m%d")
    end = datetime.strptime(end_ymd, "%Y%m%d")
    if end < start:
        start, end = end, start
    # API는 여러 날짜를 overlap 저장하므로 start-3 ~ end+3 범위 pkl까지 읽어 dedup 후 실제 날짜 필터
    scan_start = start - timedelta(days=3)
    scan_end = end + timedelta(days=3)
    want = set(pd.date_range(scan_start, scan_end).strftime("%Y%m%d"))
    pkls = sorted(
        f for f in os.listdir(daily_dir)
        if f.startswith("flight_schedule_") and f.endswith(".pkl")
        and f[len("flight_schedule_"):-len(".pkl")] in want
    )
    if not pkls:
        return pd.DataFrame()
    dfs = [pd.read_pickle(os.path.join(daily_dir, f)) for f in pkls]
    raw = pd.concat(dfs, ignore_index=True)
    return raw


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
        g = x["탑승구"]
        if pd.notna(g) and g != "": return 2
        return 3

    df["priority"] = df.apply(_prior, axis=1)
    # fId가 API 명세상 스케줄별 unique key — Flight_Key(편명+일자)는 자정 넘기는 편의 estimatedDateTime
    # 변경 시 같은 운항이 두 키로 분리될 수 있어 fid 우선. 누락 시에만 Flight_Key fallback.
    if "fid" in df.columns:
        df["_dedup_key"] = df["fid"].where(df["fid"].notna() & (df["fid"] != ""), df["Flight_Key"])
    else:
        df["_dedup_key"] = df["Flight_Key"]
    df = df.sort_values(["_dedup_key", "priority"]).drop_duplicates("_dedup_key", keep="first")
    df = df.drop(columns=["priority", "_dedup_key"])
    df = pd.merge(df, dest_df, on="목적지", how="left")
    return df


def build_current_month(daily_dir, dest_df, service_key, year, month, raw_api=None):
    """이번달 데이터 = Daily_Data 과거 pkl + 최신 API (D-3~D+6) 병합 → 가공."""
    yyyymm = f"{year:04d}{month:02d}"
    raw_daily = load_daily_month(daily_dir, yyyymm)
    if raw_api is None:
        raw_api = fetch_recent(service_key)
    raw = pd.concat([raw_daily, raw_api], ignore_index=True) if len(raw_daily) and len(raw_api) else (raw_daily if len(raw_daily) else raw_api)
    if len(raw) == 0:
        return pd.DataFrame()
    df = process_raw(raw, dest_df)
    df = df[(df["YYYY"] == year) & (df["MM"] == month)]
    df = df.drop_duplicates("Flight_Key")
    return df


def build_previous_month(final_dir, daily_dir, dest_df, year, month, raw_api=None, today=None):
    """지난달 데이터 = Final_Data cum pkl 우선, 없으면 Daily_Data + API(prev월 분만) 재가공.

    raw_api가 주어지면 D-3~D+6 윈도우 중 prev월에 속한 일자도 보강 — 이번달 1~3일이나
    미리보기 모드(이번달 말일에 다음달 미리보기)에서 Daily_Data 백필 누락분을 메운다.

    today: cum pkl을 신뢰할지 판단용. (year, month)가 today의 월 이상이면 진행중·미래월
    이라 cum pkl이 부분 데이터일 수 있어 무시하고 Daily+API로 재구성한다.
    """
    yyyymm = f"{year:04d}{month:02d}"
    if today is None:
        today = date.today()
    is_completed_month = (year, month) < (today.year, today.month)
    if is_completed_month:
        cum = load_final_month(final_dir, yyyymm)
        if len(cum) > 0:
            df = cum.drop_duplicates("Flight_Key")
            return df[(df["YYYY"] == year) & (df["MM"] == month)]
    raw_daily = load_daily_month(daily_dir, yyyymm)
    parts = [d for d in [raw_daily, raw_api] if d is not None and len(d) > 0]
    if not parts:
        return pd.DataFrame()
    raw = parts[0] if len(parts) == 1 else pd.concat(parts, ignore_index=True)
    df = process_raw(raw, dest_df).drop_duplicates("Flight_Key")
    return df[(df["YYYY"] == year) & (df["MM"] == month)]
