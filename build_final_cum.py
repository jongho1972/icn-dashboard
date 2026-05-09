"""월 종료 후 Daily_Data를 모아 Final_Data/flight_schedule_YYYYMM_cum.pkl 생성.

매월 1일 자동 cron(`monthly-cum.yml`)이 직전 월을 대상으로 호출.
수동 재생성: `python3 build_final_cum.py 202604`

핵심 규칙:
- process_raw가 typeOfFlight·fid 컬럼을 보존해 강한 필터·강한 dedup 자동 적용
- 말일까지 DD가 채워졌는지 확인해 부분 cum 저장 방지 (exit 1)
"""
from __future__ import annotations

import calendar
import sys
from datetime import date
from pathlib import Path

import pandas as pd

from icn_utils.data_loader import load_daily_month, process_raw

BASE = Path(__file__).resolve().parent
DAILY_DIR = BASE / "Daily_Data"
FINAL_DIR = BASE / "Final_Data"
DEST_PATH = BASE / "항공편목적지.txt"


def _resolve_yyyymm(arg: str | None) -> str:
    if arg:
        if len(arg) != 6 or not arg.isdigit():
            raise SystemExit(f"YYYYMM 형식이 아님: {arg!r}")
        return arg
    today = date.today()
    y, m = (today.year - 1, 12) if today.month == 1 else (today.year, today.month - 1)
    return f"{y:04d}{m:02d}"


def main(argv: list[str]) -> int:
    yyyymm = _resolve_yyyymm(argv[1] if len(argv) > 1 else None)
    y, m = int(yyyymm[:4]), int(yyyymm[4:])
    print(f"target: {yyyymm}")

    raw = load_daily_month(str(DAILY_DIR), yyyymm)
    if len(raw) == 0:
        sys.stderr.write(f"FATAL: Daily_Data has no rows for {yyyymm}\n")
        return 1

    dest_df = pd.read_table(DEST_PATH)
    df = process_raw(raw, dest_df)
    df = df[(df["YYYY"] == y) & (df["MM"] == m)]
    df = df.drop_duplicates("Flight_Key")

    last_dom = calendar.monthrange(y, m)[1]
    if "DD" not in df.columns or len(df) == 0:
        sys.stderr.write(f"FATAL: empty processed df for {yyyymm}\n")
        return 1
    max_dd = int(df["DD"].max())
    if max_dd < last_dom:
        sys.stderr.write(
            f"FATAL: partial cum (max DD={max_dd} < last_dom={last_dom}) — Daily_Data 누락\n"
        )
        return 1

    FINAL_DIR.mkdir(exist_ok=True)
    out = FINAL_DIR / f"flight_schedule_{yyyymm}_cum.pkl"
    df.to_pickle(out)
    has_typeof = "typeOfFlight" in df.columns
    has_fid = "fid" in df.columns
    print(
        f"saved: {out.name} rows={len(df):,} cols={len(df.columns)} "
        f"typeOfFlight={has_typeof} fid={has_fid}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
