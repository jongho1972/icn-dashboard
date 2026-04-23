"""인천공항 국제선 출발편 현황 대시보드 (FastAPI + Jinja2).

기존 Streamlit 앱을 Render 배포 가능하도록 포팅.
집계 로직은 icn_utils/ 그대로 재사용.
"""
from __future__ import annotations

import json
import math
import os
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd
from dotenv import load_dotenv
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from icn_utils.aggregator import (
    GATES, REGIONS, AIRLINES,
    agg_airline, agg_daily, agg_gate, agg_region, agg_total,
    pct, prepare, rows_to_df,
)
from icn_utils.data_loader import build_current_month, build_previous_month

load_dotenv()

KST = ZoneInfo("Asia/Seoul")
BASE = Path(__file__).resolve().parent
DAILY_DIR = BASE / "Daily_Data"
FINAL_DIR = BASE / "Final_Data"
DEST_PATH = BASE / "항공편목적지.txt"

app = FastAPI(title="인천공항 국제선 출발편 현황")
templates = Jinja2Templates(directory=str(BASE / "templates"))

if (BASE / "static").is_dir():
    app.mount("/static", StaticFiles(directory=str(BASE / "static")), name="static")

# ---------- 간단한 TTL 캐시 (모든 클라이언트 공유) ----------
_CACHE: dict[str, tuple[float, object]] = {}
_TTL_SECONDS = 3600  # 1시간


def _cache_get(key: str):
    if key not in _CACHE:
        return None
    ts, val = _CACHE[key]
    if time.time() - ts > _TTL_SECONDS:
        return None
    return val


def _cache_set(key: str, val):
    _CACHE[key] = (time.time(), val)


# ---------- 집계 ----------
def load_dest():
    return pd.read_table(DEST_PATH)


def fetch_months(curr_year, curr_month, prev_year, prev_month, service_key):
    """1시간 캐시. 반환: (prev, curr, fetched_at_kst)."""
    key = f"{curr_year}-{curr_month}-{prev_year}-{prev_month}"
    cached = _cache_get(key)
    if cached is not None:
        return cached

    dest = load_dest()
    curr = build_current_month(str(DAILY_DIR), dest, service_key, curr_year, curr_month)
    prev = build_previous_month(str(FINAL_DIR), str(DAILY_DIR), dest, prev_year, prev_month)
    fetched_at = datetime.now(KST)
    result = (prev, curr, fetched_at)
    _cache_set(key, result)
    return result


# ---------- HTML 테이블 렌더러 (Streamlit df_to_html 포팅) ----------
def df_to_html(df: pd.DataFrame, prev_label: str, curr_label: str, total_row_idx=None) -> str:
    parts = [
        '<table class="icn">',
        '<colgroup>',
        '<col class="col-label"><col><col><col><col><col><col>',
        '</colgroup>',
        '<thead>',
        '<tr>',
        '<th rowspan="2">구분</th>',
        '<th colspan="3" class="t1-group t1-last">T1</th>',
        '<th colspan="3" class="t2-group">T2</th>',
        '</tr><tr>',
        f'<th>{prev_label}</th><th>{curr_label}</th><th class="t1-last">전월비</th>',
        f'<th>{prev_label}</th><th>{curr_label}</th><th>전월비</th>',
        '</tr></thead><tbody>',
    ]
    cols = list(df.columns)
    pct_idx = {3, 6}
    t1_last_idx = 3
    for ri, (_, row) in enumerate(df.iterrows()):
        tr_cls = ' class="total-row"' if total_row_idx is not None and ri == total_row_idx else ""
        parts.append(f"<tr{tr_cls}>")
        for ci, c in enumerate(cols):
            v = row[c]
            t1last = " t1-last" if ci == t1_last_idx else ""
            if c == "구분":
                parts.append(f'<td class="label">{v}</td>')
            elif ci in pct_idx:
                if pd.isna(v):
                    parts.append(f'<td class="{t1last.strip()}"></td>')
                elif v > 0:
                    parts.append(f'<td class="pos{t1last}">+{v:.1%}</td>')
                elif v < 0:
                    parts.append(f'<td class="neg{t1last}">−{abs(v):.1%}</td>')
                else:
                    parts.append(f'<td class="{t1last.strip()}">{v:+.1%}</td>')
            else:
                if pd.isna(v) or v == 0:
                    parts.append(f'<td class="{t1last.strip()}"></td>')
                else:
                    cls_attr = f' class="{t1last.strip()}"' if t1last else ""
                    parts.append(f"<td{cls_attr}>{v:,.0f}</td>")
        parts.append("</tr>")
    parts.append("</tbody></table>")
    return "".join(parts)


def _trend_html(c, p) -> str:
    """요약 텍스트용 전월비 span."""
    if p == 0:
        return ""
    r = pct(c, p)
    if math.isnan(r):
        return ""
    color = "#1F6FEB" if r > 0 else ("#B42318" if r < 0 else "#64748B")
    sign = "+" if r > 0 else ("−" if r < 0 else "±")
    return (f' <span style="color:{color};font-weight:500;'
            f'font-variant-numeric:tabular-nums;">'
            f'{sign}{abs(r):.1%}</span>')


# ---------- 주말·공휴일 계산 ----------
def _red_days(year: int, month: int, max_day: int) -> list[int]:
    try:
        import holidays as _h
        kr_hol = _h.KR(years=year)
    except Exception:
        kr_hol = {}
    reds = []
    for d in range(1, max_day + 1):
        dt = date(year, month, d)
        if dt.weekday() >= 5 or dt in kr_hol:
            reds.append(d)
    return reds


# ---------- 메인 라우트 ----------
@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    service_key = os.environ.get("INCHEON_API_KEY", "")
    if not service_key:
        return HTMLResponse(
            "<h1>설정 오류</h1><p><code>INCHEON_API_KEY</code> 환경변수를 설정하세요.</p>",
            status_code=500,
        )

    today = date.today()
    curr_year, curr_month = today.year, today.month
    prev_year, prev_month = (
        (curr_year - 1, 12) if curr_month == 1 else (curr_year, curr_month - 1)
    )

    prev, curr, fetched_at = fetch_months(
        curr_year, curr_month, prev_year, prev_month, service_key
    )

    if len(curr) == 0:
        return HTMLResponse(
            "<h1>데이터 없음</h1><p>이번달 데이터를 불러오지 못했습니다.</p>",
            status_code=500,
        )

    prev = prepare(prev)
    curr = prepare(curr)
    max_day = int(curr["DD"].max())
    prev_same = prev[prev["DD"] <= max_day]

    prev_label = f"{prev_month}월"
    curr_label = f"{curr_month}월"

    # 요약
    t1_p = len(prev_same[prev_same["터미널"] == "T1"])
    t1_c = len(curr[curr["터미널"] == "T1"])
    t2_p = len(prev_same[prev_same["터미널"] == "T2"])
    t2_c = len(curr[curr["터미널"] == "T2"])
    tot_p, tot_c = t1_p + t2_p, t1_c + t2_c
    avg_p, avg_c = tot_p / max_day, tot_c / max_day

    summary_html = (
        f'<b>월누적</b> {tot_c:,} 편{_trend_html(tot_c, tot_p)}'
        f'&nbsp;&nbsp;·&nbsp;&nbsp;'
        f'<b>일평균</b> {avg_c:,.0f} 편'
    )

    # 각 섹션 표
    df_total = rows_to_df(agg_total(prev_same, curr, max_day), prev_label, curr_label)
    total_html = df_to_html(df_total, prev_label, curr_label)

    df_airline = rows_to_df(agg_airline(prev_same, curr), prev_label, curr_label)
    airline_html = df_to_html(df_airline, prev_label, curr_label)

    df_region = rows_to_df(agg_region(prev_same, curr), prev_label, curr_label)
    region_html = df_to_html(df_region, prev_label, curr_label)

    # 게이트별: D-1 기준
    d_minus_1 = today - timedelta(days=1)
    gate_cutoff = (
        d_minus_1.day
        if (d_minus_1.year == curr_year and d_minus_1.month == curr_month)
        else max_day
    )
    gate_prev = prev_same[prev_same["DD"] <= gate_cutoff]
    gate_curr = curr[curr["DD"] <= gate_cutoff]
    df_gate = rows_to_df(agg_gate(gate_prev, gate_curr), prev_label, curr_label)
    gate_html = df_to_html(df_gate, prev_label, curr_label, total_row_idx=0)
    gate_note = (
        f'기간 : {prev_label}/{curr_label} 1~{gate_cutoff}일 (운항정보 마감 기준)'
    )

    # 일자별 표
    df_daily = rows_to_df(agg_daily(prev_same, curr, max_day), prev_label, curr_label)
    daily_html = df_to_html(df_daily, prev_label, curr_label)

    # 일자별 차트용 데이터
    chart_data = {
        "max_day": max_day,
        "curr_label": curr_label,
        "prev_label": prev_label,
        "red_days": _red_days(curr_year, curr_month, max_day),
        "today_day": today.day if (today.year == curr_year and today.month == curr_month) else None,
        "series": {
            "T1_prev": df_daily[f"T1_{prev_label}"].astype(int).tolist(),
            "T1_curr": df_daily[f"T1_{curr_label}"].astype(int).tolist(),
            "T2_prev": df_daily[f"T2_{prev_label}"].astype(int).tolist(),
            "T2_curr": df_daily[f"T2_{curr_label}"].astype(int).tolist(),
        },
    }

    return templates.TemplateResponse(
        request,
        "index.html",
        {
            "prev_label": prev_label,
            "curr_label": curr_label,
            "max_day": max_day,
            "fetched_at": fetched_at.strftime("%Y-%m-%d %H:%M"),
            "period_note": f"기간 : {prev_label}/{curr_label} 1~{max_day}일 동일기간",
            "summary_html": summary_html,
            "total_html": total_html,
            "airline_html": airline_html,
            "region_html": region_html,
            "gate_html": gate_html,
            "gate_note": gate_note,
            "daily_html": daily_html,
            "chart_data_json": json.dumps(chart_data, ensure_ascii=False),
        },
    )


@app.get("/healthz")
async def healthz():
    return {"ok": True, "time": datetime.now(KST).isoformat()}
