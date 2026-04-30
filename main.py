"""인천공항 국제선 출발편 현황 대시보드 (FastAPI + Jinja2).

기존 Streamlit 앱을 Render 배포 가능하도록 포팅.
집계 로직은 icn_utils/ 그대로 재사용.
"""
from __future__ import annotations

import base64
import calendar
import json
import math
import os
import pickle
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd
import requests
from dotenv import load_dotenv
from fastapi import FastAPI, Header, HTTPException, Request
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel, Field

from icn_utils.aggregator import (
    GATES, REGIONS, AIRLINES, REGION_MERGE,
    agg_airline, agg_daily, agg_gate, agg_region, agg_total,
    airline_group, gate_group,
    pct, prepare, rows_to_df,
)
from icn_utils.data_loader import (
    build_current_month, build_previous_month,
    load_daily_month, load_final_month, process_raw,
)

load_dotenv()

KST = ZoneInfo("Asia/Seoul")
BASE = Path(__file__).resolve().parent
DAILY_DIR = BASE / "Daily_Data"
FINAL_DIR = BASE / "Final_Data"
DEST_PATH = BASE / "항공편목적지.txt"

app = FastAPI(title="인천공항 국제선 출발편 현황")
app.add_middleware(GZipMiddleware, minimum_size=500)
templates = Jinja2Templates(directory=str(BASE / "templates"))

if (BASE / "static").is_dir():
    app.mount("/static", StaticFiles(directory=str(BASE / "static")), name="static")

# ---------- TTL 캐시 (메모리 + 디스크 pickle) ----------
# 갱신은 매일 10:00 / 17:00 KST cron(/api/refresh)이 담당.
# TTL은 cron 누락 대비 안전 마진 (48h). 그 사이엔 디스크 캐시로 즉시 응답.
_CACHE: dict[str, tuple[float, object]] = {}
_TTL_SECONDS = 60 * 60 * 48  # 48시간
CACHE_FILE = Path("/tmp") / "icn_dashboard_cache.pkl"


def _cache_get(key: str):
    if key not in _CACHE:
        return None
    ts, val = _CACHE[key]
    if time.time() - ts > _TTL_SECONDS:
        return None
    return val


def _cache_set(key: str, val):
    _CACHE[key] = (time.time(), val)
    _save_disk_cache()


def _load_disk_cache() -> None:
    if not CACHE_FILE.exists():
        return
    try:
        with CACHE_FILE.open("rb") as f:
            data = pickle.load(f)
        if isinstance(data, dict):
            _CACHE.update(data)
    except Exception as exc:
        print(f"[disk cache load] skipped: {exc!r}")


def _save_disk_cache() -> None:
    try:
        CACHE_FILE.parent.mkdir(parents=True, exist_ok=True)
        tmp = CACHE_FILE.with_suffix(".pkl.tmp")
        with tmp.open("wb") as f:
            pickle.dump(_CACHE, f)
        tmp.replace(CACHE_FILE)
    except Exception as exc:
        print(f"[disk cache save] skipped: {exc!r}")


# ---------- 집계 ----------
def load_dest():
    return pd.read_table(DEST_PATH)


def _latest_available_date() -> date:
    """Daily_Data 내 가장 최근 일자의 pkl 파일에서 날짜 추출."""
    if not DAILY_DIR.is_dir():
        return datetime.now(KST).date()
    pkls = sorted(
        f.name for f in DAILY_DIR.iterdir()
        if f.name.startswith("flight_schedule_") and f.suffix == ".pkl"
    )
    if not pkls:
        return datetime.now(KST).date()
    ymd = pkls[-1][len("flight_schedule_"):-len(".pkl")]
    try:
        return datetime.strptime(ymd, "%Y%m%d").date()
    except ValueError:
        return datetime.now(KST).date()


def _earliest_available_date() -> date:
    """Final_Data 에서 가장 오래된 월의 1일(없으면 Daily_Data 최소)."""
    months: list[str] = []
    if FINAL_DIR.is_dir():
        for f in FINAL_DIR.iterdir():
            if f.name.startswith("flight_schedule_") and f.name.endswith("_cum.pkl"):
                months.append(f.name[len("flight_schedule_"):-len("_cum.pkl")])
    if months:
        ym = sorted(months)[0]
        try:
            return date(int(ym[:4]), int(ym[4:]), 1)
        except ValueError:
            pass
    if DAILY_DIR.is_dir():
        pkls = sorted(
            f.name for f in DAILY_DIR.iterdir()
            if f.name.startswith("flight_schedule_") and f.suffix == ".pkl"
        )
        if pkls:
            ymd = pkls[0][len("flight_schedule_"):-len(".pkl")]
            try:
                return datetime.strptime(ymd, "%Y%m%d").date()
            except ValueError:
                pass
    return datetime.now(KST).date()


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


@app.on_event("startup")
def warm_cache_on_startup() -> None:
    """앱 기동 직후 디스크 캐시 → 메모리 로드. 비어있거나 만료면 fetch.
    실패해도 부팅을 막지 않는다 (요청 시 재시도)."""
    _load_disk_cache()

    service_key = os.environ.get("INCHEON_API_KEY", "")
    if not service_key:
        return
    try:
        today = datetime.now(KST).date()
        curr_year, curr_month = today.year, today.month
        prev_year, prev_month = (
            (curr_year - 1, 12) if curr_month == 1 else (curr_year, curr_month - 1)
        )
        # 디스크에서 이미 유효 캐시를 로드했다면 fetch_months가 그대로 반환.
        fetch_months(curr_year, curr_month, prev_year, prev_month, service_key)
    except Exception as exc:
        print(f"[warm_cache_on_startup] skipped: {exc!r}")


@app.post("/api/refresh")
async def refresh_cache(x_refresh_token: str | None = Header(None)):
    """캐시 강제 갱신. cron(매일 10:00, 17:00 KST)이 호출.

    REFRESH_TOKEN 환경변수가 설정돼 있으면 X-Refresh-Token 헤더 일치 필요.
    """
    expected = os.environ.get("REFRESH_TOKEN", "")
    if expected and x_refresh_token != expected:
        raise HTTPException(401, "invalid token")

    service_key = os.environ.get("INCHEON_API_KEY", "")
    if not service_key:
        raise HTTPException(500, "INCHEON_API_KEY not set")

    today = datetime.now(KST).date()
    curr_year, curr_month = today.year, today.month
    prev_year, prev_month = (
        (curr_year - 1, 12) if curr_month == 1 else (curr_year, curr_month - 1)
    )
    _CACHE.clear()
    try:
        prev, curr, fetched_at = fetch_months(
            curr_year, curr_month, prev_year, prev_month, service_key
        )
    except Exception as exc:
        raise HTTPException(502, f"fetch failed: {exc!r}")
    return {
        "ok": True,
        "fetched_at": fetched_at.isoformat(),
        "curr_rows": int(len(curr)),
        "prev_rows": int(len(prev)),
    }


# ---------- HTML 테이블 렌더러 (Streamlit df_to_html 포팅) ----------
def df_to_html(df: pd.DataFrame, prev_label: str, curr_label: str, total_row_idx=None) -> str:
    parts = [
        '<div class="table-wrap">',
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
                    cls = ("dash " + t1last.strip()).strip()
                    parts.append(f'<td class="{cls}">—</td>')
                elif v > 0:
                    parts.append(f'<td class="pos{t1last}">+{v:.1%}</td>')
                elif v < 0:
                    parts.append(f'<td class="neg{t1last}">−{abs(v):.1%}</td>')
                else:
                    parts.append(f'<td class="{t1last.strip()}">{v:+.1%}</td>')
            else:
                if pd.isna(v) or v == 0:
                    cls = ("dash " + t1last.strip()).strip()
                    parts.append(f'<td class="{cls}">—</td>')
                else:
                    cls_attr = f' class="{t1last.strip()}"' if t1last else ""
                    parts.append(f"<td{cls_attr}>{v:,.0f}</td>")
        parts.append("</tr>")
    parts.append("</tbody></table></div>")
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
            f'(전월비 {sign}{abs(r):.1%})</span>')


def _dow_diff_html(curr_cnt: int, avg: float | None) -> str:
    """D+1일 T1/T2 요약용: '전월 동요일 평균 대비 +xx편(+xx.x%)' span."""
    if not avg or avg <= 0:
        return ""
    diff = curr_cnt - avg
    r = diff / avg
    color = "#1F6FEB" if diff > 0 else ("#B42318" if diff < 0 else "#64748B")
    sign_n = "+" if diff > 0 else ("−" if diff < 0 else "±")
    sign_p = "+" if r > 0 else ("−" if r < 0 else "±")
    return (f', 전월 동요일 평균 대비 '
            f'<span style="color:{color};font-weight:500;'
            f'font-variant-numeric:tabular-nums;">'
            f'{sign_n}{abs(diff):.0f}편({sign_p}{abs(r):.1%})</span>')


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


WEEKDAY_KR = ["월", "화", "수", "목", "금", "토", "일"]


def _kr_holidays(year):
    try:
        import holidays as _h
        return _h.KR(years=year)
    except Exception:
        return {}


def _prev_dow_avg(prev_t, prev_year, prev_month):
    """전월 터미널별 일자 카운트 → 요일별 평균."""
    cnt = prev_t.groupby("DD").size().to_dict()
    groups: dict[int, list[int]] = {}
    for d, c in cnt.items():
        wd = date(prev_year, prev_month, int(d)).weekday()
        groups.setdefault(wd, []).append(int(c))
    return {wd: sum(v) / len(v) for wd, v in groups.items()}


def _ratio_td(curr_cnt: int, avg: float | None, extra_cls: str = "") -> str:
    base = (extra_cls + " ").strip()
    if not avg or avg <= 0:
        return f'<td class="{base}"></td>' if base else '<td></td>'
    r = (curr_cnt - avg) / avg
    if r > 0:
        return f'<td class="{(base + " pos").strip()}">+{r:.1%}</td>'
    if r < 0:
        return f'<td class="{(base + " neg").strip()}">−{abs(r):.1%}</td>'
    return f'<td class="{base}">{r:+.1%}</td>' if base else f'<td>{r:+.1%}</td>'


def daily_combined_html(curr, prev, curr_label: str, prev_label: str,
                        curr_year: int, curr_month: int,
                        prev_year: int, prev_month: int,
                        max_day: int, today_day: int | None) -> str:
    """T1·T2 통합 일별 표: 날짜·요일 공유, 터미널별 [전월·이번달·전월동요일비]."""
    curr_t1 = curr[curr["터미널"] == "T1"]
    curr_t2 = curr[curr["터미널"] == "T2"]
    prev_t1 = prev[prev["터미널"] == "T1"]
    prev_t2 = prev[prev["터미널"] == "T2"]

    avg_t1 = _prev_dow_avg(prev_t1, prev_year, prev_month)
    avg_t2 = _prev_dow_avg(prev_t2, prev_year, prev_month)
    prev_t1_cnt = prev_t1.groupby("DD").size().to_dict()
    prev_t2_cnt = prev_t2.groupby("DD").size().to_dict()

    curr_hol = _kr_holidays(curr_year)

    parts = [
        '<div class="table-wrap">',
        '<table class="icn daily-t">',
        '<colgroup><col><col><col><col><col><col><col><col></colgroup>',
        '<thead>',
        '<tr>',
        '<th rowspan="2">날짜</th>',
        '<th rowspan="2">요일</th>',
        '<th colspan="3" class="t1-group t1-last">T1</th>',
        '<th colspan="3" class="t2-group">T2</th>',
        '</tr>',
        '<tr>',
        f'<th>{prev_label}</th><th>{curr_label}</th><th class="t1-last">전월동요일비</th>',
        f'<th>{prev_label}</th><th>{curr_label}</th><th>전월동요일비</th>',
        '</tr>',
        '</thead><tbody>',
    ]
    for d in range(1, max_day + 1):
        dt = date(curr_year, curr_month, d)
        wd = dt.weekday()
        is_red = (wd >= 5) or (dt in curr_hol)
        c1 = int((curr_t1["DD"] == d).sum())
        c2 = int((curr_t2["DD"] == d).sum())
        p1 = int(prev_t1_cnt.get(d, 0))
        p2 = int(prev_t2_cnt.get(d, 0))

        cls_list = []
        if today_day is not None and d > today_day:
            cls_list.append("future-row")
        tr_cls = f' class="{" ".join(cls_list)}"' if cls_list else ""
        date_cls = ' class="label red-day"' if is_red else ' class="label"'
        wd_cls = ' class="dow red-day"' if is_red else ' class="dow"'

        parts.append(f"<tr{tr_cls}>")
        parts.append(f'<td{date_cls}>{d}일</td>')
        parts.append(f'<td{wd_cls}>{WEEKDAY_KR[wd]}</td>')
        parts.append(f'<td>{p1:,}</td>' if p1 else '<td></td>')
        parts.append(f'<td>{c1:,}</td>')
        parts.append(_ratio_td(c1, avg_t1.get(wd), extra_cls="t1-last"))
        parts.append(f'<td>{p2:,}</td>' if p2 else '<td></td>')
        parts.append(f'<td>{c2:,}</td>')
        parts.append(_ratio_td(c2, avg_t2.get(wd)))
        parts.append("</tr>")
    parts.append("</tbody></table></div>")
    return "".join(parts)


# ---------- 메인 라우트 ----------
@app.get("/", response_class=HTMLResponse)
async def index(request: Request, view: str | None = None):
    service_key = os.environ.get("INCHEON_API_KEY", "")
    if not service_key:
        return HTMLResponse(
            "<h1>설정 오류</h1><p><code>INCHEON_API_KEY</code> 환경변수를 설정하세요.</p>",
            status_code=500,
        )

    today = datetime.now(KST).date()
    last_dom = calendar.monthrange(today.year, today.month)[1]
    is_month_end = (today.day == last_dom)

    # 말일에는 다음달 미리보기를 default로 (?view=current로 이번달 강제 가능)
    is_next_preview = is_month_end and view != "current"

    nxt_year, nxt_month = (today.year + 1, 1) if today.month == 12 else (today.year, today.month + 1)

    if is_next_preview:
        curr_year, curr_month = nxt_year, nxt_month
        prev_year, prev_month = today.year, today.month
    else:
        curr_year, curr_month = today.year, today.month
        prev_year, prev_month = (
            (curr_year - 1, 12) if curr_month == 1 else (curr_year, curr_month - 1)
        )

    prev, curr, fetched_at = fetch_months(
        curr_year, curr_month, prev_year, prev_month, service_key
    )
    dest_df = load_dest()
    countries = sorted(dest_df["국가"].dropna().astype(str).str.strip().unique().tolist())

    if len(curr) == 0:
        return HTMLResponse(
            "<h1>데이터 없음</h1><p>이번달 데이터를 불러오지 못했습니다.</p>",
            status_code=500,
        )

    prev = prepare(prev)
    curr = prepare(curr)
    max_day = int(curr["DD"].max())
    prev_same = prev[prev["DD"] <= max_day]

    # 미매핑 도착지(매핑 테이블에 없는 목적지) 감지
    _all = pd.concat([prev_same, curr], ignore_index=True)
    unmapped = sorted(
        _all.loc[_all["국가"].isna(), "목적지"].dropna().unique().tolist()
    )

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
        f'<b>T1+T2 기준</b> {tot_c:,} 편{_trend_html(tot_c, tot_p)}'
        f'&nbsp;&nbsp;·&nbsp;&nbsp;'
        f'<b>일평균</b> {avg_c:,.0f} 편'
    )

    # D+1일 요약 (내일 예정 편수 + 전월 동요일 평균 대비)
    # 다음달 미리보기 모드에서도 today+1=다음달 1일이 curr_month에 속해 자연스럽게 표시됨
    tomorrow = today + timedelta(days=1)
    tomorrow_summary_html = None
    if (tomorrow.year == curr_year and tomorrow.month == curr_month
            and tomorrow.day <= max_day):
        tmr_dd = tomorrow.day
        tmr_wd = tomorrow.weekday()
        prev_t1 = prev[prev["터미널"] == "T1"]
        prev_t2 = prev[prev["터미널"] == "T2"]
        avg_t1 = _prev_dow_avg(prev_t1, prev_year, prev_month).get(tmr_wd)
        avg_t2 = _prev_dow_avg(prev_t2, prev_year, prev_month).get(tmr_wd)
        t1_tmr = int(((curr["터미널"] == "T1") & (curr["DD"] == tmr_dd)).sum())
        t2_tmr = int(((curr["터미널"] == "T2") & (curr["DD"] == tmr_dd)).sum())
        tomorrow_summary_html = (
            f'<b>{curr_month}/{tmr_dd}({WEEKDAY_KR[tmr_wd]}) 항공편수</b><br>'
            f'• <b>T1</b> : {t1_tmr:,}편{_dow_diff_html(t1_tmr, avg_t1)}<br>'
            f'• <b>T2</b> : {t2_tmr:,}편{_dow_diff_html(t2_tmr, avg_t2)}'
        )

    # 각 섹션 표
    df_total = rows_to_df(agg_total(prev_same, curr, max_day), prev_label, curr_label)
    total_html = df_to_html(df_total, prev_label, curr_label)

    df_airline = rows_to_df(agg_airline(prev_same, curr), prev_label, curr_label)
    airline_html = df_to_html(df_airline, prev_label, curr_label)

    df_region = rows_to_df(agg_region(prev_same, curr), prev_label, curr_label)
    region_html = df_to_html(df_region, prev_label, curr_label)

    # 게이트별: D-1 기준 (운항정보 마감)
    d_minus_1 = today - timedelta(days=1)
    if d_minus_1.year == curr_year and d_minus_1.month == curr_month:
        # 일반: D-1이 이번달 → 양쪽 모두 1~D-1일
        gate_cutoff_curr = d_minus_1.day
        gate_cutoff_prev = d_minus_1.day
        gate_note = f'기간 : {prev_label}/{curr_label} 1~{gate_cutoff_curr}일 (운항정보 마감 기준)'
    elif d_minus_1.year == prev_year and d_minus_1.month == prev_month:
        # 미리보기/월초: D-1이 전월 → 전월 1~D-1일, 이번달은 미운항
        gate_cutoff_curr = 0
        gate_cutoff_prev = d_minus_1.day
        gate_note = (
            f'기간 : {prev_label} 1~{gate_cutoff_prev}일 '
            f'(D-1 운항정보 마감 기준, {curr_label}은 미운항)'
        )
    else:
        gate_cutoff_curr = max_day
        gate_cutoff_prev = max_day
        gate_note = f'기간 : {prev_label}/{curr_label} 1~{max_day}일 (운항정보 마감 기준)'

    gate_prev = prev[prev["DD"] <= gate_cutoff_prev]
    gate_curr = (
        curr[curr["DD"] <= gate_cutoff_curr] if gate_cutoff_curr > 0 else curr.iloc[0:0]
    )
    df_gate = rows_to_df(agg_gate(gate_prev, gate_curr), prev_label, curr_label)
    if gate_cutoff_curr == 0:
        df_gate["T1_전월비"] = float("nan")
        df_gate["T2_전월비"] = float("nan")
    gate_html = df_to_html(df_gate, prev_label, curr_label, total_row_idx=0)

    # 일자별 표
    today_day_for_daily = (
        today.day if (today.year == curr_year and today.month == curr_month) else None
    )
    daily_html = daily_combined_html(
        curr, prev, curr_label, prev_label,
        curr_year, curr_month, prev_year, prev_month,
        max_day, today_day_for_daily,
    )

    # 일자별 차트용 데이터: x축은 1~말일 전체, 데이터 없는 날은 null
    last_day_prev = calendar.monthrange(prev_year, prev_month)[1]
    last_day_curr = calendar.monthrange(curr_year, curr_month)[1]
    chart_last_day = max(last_day_curr, last_day_prev)

    def _day_series(df, term, data_until):
        cnt = df[df["터미널"] == term].groupby("DD").size().to_dict()
        return [int(cnt.get(d, 0)) if d <= data_until else None
                for d in range(1, chart_last_day + 1)]

    chart_data = {
        "max_day": max_day,
        "chart_last_day": chart_last_day,
        "curr_label": curr_label,
        "prev_label": prev_label,
        "red_days": _red_days(curr_year, curr_month, chart_last_day),
        "today_day": today.day if (today.year == curr_year and today.month == curr_month) else None,
        "series": {
            "T1_prev": _day_series(prev, "T1", last_day_prev),
            "T1_curr": _day_series(curr, "T1", max_day),
            "T2_prev": _day_series(prev, "T2", last_day_prev),
            "T2_curr": _day_series(curr, "T2", max_day),
        },
    }

    response = templates.TemplateResponse(
        request,
        "index.html",
        {
            "is_next_preview": is_next_preview,
            "is_month_end": is_month_end,
            "nxt_month": nxt_month,
            "today_month": today.month,
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
            "tomorrow_summary_html": tomorrow_summary_html,
            "chart_data_json": json.dumps(chart_data, ensure_ascii=False),
            "unmapped": unmapped,
            "regions": REGIONS + ["중동", "대양주", "국내선"],  # 입력 시 원본 지역 허용
            "countries": countries,
            "export_default_start": date(prev_year, prev_month, 1).isoformat(),
            "export_default_end": _latest_available_date().isoformat(),
            "export_min_date": _earliest_available_date().isoformat(),
            "export_max_date": _latest_available_date().isoformat(),
        },
    )
    response.headers["Cache-Control"] = "public, max-age=300, stale-while-revalidate=600"
    return response


# ---------- 도착지 매핑 추가 (GitHub Contents API) ----------
GH_OWNER = "jongho1972"
GH_REPO = "icn-dashboard"
GH_PATH = "항공편목적지.txt"
GH_BRANCH = "main"
GH_API = f"https://api.github.com/repos/{GH_OWNER}/{GH_REPO}/contents/{GH_PATH}"


class DestEntry(BaseModel):
    destination: str = Field(..., min_length=1)
    country: str = Field(..., min_length=1)
    region: str = Field(..., min_length=1)


class AddDestRequest(BaseModel):
    entries: list[DestEntry] = Field(..., min_length=1)


def _github_append(entries: list[DestEntry], token: str) -> dict:
    """GitHub Contents API로 항공편목적지.txt 수정 커밋."""
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }
    # 1) 현재 파일 가져오기
    r = requests.get(GH_API, headers=headers, params={"ref": GH_BRANCH}, timeout=20)
    r.raise_for_status()
    data = r.json()
    sha = data["sha"]
    current = base64.b64decode(data["content"]).decode("utf-8")

    # 2) 신규 라인 append
    lines = current.rstrip("\n").split("\n")
    for e in entries:
        lines.append(f"{e.destination}\t{e.country}\t{e.region}")
    new_content = "\n".join(lines) + "\n"

    # 3) PUT 으로 커밋
    body = {
        "message": f"data: 신규 도착지 {len(entries)}개 추가",
        "content": base64.b64encode(new_content.encode("utf-8")).decode("ascii"),
        "sha": sha,
        "branch": GH_BRANCH,
    }
    r2 = requests.put(GH_API, headers=headers, json=body, timeout=20)
    r2.raise_for_status()
    return r2.json()


@app.get("/api/destinations-health")
async def destinations_health():
    """PAT + GitHub Contents API 접근 가능 여부만 확인 (커밋 없음)."""
    token = os.environ.get("GITHUB_TOKEN", "")
    if not token:
        return JSONResponse(
            {"ok": False, "error": "GITHUB_TOKEN env not set"},
            status_code=500,
        )
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }
    try:
        r = requests.get(GH_API, headers=headers, params={"ref": GH_BRANCH}, timeout=15)
    except Exception as e:
        return JSONResponse({"ok": False, "error": f"request failed: {e}"}, status_code=500)
    if r.status_code == 200:
        d = r.json()
        return {
            "ok": True,
            "file_path": d.get("path"),
            "file_sha": (d.get("sha") or "")[:7],
            "file_size": d.get("size"),
        }
    return JSONResponse(
        {"ok": False, "status": r.status_code, "body": r.text[:200]},
        status_code=r.status_code,
    )


@app.post("/api/add-destinations")
async def add_destinations(req: AddDestRequest):
    token = os.environ.get("GITHUB_TOKEN", "")
    if not token:
        raise HTTPException(
            500,
            "GITHUB_TOKEN 환경변수가 설정되지 않았습니다. "
            "Render Dashboard → Environment 에서 추가하세요.",
        )
    try:
        result = _github_append(req.entries, token)
    except requests.HTTPError as e:
        raise HTTPException(
            502, f"GitHub API 오류 ({e.response.status_code}): {e.response.text[:200]}"
        )
    except Exception as e:
        raise HTTPException(500, f"커밋 실패: {e}")

    # 캐시 무효화 (다음 접속 시 최신 데이터 재로드)
    _CACHE.clear()
    commit_sha = (result.get("commit") or {}).get("sha", "")
    return JSONResponse({"ok": True, "commit_sha": commit_sha, "count": len(req.entries)})


@app.get("/healthz")
async def healthz():
    return {"ok": True, "time": datetime.now(KST).isoformat()}


# ---------- Raw 데이터 Excel 다운로드 ----------
MAX_EXPORT_DAYS = 366  # 한번에 최대 1년 (윤년 포함)


@app.get("/api/export-raw")
async def export_raw(start: str, end: str):
    """start/end (YYYYMMDD) 기간의 Raw 데이터를 Excel로 다운로드.

    Daily_Data pkl → 가공(process_raw) → 날짜 필터 → .xlsx 반환.
    """
    from io import BytesIO

    try:
        start_dt = datetime.strptime(start, "%Y%m%d")
        end_dt = datetime.strptime(end, "%Y%m%d")
    except ValueError:
        raise HTTPException(400, "start/end 는 YYYYMMDD 형식이어야 합니다.")
    if end_dt < start_dt:
        start_dt, end_dt = end_dt, start_dt
    span = (end_dt - start_dt).days + 1
    if span > MAX_EXPORT_DAYS:
        raise HTTPException(
            400, f"최대 {MAX_EXPORT_DAYS}일 범위까지만 내보낼 수 있습니다 (요청: {span}일)."
        )

    # 범위에 걸친 월 목록 (YYYYMM)
    months: list[str] = []
    cur = start_dt.replace(day=1)
    while cur <= end_dt:
        months.append(cur.strftime("%Y%m"))
        cur = (cur.replace(year=cur.year + 1, month=1)
               if cur.month == 12 else cur.replace(month=cur.month + 1))

    dest = load_dest()
    # 월별로 로드: Final_Data cum(이미 가공) 우선, 없으면 Daily_Data + process_raw
    dfs = []
    for yyyymm in months:
        y, m = int(yyyymm[:4]), int(yyyymm[4:])
        cum = load_final_month(str(FINAL_DIR), yyyymm)
        if len(cum) > 0:
            part = cum[(cum["YYYY"] == y) & (cum["MM"] == m)]
        else:
            raw_daily = load_daily_month(str(DAILY_DIR), yyyymm)
            if len(raw_daily) == 0:
                continue
            part = process_raw(raw_daily, dest)
            part = part[(part["YYYY"] == y) & (part["MM"] == m)]
        if len(part) > 0:
            dfs.append(part)

    if not dfs:
        raise HTTPException(404, "해당 기간의 데이터가 없습니다.")

    df = pd.concat(dfs, ignore_index=True).drop_duplicates("Flight_Key")
    # 실제 운항일 기준 필터
    df = df[(df["YYYYMMDD"] >= start_dt) & (df["YYYYMMDD"] <= end_dt)]
    if len(df) == 0:
        raise HTTPException(404, "해당 기간의 데이터가 없습니다.")
    df = df.sort_values(["YYYYMMDD", "출발시각", "출발분", "운항편명"]).reset_index(drop=True)

    # 대시보드 집계 기준 구분 컬럼 추가
    df["항공사 구분"] = df["항공사"].fillna("").apply(airline_group)
    df["도착지 구분"] = df["지역"].replace(REGION_MERGE)
    df["게이트 구분"] = df["탑승구"].apply(gate_group)

    # 출발시간 (HH:MM) — 출발시각·출발분 결합
    # Excel이 시간 타입으로 자동 변환하지 않도록 ="HH:MM" 수식 형태로 저장
    df["출발시간"] = (
        '="'
        + df["출발시각"].astype(int).map("{:02d}".format)
        + ":" + df["출발분"].astype(int).map("{:02d}".format)
        + '"'
    )

    # Raw_Data_Format.txt 순서
    cols = [
        "YYYYMMDD", "출발시간", "목적지", "항공사", "운항편명",
        "터미널", "체크인 카운터", "탑승구",
        "remark", "CODESHARE",
        "항공사 구분", "국가", "도착지 구분", "게이트 구분",
        "Master_Flight", "scheduleDateTime", "estimatedDateTime", "Flight_Key",
    ]
    df = df[[c for c in cols if c in df.columns]]
    df["YYYYMMDD"] = df["YYYYMMDD"].dt.strftime("%Y-%m-%d")
    for dt_col in ("scheduleDateTime", "estimatedDateTime"):
        if dt_col in df.columns:
            df[dt_col] = df[dt_col].dt.strftime("%Y-%m-%d %-H:%M")

    # CSV (UTF-8 BOM) — Excel로 열어도 한글 깨지지 않음, 매우 빠름
    buf = BytesIO()
    df.to_csv(buf, index=False, encoding="utf-8-sig")
    size = buf.tell()
    buf.seek(0)

    fname = f"icn_flights_{start_dt.strftime('%Y%m%d')}_{end_dt.strftime('%Y%m%d')}.csv"
    return StreamingResponse(
        buf,
        media_type="text/csv; charset=utf-8-sig",
        headers={
            "Content-Disposition": f'attachment; filename="{fname}"',
            "Content-Length": str(size),
            "Cache-Control": "no-store",
        },
    )
