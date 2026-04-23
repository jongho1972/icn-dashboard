"""인천공항 국제선 출발편 현황 대시보드 (FastAPI + Jinja2).

기존 Streamlit 앱을 Render 배포 가능하도록 포팅.
집계 로직은 icn_utils/ 그대로 재사용.
"""
from __future__ import annotations

import base64
import json
import math
import os
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd
import requests
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Request
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
    load_daily_range, process_raw,
)

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


def _latest_available_date() -> date:
    """Daily_Data 내 가장 최근 일자의 pkl 파일에서 날짜 추출."""
    if not DAILY_DIR.is_dir():
        return date.today()
    pkls = sorted(
        f.name for f in DAILY_DIR.iterdir()
        if f.name.startswith("flight_schedule_") and f.suffix == ".pkl"
    )
    if not pkls:
        return date.today()
    ymd = pkls[-1][len("flight_schedule_"):-len(".pkl")]
    try:
        return datetime.strptime(ymd, "%Y%m%d").date()
    except ValueError:
        return date.today()


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
            f'(전월비 {sign}{abs(r):.1%})</span>')


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
            "unmapped": unmapped,
            "regions": REGIONS + ["중동", "대양주", "국내선"],  # 입력 시 원본 지역 허용
            "export_default_start": date(prev_year, prev_month, 1).isoformat(),
            "export_default_end": _latest_available_date().isoformat(),
        },
    )


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
MAX_EXPORT_DAYS = 180  # 한번에 최대 180일


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

    raw = load_daily_range(
        str(DAILY_DIR),
        start_dt.strftime("%Y%m%d"),
        end_dt.strftime("%Y%m%d"),
    )
    if len(raw) == 0:
        raise HTTPException(404, "해당 기간의 데이터가 없습니다.")

    dest = load_dest()
    df = process_raw(raw, dest)
    # 실제 운항일 기준 필터
    df = df[(df["YYYYMMDD"] >= start_dt) & (df["YYYYMMDD"] <= end_dt)]
    df = df.sort_values(["YYYYMMDD", "출발시각", "출발분", "운항편명"]).reset_index(drop=True)

    # 대시보드 집계 기준 구분 컬럼 추가
    df["항공사 구분"] = df["항공사"].fillna("").apply(airline_group)
    df["도착지 구분"] = df["지역"].replace(REGION_MERGE)
    df["게이트 구분"] = df["탑승구"].apply(gate_group)

    # 정렬 · 컬럼 정리 (엑셀에 무리 없는 순서)
    cols = [
        "YYYYMMDD", "YYYY", "MM", "DD", "출발시각", "출발분",
        "터미널",
        "운항편명", "항공사", "항공사 구분",
        "목적지", "국가", "지역", "도착지 구분",
        "체크인 카운터", "탑승구", "게이트 구분",
        "CODESHARE", "Master_Flight",
        "remark", "scheduleDateTime", "estimatedDateTime", "Flight_Key",
    ]
    df = df[[c for c in cols if c in df.columns]]
    df["YYYYMMDD"] = df["YYYYMMDD"].dt.strftime("%Y-%m-%d")
    df["scheduleDateTime"] = df["scheduleDateTime"].dt.strftime("%Y-%m-%d %H:%M")
    df["estimatedDateTime"] = df["estimatedDateTime"].dt.strftime("%Y-%m-%d %H:%M")

    buf = BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as w:
        df.to_excel(w, index=False, sheet_name="Raw")
        ws = w.sheets["Raw"]
        # 첫 행 고정, 열 너비 자동(approx)
        ws.freeze_panes = "A2"
        for i, col in enumerate(df.columns, start=1):
            max_len = max(len(str(col)), int(df[col].astype(str).str.len().quantile(0.95)) if len(df) else 0)
            ws.column_dimensions[ws.cell(row=1, column=i).column_letter].width = min(max_len + 2, 28)
    buf.seek(0)

    fname = f"icn_flights_{start_dt.strftime('%Y%m%d')}_{end_dt.strftime('%Y%m%d')}.xlsx"
    return StreamingResponse(
        buf,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={
            "Content-Disposition": f'attachment; filename="{fname}"',
            "Cache-Control": "no-store",
        },
    )
