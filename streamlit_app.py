"""✈️ 인천공항 출발편 현황 대시보드 — 접속 시 API로 최신 데이터 수집."""
import math
import os
import sys
from datetime import date, datetime

import pandas as pd
import streamlit as st

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from data_loader import build_current_month, build_previous_month
from aggregator import (agg_airline, agg_daily, agg_gate, agg_region, agg_total,
                        pct, prepare, rows_to_df)

st.set_page_config(page_title="인천공항 출발편 현황", page_icon="✈️", layout="wide")

BASE = os.path.dirname(os.path.abspath(__file__))
DAILY_DIR = os.path.join(BASE, "Daily_Data")
FINAL_DIR = os.path.join(BASE, "Final_Data")
DEST_PATH = os.path.join(BASE, "항공편목적지.txt")


@st.cache_data(ttl=600, show_spinner=False)
def load_dest():
    return pd.read_table(DEST_PATH)


@st.cache_data(ttl=600, show_spinner=False)
def fetch_months(curr_year, curr_month, prev_year, prev_month, service_key, cache_key):
    """이번달 + 지난달 데이터 로드 (cache_key로 무효화 제어)."""
    dest = load_dest()
    curr = build_current_month(DAILY_DIR, dest, service_key, curr_year, curr_month)
    prev = build_previous_month(FINAL_DIR, DAILY_DIR, dest, prev_year, prev_month)
    return prev, curr


# ---------- 사이드바 ----------
st.sidebar.title("✈️ ICN Dashboard")
st.sidebar.markdown("인천공항 출발편 월간 비교 현황")
st.sidebar.divider()

if st.sidebar.button("🔄 최신 데이터로 새로고침", type="primary", width="stretch"):
    st.cache_data.clear()
    st.rerun()

st.sidebar.divider()
st.sidebar.caption("ℹ️ 접속 시 최근 10일치(D-3~D+6)는 API 실시간 조회, 그 외 과거 데이터는 누적 저장본 사용")

# ---------- 데이터 로드 ----------
today = date.today()
curr_year, curr_month = today.year, today.month
prev_year, prev_month = (curr_year - 1, 12) if curr_month == 1 else (curr_year, curr_month - 1)

try:
    service_key = st.secrets["INCHEON_API_KEY"]
except (KeyError, FileNotFoundError):
    st.error("⚠️ `INCHEON_API_KEY`가 Streamlit secrets에 설정되지 않았습니다.\n\n로컬: `.streamlit/secrets.toml`에 `INCHEON_API_KEY = \"...\"` 추가\nCloud: 앱 설정 → Secrets")
    st.stop()

cache_key = today.isoformat()  # 하루 단위 캐시

with st.spinner("인천공항 API 호출 중... (최근 10일치 실시간 조회)"):
    prev, curr = fetch_months(curr_year, curr_month, prev_year, prev_month, service_key, cache_key)

if len(curr) == 0:
    st.error("이번달 데이터를 불러오지 못했습니다. API serviceKey와 Daily_Data 폴더를 확인하세요.")
    st.stop()

prev = prepare(prev)
curr = prepare(curr)

max_day = int(curr["DD"].max())
prev_same = prev[prev["DD"] <= max_day]

prev_label = f"{prev_month}월"
curr_label = f"{curr_month}월"

# ---------- 헤더 ----------
st.title("✈️ 인천공항 출발편 현황")
st.caption(
    f"기간: **{prev_label}·{curr_label} 1~{max_day}일 동일기간 비교**  ·  Master(실제 운항)만 · 국내선 제외  ·  "
    f"업데이트: {datetime.now().strftime('%Y-%m-%d %H:%M')}"
)

# ---------- 요약 카드 ----------
t1_p = len(prev_same[prev_same["터미널"] == "T1"])
t1_c = len(curr[curr["터미널"] == "T1"])
t2_p = len(prev_same[prev_same["터미널"] == "T2"])
t2_c = len(curr[curr["터미널"] == "T2"])

def _delta(c, p):
    v = pct(c, p)
    return f"{v:+.1%}" if not math.isnan(v) else None

c1, c2, c3, c4 = st.columns(4)
c1.metric(f"T1 ({curr_label})", f"{t1_c:,}", _delta(t1_c, t1_p))
c2.metric(f"T1 ({prev_label})", f"{t1_p:,}")
c3.metric(f"T2 ({curr_label})", f"{t2_c:,}", _delta(t2_c, t2_p))
c4.metric(f"T2 ({prev_label})", f"{t2_p:,}")

st.divider()

# ---------- 테이블 스타일 ----------
TABLE_CSS = """
<style>
table.icn { border-collapse: collapse; width: 100%; font-size: 13px; font-family: -apple-system, system-ui, sans-serif; }
table.icn th, table.icn td { border: 1px solid #d0d0d0; padding: 4px 8px; }
table.icn th { background: #f0f2f6; text-align: center; font-weight: 600; color: #333; }
table.icn td { text-align: right; }
table.icn td.label { text-align: center; font-weight: 600; background: #fafafa; }
table.icn td.pos { color: #0070C0; font-weight: 600; }
table.icn td.neg { color: #C00000; font-weight: 600; }
table.icn td.zero { color: #bbb; }
</style>
"""


def df_to_html(df):
    """DataFrame을 스타일 적용된 HTML 테이블 문자열로 변환."""
    pct_cols = {c for c in df.columns if "전월비" in c}
    cols = list(df.columns)
    parts = ['<table class="icn"><thead><tr>']
    for c in cols:
        parts.append(f"<th>{c}</th>")
    parts.append("</tr></thead><tbody>")
    for _, row in df.iterrows():
        parts.append("<tr>")
        for c in cols:
            v = row[c]
            if c == "구분":
                parts.append(f'<td class="label">{v}</td>')
            elif c in pct_cols:
                if pd.isna(v):
                    parts.append('<td class="zero"></td>')
                elif v > 0:
                    parts.append(f'<td class="pos">{v:+.1%}</td>')
                elif v < 0:
                    parts.append(f'<td class="neg">{v:+.1%}</td>')
                else:
                    parts.append(f'<td>{v:+.1%}</td>')
            else:
                if pd.isna(v) or v == 0:
                    parts.append('<td class="zero"></td>')
                else:
                    parts.append(f"<td>{v:,.0f}</td>")
        parts.append("</tr>")
    parts.append("</tbody></table>")
    return "".join(parts)


def render_table(df):
    st.markdown(df_to_html(df), unsafe_allow_html=True)


st.markdown(TABLE_CSS, unsafe_allow_html=True)


# ---------- 섹션: 전체 ----------
st.subheader("📊 전체")
df_total = rows_to_df(agg_total(prev_same, curr), prev_label, curr_label)
render_table(df_total)

st.subheader("✈️ 항공사별")
df_airline = rows_to_df(agg_airline(prev_same, curr), prev_label, curr_label)
render_table(df_airline)

st.subheader("🌏 목적지별 (지역)")
df_region = rows_to_df(agg_region(prev_same, curr), prev_label, curr_label)
render_table(df_region)

st.subheader("🚪 게이트별")
st.caption(f"기준: 1~{max_day}일 MTD  ·  ≤25·251-299=동편  ·  26-28=중앙  ·  29-50·200-250=서편  ·  51-199=탑승동")
df_gate = rows_to_df(agg_gate(prev_same, curr), prev_label, curr_label)
render_table(df_gate)

# ---------- 일자별 ----------
st.subheader(f"📅 일자별 (1~{max_day}일)")
df_daily = rows_to_df(agg_daily(prev_same, curr, max_day), prev_label, curr_label)

tab1, tab2 = st.tabs(["📈 추이 차트", "📋 상세 테이블"])

with tab1:
    chart_df = pd.DataFrame({
        f"T1 {prev_label}": df_daily[f"T1_{prev_label}"].values,
        f"T1 {curr_label}": df_daily[f"T1_{curr_label}"].values,
        f"T2 {prev_label}": df_daily[f"T2_{prev_label}"].values,
        f"T2 {curr_label}": df_daily[f"T2_{curr_label}"].values,
    }, index=range(1, max_day + 1))
    chart_df.index.name = "일"
    st.line_chart(chart_df, height=400)

with tab2:
    render_table(df_daily)

# ---------- 푸터 ----------
st.divider()
st.caption(
    f"데이터 출처: 한국공항공사 인천공항 실시간 출발 API  ·  "
    f"이번달 Raw: {len(curr):,}건 / 지난달 동일기간: {len(prev_same):,}건"
)
