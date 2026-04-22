"""✈️ 인천공항 출발편 현황 대시보드 — 접속 시 API로 최신 데이터 수집."""
import math
import os
import sys
from datetime import date, datetime

import altair as alt
import pandas as pd
import streamlit as st

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from data_loader import build_current_month, build_previous_month
from aggregator import (agg_airline, agg_daily, agg_gate, agg_region, agg_total,
                        pct, prepare, rows_to_df)

st.set_page_config(
    page_title="인천공항 출발편 현황",
    page_icon="✈️",
    layout="wide",
    initial_sidebar_state="collapsed",
)

BASE = os.path.dirname(os.path.abspath(__file__))
DAILY_DIR = os.path.join(BASE, "Daily_Data")
FINAL_DIR = os.path.join(BASE, "Final_Data")
DEST_PATH = os.path.join(BASE, "항공편목적지.txt")

# ---------- 전역 스타일 ----------
STYLE = """
<style>
.stApp { background: #ffffff; }
.block-container { padding-top: 1.4rem; max-width: 1320px; }
.block-container [data-testid="stVerticalBlock"] { gap: 0.6rem; }

h1.page-title {
  font-weight: 800; color: #003875; margin: 0; font-size: 2rem; letter-spacing: -0.02em;
}
h3 {
  border-left: 4px solid #003875;
  padding: 2px 0 2px 10px;
  font-weight: 600; font-size: 1.1rem; color: #222;
  margin: 1.4rem 0 0.5rem 0;
}
.period-note { color: #555; font-size: 13px; margin-top: 2px; }
.update-badge {
  display: inline-block; background: #f4f6fa; color: #4a5568;
  padding: 6px 12px; border-radius: 4px; font-size: 13px;
  font-family: ui-monospace, "SF Mono", monospace; border: 1px solid #dde2ea;
}

div[data-testid="stMetricValue"] { font-size: 1.9rem; font-weight: 700; color: #003875; }
div[data-testid="stMetricLabel"] { font-size: 0.9rem; color: #666; font-weight: 500; }
div[data-testid="stMetricDelta"] { font-size: 0.85rem; }

table.icn {
  border-collapse: collapse; width: 100%; margin: 0;
  font-size: 13px; font-family: -apple-system, system-ui, "Noto Sans KR", sans-serif;
}
table.icn th, table.icn td { border: 1px solid #dde2ea; padding: 6px 10px; }
table.icn thead th {
  background: #f4f6fa; font-weight: 700; color: #333; text-align: center;
  line-height: 1.3;
}
table.icn thead th.t1-group { background: #e8f0f8; color: #003875; }
table.icn thead th.t2-group { background: #fdf2e9; color: #a04016; }
table.icn td { text-align: right; }
table.icn td.label {
  text-align: center; font-weight: 600; background: #fafbfc; color: #333;
}
table.icn td.pos { color: #0070C0; font-weight: 600; }
table.icn td.neg { color: #C00000; font-weight: 600; }
table.icn td.dash { color: #c8c8c8; text-align: center; }
table.icn th.t1-last, table.icn td.t1-last { border-right: 2px solid #888; }
table.icn tr.total-row td {
  background: #eef3f9; font-weight: 700; color: #003875;
}
table.icn tr.total-row td.label { background: #e4ecf6; color: #003875; }
</style>
"""


@st.cache_data(ttl=600, show_spinner=False)
def load_dest():
    return pd.read_table(DEST_PATH)


@st.cache_data(ttl=600, show_spinner=False)
def fetch_months(curr_year, curr_month, prev_year, prev_month, service_key, cache_key):
    dest = load_dest()
    curr = build_current_month(DAILY_DIR, dest, service_key, curr_year, curr_month)
    prev = build_previous_month(FINAL_DIR, DAILY_DIR, dest, prev_year, prev_month)
    return prev, curr


st.markdown(STYLE, unsafe_allow_html=True)

# ---------- 데이터 로드 ----------
today = date.today()
curr_year, curr_month = today.year, today.month
prev_year, prev_month = (curr_year - 1, 12) if curr_month == 1 else (curr_year, curr_month - 1)

try:
    service_key = st.secrets["INCHEON_API_KEY"]
except (KeyError, FileNotFoundError):
    st.error("`INCHEON_API_KEY` 가 Streamlit secrets에 설정되지 않았습니다.")
    st.stop()

cache_key = today.isoformat()

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
hc1, hc2 = st.columns([3, 1])
with hc1:
    st.markdown('<h1 class="page-title">인천공항 출발편 현황</h1>', unsafe_allow_html=True)
    st.markdown(
        f'<div class="period-note">'
        f'기간: <b>{prev_label}·{curr_label} 1~{max_day}일 동일기간</b> · '
        f'Master(실제 운항)만 · 국제선'
        f'</div>',
        unsafe_allow_html=True,
    )
with hc2:
    st.markdown(
        f'<div style="text-align:right; margin-top:6px;">'
        f'<span class="update-badge">업데이트 {datetime.now().strftime("%Y-%m-%d %H:%M")}</span>'
        f'</div>',
        unsafe_allow_html=True,
    )
    if st.button("최신 데이터 가져오기", key="refresh", width="stretch"):
        st.cache_data.clear()
        st.rerun()

# ---------- 요약 카드 ----------
t1_p = len(prev_same[prev_same["터미널"] == "T1"])
t1_c = len(curr[curr["터미널"] == "T1"])
t2_p = len(prev_same[prev_same["터미널"] == "T2"])
t2_c = len(curr[curr["터미널"] == "T2"])
tot_p, tot_c = t1_p + t2_p, t1_c + t2_c
avg_p, avg_c = tot_p / max_day, tot_c / max_day


def _delta(c, p, int_ref=True):
    if p == 0: return None
    v = pct(c, p)
    if math.isnan(v): return None
    ref = f"{p:,.0f}" if int_ref else f"{p:,.1f}"
    return f"{v:+.1%}  vs {ref}"


c1, c2, c3, c4 = st.columns(4)
c1.metric(f"T1 ({curr_label})", f"{t1_c:,} 편", _delta(t1_c, t1_p))
c2.metric(f"T2 ({curr_label})", f"{t2_c:,} 편", _delta(t2_c, t2_p))
c3.metric(f"합계 ({curr_label})", f"{tot_c:,} 편", _delta(tot_c, tot_p))
c4.metric(f"일평균 ({curr_label})", f"{avg_c:,.0f} 편", _delta(avg_c, avg_p, int_ref=False))


# ---------- 테이블 렌더러 (2단 헤더 · NaN/0 구분 · 합계 강조) ----------
def df_to_html(df, prev_label, curr_label, total_row_idx=None):
    parts = ['<table class="icn"><thead>']
    parts.append('<tr>')
    parts.append('<th rowspan="2">구분</th>')
    parts.append('<th colspan="3" class="t1-group t1-last">T1</th>')
    parts.append('<th colspan="3" class="t2-group">T2</th>')
    parts.append('</tr><tr>')
    parts.append(f'<th>{prev_label}</th><th>{curr_label}</th><th class="t1-last">전월비</th>')
    parts.append(f'<th>{prev_label}</th><th>{curr_label}</th><th>전월비</th>')
    parts.append('</tr></thead><tbody>')
    cols = list(df.columns)  # [구분, T1_prev, T1_curr, T1_전월비, T2_prev, T2_curr, T2_전월비]
    pct_idx = {3, 6}
    t1_last_idx = 3
    for ri, (_, row) in enumerate(df.iterrows()):
        tr_cls = ' class="total-row"' if total_row_idx is not None and ri == total_row_idx else ""
        parts.append(f'<tr{tr_cls}>')
        for ci, c in enumerate(cols):
            v = row[c]
            t1last = " t1-last" if ci == t1_last_idx else ""
            if c == "구분":
                parts.append(f'<td class="label">{v}</td>')
            elif ci in pct_idx:
                if pd.isna(v):
                    parts.append(f'<td class="dash{t1last}">–</td>')
                elif v > 0:
                    parts.append(f'<td class="pos{t1last}">{v:+.1%}</td>')
                elif v < 0:
                    parts.append(f'<td class="neg{t1last}">{v:+.1%}</td>')
                else:
                    parts.append(f'<td class="{t1last.strip()}">{v:+.1%}</td>')
            else:
                if pd.isna(v):
                    parts.append(f'<td class="dash{t1last}">–</td>')
                elif v == 0:
                    parts.append(f'<td class="dash{t1last}">0</td>')
                else:
                    cls = f' class="{t1last.strip()}"' if t1last else ""
                    parts.append(f"<td{cls}>{v:,.0f}</td>")
        parts.append("</tr>")
    parts.append("</tbody></table>")
    return "".join(parts)


def render_table(df, total_row_idx=None):
    st.markdown(df_to_html(df, prev_label, curr_label, total_row_idx), unsafe_allow_html=True)


# ---------- 섹션들 ----------
st.markdown("### 전체")
df_total = rows_to_df(agg_total(prev_same, curr), prev_label, curr_label)
render_table(df_total, total_row_idx=0)

st.markdown("### 항공사별")
df_airline = rows_to_df(agg_airline(prev_same, curr), prev_label, curr_label)
render_table(df_airline)

st.markdown("### 목적지별 (지역)")
df_region = rows_to_df(agg_region(prev_same, curr), prev_label, curr_label)
render_table(df_region)

st.markdown("### 게이트별")
df_gate = rows_to_df(agg_gate(prev_same, curr), prev_label, curr_label)
render_table(df_gate, total_row_idx=0)
with st.expander("게이트 분류 기준"):
    st.markdown(
        "- **동편**: 탑승구 ≤ 25 또는 251~299\n"
        "- **중앙**: 26~28\n"
        "- **서편**: 29~50 또는 200~250\n"
        "- **탑승동**: 51~199"
    )

# ---------- 일자별 ----------
st.markdown(f"### 일자별 (1~{max_day}일)")
df_daily = rows_to_df(agg_daily(prev_same, curr, max_day), prev_label, curr_label)

tab1, tab2 = st.tabs(["추이 차트", "상세 테이블"])
with tab1:
    chart_src = pd.DataFrame({
        f"T1|{prev_label}": df_daily[f"T1_{prev_label}"].values,
        f"T1|{curr_label}": df_daily[f"T1_{curr_label}"].values,
        f"T2|{prev_label}": df_daily[f"T2_{prev_label}"].values,
        f"T2|{curr_label}": df_daily[f"T2_{curr_label}"].values,
    })
    chart_src["일"] = list(range(1, max_day + 1))
    long = pd.melt(chart_src, id_vars="일", var_name="시리즈", value_name="편수")
    long[["터미널", "기간"]] = long["시리즈"].str.split("|", expand=True)

    chart = (
        alt.Chart(long)
        .mark_line(strokeWidth=2, point=alt.OverlayMarkDef(size=30, filled=True))
        .encode(
            x=alt.X("일:Q", title="일자",
                    axis=alt.Axis(tickMinStep=1, labelFontSize=11)),
            y=alt.Y("편수:Q", title="편수",
                    axis=alt.Axis(labelFontSize=11, format=",")),
            color=alt.Color(
                "터미널:N",
                scale=alt.Scale(domain=["T1", "T2"], range=["#0070C0", "#E8833A"]),
                legend=alt.Legend(title="터미널", orient="top"),
            ),
            strokeDash=alt.StrokeDash(
                "기간:N",
                scale=alt.Scale(domain=[prev_label, curr_label], range=[[5, 4], [1, 0]]),
                legend=alt.Legend(title="기간", orient="top"),
            ),
            tooltip=[
                alt.Tooltip("일:Q", title="일자"),
                alt.Tooltip("터미널:N"),
                alt.Tooltip("기간:N"),
                alt.Tooltip("편수:Q", format=","),
            ],
        )
        .properties(height=380)
        .configure_view(strokeWidth=0)
    )
    st.altair_chart(chart, width="stretch")

with tab2:
    render_table(df_daily)

# ---------- 푸터 (QA 정보는 접기) ----------
with st.expander("데이터 출처·QA"):
    st.markdown(
        f"- 출처: 한국공항공사 인천공항 실시간 출발 API\n"
        f"- 이번달 Raw: **{len(curr):,}건** · 지난달 동일기간: **{len(prev_same):,}건**\n"
        f"- 접속 시 최근 10일치(D-3 ~ D+6) API 실시간 조회 + 과거 누적 저장본 병합\n"
        f"- 매일 17:00 KST 자동 수집으로 누적 저장본 갱신"
    )
