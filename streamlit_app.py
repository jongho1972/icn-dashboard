"""✈️ 인천공항 출발편 현황 대시보드 — 접속 시 API로 최신 데이터 수집."""
import math
import os
import sys
from datetime import date, datetime, timedelta

import altair as alt
import pandas as pd
import streamlit as st

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from data_loader import build_current_month, build_previous_month
from aggregator import (agg_airline, agg_daily, agg_gate, agg_region, agg_total,
                        pct, prepare, rows_to_df)

st.set_page_config(
    page_title="인천공항 국제선 출발편 현황",
    page_icon="✈️",
    layout="wide",
    initial_sidebar_state="collapsed",
)

BASE = os.path.dirname(os.path.abspath(__file__))
DAILY_DIR = os.path.join(BASE, "Daily_Data")
FINAL_DIR = os.path.join(BASE, "Final_Data")
DEST_PATH = os.path.join(BASE, "항공편목적지.txt")

# ---------- 전역 스타일 (웹 디자이너 리뷰 반영: 1.28 비율 타입 스케일 + 4pt 리듬) ----------
STYLE = """
<style>
/* ===== 기본 ===== */
.stApp { background: #ffffff; }
.block-container {
  padding-top: 2.75rem;
  padding-bottom: 3rem;
  max-width: 1320px;
  font-family: -apple-system, BlinkMacSystemFont, "Pretendard",
               "Noto Sans KR", "Apple SD Gothic Neo", sans-serif;
  color: #111827;
  letter-spacing: -0.011em;
}
.block-container [data-testid="stVerticalBlock"] { gap: 1rem; }

/* ===== 타이틀 ===== */
h1.page-title {
  font-weight: 800; color: #003875;
  margin: 0; font-size: 1.875rem;      /* 30px */
  letter-spacing: -0.025em; line-height: 1.2;
}
.period-note {
  color: #64748b; font-size: 13px;
  margin-top: 6px; line-height: 1.5;
  letter-spacing: -0.01em;
}
.period-note b { color: #334155; font-weight: 600; }

/* ===== 섹션 헤더 (h3) : 앵커 숨기고 위계 강화 ===== */
.block-container h3 {
  border-left: 4px solid #003875;
  padding: 6px 0 6px 12px;
  font-weight: 700; font-size: 1.3125rem;   /* 21px */
  color: #1a1a1a;
  letter-spacing: -0.02em; line-height: 1.3;
  margin: 2.5rem 0 0.75rem 0;
}
.block-container h3:first-of-type { margin-top: 1.75rem; }
.block-container h3 a,
.block-container h3 [data-testid="stHeaderActionElements"] { display: none !important; }

/* ===== 업데이트 배지 + 버튼 ===== */
.header-right {
  display: flex; flex-direction: column; align-items: flex-end;
  gap: 8px; margin-bottom: 12px;
}
.update-badge {
  display: inline-flex; align-items: center;
  background: #f1f5f9; color: #475569;
  padding: 6px 12px; border-radius: 4px;
  font-size: 12px; line-height: 1.5;
  font-family: ui-monospace, "SF Mono", monospace;
  border: 1px solid #e2e8f0; letter-spacing: 0;
}
.stButton > button {
  font-size: 13.5px; font-weight: 600;
  padding: 6px 14px; border-radius: 6px;
}

/* ===== 요약 카드 (metric) ===== */
div[data-testid="stMetric"] { background: #ffffff; padding: 4px 0 0 0; }
div[data-testid="stMetricLabel"] {
  font-size: 13px; color: #667085; font-weight: 500;
  margin-bottom: 4px; letter-spacing: -0.01em;
}
div[data-testid="stMetricLabel"] > div { font-size: 13px !important; }
div[data-testid="stMetricValue"] {
  font-size: 1.75rem; font-weight: 700; color: #003875;
  line-height: 1.15; margin-bottom: 6px;
  font-variant-numeric: tabular-nums;
  letter-spacing: -0.02em;
}
div[data-testid="stMetricValue"] > div { font-size: 1.75rem !important; }
div[data-testid="stMetricDelta"] {
  font-size: 12.5px; font-weight: 500;
  font-variant-numeric: tabular-nums;
}

/* ===== 테이블 ===== */
table.icn {
  border-collapse: collapse; width: 100%;
  margin: 4px 0 0 0;
  font-size: 13.5px; line-height: 1.45;
  font-variant-numeric: tabular-nums;
  font-family: -apple-system, BlinkMacSystemFont,
               "Pretendard", "Noto Sans KR", sans-serif;
  color: #111827; letter-spacing: -0.01em;
}
table.icn th, table.icn td {
  border: 1px solid #e2e8f0; padding: 8px 12px;
}
table.icn thead th {
  background: #f8fafc; font-weight: 700; font-size: 12.5px;
  color: #475569; text-align: center;
  padding: 10px 12px; line-height: 1.35;
  letter-spacing: 0;
}
table.icn thead th.t1-group { background: #e8f0f8; color: #003875; }
table.icn thead th.t2-group { background: #fdf2e9; color: #a04016; }
table.icn td { text-align: right; }
table.icn td.label {
  text-align: center; font-weight: 600;
  background: #fafbfc; color: #334155;
}
table.icn td.pos { color: #0070C0; font-weight: 600; }
table.icn td.neg { color: #C00000; font-weight: 600; }
table.icn td.dash { color: #cbd5e1; text-align: center; }
table.icn th.t1-last, table.icn td.t1-last { border-right: 2px solid #94a3b8; }
table.icn tr.total-row td {
  background: #eef3f9; font-weight: 700; color: #003875;
}
table.icn tr.total-row td.label { background: #e4ecf6; color: #003875; }

/* ===== Streamlit 기본 간격 누수 차단 ===== */
.block-container [data-testid="stMarkdownContainer"] p { margin: 0; }
.stTabs [data-baseweb="tab-list"] { gap: 24px; }
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
hc1, hc_badge, hc_btn = st.columns([5, 2, 2], vertical_alignment="bottom")
with hc1:
    st.markdown('<h1 class="page-title">인천공항 국제선 출발편 현황</h1>', unsafe_allow_html=True)
    st.markdown(
        f'<div class="period-note">'
        f'기간: <b>{prev_label}·{curr_label} 1~{max_day}일 동일기간</b>'
        f'</div>',
        unsafe_allow_html=True,
    )
with hc_badge:
    st.markdown(
        f'<div style="text-align:right;">'
        f'<span class="update-badge">업데이트 {datetime.now().strftime("%Y-%m-%d %H:%M")}</span>'
        f'</div>',
        unsafe_allow_html=True,
    )
with hc_btn:
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


c1, c2, c3, c4 = st.columns(4, gap="medium")
c1.metric(f"T1 ({curr_label})", f"{t1_c:,} 편", _delta(t1_c, t1_p))
c2.metric(f"T2 ({curr_label})", f"{t2_c:,} 편", _delta(t2_c, t2_p))
c3.metric(f"합계 ({curr_label})", f"{tot_c:,} 편", _delta(tot_c, tot_p))
c4.metric(f"일평균 ({curr_label})", f"{avg_c:,.0f} 편", _delta(avg_c, avg_p, int_ref=False))


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
# D-1 기준으로 재필터 (탑승구 미배정 미래편 영향 제거)
d_minus_1 = today - timedelta(days=1)
gate_cutoff = d_minus_1.day if (d_minus_1.year == curr_year and d_minus_1.month == curr_month) else max_day
gate_prev = prev_same[prev_same["DD"] <= gate_cutoff]
gate_curr = curr[curr["DD"] <= gate_cutoff]
st.caption(
    f"기준: **{prev_label}·{curr_label} 1~{gate_cutoff}일** "
    f"(오늘 D-1 = {d_minus_1.strftime('%Y-%m-%d')}까지, 실제 운항 완료분 기준)"
)
df_gate = rows_to_df(agg_gate(gate_prev, gate_curr), prev_label, curr_label)
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
    # 수동 범례 (실선=이번달, 점선=지난달)
    st.markdown(
        f"""<div style="display:flex;gap:24px;font-size:13px;color:#475569;
                        margin:0 0 10px 8px;align-items:center;">
          <span style="font-weight:600;color:#64748b;">기간</span>
          <span style="display:inline-flex;align-items:center;gap:8px;">
            <svg width="32" height="4"><line x1="0" y1="2" x2="32" y2="2"
              stroke="#334155" stroke-width="2.5"/></svg>
            <b style="color:#1a1a1a;">{curr_label}</b>
          </span>
          <span style="display:inline-flex;align-items:center;gap:8px;">
            <svg width="32" height="4"><line x1="0" y1="2" x2="32" y2="2"
              stroke="#334155" stroke-width="2.5" stroke-dasharray="5,4"/></svg>
            <b style="color:#1a1a1a;">{prev_label}</b>
          </span>
        </div>""",
        unsafe_allow_html=True,
    )

    chart_src = pd.DataFrame({
        f"T1|{prev_label}": df_daily[f"T1_{prev_label}"].values,
        f"T1|{curr_label}": df_daily[f"T1_{curr_label}"].values,
        f"T2|{prev_label}": df_daily[f"T2_{prev_label}"].values,
        f"T2|{curr_label}": df_daily[f"T2_{curr_label}"].values,
    })
    chart_src["일"] = list(range(1, max_day + 1))
    long = pd.melt(chart_src, id_vars="일", var_name="시리즈", value_name="편수")
    long[["터미널", "기간"]] = long["시리즈"].str.split("|", expand=True)

    # 현재달 기준 주말·공휴일 일자 → X축 라벨 색상용
    try:
        import holidays as _holidays
        _kr_hol = _holidays.KR(years=curr_year)
    except Exception:
        _kr_hol = {}
    red_days = []
    for _d in range(1, max_day + 1):
        _dt = date(curr_year, curr_month, _d)
        if _dt.weekday() >= 5 or _dt in _kr_hol:
            red_days.append(_d)
    red_expr = f"indexof({red_days}, datum.value) >= 0 ? '#C00000' : '#334155'"

    # 두 터미널 공통 Y축 도메인 (절대 비교)
    y_min = int(long["편수"].min()) - 10
    y_max = int(long["편수"].max()) + 10

    def _terminal_chart(terminal, color):
        df_t = long[long["터미널"] == terminal]
        return (
            alt.Chart(df_t)
            .mark_line(strokeWidth=2.5, point=alt.OverlayMarkDef(size=25, filled=True))
            .encode(
                x=alt.X(
                    "일:Q", title="일자",
                    scale=alt.Scale(domain=[1, max_day], nice=False, padding=6),
                    axis=alt.Axis(
                        values=list(range(1, max_day + 1)),
                        tickMinStep=1, labelFontSize=11,
                        labelColor={"expr": red_expr},
                        labelFontWeight={"expr": f"indexof({red_days}, datum.value) >= 0 ? 'bold' : 'normal'"},
                    ),
                ),
                y=alt.Y("편수:Q", title="항공편수",
                        scale=alt.Scale(domain=[y_min, y_max], nice=True),
                        axis=alt.Axis(labelFontSize=11, format=",d", tickCount=8)),
                color=alt.value(color),
                strokeDash=alt.StrokeDash(
                    "기간:N",
                    scale=alt.Scale(domain=[curr_label, prev_label], range=[[1, 0], [5, 4]]),
                    legend=None,
                ),
                tooltip=[
                    alt.Tooltip("일:Q", title="일자"),
                    alt.Tooltip("기간:N"),
                    alt.Tooltip("편수:Q", title="편수", format=","),
                ],
            )
            .properties(
                width="container", height=240,
                title=alt.Title(
                    text=terminal, anchor="start",
                    fontSize=15, fontWeight="bold", color="#1a1a1a",
                    dx=4, offset=6,
                ),
            )
        )

    st.altair_chart(_terminal_chart("T1", "#0070C0"), width="stretch")
    st.altair_chart(_terminal_chart("T2", "#E8833A"), width="stretch")

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
