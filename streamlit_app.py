"""✈️ 인천공항 국제선 출발편 현황 대시보드 — 접속 시 API로 최신 데이터 수집."""
import math
import os
import sys
from datetime import date, datetime, timedelta

import altair as alt
import pandas as pd
import streamlit as st

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from icn_utils.data_loader import build_current_month, build_previous_month
from icn_utils.aggregator import (agg_airline, agg_daily, agg_gate, agg_region, agg_total,
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

# ---------- 전역 스타일 (프로 디자이너 리뷰 반영: Carbon/Bloomberg 톤) ----------
STYLE = """
<style>
:root {
  --navy-900: #0B2E5C;
  --navy-700: #13407F;
  --blue-500: #1F6FEB;
  --blue-050: #DCE8F6;
  --blue-025: #F4F7FB;
  --orange-600: #C26420;
  --orange-025: #FBF3EB;
  --red-600: #B42318;
  --slate-700: #334155;
  --slate-500: #64748B;
  --slate-400: #94A3B8;
  --slate-200: #CBD5E1;
  --slate-100: #E5EAF0;
  --slate-050: #EEF2F6;
}

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
.header-row {
  display: flex; align-items: baseline;
  justify-content: space-between; gap: 16px;
  flex-wrap: wrap;
}
h1.page-title {
  font-weight: 800; color: var(--navy-900);
  margin: 0; font-size: 1.875rem;
  letter-spacing: -0.025em; line-height: 1.2;
}
.period-note {
  background: transparent; border: none; padding: 0 0 0 2px;
  color: var(--slate-500); font-size: 13px; line-height: 1.5;
  margin-top: 8px; letter-spacing: -0.01em;
}

/* ===== 섹션 헤더 (h3) ===== */
.block-container h3 {
  border-left: 4px solid var(--navy-900);
  padding: 6px 0 6px 12px;
  font-weight: 700; font-size: 1.3125rem;
  color: var(--navy-900);
  letter-spacing: -0.02em; line-height: 1.3;
  margin: 2.25rem 0 0.75rem 0;
}
.block-container h3:first-of-type { margin-top: 1.25rem; }
.block-container h3 a,
.block-container h3 [data-testid="stHeaderActionElements"] { display: none !important; }

/* ===== 업데이트 배지 ===== */
.update-badge {
  display: inline !important;
  background: transparent !important;
  border: none !important;
  box-shadow: none !important;
  padding: 0 !important; border-radius: 0 !important;
  color: var(--slate-500);
  font-size: 13px; line-height: 1.5; font-weight: 500;
  font-family: ui-monospace, "SF Mono", monospace;
  letter-spacing: 0;
}

/* ===== 테이블 (Carbon style: 가로선 제거, 행 구분만) ===== */
table.icn {
  border-collapse: collapse; width: 100%;
  margin: 4px 0 0 0;
  font-size: 13.5px; line-height: 1.45;
  font-variant-numeric: tabular-nums;
  font-feature-settings: "tnum" 1, "ss01" 1;
  font-family: -apple-system, BlinkMacSystemFont,
               "Pretendard", "Noto Sans KR", sans-serif;
  color: #111827; letter-spacing: -0.01em;
  border-top: 1.5px solid var(--navy-900);
}
table.icn th, table.icn td {
  border: none;
  border-bottom: 1px solid var(--slate-050);
  padding: 7px 14px 7px 10px;
}
table.icn thead th {
  background: transparent; font-weight: 600; font-size: 12px;
  color: var(--slate-500); text-align: center;
  padding: 10px 12px; line-height: 1.35;
  letter-spacing: 0.01em;
  border-bottom: 1px solid var(--slate-200);
}
table.icn thead th.t1-group {
  background: var(--blue-025); color: var(--navy-900);
  border-bottom: 1px solid var(--slate-200);
}
table.icn thead th.t2-group {
  background: var(--orange-025); color: var(--orange-600);
  border-bottom: 1px solid var(--slate-200);
}
table.icn td { text-align: center; }
table.icn td.label {
  text-align: center;
  background: transparent; color: var(--slate-700); font-weight: 500;
}
table.icn td.pos { color: var(--blue-500); font-weight: 500; }
table.icn td.neg { color: var(--red-600); font-weight: 500; }
table.icn td.dash { color: var(--slate-400); font-weight: 400; text-align: center; }

/* T1/T2 구분: 1px + padding 호흡 */
table.icn th.t1-last, table.icn td.t1-last {
  border-right: 1px solid var(--slate-200);
  padding-right: 18px;
}
table.icn th.t1-last + th, table.icn td.t1-last + td { padding-left: 18px; }

/* 합계행 강조 제거 — 모든 행 동일 스타일로 통일 */

/* ===== 캡션·탭 ===== */
[data-testid="stCaptionContainer"] {
  color: var(--slate-500); font-size: 12.5px;
}
.stTabs [data-baseweb="tab-list"] { gap: 24px; }

/* ===== Streamlit 기본 간격 누수 차단 ===== */
.block-container [data-testid="stMarkdownContainer"] p { margin: 0; }
</style>
"""


@st.cache_data(ttl=600, show_spinner=False)
def load_dest():
    return pd.read_table(DEST_PATH)


@st.cache_data(ttl=3600, show_spinner=False)
def fetch_months(curr_year, curr_month, prev_year, prev_month, service_key):
    """1시간 캐시. 모든 세션이 캐시 공유 → 새로고침 즉시 표시."""
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

with st.spinner("인천공항 API 호출 중... (최근 10일치 실시간 조회)"):
    prev, curr = fetch_months(curr_year, curr_month, prev_year, prev_month, service_key)

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
st.markdown(
    f'<div class="header-row">'
    f'<h1 class="page-title">인천공항 국제선 출발편 현황</h1>'
    f'<span class="update-badge">업데이트 {datetime.now().strftime("%Y-%m-%d %H:%M")}</span>'
    f'</div>'
    f'<div class="period-note">기간 : {prev_label}/{curr_label} 1~{max_day}일 동일기간</div>',
    unsafe_allow_html=True,
)

# ---------- 요약 집계 ----------
t1_p = len(prev_same[prev_same["터미널"] == "T1"])
t1_c = len(curr[curr["터미널"] == "T1"])
t2_p = len(prev_same[prev_same["터미널"] == "T2"])
t2_c = len(curr[curr["터미널"] == "T2"])
tot_p, tot_c = t1_p + t2_p, t1_c + t2_c
avg_p, avg_c = tot_p / max_day, tot_c / max_day


def _trend(c, p):
    """전월비 부호+퍼센트 HTML span. arrow 없이 색상만."""
    if p == 0:
        return ""
    r = pct(c, p)
    if math.isnan(r):
        return ""
    color = "#1F6FEB" if r > 0 else ("#B42318" if r < 0 else "#64748B")
    sign = "+" if r > 0 else ("−" if r < 0 else "±")  # U+2212 minus
    return (f' <span style="color:{color};font-weight:500;'
            f'font-variant-numeric:tabular-nums;">'
            f'{sign}{abs(r):.1%}</span>')


# ---------- 테이블 렌더러 ----------
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
    cols = list(df.columns)
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
                # 값 없음(NaN 또는 관련 기본 데이터가 0) → 공란
                if pd.isna(v):
                    parts.append(f'<td class="{t1last.strip()}"></td>')
                elif v > 0:
                    parts.append(f'<td class="pos{t1last}">+{v:.1%}</td>')
                elif v < 0:
                    parts.append(f'<td class="neg{t1last}">−{abs(v):.1%}</td>')
                else:
                    parts.append(f'<td class="{t1last.strip()}">{v:+.1%}</td>')
            else:
                # 데이터가 없거나 0 → 공란 처리
                if pd.isna(v) or v == 0:
                    parts.append(f'<td class="{t1last.strip()}"></td>')
                else:
                    cls = f' class="{t1last.strip()}"' if t1last else ""
                    parts.append(f"<td{cls}>{v:,.0f}</td>")
        parts.append("</tr>")
    parts.append("</tbody></table>")
    return "".join(parts)


def render_table(df, total_row_idx=None):
    st.markdown(df_to_html(df, prev_label, curr_label, total_row_idx), unsafe_allow_html=True)


# ---------- 전체 (섹션 헤더 없이 요약+표만) ----------
st.markdown(
    f'<div style="color:#0B2E5C;font-size:17px;font-weight:500;'
    f'margin:1.25rem 0 12px 2px;line-height:1.7;letter-spacing:-0.01em;">'
    f'<b>월누적</b> {tot_c:,} 편{_trend(tot_c, tot_p)}'
    f'&nbsp;&nbsp;&middot;&nbsp;&nbsp;'
    f'<b>일평균</b> {avg_c:,.0f} 편'
    f'</div>',
    unsafe_allow_html=True,
)
df_total = rows_to_df(agg_total(prev_same, curr, max_day), prev_label, curr_label)
render_table(df_total)

# ---------- 일자별 (전체 다음 위치) ----------
st.markdown("### 일자별")
df_daily = rows_to_df(agg_daily(prev_same, curr, max_day), prev_label, curr_label)

# 추이 차트
if True:
    st.markdown(
        f"""<div style="display:flex;gap:24px;font-size:13px;color:#475569;
                        margin:0 0 10px 8px;align-items:center;">
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

    # 현재달 기준 주말·공휴일 (X축 라벨 slate-400 bold)
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
    weekend_color_expr = (
        f"indexof({red_days}, datum.value) >= 0 ? '#C00000' : '#64748B'"
    )
    weekend_weight_expr = (
        f"indexof({red_days}, datum.value) >= 0 ? 'bold' : 'normal'"
    )

    # 두 터미널 공통 Y축 도메인
    y_min = int(long["편수"].min()) - 10
    y_max = int(long["편수"].max()) + 10

    def _terminal_chart(terminal, color):
        df_t = long[long["터미널"] == terminal]
        base = alt.Chart(df_t).encode(
            x=alt.X(
                "일:Q", title=None,
                scale=alt.Scale(domain=[1, max_day], nice=False, padding=6),
                axis=alt.Axis(
                    values=list(range(1, max_day + 1)),
                    tickMinStep=1, labelFontSize=11,
                    labelColor={"expr": weekend_color_expr},
                    labelFontWeight={"expr": weekend_weight_expr},
                    labelOverlap=False, labelPadding=6,
                    domain=True, domainColor="#CBD5E1",
                    ticks=True, tickColor="#CBD5E1", tickSize=4,
                    grid=False,
                ),
            ),
            y=alt.Y(
                "편수:Q", title=None,
                scale=alt.Scale(domain=[y_min, y_max], nice=True),
                axis=alt.Axis(
                    labelFontSize=11, format=",d", tickCount=6,
                    labelColor="#64748B",
                    domain=True, domainColor="#CBD5E1",
                    ticks=True, tickColor="#CBD5E1", tickSize=4,
                    grid=True, gridColor="#F1F5F9", gridDash=[2, 3],
                ),
            ),
            strokeDash=alt.StrokeDash(
                "기간:N",
                scale=alt.Scale(
                    domain=[curr_label, prev_label],
                    range=[[1, 0], [4, 4]],
                ),
                legend=None,
            ),
            color=alt.value(color),
            tooltip=[
                alt.Tooltip("일:Q", title="일자"),
                alt.Tooltip("기간:N"),
                alt.Tooltip("편수:Q", title="편수", format=","),
            ],
        )
        line = base.mark_line(strokeWidth=2)
        # 이번달만 point (filled)
        df_curr_only = df_t[df_t["기간"] == curr_label]
        pts = (
            alt.Chart(df_curr_only)
            .mark_point(size=18, filled=True, color=color)
            .encode(
                x=alt.X("일:Q", scale=alt.Scale(domain=[1, max_day], nice=False, padding=6)),
                y=alt.Y("편수:Q", scale=alt.Scale(domain=[y_min, y_max], nice=True)),
            )
        )
        return (
            (line + pts)
            .properties(
                width="container", height=220,
                title=alt.Title(
                    text=terminal, anchor="start",
                    fontSize=13, fontWeight=600, color="#0B2E5C",
                    dy=-4, offset=4,
                ),
            )
            .configure_view(stroke=None)
        )

    st.altair_chart(_terminal_chart("T1", "#1F6FEB"), width="stretch")
    st.altair_chart(_terminal_chart("T2", "#C26420"), width="stretch")

# 상세 테이블 (차트 바로 아래)
render_table(df_daily)

# ---------- 항공사별 ----------
st.markdown("### 항공사별")
df_airline = rows_to_df(agg_airline(prev_same, curr), prev_label, curr_label)
render_table(df_airline)

# ---------- 도착지별 ----------
st.markdown("### 도착지별")
df_region = rows_to_df(agg_region(prev_same, curr), prev_label, curr_label)
render_table(df_region)

# ---------- 게이트별 ----------
st.markdown("### 게이트별")
# D-1 기준으로 재필터 (탑승구 미배정 미래편 영향 제거)
d_minus_1 = today - timedelta(days=1)
gate_cutoff = d_minus_1.day if (d_minus_1.year == curr_year and d_minus_1.month == curr_month) else max_day
gate_prev = prev_same[prev_same["DD"] <= gate_cutoff]
gate_curr = curr[curr["DD"] <= gate_cutoff]
st.markdown(
    f'<div class="period-note">'
    f'기간 : {prev_label}/{curr_label} 1~{gate_cutoff}일 (운항정보 마감 기준)'
    f'</div>',
    unsafe_allow_html=True,
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

# ---------- 푸터 ----------
with st.expander("데이터 출처·QA"):
    st.markdown(
        f"- 출처: 한국공항공사 인천공항 실시간 출발 API\n"
        f"- 이번달 Raw: **{len(curr):,}건** · 지난달 동일기간: **{len(prev_same):,}건**\n"
        f"- 접속 시 최근 10일치(D-3 ~ D+6) API 실시간 조회 + 과거 누적 저장본 병합\n"
        f"- 매일 17:00 KST 자동 수집으로 누적 저장본 갱신"
    )
