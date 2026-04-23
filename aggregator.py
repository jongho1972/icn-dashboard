"""월간 비교 집계 (전체·일자별·항공사별·목적지별·게이트별)."""
import math

import pandas as pd

KR_ETC = {"이스타항공", "에어부산", "에어서울", "에어프레미아", "플라이강원",
          "하이에어", "파라타항공", "에어인천", "플라이나스"}

AIRLINES = ["대한항공", "아시아나", "진에어", "제주항공", "티웨이", "국내기타", "외국항공"]
REGIONS = ["일본", "동남아", "중국", "미주", "동북아", "유럽", "중동", "대양주", "기타"]
GATES = ["동편", "서편", "중앙", "탑승동"]


def airline_group(a):
    if a == "대한항공": return "대한항공"
    if a == "아시아나항공": return "아시아나"
    if a == "진에어": return "진에어"
    if a == "제주항공": return "제주항공"
    if a == "티웨이항공": return "티웨이"
    if a in KR_ETC: return "국내기타"
    return "외국항공"


def gate_group(g):
    try:
        n = int(g)
    except (ValueError, TypeError):
        return None
    if n <= 25: return "동편"
    if n <= 28: return "중앙"
    if n <= 50: return "서편"
    if n <= 199: return "탑승동"
    if n <= 250: return "서편"
    if n <= 299: return "동편"
    return None


def prepare(df):
    """집계 전 보조 컬럼 추가 + Master + 국제선 필터."""
    df = df.copy()
    df["항공사"] = df["항공사"].fillna("")
    df = df[(df["CODESHARE"] == "Master") & (df["지역"] != "국내선")]
    df["항공사그룹"] = df["항공사"].apply(airline_group)
    df["게이트그룹"] = df["탑승구"].apply(gate_group)
    return df


def pct(c, p):
    return math.nan if p == 0 else (c - p) / p


def _cnt(df, terminal, **kwargs):
    d = df[df["터미널"] == terminal]
    for k, v in kwargs.items():
        d = d[d[k] == v]
    return len(d)


def agg_total(prev, curr):
    return [{
        "구분": "월누적",
        "T1_prev": _cnt(prev, "T1"), "T1_curr": _cnt(curr, "T1"),
        "T2_prev": _cnt(prev, "T2"), "T2_curr": _cnt(curr, "T2"),
    }]


def agg_daily(prev, curr, max_day):
    rows = []
    for d in range(1, max_day + 1):
        rows.append({
            "구분": f"{d}일",
            "T1_prev": _cnt(prev, "T1", DD=d), "T1_curr": _cnt(curr, "T1", DD=d),
            "T2_prev": _cnt(prev, "T2", DD=d), "T2_curr": _cnt(curr, "T2", DD=d),
        })
    return rows


def agg_airline(prev, curr):
    rows = []
    for a in AIRLINES:
        rows.append({
            "구분": a,
            "T1_prev": _cnt(prev, "T1", 항공사그룹=a), "T1_curr": _cnt(curr, "T1", 항공사그룹=a),
            "T2_prev": _cnt(prev, "T2", 항공사그룹=a), "T2_curr": _cnt(curr, "T2", 항공사그룹=a),
        })
    return rows


def agg_region(prev, curr):
    rows = []
    for r in REGIONS:
        rows.append({
            "구분": r,
            "T1_prev": _cnt(prev, "T1", 지역=r), "T1_curr": _cnt(curr, "T1", 지역=r),
            "T2_prev": _cnt(prev, "T2", 지역=r), "T2_curr": _cnt(curr, "T2", 지역=r),
        })
    return rows


def agg_gate(prev, curr):
    rows = []
    p_t1 = prev[(prev["터미널"] == "T1") & prev["게이트그룹"].notna()]
    c_t1 = curr[(curr["터미널"] == "T1") & curr["게이트그룹"].notna()]
    p_t2 = prev[(prev["터미널"] == "T2") & prev["게이트그룹"].notna()]
    c_t2 = curr[(curr["터미널"] == "T2") & curr["게이트그룹"].notna()]
    rows.append({"구분": "소계", "T1_prev": len(p_t1), "T1_curr": len(c_t1), "T2_prev": len(p_t2), "T2_curr": len(c_t2)})
    for g in GATES:
        rows.append({
            "구분": g,
            "T1_prev": int((p_t1["게이트그룹"] == g).sum()),
            "T1_curr": int((c_t1["게이트그룹"] == g).sum()),
            "T2_prev": int((p_t2["게이트그룹"] == g).sum()),
            "T2_curr": int((c_t2["게이트그룹"] == g).sum()),
        })
    return rows


def rows_to_df(rows, prev_label, curr_label):
    """집계 dict 리스트를 표시용 DataFrame으로. 전월비 컬럼 추가."""
    df = pd.DataFrame(rows)
    df[f"T1_{prev_label}"] = df["T1_prev"]
    df[f"T1_{curr_label}"] = df["T1_curr"]
    df["T1_전월비"] = df.apply(lambda r: pct(r["T1_curr"], r["T1_prev"]), axis=1)
    df[f"T2_{prev_label}"] = df["T2_prev"]
    df[f"T2_{curr_label}"] = df["T2_curr"]
    df["T2_전월비"] = df.apply(lambda r: pct(r["T2_curr"], r["T2_prev"]), axis=1)
    cols = ["구분", f"T1_{prev_label}", f"T1_{curr_label}", "T1_전월비",
            f"T2_{prev_label}", f"T2_{curr_label}", "T2_전월비"]
    return df[cols]
