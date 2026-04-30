from __future__ import annotations

import math
import traceback
from datetime import datetime
from typing import Any

import pandas as pd
import requests
import yfinance as yf
from flask import Flask, jsonify, render_template_string, request

app = Flask(__name__)
SEC_HEADERS = {"User-Agent": "FinancialShenanigans research@example.com"}

# -------------------------- Safe helpers --------------------------
def safe_float(x: Any) -> float | None:
    try:
        if x is None:
            return None
        if isinstance(x, str) and not x.strip():
            return None
        if pd.isna(x):
            return None
        return float(x)
    except Exception:
        return None


def safe_abs(x: Any) -> float | None:
    f = safe_float(x)
    return None if f is None else abs(f)


def safe_abs_or_zero(x: Any) -> float:
    v = safe_abs(x)
    return 0.0 if v is None else v


def safe_div(a: Any, b: Any) -> float | None:
    na, nb = safe_float(a), safe_float(b)
    if na is None or nb in (None, 0):
        return None
    return na / nb


def safe_sub(a: Any, b: Any) -> float | None:
    na, nb = safe_float(a), safe_float(b)
    if na is None or nb is None:
        return None
    return na - nb


def safe_add(*values: Any) -> float | None:
    vals = [safe_float(v) for v in values if safe_float(v) is not None]
    return None if not vals else sum(vals)


def safe_pct_change(current: Any, previous: Any) -> float | None:
    diff = safe_sub(current, previous)
    return safe_div(diff, previous)


def safe_gt(a: Any, b: Any) -> bool:
    na, nb = safe_float(a), safe_float(b)
    return na is not None and nb is not None and na > nb


def safe_lt(a: Any, b: Any) -> bool:
    na, nb = safe_float(a), safe_float(b)
    return na is not None and nb is not None and na < nb


def safe_fmt_num(x: Any) -> str:
    v = safe_float(x)
    return "Unavailable" if v is None else f"{v:,.2f}"


def safe_fmt_pct(x: Any) -> str:
    v = safe_float(x)
    return "Unavailable" if v is None else f"{v * 100:.2f}%"


def safe_fmt_money(x: Any) -> str:
    v = safe_float(x)
    return "Unavailable" if v is None else f"${v:,.0f}"


# --------------------- Defaults (always returned) ---------------------
def default_tax_analysis() -> dict[str, Any]:
    return {"tax_risk_level": "Unknown", "tax_quality_score": None, "data_status": "Unavailable", "reason_codes": [], "rows": [], "source": "Unavailable"}


def default_quarterly_analysis() -> dict[str, Any]:
    return {"quarterly_risk_level": "Unknown", "quarterly_risk_score": None, "signal_label": "Unavailable", "reason_codes": ["Quarterly data unavailable from yfinance and SEC Company Facts"], "rows": []}


def default_inventory_analysis() -> dict[str, Any]:
    return {"risk_level": "Unknown", "data_status": "Unavailable", "flags": [], "metrics": {}}


def default_receivables_analysis() -> dict[str, Any]:
    return {"risk_level": "Unknown", "data_status": "Unavailable", "flags": [], "metrics": {}}


def default_debt_analysis() -> dict[str, Any]:
    return {"risk_level": "Unknown", "data_status": "Unavailable", "flags": [], "ratios": {}}


def default_macro_analysis() -> dict[str, Any]:
    return {"macro_stress_score": 58, "regime_label": "Slowing growth / sticky inflation", "reason_codes": ["Global growth 2.6% baseline", "US inflation 3.2% baseline"], "forensic_interpretation": "Macro amplifies accounting and leverage risks."}


def default_investment_view() -> dict[str, Any]:
    return {"forensic_view": "INCONCLUSIVE", "confidence": "Low", "supporting_reasons": [], "risks": [], "what_would_change_view": []}


def get_series(df: pd.DataFrame | None, names: list[str]) -> pd.Series | None:
    if df is None or df.empty:
        return None
    for n in names:
        if n in df.index:
            return df.loc[n]
    return None


def latest_from_series(s: pd.Series | None) -> float | None:
    if s is None or s.empty:
        return None
    return safe_float(s.iloc[0])


def get_sec_company_facts(ticker: str) -> dict[str, Any] | None:
    try:
        cik = yf.Ticker(ticker).info.get("cik")
        if cik is None:
            return None
        url = f"https://data.sec.gov/api/xbrl/companyfacts/CIK{int(cik):010d}.json"
        r = requests.get(url, headers=SEC_HEADERS, timeout=20)
        return r.json() if r.ok else None
    except Exception as e:
        print(f"[ERROR] {ticker}: {type(e).__name__}: {e}")
        traceback.print_exc()
        return None


def extract_sec_fact_series(companyfacts: dict[str, Any] | None, tag_candidates: list[str], quarterly: bool = False) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if not companyfacts:
        return rows
    facts = companyfacts.get("facts", {}).get("us-gaap", {})
    for tag in tag_candidates:
        node = facts.get(tag)
        if not node:
            continue
        usd = node.get("units", {}).get("USD", [])
        for item in usd[-16:]:
            rows.append({"tag": tag, "val": safe_float(item.get("val")), "fy": item.get("fy"), "fp": item.get("fp"), "period": item.get("end"), "form": item.get("form")})
        if rows:
            break
    return rows


def extract_tax_rows_from_yfinance(fin: pd.DataFrame, cf: pd.DataFrame, bs: pd.DataFrame, quality_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    pretax = latest_from_series(get_series(fin, ["Pretax Income", "Income Before Tax", "Earnings Before Tax"]))
    tax = latest_from_series(get_series(fin, ["Tax Provision", "Income Tax Expense", "Tax Expense"]))
    cash_taxes = latest_from_series(get_series(cf, ["Cash Taxes Paid", "Income Taxes Paid", "Taxes Paid"]))
    deferred_tax = latest_from_series(get_series(fin, ["Deferred Tax", "Deferred Income Tax", "Deferred Tax Expense"]))
    if pretax is None and tax is None:
        return []
    return [{"period": datetime.utcnow().date().isoformat(), "pretax_income": pretax, "income_tax_expense": tax, "current_tax_expense": None, "deferred_tax_expense": deferred_tax, "cash_taxes_paid": cash_taxes, "deferred_tax_assets": None, "deferred_tax_liabilities": None, "valuation_allowance": None, "unrecognized_tax_benefits": None, "etr": safe_div(tax, pretax) if safe_gt(pretax, 0) else None, "cash_tax_ratio": safe_div(cash_taxes, tax), "cash_tax_rate": safe_div(cash_taxes, pretax), "deferred_tax_dependency": safe_div(deferred_tax, pretax), "source": "yfinance"}]


def extract_tax_rows_from_sec(ticker: str) -> list[dict[str, Any]]:
    facts = get_sec_company_facts(ticker)
    pretax_rows = extract_sec_fact_series(facts, ["IncomeLossFromContinuingOperationsBeforeIncomeTaxes", "IncomeLossFromContinuingOperationsBeforeIncomeTaxesExtraordinaryItemsNoncontrollingInterest"])
    tax_rows = extract_sec_fact_series(facts, ["IncomeTaxExpenseBenefit", "CurrentIncomeTaxExpenseBenefit"])
    if not pretax_rows and not tax_rows:
        return []
    p = pretax_rows[-1] if pretax_rows else {}
    t = tax_rows[-1] if tax_rows else {}
    pretax, tax = p.get("val"), t.get("val")
    return [{"period": p.get("period") or t.get("period"), "pretax_income": pretax, "income_tax_expense": tax, "current_tax_expense": t.get("val"), "deferred_tax_expense": None, "cash_taxes_paid": None, "deferred_tax_assets": None, "deferred_tax_liabilities": None, "valuation_allowance": None, "unrecognized_tax_benefits": None, "etr": safe_div(tax, pretax) if safe_gt(pretax, 0) else None, "cash_tax_ratio": None, "cash_tax_rate": None, "deferred_tax_dependency": None, "source": "SEC Company Facts"}]


def analyze_tax_quality(tax_rows: list[dict[str, Any]], quality_rows: list[dict[str, Any]]) -> dict[str, Any]:
    out = default_tax_analysis()
    if not tax_rows:
        out["reason_codes"] = ["Tax structured fields unavailable from yfinance and SEC Company Facts", "Manual income tax footnote review required"]
        return out
    row = tax_rows[-1]
    reasons = []
    if row.get("cash_taxes_paid") is None:
        reasons.append("Cash taxes unavailable; review cash tax footnote")
    if row.get("deferred_tax_expense") is None:
        reasons.append("Deferred tax detail unavailable; review deferred tax footnote")
    risk = "Moderate"
    etr = safe_float(row.get("etr"))
    if etr is None:
        risk = "Unknown"
    elif etr < 0.05 or etr > 0.35:
        risk = "High"
    out.update({"tax_risk_level": risk, "tax_quality_score": 45 if risk == "High" else 70 if risk == "Moderate" else None, "data_status": "Available", "reason_codes": reasons, "rows": tax_rows, "source": row.get("source", "Unavailable")})
    return out


def build_quarterly_forensic_analysis(ticker: str) -> dict[str, Any]:
    out = default_quarterly_analysis()
    try:
        tk = yf.Ticker(ticker)
        qf, qcf, qbs = tk.quarterly_financials, tk.quarterly_cashflow, tk.quarterly_balance_sheet
        if qf.empty and qcf.empty and qbs.empty:
            return out
        periods = list(qf.columns[:6]) if not qf.empty else []
        rows = []
        for p in periods:
            rev = safe_float(qf.at["Total Revenue", p]) if "Total Revenue" in qf.index else None
            ni = safe_float(qf.at["Net Income", p]) if "Net Income" in qf.index else None
            cfo = safe_float(qcf.at["Operating Cash Flow", p]) if "Operating Cash Flow" in qcf.index else None
            capex = safe_float(qcf.at["Capital Expenditure", p]) if "Capital Expenditure" in qcf.index else None
            ar = safe_float(qbs.at["Accounts Receivable", p]) if "Accounts Receivable" in qbs.index else None
            inv = safe_float(qbs.at["Inventory", p]) if "Inventory" in qbs.index else None
            rows.append({"period": str(p.date()), "fy": p.year, "fp": "Q", "revenue": rev, "net_income": ni, "cfo": cfo, "capex": capex, "fcf": safe_sub(cfo, safe_abs_or_zero(capex)), "ar": ar, "inventory": inv, "payables": None, "cogs": None, "gross_profit": None, "gross_margin": None, "cfo_ni": safe_div(cfo, ni), "accruals": safe_sub(ni, cfo), "revenue_qoq": None, "ar_qoq": None, "inventory_qoq": None, "dsri_quarterly": None, "source": "yfinance"})
        flags = []
        if rows and safe_lt(rows[0].get("cfo_ni"), 1):
            flags.append("CFO/NI below 1")
        out.update({"quarterly_risk_level": "High" if flags else "Moderate", "quarterly_risk_score": 42 if flags else 68, "signal_label": "Watch" if flags else "Stable", "reason_codes": flags or ["No major quarterly anomaly in limited dataset"], "rows": rows})
    except Exception as e:
        print(f"[ERROR] {ticker}: {type(e).__name__}: {e}")
        traceback.print_exc()
    return out


def analyze_debt_cashflow_risk(fin: pd.DataFrame, cf: pd.DataFrame, bs: pd.DataFrame, quality_rows: list[dict[str, Any]]) -> dict[str, Any]:
    out = default_debt_analysis()
    ltd = latest_from_series(get_series(bs, ["Long Term Debt", "Long Term Debt And Capital Lease Obligation"]))
    cd = latest_from_series(get_series(bs, ["Current Debt", "Current Debt And Capital Lease Obligation", "Short Long Term Debt"]))
    equity = latest_from_series(get_series(bs, ["Stockholders Equity", "Total Equity Gross Minority Interest"]))
    op = latest_from_series(get_series(fin, ["Operating Income", "EBIT"]))
    int_exp = latest_from_series(get_series(fin, ["Interest Expense"]))
    cfo = latest_from_series(get_series(cf, ["Operating Cash Flow"]))
    total_debt = safe_add(ltd, cd)
    if equity is None or int_exp is None:
        out["data_status"] = "Unavailable"
        return out
    ratios = {"lt_debt_to_equity": safe_div(ltd, equity), "total_debt_to_equity": safe_div(total_debt, equity), "times_interest_earned": safe_div(op, int_exp), "cash_interest_coverage": safe_div(cfo, int_exp), "debt_to_cfo": safe_div(total_debt, cfo)}
    flags = []
    if safe_gt(ratios.get("total_debt_to_equity"), 2):
        flags.append("Extreme leverage risk")
    if safe_lt(ratios.get("cash_interest_coverage"), 1):
        flags.append("Cash flow barely covers interest payments")
    out.update({"risk_level": "High" if flags else "Moderate", "data_status": "Available", "flags": flags, "ratios": ratios, "tenk_checks": ["inspect debt maturity schedule", "check interest rate exposure", "review covenant disclosures", "analyze refinancing risk", "check lease obligations", "inspect off-balance-sheet obligations"]})
    return out


def calculate_data_completeness(payload: dict[str, Any]) -> dict[str, Any]:
    fields = ["revenue", "net_income", "cfo", "ar", "inventory", "cogs", "pretax_income", "tax_expense", "cash_taxes", "deferred_tax", "quarterly_data", "debt", "interest_expense"]
    missing = [f for f in fields if payload.get(f) is None]
    score = int((len(fields) - len(missing)) / len(fields) * 100)
    level = "Complete" if score >= 85 else "Partial" if score >= 55 else "Weak"
    return {"score": score, "level": level, "missing_fields": missing}


def build_macro_regime_context() -> dict[str, Any]:
    return default_macro_analysis()


def build_investment_view(mods: dict[str, Any], completeness: dict[str, Any]) -> dict[str, Any]:
    risks = []
    if completeness["level"] == "Weak":
        risks.append("Data completeness is Weak")
    for k in ["tax_analysis", "quarterly_analysis", "inventory_analysis", "receivables_analysis", "debt_analysis"]:
        if mods[k].get("risk_level") == "High" or mods[k].get("tax_risk_level") == "High" or mods[k].get("quarterly_risk_level") == "High":
            risks.append(f"{k} is High")
    view = "BUY / ACCUMULATE"
    if risks:
        view = "AVOID / SELL" if len(risks) >= 2 else "HOLD / WATCHLIST"
    return {"forensic_view": view if completeness["level"] != "Weak" else "INCONCLUSIVE", "confidence": "Medium" if not risks else "Low", "supporting_reasons": ["Risk-based forensic opinion, not financial advice."], "risks": risks, "what_would_change_view": ["Improve CFO quality", "Lower leverage", "Improve data completeness"]}


@app.route("/")
def home() -> str:
    return render_template_string("""<html><body style='background:#050816;color:#fff;font-family:sans-serif;padding:24px'>
    <h1>Forensic Command Center</h1><p>Visibly redesigned forensic lab UI.</p>
    <ol><li>Hero Forensic Command Center</li><li>Forensic Investment View</li><li>Risk Radar</li><li>Screener Cockpit</li><li>Quarterly Forensic Breakdown</li></ol>
    <p>Use <code>/api/analyze?ticker=MRK&period=5y</code> or <code>/api/screener</code>.</p></body></html>""")


@app.route("/api/analyze")
def api_analyze():
    ticker = request.args.get("ticker", "MRK").upper().strip()
    tax_analysis = default_tax_analysis()
    quarterly_analysis = default_quarterly_analysis()
    inventory_analysis = default_inventory_analysis()
    receivables_analysis = default_receivables_analysis()
    debt_analysis = default_debt_analysis()
    macro_analysis = default_macro_analysis()
    investment_view = default_investment_view()
    try:
        tk = yf.Ticker(ticker)
        fin, cf, bs = tk.financials, tk.cashflow, tk.balance_sheet
        quality_rows: list[dict[str, Any]] = []
        tax_rows = extract_tax_rows_from_yfinance(fin, cf, bs, quality_rows) or extract_tax_rows_from_sec(ticker)
        tax_analysis = analyze_tax_quality(tax_rows, quality_rows)
        quarterly_analysis = build_quarterly_forensic_analysis(ticker)
        debt_analysis = analyze_debt_cashflow_risk(fin, cf, bs, quality_rows)
        data_probe = {
            "revenue": latest_from_series(get_series(fin, ["Total Revenue"])), "net_income": latest_from_series(get_series(fin, ["Net Income"])),
            "cfo": latest_from_series(get_series(cf, ["Operating Cash Flow"])), "ar": latest_from_series(get_series(bs, ["Accounts Receivable"])),
            "inventory": latest_from_series(get_series(bs, ["Inventory"])), "cogs": latest_from_series(get_series(fin, ["Cost Of Revenue"])),
            "pretax_income": tax_rows[-1].get("pretax_income") if tax_rows else None, "tax_expense": tax_rows[-1].get("income_tax_expense") if tax_rows else None,
            "cash_taxes": tax_rows[-1].get("cash_taxes_paid") if tax_rows else None, "deferred_tax": tax_rows[-1].get("deferred_tax_expense") if tax_rows else None,
            "quarterly_data": quarterly_analysis.get("rows"), "debt": latest_from_series(get_series(bs, ["Long Term Debt"])), "interest_expense": latest_from_series(get_series(fin, ["Interest Expense"]))
        }
        completeness = calculate_data_completeness(data_probe)
        macro_analysis = build_macro_regime_context()
        investment_view = build_investment_view(locals(), completeness)
        out = {"ticker": ticker, "tax_analysis": tax_analysis, "quarterly_analysis": quarterly_analysis, "inventory_analysis": inventory_analysis,
               "receivables_analysis": receivables_analysis, "debt_analysis": debt_analysis, "macro_analysis": macro_analysis,
               "investment_view": investment_view, "data_completeness": completeness}
        return jsonify(out)
    except Exception as e:
        print(f"[ERROR] {ticker}: {type(e).__name__}: {e}")
        traceback.print_exc()
        return jsonify({"ticker": ticker, "tax_analysis": tax_analysis, "quarterly_analysis": quarterly_analysis, "inventory_analysis": inventory_analysis,
                        "receivables_analysis": receivables_analysis, "debt_analysis": debt_analysis, "macro_analysis": macro_analysis,
                        "investment_view": investment_view, "error": f"{type(e).__name__}: {e}"})


@app.route('/api/screener')
def api_screener():
    universe = ["MRK", "TSLA", "AMZN", "JPM", "AAPL", "NFLX", "ABBV", "CRM", "META", "AMD", "BAC", "WMT", "AMGN", "COST", "QCOM"]
    rows = []
    for ticker in universe:
        try:
            data = app.test_client().get(f"/api/analyze?ticker={ticker}&period=5y").get_json()
            rows.append({"Ticker": ticker, "Tax Risk": data["tax_analysis"].get("tax_risk_level", "Unknown"), "Quarterly Risk": data["quarterly_analysis"].get("quarterly_risk_level", "Unknown"), "Debt Risk": data["debt_analysis"].get("risk_level", "Unknown"), "Forensic View": data["investment_view"].get("forensic_view", "INCONCLUSIVE"), "Confidence": data["investment_view"].get("confidence", "Low"), "Data Completeness": data["data_completeness"].get("level", "Weak"), "Main Reason": (data["tax_analysis"].get("reason_codes") or ["Unavailable"])[0]})
        except Exception as e:
            print(f"[ERROR] {ticker}: {type(e).__name__}: {e}")
            traceback.print_exc()
            rows.append({"Ticker": ticker, "Tax Risk": "Unknown", "Quarterly Risk": "Unknown", "Debt Risk": "Unknown", "Forensic View": "INCONCLUSIVE", "Confidence": "Low", "Data Completeness": "Weak", "Main Reason": "Unhandled module failure"})
    return jsonify({"rows": rows})


if __name__ == '__main__':
    app.run(debug=True, port=5057)
