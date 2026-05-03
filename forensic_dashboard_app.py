from __future__ import annotations

import math
import re
import traceback
from datetime import datetime
from typing import Any

import pandas as pd
import requests
import yfinance as yf
from flask import Flask, jsonify, render_template_string, request

app = Flask(__name__)
SEC_USER_AGENT = "FinancialShenanigans/1.0 contact@example.com"
SEC_HEADERS = {"User-Agent": SEC_USER_AGENT}
SEC_TICKER_CIK_CACHE: dict[str, str] = {}
SEC_SUBMISSIONS_CACHE: dict[str, dict[str, Any]] = {}
SEC_FILING_TEXT_CACHE: dict[str, dict[str, Any]] = {}
SEC_COMPANY_FACTS_CACHE: dict[str, dict[str, Any]] = {}

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
    if ticker in SEC_COMPANY_FACTS_CACHE:
        return SEC_COMPANY_FACTS_CACHE[ticker]
    try:
        cik = yf.Ticker(ticker).info.get("cik")
        if cik is None:
            return None
        url = f"https://data.sec.gov/api/xbrl/companyfacts/CIK{int(cik):010d}.json"
        r = requests.get(url, headers=SEC_HEADERS, timeout=20)
        if not r.ok:
            return None
        payload = r.json()
        SEC_COMPANY_FACTS_CACHE[ticker] = payload
        return payload
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




def get_cik_from_ticker(ticker: str) -> str | None:
    symbol = (ticker or "").upper().strip()
    if not symbol:
        return None
    if symbol in SEC_TICKER_CIK_CACHE:
        return SEC_TICKER_CIK_CACHE[symbol]
    try:
        r = requests.get("https://www.sec.gov/files/company_tickers.json", headers=SEC_HEADERS, timeout=20)
        if r.ok:
            payload = r.json()
            for row in payload.values() if isinstance(payload, dict) else []:
                if str(row.get("ticker", "")).upper() == symbol:
                    cik = f"{int(row.get('cik_str')):010d}"
                    SEC_TICKER_CIK_CACHE[symbol] = cik
                    return cik
    except Exception as e:
        print(f"[WARN] SEC ticker mapping failed for {symbol}: {e}")

    try:
        cik = yf.Ticker(symbol).info.get("cik")
        if cik is not None:
            cik_fmt = f"{int(cik):010d}"
            SEC_TICKER_CIK_CACHE[symbol] = cik_fmt
            return cik_fmt
    except Exception as e:
        print(f"[WARN] yfinance CIK fallback failed for {symbol}: {e}")
    return None


def get_sec_submissions(cik: str) -> dict[str, Any] | None:
    if cik in SEC_SUBMISSIONS_CACHE:
        return SEC_SUBMISSIONS_CACHE[cik]
    try:
        url = f"https://data.sec.gov/submissions/CIK{cik}.json"
        r = requests.get(url, headers=SEC_HEADERS, timeout=20)
        if not r.ok:
            return None
        payload = r.json()
        SEC_SUBMISSIONS_CACHE[cik] = payload
        return payload
    except Exception as e:
        print(f"[ERROR] SEC submissions {cik}: {e}")
        return None


def get_latest_filing_metadata(ticker: str, form_type: str) -> dict[str, Any]:
    cik = get_cik_from_ticker(ticker)
    if not cik:
        return {"form": form_type, "available": False}
    subs = get_sec_submissions(cik)
    if not subs:
        return {"form": form_type, "available": False, "cik": cik}
    recent = subs.get("filings", {}).get("recent", {})
    forms = recent.get("form", [])
    for idx, form in enumerate(forms):
        if form != form_type:
            continue
        accession = recent.get("accessionNumber", [None])[idx]
        primary_doc = recent.get("primaryDocument", [None])[idx]
        filing_date = recent.get("filingDate", [None])[idx]
        accession_no_dash = (accession or "").replace("-", "")
        filing_url = f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/{accession_no_dash}/{primary_doc}" if accession and primary_doc else None
        return {"form": form_type, "cik": cik, "filing_date": filing_date, "accession_number": accession, "primary_document": primary_doc, "filing_url": filing_url, "available": bool(filing_url)}
    return {"form": form_type, "cik": cik, "available": False}


def download_sec_filing_html(filing_url: str | None) -> str:
    if not filing_url:
        return ""
    if filing_url in SEC_FILING_TEXT_CACHE:
        return SEC_FILING_TEXT_CACHE[filing_url].get("html", "")
    try:
        r = requests.get(filing_url, headers=SEC_HEADERS, timeout=25)
        html = r.text if r.ok else ""
        SEC_FILING_TEXT_CACHE[filing_url] = {"html": html}
        return html
    except Exception as e:
        print(f"[ERROR] SEC filing download failed: {e}")
        return ""


def clean_sec_html_to_text(html: str) -> str:
    if not html:
        return ""
    text = re.sub(r"<script.*?</script>", " ", html, flags=re.S | re.I)
    text = re.sub(r"<style.*?</style>", " ", text, flags=re.S | re.I)
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"\s+", " ", text)
    return text[:400000]


def _extract_keyword_window(text: str, keyword: str, window: int = 1500) -> list[str]:
    hits = []
    for m in re.finditer(re.escape(keyword), text, flags=re.I):
        start = max(0, m.start() - window)
        end = min(len(text), m.end() + window)
        hits.append(text[start:end])
        if len(hits) >= 2:
            break
    return hits


def extract_filing_sections(text: str) -> dict[str, str]:
    patterns = {"md&a": ["management's discussion", "md&a", "liquidity and capital resources"], "risk_factors": ["risk factors"], "notes_financials": ["notes to consolidated financial statements"], "income_taxes": ["income taxes", "effective tax rate"], "revenue": ["revenue recognition", "contract liabilities"], "receivables": ["allowance for doubtful accounts", "credit losses"], "inventory": ["inventory", "inventories"], "debt": ["debt", "borrowings", "revolving credit"], "legal": ["legal proceedings", "litigation"], "subsequent_events": ["subsequent events"]}
    out = {}
    low = text.lower()
    for name, keys in patterns.items():
        section = ""
        for k in keys:
            idx = low.find(k)
            if idx >= 0:
                section = text[max(0, idx-1500): min(len(text), idx+1500)]
                break
        out[name] = section
    return out


def _analyze_text_block(filing_text: str, keyword_map: dict[str, list[str]], flag_map: list[tuple[str, list[str]]]) -> dict[str, Any]:
    low = filing_text.lower()
    hits, excerpts, flags = [], [], []
    for group, kws in keyword_map.items():
        for kw in kws:
            if kw in low:
                hits.append(kw)
                excerpts.extend(_extract_keyword_window(filing_text, kw))
    for flag, kws in flag_map:
        if any(k in low for k in kws):
            flags.append(flag)
    hits = sorted(set(hits))
    excerpts = excerpts[:6]
    risk = "High" if len(flags) >= 4 else "Medium" if flags else "Low"
    priority = "High" if risk == "High" else "Medium" if risk == "Medium" else "Low"
    return {"risk_level": risk, "flags": flags, "keyword_hits": hits, "excerpt_windows": excerpts, "manual_review_priority": priority}


def analyze_tax_footnote_text(filing_text: str) -> dict[str, Any]:
    kws = {"tax": ["effective tax rate","income tax expense","tax provision","tax benefit","deferred tax","deferred tax assets","deferred tax liabilities","valuation allowance","unrecognized tax benefits","tax credits","tax loss carryforwards","net operating loss","tax receivable","uncertain tax positions","foreign tax","repatriation","tax audit","tax settlement"]}
    flags = [("Valuation allowance mentioned", ["valuation allowance"]),("Unrecognized tax benefits mentioned", ["unrecognized tax benefits","uncertain tax positions"]),("Deferred tax complexity detected", ["deferred tax assets","deferred tax liabilities","deferred tax"]),("Tax benefit language detected", ["tax benefit"]),("Effective tax rate reconciliation requires manual review", ["effective tax rate"]),("Tax loss carryforward dependency detected", ["tax loss carryforwards","net operating loss"]),("Potential book-vs-tax complexity", ["tax provision","foreign tax","repatriation"])]
    r = _analyze_text_block(filing_text, kws, flags)
    return {"tax_text_risk_level": r["risk_level"], "tax_text_flags": r["flags"], "tax_keyword_hits": r["keyword_hits"], "tax_excerpt_windows": r["excerpt_windows"], "manual_review_priority": r["manual_review_priority"]}


def analyze_receivables_footnote_text(filing_text: str) -> dict[str, Any]:
    kws = {"recv": ["allowance for doubtful accounts","doubtful accounts","expected credit losses","credit losses","bad debt expense","write-offs","allowance methodology","aging of receivables","customer concentration","changes in payment terms","collection risk"]}
    flags = [("Allowance methodology disclosed", ["allowance methodology","allowance for doubtful accounts"]),("Credit loss risk language detected", ["expected credit losses","credit losses"]),("Receivables aging / collection risk mentioned", ["aging of receivables","collection risk"]),("Customer concentration risk mentioned", ["customer concentration"]),("Bad debt / write-off language detected", ["bad debt expense","write-offs"])]
    return _analyze_text_block(filing_text, kws, flags)


def analyze_inventory_footnote_text(filing_text: str) -> dict[str, Any]:
    kws={"inv":["inventories","finished goods","work in process","raw materials","lower of cost or net realizable value","lower of cost or market","inventory write-down","obsolescence","excess inventory","slow-moving inventory","demand uncertainty","channel inventory","backlog"]}
    flags=[("Inventory write-down language detected", ["inventory write-down"]),("Obsolescence reserve mentioned", ["obsolescence"]),("Finished goods / WIP / raw materials breakdown mentioned", ["finished goods","work in process","raw materials"]),("Demand uncertainty linked to inventory", ["demand uncertainty","slow-moving inventory","excess inventory"]),("Channel inventory risk mentioned", ["channel inventory"])]
    return _analyze_text_block(filing_text,kws,flags)

def analyze_debt_footnote_text(filing_text: str) -> dict[str, Any]:
    kws={"debt":["debt","borrowings","notes payable","senior notes","revolving credit facility","covenants","maturity","variable rate","fixed rate","interest rate","refinancing","liquidity","going concern","off-balance sheet","lease obligations"]}
    flags=[("Debt maturity schedule requires review", ["maturity"]),("Covenant language detected", ["covenants"]),("Variable rate exposure detected", ["variable rate"]),("Refinancing risk language detected", ["refinancing"]),("Liquidity pressure language detected", ["liquidity","going concern"]),("Off-balance-sheet obligation language detected", ["off-balance sheet","lease obligations"])]
    return _analyze_text_block(filing_text,kws,flags)

def analyze_revenue_and_nonrecurring_text(filing_text: str) -> dict[str, Any]:
    kws={"rev":["revenue recognition","remaining performance obligations","contract assets","contract liabilities","bill-and-hold","channel stuffing","returns","rebates","discounts","deferred revenue","customer incentives","restructuring","impairment","gain on sale","divestiture","fair value gain","litigation settlement","one-time","non-recurring","unusual item","discontinued operations"]}
    flags=[("Revenue recognition complexity detected", ["revenue recognition","bill-and-hold","channel stuffing"]),("Contract asset / liability language detected", ["contract assets","contract liabilities","deferred revenue"]),("Customer incentives / rebates may affect revenue quality", ["customer incentives","rebates","discounts","returns"]),("Nonrecurring item language detected", ["one-time","non-recurring","unusual item","discontinued operations"]),("Restructuring / impairment language detected", ["restructuring","impairment"]),("Gain on sale / divestiture language detected", ["gain on sale","divestiture","fair value gain"])]
    return _analyze_text_block(filing_text,kws,flags)

def build_filing_text_forensic_score(text_analyses: dict[str, Any]) -> dict[str, Any]:
    if not text_analyses:
        return {"filing_text_score":None,"filing_text_risk_level":"Unknown","top_text_red_flags":["SEC filing text unavailable"],"most_relevant_excerpts":[],"filing_sources":[]}
    score=0; flags=[]; excerpts=[]
    for key in ["tax","receivables","inventory","debt","revenue_nonrecurring"]:
        a=text_analyses.get(key,{})
        r=a.get("risk_level") or a.get("tax_text_risk_level")
        score += 20 if r=="High" else 10 if r=="Medium" else 2 if r=="Low" else 0
        flags.extend(a.get("flags",[])+a.get("tax_text_flags",[]))
        excerpts.extend(a.get("excerpt_windows",[])+a.get("tax_excerpt_windows",[]))
    level="High" if score>=60 else "Medium" if score>=30 else "Low"
    return {"filing_text_score": min(score,100), "filing_text_risk_level": level, "top_text_red_flags": list(dict.fromkeys(flags))[:8], "most_relevant_excerpts": excerpts[:8], "filing_sources": text_analyses.get("sources", [])}

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
    return render_template_string("""
<!doctype html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>Forensic Command Center</title>
  <style>
    :root { --bg:#070b14; --panel:#0f1628; --panel2:#111b31; --text:#d8e3ff; --muted:#8fa4d1; --line:#243556; --ok:#3ddc97; --warn:#f6c453; --bad:#ff6b6b; --accent:#57a6ff; }
    *{box-sizing:border-box} body{margin:0;background:radial-gradient(circle at 20% 0%, #182646 0%, var(--bg) 40%);color:var(--text);font-family:Inter,Segoe UI,system-ui,sans-serif}
    .wrap{max-width:1320px;margin:0 auto;padding:22px} .head{display:flex;justify-content:space-between;gap:16px;align-items:flex-end;margin-bottom:18px}
    h1{margin:0;font-size:1.7rem} .muted{color:var(--muted);font-size:.92rem}
    .ctl{display:flex;gap:10px;flex-wrap:wrap}.ctl input,.ctl button{background:var(--panel);border:1px solid var(--line);color:var(--text);padding:10px 12px;border-radius:10px}
    .ctl button{background:linear-gradient(90deg,#1e4f89,#2769b6);cursor:pointer;font-weight:700}
    .grid{display:grid;grid-template-columns:repeat(12,1fr);gap:12px}.card{background:linear-gradient(160deg,var(--panel),var(--panel2));border:1px solid var(--line);border-radius:12px;padding:14px;box-shadow:0 8px 22px rgba(0,0,0,.25)}
    .span-3{grid-column:span 3}.span-4{grid-column:span 4}.span-6{grid-column:span 6}.span-8{grid-column:span 8}.span-12{grid-column:span 12}
    .k{font-size:.76rem;color:var(--muted);text-transform:uppercase;letter-spacing:.06em}.v{font-size:1.1rem;font-weight:700;margin-top:5px}
    .risk{display:inline-block;padding:4px 8px;border-radius:999px;font-size:.75rem;font-weight:700;border:1px solid var(--line)} .r-high{color:var(--bad)} .r-mod{color:var(--warn)} .r-low{color:var(--ok)} .r-unk{color:var(--muted)}
    table{width:100%;border-collapse:collapse;font-size:.86rem} th,td{border-bottom:1px solid #1b2a48;padding:8px;text-align:left} th{color:#a8bce8;font-weight:600}
    ul{margin:8px 0 0 18px;padding:0} .mono{font-family:ui-monospace,Menlo,Consolas,monospace}.loading{opacity:.8}
    @media(max-width:1024px){.span-3,.span-4,.span-6,.span-8{grid-column:span 12}}
  </style>
</head>
<body>
  <div class="wrap">
    <div class="head">
      <div><h1>Forensic Command Center</h1><div class="muted">Hedge-fund style forensic dashboard for tax, quarterly, debt, and investment signals.</div></div>
      <div class="ctl"><input id="ticker" value="TSLA" aria-label="Ticker" /><button id="analyzeBtn">Analyze</button><button id="reloadScreener">Reload Screener</button></div>
    </div>
    <div class="grid" id="topCards"></div>
    <div class="grid" style="margin-top:12px">
      <section class="card span-6"><div class="k">Tax Analysis</div><div id="tax"></div></section>
      <section class="card span-6"><div class="k">Debt Analysis</div><div id="debt"></div></section>
      <section class="card span-12"><div class="k">Quarterly Analysis</div><div id="quarterly"></div></section>
      <section class="card span-8"><div class="k">Investment View</div><div id="invest"></div></section>
      <section class="card span-4"><div class="k">Data Completeness</div><div id="complete"></div></section>
      <section class="card span-12"><div class="k">SEC Filing Intelligence</div><div id="filingintel"></div></section>
      <section class="card span-12"><div class="k">Screener</div><div id="screener" class="loading">Loading screener...</div></section>
    </div>
  </div>
<script>
const $ = (id)=>document.getElementById(id);
const riskCls=(r='')=>/high/i.test(r)?'r-high':/mod|partial|watch/i.test(r)?'r-mod':/low|complete|stable|buy/i.test(r)?'r-low':'r-unk';
const pill=(r)=>`<span class="risk ${riskCls(r)}">${r||'Unknown'}</span>`;
const n=(v)=>v==null?'—':(typeof v==='number'?v.toLocaleString(undefined,{maximumFractionDigits:2}):v);
const reasonList=(arr)=>!arr||!arr.length?'<div class="muted">None</div>':`<ul>${arr.map(x=>`<li>${x}</li>`).join('')}</ul>`;

async function analyze(){
  const t= $('ticker').value.trim().toUpperCase() || 'TSLA';
  $('analyzeBtn').disabled=true;
  try{
    const r=await fetch(`/api/analyze?ticker=${encodeURIComponent(t)}&period=5y`); const d=await r.json(); render(d);
  }catch(e){ alert('Analyze failed: '+e.message); }
  finally{ $('analyzeBtn').disabled=false; }
}

function render(d){
  const tax=d.tax_analysis||{}, q=d.quarterly_analysis||{}, debt=d.debt_analysis||{}, inv=d.investment_view||{}, comp=d.data_completeness||{}, filing=d.sec_filing_intelligence||{}, fs=filing.summary||{};
  $('topCards').innerHTML = `
    <div class="card span-3"><div class="k">Ticker</div><div class="v mono">${d.ticker||'—'}</div></div>
    <div class="card span-3"><div class="k">Tax Risk</div><div class="v">${pill(tax.tax_risk_level)}</div></div>
    <div class="card span-3"><div class="k">Quarterly Risk</div><div class="v">${pill(q.quarterly_risk_level)}</div></div>
    <div class="card span-3"><div class="k">Debt Risk</div><div class="v">${pill(debt.risk_level)}</div></div>`;

  const tr=(tax.rows&&tax.rows[0])||{};
  $('tax').innerHTML = `<table><tr><th>Metric</th><th>Value</th></tr>
    <tr><td>Risk</td><td>${pill(tax.tax_risk_level)}</td></tr><tr><td>Quality Score</td><td>${n(tax.tax_quality_score)}</td></tr>
    <tr><td>ETR</td><td>${n(tr.etr)}</td></tr><tr><td>Pretax Income</td><td>${n(tr.pretax_income)}</td></tr>
    <tr><td>Tax Expense</td><td>${n(tr.income_tax_expense)}</td></tr><tr><td>Source</td><td>${tax.source||'—'}</td></tr></table>
    <div class="k" style="margin-top:10px">Reason Codes</div>${reasonList(tax.reason_codes)}`;

  $('debt').innerHTML = `<table><tr><th>Ratio</th><th>Value</th></tr>${Object.entries(debt.ratios||{}).map(([k,v])=>`<tr><td>${k}</td><td>${n(v)}</td></tr>`).join('')||'<tr><td colspan="2" class="muted">No ratios available</td></tr>'}</table>
  <div class="k" style="margin-top:10px">Risk Flags</div>${reasonList(debt.flags)}`;

  $('quarterly').innerHTML = `<div style="margin-bottom:8px">${pill(q.quarterly_risk_level)} <span class="muted">Signal: ${q.signal_label||'—'}</span></div>
    <table><tr><th>Period</th><th>Revenue</th><th>Net Income</th><th>CFO</th><th>FCF</th><th>CFO/NI</th></tr>
    ${(q.rows||[]).map(r=>`<tr><td>${r.period||'—'}</td><td>${n(r.revenue)}</td><td>${n(r.net_income)}</td><td>${n(r.cfo)}</td><td>${n(r.fcf)}</td><td>${n(r.cfo_ni)}</td></tr>`).join('')||'<tr><td colspan="6" class="muted">No quarterly rows</td></tr>'}</table>
    <div class="k" style="margin-top:10px">Reason Codes</div>${reasonList(q.reason_codes)}`;

  $('invest').innerHTML = `<div class="v">${inv.forensic_view||'INCONCLUSIVE'}</div><div class="muted">Confidence: ${inv.confidence||'Low'}</div>
  <div class="k" style="margin-top:10px">Risks</div>${reasonList(inv.risks)}<div class="k" style="margin-top:10px">Supporting Reasons</div>${reasonList(inv.supporting_reasons)}
  <div class="k" style="margin-top:10px">What Would Change View</div>${reasonList(inv.what_would_change_view)}`;

  $('complete').innerHTML = `<div class="v">${n(comp.score)} / 100</div><div>${pill(comp.level)}</div><div class="k" style="margin-top:10px">Missing Fields</div>${reasonList(comp.missing_fields)}`;

  const k=filing.latest_10k||{}, qf=filing.latest_10q||{};
  const diag=filing.diagnostics||{};
  const link=(u)=>u?`<a href="${u}" target="_blank">Open SEC filing</a>`:'—';
  const unavailableMsg=(!diag.filing_text_downloaded)?'<div class="muted">SEC filing unavailable - check CIK mapping or SEC request</div>':'';
  $('filingintel').innerHTML=`<div>${pill(fs.filing_text_risk_level)} Score: ${n(fs.filing_text_score)}</div>${unavailableMsg}
  <table><tr><th>Form</th><th>Date</th><th>Link</th></tr><tr><td>10-K</td><td>${k.filing_date||'—'}</td><td>${link(k.filing_url)}</td></tr><tr><td>10-Q</td><td>${qf.filing_date||'—'}</td><td>${link(qf.filing_url)}</td></tr></table>
  <div class="k" style="margin-top:10px">Top Red Flags</div>${reasonList(fs.top_text_red_flags)}
  <div class="k" style="margin-top:10px">Extracted Excerpts</div>${reasonList(fs.most_relevant_excerpts)}
  <div class="k" style="margin-top:10px">Tax Footnote Intelligence</div>${reasonList((filing.tax||{}).tax_text_flags)}
  <div class="k" style="margin-top:10px">Receivables / Allowance Intelligence</div>${reasonList((filing.receivables||{}).flags)}
  <div class="k" style="margin-top:10px">Inventory Footnote Intelligence</div>${reasonList((filing.inventory||{}).flags)}
  <div class="k" style="margin-top:10px">Debt & Liquidity Footnote Intelligence</div>${reasonList((filing.debt||{}).flags)}
  <div class="k" style="margin-top:10px">Revenue Recognition & One-Off Items</div>${reasonList((filing.revenue_nonrecurring||{}).flags)}
  <div class="k" style="margin-top:10px">Diagnostics</div>${reasonList([`cik_found: ${diag.cik_found}`,`submissions_loaded: ${diag.submissions_loaded}`,`latest_10k_found: ${diag.latest_10k_found}`,`latest_10q_found: ${diag.latest_10q_found}`,`filing_text_downloaded: ${diag.filing_text_downloaded}`,`error: ${diag.error||'None'}`])}`;
}


async function loadScreener(){
  const el=$('screener'); el.classList.add('loading'); el.textContent='Loading screener...';
  try{
    const r=await fetch('/api/screener'); const d=await r.json();
    const rows=d.rows||[];
    el.innerHTML = `<table><tr><th>Ticker</th><th>Tax Risk</th><th>Quarterly Risk</th><th>Debt Risk</th><th>Filing Text Risk</th><th>Top Text Flag</th><th>Latest 10-K Date</th><th>Latest 10-Q Date</th><th>Forensic View</th><th>Confidence</th><th>Completeness</th><th>Main Reason</th></tr>
      ${rows.map(x=>`<tr><td class="mono">${x.Ticker}</td><td>${pill(x['Tax Risk'])}</td><td>${pill(x['Quarterly Risk'])}</td><td>${pill(x['Debt Risk'])}</td><td>${pill(x['Filing Text Risk'])}</td><td>${x['Top Text Flag']||'—'}</td><td>${x['Latest 10-K Date']||'—'}</td><td>${x['Latest 10-Q Date']||'—'}</td><td>${x['Forensic View']||'—'}</td><td>${x.Confidence||'—'}</td><td>${pill(x['Data Completeness'])}</td><td>${x['Main Reason']||'—'}</td></tr>`).join('')}</table>`;
  }catch(e){ el.textContent='Screener failed: '+e.message; }
  finally{ el.classList.remove('loading'); }
}
$('analyzeBtn').addEventListener('click', analyze); $('reloadScreener').addEventListener('click', loadScreener);
analyze(); loadScreener();
</script>
</body>
</html>
""")


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
        filing_10k = get_latest_filing_metadata(ticker, "10-K")
        filing_10q = get_latest_filing_metadata(ticker, "10-Q")
        filing_sources = [x for x in [filing_10k.get("filing_url"), filing_10q.get("filing_url")] if x]
        filing_html_10k = download_sec_filing_html(filing_10k.get("filing_url"))
        filing_html_10q = download_sec_filing_html(filing_10q.get("filing_url"))
        filing_text = clean_sec_html_to_text(filing_html_10k + " " + filing_html_10q)
        tax_text = analyze_tax_footnote_text(filing_text) if filing_text else {"tax_text_risk_level":"Unknown","tax_text_flags":["SEC filing text unavailable"],"tax_keyword_hits":[],"tax_excerpt_windows":[],"manual_review_priority":"High"}
        recv_text = analyze_receivables_footnote_text(filing_text) if filing_text else {"risk_level":"Unknown","flags":["SEC filing text unavailable"],"keyword_hits":[],"excerpt_windows":[],"manual_review_priority":"High"}
        inv_text = analyze_inventory_footnote_text(filing_text) if filing_text else {"risk_level":"Unknown","flags":["SEC filing text unavailable"],"keyword_hits":[],"excerpt_windows":[],"manual_review_priority":"High"}
        debt_text = analyze_debt_footnote_text(filing_text) if filing_text else {"risk_level":"Unknown","flags":["SEC filing text unavailable"],"keyword_hits":[],"excerpt_windows":[],"manual_review_priority":"High"}
        rev_text = analyze_revenue_and_nonrecurring_text(filing_text) if filing_text else {"risk_level":"Unknown","flags":["SEC filing text unavailable"],"keyword_hits":[],"excerpt_windows":[],"manual_review_priority":"High"}
        filing_text_summary = build_filing_text_forensic_score({"tax":tax_text,"receivables":recv_text,"inventory":inv_text,"debt":debt_text,"revenue_nonrecurring":rev_text,"sources":filing_sources})
        if not filing_text:
            filing_text_summary = {"filing_text_score": None, "filing_text_risk_level": "Unknown", "top_text_red_flags": ["SEC filing text unavailable"], "most_relevant_excerpts": [], "filing_sources": filing_sources}
        tax_analysis["tax_text_flags"] = tax_text.get("tax_text_flags",[])
        tax_analysis["tax_excerpt_windows"] = tax_text.get("tax_excerpt_windows",[])
        tax_analysis["sec_filing_sources"] = filing_sources
        receivables_analysis.update(recv_text)
        inventory_analysis.update(inv_text)
        debt_analysis["text_signals"] = debt_text
        macro_analysis = build_macro_regime_context()
        investment_view = build_investment_view(locals(), completeness)
        if filing_text_summary.get("filing_text_risk_level") == "High" and completeness.get("level") == "Weak":
            investment_view["forensic_view"] = "INCONCLUSIVE"
        filing_diagnostics = {"cik_found": bool(filing_10k.get("cik") or filing_10q.get("cik")), "submissions_loaded": bool((filing_10k.get("cik") or filing_10q.get("cik")) and (get_sec_submissions((filing_10k.get("cik") or filing_10q.get("cik"))) is not None)), "latest_10k_found": bool(filing_10k.get("available")), "latest_10q_found": bool(filing_10q.get("available")), "filing_text_downloaded": bool(filing_text), "error": None}
        out = {"ticker": ticker, "tax_analysis": tax_analysis, "quarterly_analysis": quarterly_analysis, "inventory_analysis": inventory_analysis,
               "receivables_analysis": receivables_analysis, "debt_analysis": debt_analysis, "macro_analysis": macro_analysis,
               "investment_view": investment_view, "data_completeness": completeness, "sec_filing_intelligence": {"latest_10k": filing_10k, "latest_10q": filing_10q, "diagnostics": filing_diagnostics, "sections": extract_filing_sections(filing_text), "tax": tax_text, "receivables": recv_text, "inventory": inv_text, "debt": debt_text, "revenue_nonrecurring": rev_text, "summary": filing_text_summary}}
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
            filing = data.get("sec_filing_intelligence", {})
            summary = filing.get("summary", {})
            rows.append({"Ticker": ticker, "Tax Risk": data["tax_analysis"].get("tax_risk_level", "Unknown"), "Quarterly Risk": data["quarterly_analysis"].get("quarterly_risk_level", "Unknown"), "Debt Risk": data["debt_analysis"].get("risk_level", "Unknown"), "Filing Text Risk": summary.get("filing_text_risk_level", "Unknown"), "Top Text Flag": (summary.get("top_text_red_flags") or ["Unavailable"])[0], "Latest 10-K Date": filing.get("latest_10k", {}).get("filing_date"), "Latest 10-Q Date": filing.get("latest_10q", {}).get("filing_date"), "Forensic View": data["investment_view"].get("forensic_view", "INCONCLUSIVE"), "Confidence": data["investment_view"].get("confidence", "Low"), "Data Completeness": data["data_completeness"].get("level", "Weak"), "Main Reason": (data["tax_analysis"].get("reason_codes") or ["Unavailable"])[0]})
        except Exception as e:
            print(f"[ERROR] {ticker}: {type(e).__name__}: {e}")
            traceback.print_exc()
            rows.append({"Ticker": ticker, "Tax Risk": "Unknown", "Quarterly Risk": "Unknown", "Debt Risk": "Unknown", "Forensic View": "INCONCLUSIVE", "Confidence": "Low", "Data Completeness": "Weak", "Main Reason": "Unhandled module failure"})
    return jsonify({"rows": rows})


if __name__ == '__main__':
    app.run(debug=True, port=5057)
