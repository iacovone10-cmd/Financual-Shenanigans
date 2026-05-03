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


def default_cash_flow_analysis() -> dict[str, Any]:
    return {
        "risk_level": "Unknown",
        "score": None,
        "data_status": "Unavailable",
        "metrics": {
            "cfo": None, "net_income": None, "revenue": None, "cfo_to_net_income": None, "cfo_margin": None,
            "capex": None, "fcf": None, "fcf_margin": None, "accruals": None, "accrual_ratio": None, "fcf_conversion": None,
        },
        "flags": ["Data unavailable"],
        "normal_signals": [],
        "suspicious_signals": [],
        "rows": [],
        "tenk_checks": [
            "Compare CFO with net income", "Review working capital changes", "Inspect receivables and inventory movements",
            "Review capital expenditure requirements", "Check non-cash gains and accruals",
            "Compare free cash flow with buybacks and dividends",
        ],
    }


def default_cash_flow_ratio_matrix() -> list[dict[str, Any]]:
    return []




def default_special_items_analysis() -> dict[str, Any]:
    return {
        "risk_level": "Unknown",
        "score": None,
        "metrics": {
            "acquisitions_to_cfo": None,
            "acquisitions_to_revenue": None,
            "special_items_to_net_income": None,
            "non_operating_income_to_net_income": None,
            "restructuring_to_operating_income": None,
            "impairment_to_assets": None,
            "divestiture_gains_to_net_income": None,
        },
        "flags": ["Special items module unavailable"],
        "normal_signals": [],
        "suspicious_signals": [],
        "text_findings": [],
        "excerpt_windows": [],
        "tenk_checks": [
            "Inspect acquisitions footnote",
            "Separate organic growth from acquired growth",
            "Review purchase price allocation",
            "Check goodwill and intangible assets",
            "Inspect impairment testing assumptions",
            "Review restructuring charges across multiple years",
            "Check discontinued operations and divestiture gains",
            "Review accounting changes and ASU adoption impact",
            "Check restatements, corrections of errors and internal control weaknesses",
        ],
        "data_status": "Unavailable",
        "acquisition_risk": "Unknown",
        "accounting_change_flag": "Unavailable",
        "main_special_item_finding": "Unavailable",
    }

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


def get_period_config(period: str) -> dict[str, Any]:
    normalized = (period or "5y").lower().strip()
    mapping = {"1y": (1, 4), "3y": (3, 12), "5y": (5, 20)}
    years, quarters = mapping.get(normalized, mapping["5y"])
    return {"period": normalized if normalized in mapping else "5y", "years": years, "quarters": quarters}


def limited_columns(df: pd.DataFrame | None, count: int) -> list[Any]:
    if df is None or df.empty:
        return []
    return list(df.columns[:count])


def build_quality_rows(fin: pd.DataFrame, cf: pd.DataFrame, bs: pd.DataFrame, years: int) -> list[dict[str, Any]]:
    cols = limited_columns(fin, years)
    rows = []
    for p in cols:
        rev = safe_float(fin.at["Total Revenue", p]) if "Total Revenue" in fin.index else None
        ni = safe_float(fin.at["Net Income", p]) if "Net Income" in fin.index else None
        cfo = safe_float(cf.at["Operating Cash Flow", p]) if not cf.empty and "Operating Cash Flow" in cf.index and p in cf.columns else None
        capex = safe_float(cf.at["Capital Expenditure", p]) if not cf.empty and "Capital Expenditure" in cf.index and p in cf.columns else None
        ar = safe_float(bs.at["Accounts Receivable", p]) if not bs.empty and "Accounts Receivable" in bs.index and p in bs.columns else None
        inv = safe_float(bs.at["Inventory", p]) if not bs.empty and "Inventory" in bs.index and p in bs.columns else None
        rows.append({"period": str(p.date()), "revenue": rev, "net_income": ni, "cfo": cfo, "capex": capex, "fcf": safe_sub(cfo, safe_abs_or_zero(capex)), "ar": ar, "inventory": inv})
    return rows


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


def extract_tax_rows_from_yfinance(fin: pd.DataFrame, cf: pd.DataFrame, bs: pd.DataFrame, quality_rows: list[dict[str, Any]], years: int) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for p in limited_columns(fin, years):
        pretax = safe_float(fin.at["Pretax Income", p]) if "Pretax Income" in fin.index else safe_float(fin.at["Income Before Tax", p]) if "Income Before Tax" in fin.index else None
        tax = safe_float(fin.at["Tax Provision", p]) if "Tax Provision" in fin.index else safe_float(fin.at["Income Tax Expense", p]) if "Income Tax Expense" in fin.index else None
        cash_taxes = safe_float(cf.at["Cash Taxes Paid", p]) if not cf.empty and "Cash Taxes Paid" in cf.index and p in cf.columns else None
        deferred_tax = safe_float(fin.at["Deferred Tax", p]) if "Deferred Tax" in fin.index else None
        if pretax is None and tax is None:
            continue
        rows.append({"period": str(p.date()), "pretax_income": pretax, "income_tax_expense": tax, "current_tax_expense": None, "deferred_tax_expense": deferred_tax, "cash_taxes_paid": cash_taxes, "deferred_tax_assets": None, "deferred_tax_liabilities": None, "valuation_allowance": None, "unrecognized_tax_benefits": None, "etr": safe_div(tax, pretax) if safe_gt(pretax, 0) else None, "cash_tax_ratio": safe_div(cash_taxes, tax), "cash_tax_rate": safe_div(cash_taxes, pretax), "deferred_tax_dependency": safe_div(deferred_tax, pretax), "source": "yfinance"})
    return rows


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

def analyze_special_items_text(filing_text: str) -> dict[str, Any]:
    kws = {
        "acq": ["acquisition", "business combination", "purchase price allocation", "goodwill", "intangible assets", "earnout", "contingent consideration", "integration costs", "synergy", "pro forma", "acquired revenue", "acquired business"],
        "nonrecurring": ["non-recurring", "one-time", "unusual item", "special item", "restructuring", "severance", "impairment", "write-down", "litigation settlement", "gain on sale", "divestiture", "discontinued operations", "held for sale"],
        "accounting": ["accounting change", "change in accounting principle", "change in estimate", "adoption of new accounting standard", "asu", "restatement", "correction of error", "material weakness", "internal control", "reclassification", "retrospective adjustment"],
        "extraordinary": ["extraordinary", "unusual", "infrequent", "non-core", "transformation program", "strategic review", "exit costs", "spin-off", "carve-out"],
    }
    flags = [
        ("Acquisition and business combination language detected", kws["acq"]),
        ("Impairment / write-down language detected", ["impairment", "write-down", "write off"]),
        ("Restructuring language detected", ["restructuring", "severance", "exit costs", "transformation program"]),
        ("Accounting change affects comparability", ["accounting change", "change in accounting principle", "asu", "retrospective adjustment"]),
        ("Restatement / material weakness requires manual review", ["restatement", "correction of error", "material weakness", "internal control"]),
        ("Discontinued operations / divestiture language detected", ["discontinued operations", "divestiture", "held for sale", "spin-off", "carve-out"]),
        ("Litigation settlement or unusual item detected", ["litigation settlement", "unusual item", "special item", "extraordinary"]),
    ]
    out = _analyze_text_block(filing_text, kws, flags)
    out["short_findings"] = out.get("flags", [])[:5]
    return out


def analyze_special_items_and_acquisitions(fin: pd.DataFrame, cf: pd.DataFrame, bs: pd.DataFrame, filing_text: str, quality_rows: list[dict[str, Any]]) -> dict[str, Any]:
    out = default_special_items_analysis()
    metric_names = {
        "acquisitions_cash_paid": ["Acquisition Of Business", "Business Acquisitions", "Purchase Of Business", "Purchases Of Businesses", "Net Business Purchase And Sale"],
        "divestiture_proceeds": ["Sale Of Business", "Proceeds From Divestitures", "Net Business Purchase And Sale"],
        "gain_loss_on_sale_of_business": ["Gain On Sale Of Business", "Gain On Sale Of Assets"],
        "restructuring_charges": ["Restructuring And Mergern Acquisition", "Restructuring Charges"],
        "impairment_charges": ["Impairment Of Capital Assets", "Asset Impairment Charge"],
        "asset_write_downs": ["Write Off"],
        "litigation_settlements": ["Litigation Settlement"],
        "discontinued_operations": ["Discontinued Operations"],
        "unusual_items": ["Unusual Items"],
        "special_items": ["Special Income Charges"],
        "other_income_expense": ["Other Income Expense"],
        "non_operating_income": ["Other Non Operating Income Expenses"],
        "change_in_accounting_principle": ["Change In Accounting Principle"],
        "cumulative_effect_of_accounting_change": ["Cumulative Effect Of Accounting Change"],
    }
    values={}
    for k, names in metric_names.items():
        values[k] = latest_from_series(get_series(cf, names))
        if values[k] is None:
            values[k] = latest_from_series(get_series(fin, names))
        if values[k] is None:
            values[k] = latest_from_series(get_series(bs, names))
    revenue = latest_from_series(get_series(fin, ["Total Revenue"]))
    net_income = latest_from_series(get_series(fin, ["Net Income"]))
    cfo = latest_from_series(get_series(cf, ["Operating Cash Flow"]))
    operating_income = latest_from_series(get_series(fin, ["Operating Income", "EBIT"]))
    total_assets = latest_from_series(get_series(bs, ["Total Assets"]))
    acquisition_total = safe_add(values.get("acquisitions_cash_paid"), values.get("divestiture_proceeds"))
    special_total = safe_add(values.get("special_items"), values.get("unusual_items"), values.get("other_income_expense"), values.get("litigation_settlements"), values.get("asset_write_downs"), values.get("impairment_charges"), values.get("restructuring_charges"), values.get("discontinued_operations"))
    metrics = {
        "acquisitions_to_cfo": safe_div(safe_abs(acquisition_total), safe_abs(cfo)),
        "acquisitions_to_revenue": safe_div(safe_abs(acquisition_total), safe_abs(revenue)),
        "special_items_to_net_income": safe_div(safe_abs(special_total), safe_abs(net_income)),
        "non_operating_income_to_net_income": safe_div(safe_abs(values.get("non_operating_income")), safe_abs(net_income)),
        "restructuring_to_operating_income": safe_div(safe_abs(values.get("restructuring_charges")), safe_abs(operating_income)),
        "impairment_to_assets": safe_div(safe_abs(values.get("impairment_charges")), safe_abs(total_assets)),
        "divestiture_gains_to_net_income": safe_div(safe_abs(values.get("gain_loss_on_sale_of_business")), safe_abs(net_income)),
    }
    text = analyze_special_items_text(filing_text) if filing_text else {"risk_level":"Unknown","flags":["SEC filing text unavailable"],"keyword_hits":[],"short_findings":[],"excerpt_windows":[],"manual_review_priority":"High"}
    susp=[]; normal=[]; flags=[]; score=0
    if safe_gt(metrics["acquisitions_to_cfo"], 0.5): flags.append("Acquisitions are material relative to CFO"); susp.append(flags[-1]); score += 20
    if safe_gt(metrics["acquisitions_to_revenue"], 0.15): flags.append("Acquisition-driven growth may obscure organic performance"); susp.append(flags[-1]); score += 15
    if "goodwill" in [h.lower() for h in text.get("keyword_hits",[])]: flags.append("Large goodwill/intangible build-up requires impairment review"); susp.append(flags[-1]); score += 10
    if safe_gt(metrics["restructuring_to_operating_income"], 0.1): flags.append("Restructuring charges may be recurring despite being presented as one-time"); susp.append(flags[-1]); score += 15
    if any(x in " ".join(text.get("keyword_hits",[])).lower() for x in ["impairment","write-down"]): flags.append("Impairment / write-down language detected"); susp.append(flags[-1]); score += 10
    if safe_gt(metrics["non_operating_income_to_net_income"], 0.2): flags.append("Non-operating income materially supports earnings"); susp.append(flags[-1]); score += 12
    if safe_gt(metrics["divestiture_gains_to_net_income"], 0.2): flags.append("Divestiture gains may inflate net income"); susp.append(flags[-1]); score += 12
    if any(k in " ".join(text.get("keyword_hits",[])).lower() for k in ["accounting change", "change in accounting principle", "asu"]): flags.append("Accounting change or ASU adoption affects comparability"); susp.append(flags[-1]); score += 10
    if "discontinued operations" in " ".join(text.get("keyword_hits",[])).lower(): flags.append("Discontinued operations affect period comparability"); susp.append(flags[-1]); score += 8
    if any(k in " ".join(text.get("keyword_hits",[])).lower() for k in ["litigation settlement", "unusual item"]): flags.append("Litigation settlement or unusual item affects earnings quality"); susp.append(flags[-1]); score += 8
    if any(k in " ".join(text.get("keyword_hits",[])).lower() for k in ["restatement", "correction of error", "material weakness"]): flags.append("Restatement / correction / material weakness language requires manual review"); susp.append(flags[-1]); score += 20
    if not susp: normal.append("No major acquisition or special item anomalies detected in available data")
    level = "High" if score >= 55 else "Medium" if score >= 25 else "Low"
    data_status = "Complete" if all(v is not None for v in metrics.values()) else "Partial" if any(v is not None for v in metrics.values()) else "Unavailable"
    if data_status == "Unavailable": level = "Unknown"
    out.update({"risk_level": level, "score": min(score,100) if data_status != "Unavailable" else None, "metrics": metrics, "flags": list(dict.fromkeys(flags+text.get("flags",[]))), "normal_signals": normal, "suspicious_signals": susp, "text_findings": text.get("short_findings",[]), "excerpt_windows": text.get("excerpt_windows",[])[:6], "data_status": data_status, "raw_metrics": values, "acquisition_risk": "High" if safe_gt(metrics["acquisitions_to_cfo"],0.5) or safe_gt(metrics["acquisitions_to_revenue"],0.15) else "Unknown" if data_status=="Unavailable" else "Low", "accounting_change_flag": "Yes" if any(k in " ".join(text.get("keyword_hits",[])).lower() for k in ["accounting change","change in accounting principle","asu","restatement","material weakness"]) else "No" if data_status!="Unavailable" else "Unavailable", "main_special_item_finding": (list(dict.fromkeys(flags+text.get("flags",[]))) or ["Unavailable"])[0]})
    return out


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


def build_quarterly_forensic_analysis(ticker: str, quarter_limit: int) -> dict[str, Any]:
    out = default_quarterly_analysis()
    try:
        tk = yf.Ticker(ticker)
        qf, qcf, qbs = tk.quarterly_financials, tk.quarterly_cashflow, tk.quarterly_balance_sheet
        if qf.empty and qcf.empty and qbs.empty:
            return out
        periods = list(qf.columns[:quarter_limit]) if not qf.empty else []
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
    cfo = quality_rows[0].get("cfo") if quality_rows else latest_from_series(get_series(cf, ["Operating Cash Flow"]))
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


def build_core_ratios(quality_rows: list[dict[str, Any]], debt_analysis: dict[str, Any], special: dict[str, Any], period_cfg: dict[str, Any]) -> list[dict[str, Any]]:
    if not quality_rows:
        return [{"ratio_name": "Coverage", "value": "Unavailable", "status": "Unavailable", "interpretation": "No historical rows", "manual_check": "Confirm available fiscal history"}]
    r0 = quality_rows[0]
    prev = quality_rows[1] if len(quality_rows) > 1 else {}
    ratios = [
        ("CFO / Net Income", safe_div(r0.get("cfo"), r0.get("net_income")), "Earnings not fully converting into cash", "Review cash flow statement quality"),
        ("FCF Margin", safe_div(r0.get("fcf"), r0.get("revenue")), "Low free cash conversion", "Check capex sustainability"),
        ("Revenue Growth", safe_pct_change(r0.get("revenue"), prev.get("revenue")), "Weak topline trend", "Review segment trends"),
        ("AR / Revenue", safe_div(r0.get("ar"), r0.get("revenue")), "Possible collection or revenue quality issue", "Review allowance and aging"),
        ("Inventory / Revenue", safe_div(r0.get("inventory"), r0.get("revenue")), "Possible demand weakness or future write-down risk", "Review inventory footnotes"),
        ("Debt / Equity", (debt_analysis.get("ratios") or {}).get("total_debt_to_equity"), "Debt burden elevated", "Review debt maturity and refinancing risk"),
        ("Buybacks / CFO", (special.get("metrics") or {}).get("acquisitions_to_cfo"), "Possible financial engineering", "Review share repurchases footnote"),
        ("Special Items / Net Income", (special.get("metrics") or {}).get("special_items_to_net_income"), "Earnings may include non-core effects", "Review non-GAAP reconciliation"),
    ]
    out = []
    for name, val, interp, check in ratios:
        status = "Unavailable" if val is None else "Risk" if safe_gt(val, 1.2) or (name == "CFO / Net Income" and safe_lt(val, 1)) else "Watch" if safe_gt(val, 0.8) else "Healthy"
        out.append({"ratio_name": name, "value": val, "status": status, "interpretation": interp if status in ("Risk", "Watch") else "Within acceptable range", "manual_check": check})
    # fill requested list footprint
    requested = ["Accrual Ratio", "Gross Margin", "Net Margin", "DSO", "DIO", "ETR", "Cash Tax Rate", "Net Debt / CFO", "Interest Coverage", "Cash Interest Coverage", "Dividends / Net Income", "Total Payout / FCF", "SBC / Revenue", "SBC / CFO", "Goodwill / Assets", "Intangibles / Assets", "Other Income / Net Income", "Buybacks / FCF"]
    for x in requested:
        out.append({"ratio_name": x, "value": None, "status": "Unavailable", "interpretation": "Needs additional data coverage", "manual_check": "Verify in 10-K/10-Q footnotes"})
    return out[:25]


def build_attention_points(all_modules: dict[str, Any]) -> list[dict[str, Any]]:
    points = []
    for r in all_modules.get("core_ratios", []):
        if r.get("status") in ("Risk", "Watch"):
            points.append({"area": "Core Ratios", "severity": r.get("status"), "issue": r.get("ratio_name"), "ratio_evidence": safe_fmt_num(r.get("value")), "why_it_matters": r.get("interpretation"), "where_to_check": r.get("manual_check"), "source": "yfinance / SEC facts"})
    if all_modules.get("coverage_status") == "Partial historical coverage":
        points.append({"area": "Coverage", "severity": "Watch", "issue": "Partial historical coverage", "ratio_evidence": all_modules.get("analysis_window_label"), "why_it_matters": "Trend interpretation may be incomplete", "where_to_check": "Prior 10-Ks and 10-Qs", "source": "yfinance"})
    return points[:10]


def calculate_data_completeness(payload: dict[str, Any]) -> dict[str, Any]:
    fields = ["revenue", "net_income", "cfo", "ar", "inventory", "cogs", "pretax_income", "tax_expense", "cash_taxes", "deferred_tax", "quarterly_data", "debt", "interest_expense"]
    missing = [f for f in fields if payload.get(f) is None]
    score = int((len(fields) - len(missing)) / len(fields) * 100)
    level = "Complete" if score >= 85 else "Partial" if score >= 55 else "Weak"
    return {"score": score, "level": level, "missing_fields": missing}


def analyze_cash_flow_quality(fin: pd.DataFrame, cf: pd.DataFrame, bs: pd.DataFrame, quality_rows: list[dict[str, Any]]) -> dict[str, Any]:
    out = default_cash_flow_analysis()
    cfo = latest_from_series(get_series(cf, ["Operating Cash Flow"]))
    ni = latest_from_series(get_series(fin, ["Net Income"]))
    revenue = latest_from_series(get_series(fin, ["Total Revenue"]))
    capex = latest_from_series(get_series(cf, ["Capital Expenditure"]))
    capex_abs = safe_abs(capex)
    fcf = safe_sub(cfo, capex_abs)
    accruals = safe_sub(ni, cfo)
    metrics = {
        "cfo": cfo, "net_income": ni, "revenue": revenue, "cfo_to_net_income": safe_div(cfo, ni), "cfo_margin": safe_div(cfo, revenue),
        "capex": capex, "fcf": fcf, "fcf_margin": safe_div(fcf, revenue), "accruals": accruals, "accrual_ratio": safe_div(accruals, safe_abs(ni)),
        "fcf_conversion": safe_div(fcf, ni),
    }
    if all(v is None for v in [cfo, ni, revenue, capex]):
        return out
    out["data_status"] = "Complete" if all(v is not None for v in [cfo, ni, revenue, capex]) else "Partial"
    rows = []
    q = build_quarterly_forensic_analysis("TMP").get("rows", []) if False else quality_rows
    for r in quality_rows[:8]:
        rows.append(r)
    out["rows"] = rows
    flags, normal, suspicious = [], [], []
    if safe_lt(metrics["cfo_to_net_income"], 1):
        flags.append("CFO/NI below 1")
    if safe_gt(ni, 0) and (safe_lt(cfo, 0) or safe_lt(metrics["cfo_to_net_income"], 0.8)):
        flags.append("Positive net income but weak/negative CFO")
    if safe_lt(fcf, 0):
        flags.append("FCF negative")
    if safe_gt(metrics["accrual_ratio"], 0.25):
        flags.append("Accrual ratio high")
    if safe_gt(safe_abs(capex), safe_abs(cfo)):
        flags.append("CapEx absorbs most CFO")
    if safe_lt(metrics["fcf_conversion"], 0.5):
        flags.append("FCF conversion weak")
    if safe_gt(metrics["cfo_to_net_income"], 1.1):
        normal.append("Cash conversion appears supportive of earnings")
    if safe_gt(metrics["fcf_margin"], 0.05):
        normal.append("Positive free cash flow margin")
    suspicious.extend(flags)
    score = max(0, 100 - len(flags) * 15) if out["data_status"] != "Unavailable" else None
    risk = "High" if len(flags) >= 4 else "Medium" if len(flags) >= 2 else "Low"
    if out["data_status"] == "Unavailable":
        risk = "Unknown"
    out.update({"metrics": metrics, "flags": flags or ["No major cash flow red flag detected from available data"], "normal_signals": normal, "suspicious_signals": suspicious, "score": score, "risk_level": risk})
    return out


def build_cash_flow_ratio_matrix(fin: pd.DataFrame, cf: pd.DataFrame, bs: pd.DataFrame, cash_flow_analysis: dict[str, Any], quarterly: dict[str, Any], tax_analysis: dict[str, Any]) -> list[dict[str, Any]]:
    m = cash_flow_analysis.get("metrics", {})
    debt = safe_add(latest_from_series(get_series(bs, ["Long Term Debt", "Long Term Debt And Capital Lease Obligation"])), latest_from_series(get_series(bs, ["Current Debt", "Current Debt And Capital Lease Obligation", "Short Long Term Debt"])))
    interest = latest_from_series(get_series(fin, ["Interest Expense"]))
    buybacks = latest_from_series(get_series(cf, ["Repurchase Of Capital Stock", "Common Stock Repurchased"]))
    dividends = latest_from_series(get_series(cf, ["Cash Dividends Paid", "Common Stock Dividend Paid"]))
    ratios = [
        ("CFO / Net Income", m.get("cfo_to_net_income"), "Compare earnings conversion", "Cash flow statement"),
        ("CFO / Revenue", m.get("cfo_margin"), "Operating cash productivity", "CFO vs revenue trend"),
        ("FCF / Revenue", m.get("fcf_margin"), "Free cash yield from sales", "CapEx and CFO notes"),
        ("FCF / Net Income", m.get("fcf_conversion"), "Conversion of accounting earnings", "CFO and CapEx bridge"),
        ("Accruals / Net Income", m.get("accrual_ratio"), "High accruals may reduce quality", "Accrual and non-cash items"),
        ("CapEx / CFO", safe_div(safe_abs(m.get("capex")), m.get("cfo")), "Reinvestment intensity", "Capital expenditure policy"),
        ("Debt / CFO", safe_div(debt, m.get("cfo")), "Debt service pressure", "Debt maturity table"),
        ("Interest Expense / CFO", safe_div(interest, m.get("cfo")), "Interest burden vs cash flow", "Interest and financing cost"),
        ("Cash Interest Coverage", safe_div(m.get("cfo"), interest), "Cash coverage of interest", "Interest coverage disclosure"),
        ("Buybacks / CFO", safe_div(safe_abs(buybacks), m.get("cfo")), "Capital return affordability", "Equity transactions"),
        ("Dividends / CFO", safe_div(safe_abs(dividends), m.get("cfo")), "Dividend cash burden", "Dividend footnote"),
        ("Total Payout / FCF", safe_div(safe_add(safe_abs(buybacks), safe_abs(dividends)), m.get("fcf")), "Total payout sustainability", "Capital allocation policy"),
        ("Cash Taxes / Tax Expense", safe_div((tax_analysis.get("rows") or [{}])[0].get("cash_taxes_paid"), (tax_analysis.get("rows") or [{}])[0].get("income_tax_expense")), "Cash taxes realism", "Tax footnote"),
    ]
    out = []
    for name, value, interp, check in ratios:
        status = "Unavailable" if value is None else ("Risk" if (value < 0.5 or value > 1.2) and "Coverage" not in name else "Watch")
        if value is not None and name in ["CFO / Net Income", "Cash Interest Coverage"] and value >= 1:
            status = "Healthy"
        out.append({"ratio_name": name, "value": value, "status": status, "interpretation": interp, "manual_check": check})
    return out


def build_macro_regime_context() -> dict[str, Any]:
    return default_macro_analysis()


def build_investment_view(mods: dict[str, Any], completeness: dict[str, Any]) -> dict[str, Any]:
    risks = []
    if completeness["level"] == "Weak":
        risks.append("Data completeness is Weak")
    special = mods.get("special_items_analysis", {})
    if special.get("risk_level") == "High":
        risks.append("special items risk is High")
    if special.get("acquisition_risk") == "High":
        risks.append("acquisition dependency is High")
    if special.get("accounting_change_flag") == "Yes":
        risks.append("accounting change / restatement language detected")
    for k in ["tax_analysis", "quarterly_analysis", "inventory_analysis", "receivables_analysis", "debt_analysis"]:
        if mods[k].get("risk_level") == "High" or mods[k].get("tax_risk_level") == "High" or mods[k].get("quarterly_risk_level") == "High":
            risks.append(f"{k} is High")
    view = "BUY / ACCUMULATE"
    if risks:
        view = "AVOID / SELL" if len(risks) >= 2 else "HOLD / WATCHLIST"
    return {"forensic_view": view if completeness["level"] != "Weak" else "INCONCLUSIVE", "confidence": "Medium" if not risks else "Low", "supporting_reasons": ["Risk-based forensic opinion, not financial advice."], "risks": risks, "what_would_change_view": ["Improve CFO quality", "Lower leverage", "Improve data completeness"]}


def build_cross_module_insights(cash_flow_analysis: dict[str, Any], debt_analysis: dict[str, Any], quarterly_analysis: dict[str, Any], special_items_analysis: dict[str, Any], tax_analysis: dict[str, Any]) -> list[str]:
    m = cash_flow_analysis.get("metrics", {})
    insights = []
    first_q = (quarterly_analysis.get("rows") or [{}])[0]
    if safe_lt(m.get("cfo_to_net_income"), 1) and safe_gt(first_q.get("ar_qoq"), first_q.get("revenue_qoq")):
        insights.append("Earnings quality concern: receivables rising faster than cash conversion.")
    if safe_lt(m.get("fcf"), 0):
        insights.append("Capital return may be funded by balance sheet rather than free cash flow.")
    if cash_flow_analysis.get("risk_level") in ("High", "Medium") and debt_analysis.get("risk_level") == "High":
        insights.append("Debt servicing risk increases because operating cash flow is weak.")
    tr = (tax_analysis.get("rows") or [{}])[0]
    if tr.get("income_tax_expense") is not None and tr.get("cash_taxes_paid") is None:
        insights.append("Cash tax validation requires manual review.")
    if safe_gt(m.get("net_income"), 0) and safe_lt(m.get("cfo_to_net_income"), 1) and (special_items_analysis.get("suspicious_signals") or []):
        insights.append("Reported earnings may be supported by non-cash or one-off items.")
    return insights or ["No major cross-module forensic contradiction detected from available data."]


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
      <div><h1>Forensic Command Center</h1><div class="muted">Analyst-first forensic dashboard: what matters, why it matters, where to verify it, and how it changes the forensic view.</div></div>
      <div class="ctl"><input id="ticker" value="TSLA" aria-label="Ticker" /><button id="analyzeBtn">Analyze</button><button id="reloadScreener">Reload Screener</button></div>
    </div>
    <div class="grid" id="topCards"></div>
    <div class="grid" style="margin-top:12px">
      <section class="card span-8"><div class="k">Executive Forensic Verdict</div><div id="invest"></div></section>
      <section class="card span-4"><div class="k">Top Attention Points</div><div id="complete"></div></section>
      <section class="card span-12"><div class="k">Risk Radar</div><div id="quarterly"></div></section>
      <section class="card span-12"><div class="k">Cash Flow Quality Analysis</div><div id="cashflow"></div></section>
      <section class="card span-12"><div class="k">Cash Flow Ratio Matrix</div><div id="cfmatrix"></div></section>
      <section class="card span-12"><div class="k">Cross-Module Forensic Insights</div><div id="crossmod"></div></section>
      <section class="card span-6"><div class="k">Tax / Book-vs-Tax</div><div id="tax"></div></section>
      <section class="card span-6"><div class="k">Debt & Interest Coverage</div><div id="debt"></div></section>
      <section class="card span-12"><div class="k">Acquisitions, Nonrecurring & Accounting Changes</div><div id="specialitems"></div></section>
      <section class="card span-12"><div class="k">SEC Filing Intelligence Summary</div><div id="filingintel"></div></section>
      <section class="card span-12"><div class="k">Collapsible SEC Raw Evidence</div><div id="secevidence"></div></section>
      <section class="card span-12"><div class="k">Screener</div><div id="screener" class="loading">Loading screener...</div></section>
    </div>
  </div>
<script>
const $ = (id)=>document.getElementById(id);
const riskCls=(r='')=>/high|risk/i.test(r)?'r-high':/mod|partial|watch/i.test(r)?'r-mod':/low|healthy|complete|stable|buy/i.test(r)?'r-low':'r-unk';
const pill=(r)=>`<span class="risk ${riskCls(r)}">${r||'Unknown'}</span>`;
const n=(v)=>v==null?'—':(typeof v==='number'?v.toLocaleString(undefined,{maximumFractionDigits:2}):v);
const reasonList=(arr)=>!arr||!arr.length?'<div class="muted">None</div>':`<ul>${arr.map(x=>`<li>${x}</li>`).join('')}</ul>`;
const truncateText=(text,maxLen=400)=>{ const t=(text||'').toString(); return t.length>maxLen?`${t.slice(0,maxLen)}...`:t; };
const dedupeArray=(arr)=>[...new Set((arr||[]).filter(Boolean))];
function renderAnalystFindings(findings){
  return (findings||[]).slice(0,5).map(f=>`<div class="card" style="margin:6px 0"><div><b>Area:</b> ${f.area||'General'} · <b>Severity:</b> ${f.severity||'Medium'}</div><div><b>Finding:</b> ${f.finding||'—'}</div><div><b>Why it matters:</b> ${f.why_it_matters||'—'}</div><div><b>Ratio / evidence:</b> ${f.evidence||'—'}</div><div><b>Manual check:</b> ${f.manual_check||'—'}</div></div>`).join('')||'<div class="muted">No findings</div>';
}
function renderCollapsedEvidence(excerpts){
  const items=dedupeArray(excerpts).map(x=>`<li class="excerpt">${truncateText(x,400)}</li>`).join('');
  return `<details><summary>View raw SEC evidence</summary><ul>${items||'<li>No evidence</li>'}</ul></details>`;
}

const topItems=(arr,limit=3)=> (arr||[]).filter(Boolean).slice(0,limit);
function buildAnalystBrief(d, inv, fs, filing){
  const whatMatters = topItems([...(inv.risks||[]), ...(fs.top_text_red_flags||[]), ...((d.tax_analysis||{}).reason_codes||[])]);
  const whyMatters = topItems([
    `Forensic view is ${inv.forensic_view||'INCONCLUSIVE'} with ${inv.confidence||'Low'} confidence`,
    `Data completeness is ${((d.data_completeness||{}).score ?? '—')}/100 (${(d.data_completeness||{}).level||'Unknown'})`,
    ...((d.quarterly_analysis||{}).reason_codes||[])
  ]);
  const latest10k=(filing.latest_10k||{}), latest10q=(filing.latest_10q||{});
  const whereVerify = topItems([
    latest10k.filing_url ? `10-K (${latest10k.filing_date||'date unavailable'})` : null,
    latest10q.filing_url ? `10-Q (${latest10q.filing_date||'date unavailable'})` : null,
    `SEC diagnostics: filing_text_downloaded=${(filing.diagnostics||{}).filing_text_downloaded}`
  ],4);
  const impact = topItems([
    ...((inv.supporting_reasons)||[]),
    ...((inv.what_would_change_view)||[])
  ],4);
  return {whatMatters, whyMatters, whereVerify, impact};
}

async function analyze(){
  const t= $('ticker').value.trim().toUpperCase() || 'TSLA';
  $('analyzeBtn').disabled=true;
  try{
    const period = new URLSearchParams(window.location.search).get('period') || '5y';
    const r=await fetch(`/api/analyze?ticker=${encodeURIComponent(t)}&period=${encodeURIComponent(period)}`); const d=await r.json(); render(d);
  }catch(e){ alert('Analyze failed: '+e.message); }
  finally{ $('analyzeBtn').disabled=false; }
}

function render(d){
  const tax=d.tax_analysis||{}, q=d.quarterly_analysis||{}, debt=d.debt_analysis||{}, inv=d.investment_view||{}, comp=d.data_completeness||{}, filing=d.sec_filing_intelligence||{}, fs=filing.summary||{}, sp=d.special_items_analysis||{}, cfa=d.cash_flow_analysis||{}, cfm=d.cash_flow_ratio_matrix||[], cmi=d.cross_module_insights||[];
  const brief = buildAnalystBrief(d, inv, fs, filing);
  $('topCards').innerHTML = `
    <div class="card span-3"><div class="k">Ticker</div><div class="v mono">${d.ticker||'—'}</div></div>
    <div class="card span-3"><div class="k">Tax Risk</div><div class="v">${pill(tax.tax_risk_level)}</div></div>
    <div class="card span-3"><div class="k">Quarterly Risk</div><div class="v">${pill(q.quarterly_risk_level)}</div></div>
    <div class="card span-3"><div class="k">Debt Risk</div><div class="v">${pill(debt.risk_level)}</div></div>
    <div class="card span-3"><div class="k">Special Items Risk</div><div class="v">${pill(sp.risk_level)}</div></div>`;

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
  $('attention').innerHTML = reasonList((d.top_attention_points||[]).map(x=>`${x.severity}: ${x.issue} — ${x.why_it_matters}`));
  $('coreratios').innerHTML = `<div style='margin-bottom:8px'>Filter: All / Risk / Watch / Unavailable</div><table><tr><th>Ratio</th><th>Value</th><th>Status</th><th>Interpretation</th><th>Manual check</th></tr>${(d.core_ratios||[]).map(r=>`<tr><td>${r.ratio_name}</td><td>${n(r.value)}</td><td>${pill(r.status)}</td><td>${r.interpretation}</td><td>${r.manual_check}</td></tr>`).join('')}</table>`;

  $('invest').innerHTML = `<div class="v">${inv.forensic_view||'INCONCLUSIVE'}</div><div class="muted">Confidence: ${inv.confidence||'Low'}</div>
  <div class="k" style="margin-top:10px">What matters now</div>${reasonList(brief.whatMatters)}
  <div class="k" style="margin-top:10px">Why it matters</div>${reasonList(brief.whyMatters)}
  <div class="k" style="margin-top:10px">How it affects the forensic view</div>${reasonList(brief.impact)}
  <details><summary>Full risk and support detail</summary><div class="k" style="margin-top:10px">Risks</div>${reasonList(inv.risks)}<div class="k" style="margin-top:10px">Supporting Reasons</div>${reasonList(inv.supporting_reasons)}
  <div class="k" style="margin-top:10px">What Would Change View</div>${reasonList(inv.what_would_change_view)}</details>`;

  $('complete').innerHTML = `<div>${pill(comp.level)}</div><div class="k" style="margin-top:10px">Top Attention</div>${reasonList((inv.risks||[]).slice(0,5))}`;
  const m=cfa.metrics||{};
  $('cashflow').innerHTML=`<div>${pill(cfa.risk_level)} Score: ${n(cfa.score)} <span class="muted">(${cfa.data_status||'Unavailable'})</span></div>
  <table><tr><th>CFO</th><th>Net Income</th><th>CFO/NI</th><th>FCF</th><th>FCF Margin</th><th>Accrual Ratio</th></tr>
  <tr><td>${n(m.cfo)}</td><td>${n(m.net_income)}</td><td>${n(m.cfo_to_net_income)}</td><td>${n(m.fcf)}</td><td>${n(m.fcf_margin)}</td><td>${n(m.accrual_ratio)}</td></tr></table>
  <div class="k" style="margin-top:10px">Flags</div>${reasonList(cfa.flags)}<div class="k" style="margin-top:10px">Normal signals</div>${reasonList(cfa.normal_signals)}<div class="k" style="margin-top:10px">Suspicious signals</div>${reasonList(cfa.suspicious_signals)}<div class="k" style="margin-top:10px">10-K Review Checklist</div>${reasonList(cfa.tenk_checks)}
  ${(cfa.rows||[]).length?`<div class="k" style="margin-top:10px">Trend Rows</div><table><tr><th>Period</th><th>CFO</th><th>Net Income</th><th>FCF</th><th>Accruals</th></tr>${(cfa.rows||[]).map(r=>`<tr><td>${r.period||'—'}</td><td>${n(r.cfo)}</td><td>${n(r.net_income)}</td><td>${n(r.fcf)}</td><td>${n(r.accruals)}</td></tr>`).join('')}</table>`:'<div class="muted">Cash flow trend unavailable from structured data.</div>'}`;
  $('cfmatrix').innerHTML=`<table><tr><th>Ratio</th><th>Value</th><th>Status</th><th>Interpretation</th><th>Manual check</th></tr>${(cfm||[]).map(r=>`<tr><td>${r.ratio_name}</td><td>${n(r.value)}</td><td>${pill(r.status)}</td><td>${r.interpretation}</td><td>${r.manual_check}</td></tr>`).join('')||'<tr><td colspan="5" class="muted">No ratio matrix available</td></tr>'}</table>`;
  $('crossmod').innerHTML=reasonList(cmi);

  const k=filing.latest_10k||{}, qf=filing.latest_10q||{};
  const diag=filing.diagnostics||{};
  const link=(u)=>u?`<a href="${u}" target="_blank">Open SEC filing</a>`:'—';
  const unavailableMsg=(!diag.filing_text_downloaded)?'<div class="muted">SEC filing unavailable - check CIK mapping or SEC request</div>':'';
  $('specialitems').innerHTML = `<div>${pill(sp.risk_level)} Score: ${n(sp.score)}</div><table><tr><th>Metric</th><th>Value</th></tr>${Object.entries(sp.metrics||{}).map(([k,v])=>`<tr><td>${k}</td><td>${n(v)}</td></tr>`).join('')}</table><div class='k' style='margin-top:10px'>Top Findings</div>${reasonList((sp.flags||[]).slice(0,5))}<div class='k' style='margin-top:10px'>Special Item Flags</div>${reasonList(sp.suspicious_signals)}<div class='k' style='margin-top:10px'>Accounting Change Flags</div>${reasonList((sp.flags||[]).filter(x=>/accounting|restatement|material weakness/i.test(x)))}<div class='k' style='margin-top:10px'>Acquisition Flags</div>${reasonList((sp.flags||[]).filter(x=>/acquisition|goodwill|organic/i.test(x)))}<div class='k' style='margin-top:10px'>Short Excerpts</div>${reasonList((sp.excerpt_windows||[]).slice(0,3))}<details><summary>Raw excerpts</summary>${reasonList(sp.excerpt_windows)}</details>`;
  const cap = d.capital_allocation_analysis||{};
  $('capital').innerHTML = `<div>${pill(cap.risk_level||'Unknown')}</div><div class='k' style='margin-top:10px'>Flags</div>${reasonList(cap.flags)}<table><tr><th>Metric</th><th>Value</th></tr>${Object.entries(cap.metrics||{}).map(([k,v])=>`<tr><td>${k}</td><td>${n(v)}</td></tr>`).join('')}</table>`;

  const analystFindings = filing.analyst_findings || [];
  $('filingintel').innerHTML=`<div>${pill(fs.filing_text_risk_level)} Score: ${n(fs.filing_text_score)}</div>${unavailableMsg}
  <table><tr><th>Form</th><th>Date</th><th>Link</th></tr><tr><td>10-K</td><td>${k.filing_date||'—'}</td><td>${link(k.filing_url)}</td></tr><tr><td>10-Q</td><td>${qf.filing_date||'—'}</td><td>${link(qf.filing_url)}</td></tr></table>
  <div class="k" style="margin-top:10px">Top 5 Analyst Findings</div>${renderAnalystFindings(analystFindings)}
  <div class="k" style="margin-top:10px">Top 5 Manual Review Priorities</div>${reasonList((analystFindings||[]).map(x=>x.manual_check).slice(0,5))}`;
  const allEvidence = [...(fs.most_relevant_excerpts||[]), ...((filing.tax||{}).tax_excerpt_windows||[]), ...((filing.receivables||{}).excerpt_windows||[]), ...((filing.inventory||{}).excerpt_windows||[]), ...((filing.debt||{}).excerpt_windows||[]), ...((filing.revenue_nonrecurring||{}).excerpt_windows||[])];
  $('secevidence').innerHTML = renderCollapsedEvidence(allEvidence);
}


async function loadScreener(){
  const el=$('screener'); el.classList.add('loading'); el.textContent='Loading screener...';
  try{
    const r=await fetch('/api/screener'); const d=await r.json();
    const rows=d.rows||[];
    el.innerHTML = `<table><tr><th>Ticker</th><th>Tax Risk</th><th>Quarterly Risk</th><th>Debt Risk</th><th>Cash Flow Risk</th><th>CFO / NI</th><th>FCF Margin</th><th>Accrual Ratio</th><th>Cross-Module Main Insight</th><th>Forensic View</th></tr>
      ${rows.map(x=>`<tr><td class="mono">${x.Ticker}</td><td>${pill(x['Tax Risk'])}</td><td>${pill(x['Quarterly Risk'])}</td><td>${pill(x['Debt Risk'])}</td><td>${pill(x['Cash Flow Risk'])}</td><td>${n(x['CFO / NI'])}</td><td>${n(x['FCF Margin'])}</td><td>${n(x['Accrual Ratio'])}</td><td>${x['Cross-Module Main Insight']||'—'}</td><td>${x['Forensic View']||'—'}</td></tr>`).join('')}</table>`;
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
    period_cfg = get_period_config(request.args.get("period", "5y"))
    tax_analysis = default_tax_analysis()
    quarterly_analysis = default_quarterly_analysis()
    inventory_analysis = default_inventory_analysis()
    receivables_analysis = default_receivables_analysis()
    debt_analysis = default_debt_analysis()
    macro_analysis = default_macro_analysis()
    investment_view = default_investment_view()
    special_items_analysis = default_special_items_analysis()
    cash_flow_analysis = default_cash_flow_analysis()
    cash_flow_ratio_matrix = default_cash_flow_ratio_matrix()
    cross_module_insights = ["Data unavailable"]
    try:
        tk = yf.Ticker(ticker)
        fin, cf, bs = tk.financials, tk.cashflow, tk.balance_sheet
        quality_rows: list[dict[str, Any]] = build_quality_rows(fin, cf, bs, period_cfg["years"])
        tax_rows = extract_tax_rows_from_yfinance(fin, cf, bs, quality_rows, period_cfg["years"]) or extract_tax_rows_from_sec(ticker)
        tax_analysis = analyze_tax_quality(tax_rows, quality_rows)
        quarterly_analysis = build_quarterly_forensic_analysis(ticker, period_cfg["quarters"])
        debt_analysis = analyze_debt_cashflow_risk(fin, cf, bs, quality_rows)
        cash_flow_analysis = analyze_cash_flow_quality(fin, cf, bs, quality_rows)
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
        special_items_analysis = analyze_special_items_and_acquisitions(fin, cf, bs, filing_text, quality_rows)
        divs = latest_from_series(get_series(cf, ["Cash Dividends Paid"]))
        buybacks = latest_from_series(get_series(cf, ["Repurchase Of Capital Stock", "Common Stock Repurchased"]))
        cfo_latest = quality_rows[0].get("cfo") if quality_rows else latest_from_series(get_series(cf, ["Operating Cash Flow"]))
        fcf_latest = quality_rows[0].get("fcf") if quality_rows else None
        cap_metrics = {"dividends_paid": divs, "buybacks": buybacks, "dividends_net_income": safe_div(safe_abs(divs), quality_rows[0].get("net_income") if quality_rows else None), "buybacks_cfo": safe_div(safe_abs(buybacks), cfo_latest), "buybacks_fcf": safe_div(safe_abs(buybacks), fcf_latest), "total_payout_fcf": safe_div(safe_add(safe_abs(divs), safe_abs(buybacks)), fcf_latest), "capex_cfo": safe_div(safe_abs(quality_rows[0].get("capex") if quality_rows else None), cfo_latest)}
        cap_flags = []
        if safe_gt(cap_metrics["buybacks_cfo"], 1): cap_flags.append("Buybacks exceed CFO")
        if safe_gt(cap_metrics["total_payout_fcf"], 1): cap_flags.append("Total payout exceeds FCF")
        capital_allocation_analysis = {"risk_level": "High" if cap_flags else "Watch" if any(v is not None for v in cap_metrics.values()) else "Unknown", "flags": cap_flags or ["No major payout stress detected in limited data"], "metrics": cap_metrics}
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
        cash_flow_ratio_matrix = build_cash_flow_ratio_matrix(fin, cf, bs, cash_flow_analysis, quarterly_analysis, tax_analysis)
        cross_module_insights = build_cross_module_insights(cash_flow_analysis, debt_analysis, quarterly_analysis, special_items_analysis, tax_analysis)
        if filing_text_summary.get("filing_text_risk_level") == "High" and completeness.get("level") == "Weak":
            investment_view["forensic_view"] = "INCONCLUSIVE"
        filing_diagnostics = {"cik_found": bool(filing_10k.get("cik") or filing_10q.get("cik")), "submissions_loaded": bool((filing_10k.get("cik") or filing_10q.get("cik")) and (get_sec_submissions((filing_10k.get("cik") or filing_10q.get("cik"))) is not None)), "latest_10k_found": bool(filing_10k.get("available")), "latest_10q_found": bool(filing_10q.get("available")), "filing_text_downloaded": bool(filing_text), "error": None}
        analyst_findings = []
        for area, flags in [("Tax", tax_text.get("tax_text_flags", [])), ("Receivables", recv_text.get("flags", [])), ("Inventory", inv_text.get("flags", [])), ("Debt", debt_text.get("flags", [])), ("Revenue", rev_text.get("flags", []))]:
            for flag in flags[:2]:
                analyst_findings.append({"area": area, "severity": "High" if "unavailable" not in flag.lower() else "Low", "finding": flag, "why_it_matters": "Potential earnings quality and cash conversion impact.", "evidence": flag, "manual_check": "Review related 10-K/10-Q footnote and rollforward tables."})
        out = {"ticker": ticker, "tax_analysis": tax_analysis, "quarterly_analysis": quarterly_analysis, "inventory_analysis": inventory_analysis,
               "receivables_analysis": receivables_analysis, "debt_analysis": debt_analysis, "macro_analysis": macro_analysis,
               "investment_view": investment_view, "special_items_analysis": special_items_analysis, "cash_flow_analysis": cash_flow_analysis, "cash_flow_ratio_matrix": cash_flow_ratio_matrix, "cross_module_insights": cross_module_insights, "data_completeness": completeness, "sec_filing_intelligence": {"latest_10k": filing_10k, "latest_10q": filing_10q, "diagnostics": filing_diagnostics, "sections": extract_filing_sections(filing_text), "tax": tax_text, "receivables": recv_text, "inventory": inv_text, "debt": debt_text, "revenue_nonrecurring": rev_text, "analyst_findings": analyst_findings[:5], "special_items": analyze_special_items_text(filing_text) if filing_text else {"risk_level":"Unknown","flags":["SEC filing text unavailable"],"keyword_hits":[],"short_findings":[],"excerpt_windows":[],"manual_review_priority":"High"}, "summary": filing_text_summary}}
        return jsonify(out)
    except Exception as e:
        print(f"[ERROR] {ticker}: {type(e).__name__}: {e}")
        traceback.print_exc()
        return jsonify({"ticker": ticker, "tax_analysis": tax_analysis, "quarterly_analysis": quarterly_analysis, "inventory_analysis": inventory_analysis,
                        "receivables_analysis": receivables_analysis, "debt_analysis": debt_analysis, "macro_analysis": macro_analysis,
                        "investment_view": investment_view, "special_items_analysis": special_items_analysis, "cash_flow_analysis": cash_flow_analysis, "cash_flow_ratio_matrix": cash_flow_ratio_matrix, "cross_module_insights": cross_module_insights, "error": f"{type(e).__name__}: {e}"})


@app.route('/api/screener')
def api_screener():
    universe = ["MRK", "TSLA", "AMZN", "JPM", "AAPL", "NFLX", "ABBV", "CRM", "META", "AMD", "BAC", "WMT", "AMGN", "COST", "QCOM"]
    rows = []
    for ticker in universe:
        try:
            data = app.test_client().get(f"/api/analyze?ticker={ticker}&period=5y").get_json()
            filing = data.get("sec_filing_intelligence", {})
            summary = filing.get("summary", {})
            special = data.get("special_items_analysis", {})
            cfa = data.get("cash_flow_analysis", {})
            cm = cfa.get("metrics", {})
            rows.append({"Ticker": ticker, "Tax Risk": data["tax_analysis"].get("tax_risk_level", "Unknown"), "Quarterly Risk": data["quarterly_analysis"].get("quarterly_risk_level", "Unknown"), "Debt Risk": data["debt_analysis"].get("risk_level", "Unknown"), "Cash Flow Risk": cfa.get("risk_level", "Unknown"), "CFO / NI": cm.get("cfo_to_net_income"), "FCF Margin": cm.get("fcf_margin"), "Accrual Ratio": cm.get("accrual_ratio"), "Cross-Module Main Insight": (data.get("cross_module_insights") or ["Unavailable"])[0], "Forensic View": data["investment_view"].get("forensic_view", "INCONCLUSIVE")})
        except Exception as e:
            print(f"[ERROR] {ticker}: {type(e).__name__}: {e}")
            traceback.print_exc()
            rows.append({"Ticker": ticker, "Tax Risk": "Unknown", "Quarterly Risk": "Unknown", "Debt Risk": "Unknown", "Cash Flow Risk": "Unknown", "CFO / NI": None, "FCF Margin": None, "Accrual Ratio": None, "Cross-Module Main Insight": "Unhandled module failure", "Forensic View": "INCONCLUSIVE"})
    return jsonify({"rows": rows})


if __name__ == '__main__':
    app.run(debug=True, port=5057)
