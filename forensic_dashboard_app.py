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
  const tax=d.tax_analysis||{}, q=d.quarterly_analysis||{}, debt=d.debt_analysis||{}, inv=d.investment_view||{}, comp=d.data_completeness||{};
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
}

async function loadScreener(){
  const el=$('screener'); el.classList.add('loading'); el.textContent='Loading screener...';
  try{
    const r=await fetch('/api/screener'); const d=await r.json();
    const rows=d.rows||[];
    el.innerHTML = `<table><tr><th>Ticker</th><th>Tax Risk</th><th>Quarterly Risk</th><th>Debt Risk</th><th>Forensic View</th><th>Confidence</th><th>Completeness</th><th>Main Reason</th></tr>
      ${rows.map(x=>`<tr><td class="mono">${x.Ticker}</td><td>${pill(x['Tax Risk'])}</td><td>${pill(x['Quarterly Risk'])}</td><td>${pill(x['Debt Risk'])}</td><td>${x['Forensic View']||'—'}</td><td>${x.Confidence||'—'}</td><td>${pill(x['Data Completeness'])}</td><td>${x['Main Reason']||'—'}</td></tr>`).join('')}</table>`;
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
