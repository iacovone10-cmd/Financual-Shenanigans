from __future__ import annotations

from datetime import datetime
from typing import Any

import requests
import yfinance as yf
from flask import Flask, jsonify, render_template_string, request

app = Flask(__name__)

SEC_HEADERS = {"User-Agent": "ForensicDashboard/4.0 analyst@example.com"}
SEC_TICKERS_URL = "https://www.sec.gov/files/company_tickers.json"


def safe_float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        return float(value)
    except Exception:
        return None


def abs_safe(v: Any) -> float | None:
    x = safe_float(v)
    return abs(x) if x is not None else None


def safe_div(a: Any, b: Any) -> float | None:
    x, y = safe_float(a), safe_float(b)
    if x is None or y in (None, 0):
        return None
    return x / y


def safe_sub(a: Any, b: Any) -> float | None:
    x, y = safe_float(a), safe_float(b)
    if x is None or y is None:
        return None
    return x - y


def safe_growth(curr: Any, prev: Any) -> float | None:
    c, p = safe_float(curr), safe_float(prev)
    if c is None or p in (None, 0):
        return None
    return (c - p) / abs(p)


def fmt_money(v: float | None) -> str:
    return "Unavailable" if v is None else f"${v:,.0f}"


def fmt_pct(v: float | None) -> str:
    return "Unavailable" if v is None else f"{v*100:.1f}%"


def fmt_x(v: float | None) -> str:
    return "Unavailable" if v is None else f"{v:.2f}x"


def default_module() -> dict[str, Any]:
    return {
        "risk_level": "Unknown",
        "score": None,
        "data_status": "Unavailable",
        "flags": ["Data unavailable"],
        "metrics": {},
        "findings": [],
    }


def first_row(df, labels: list[str], col=None) -> float | None:
    if df is None or getattr(df, "empty", True):
        return None
    for label in labels:
        if label in df.index:
            s = df.loc[label]
            if col is not None and col in s.index:
                return safe_float(s[col])
            for val in s.values:
                x = safe_float(val)
                if x is not None:
                    return x
    return None


def ratio_row(category: str, name: str, value: float | None, threshold: str, explanation: str, manual_check: str, source: str, missing_reason: str | None = None) -> dict[str, Any]:
    status = "Unavailable"
    trend = "Unavailable"
    interpretation = missing_reason or "Unavailable"
    if value is not None:
        status = "Watch"
        interpretation = explanation
        if "coverage" in name.lower() or "margin" in name.lower() or name in {"CFO / Net Income", "FCF / Net Income", "Cash / Current Debt"}:
            status = "Healthy" if value >= 1 else ("Watch" if value >= 0.6 else "Risk")
        if name in {"Debt / CFO", "Net Debt / CFO", "AR / Revenue", "Inventory / Revenue", "Accrual Ratio"}:
            status = "Healthy" if value <= 1 else ("Watch" if value <= 3 else "Risk")
        if name in {"ETR", "Cash Tax Rate"}:
            status = "Healthy" if 0.1 <= value <= 0.35 else ("Watch" if -0.05 <= value <= 0.45 else "Risk")
        trend = "Stable"
    return {
        "category": category,
        "name": name,
        "value": value,
        "display_value": fmt_pct(value) if "Rate" in name or "Margin" in name or " / " in name and "Debt" not in name and "Coverage" not in name else (fmt_x(value) if "Coverage" in name or "Debt /" in name or " / CFO" in name else (fmt_money(value) if name in {"FCF"} else ("Unavailable" if value is None else f"{value:.2f}"))),
        "status": status,
        "trend": trend,
        "interpretation": interpretation,
        "explanation": explanation,
        "threshold": threshold,
        "manual_check": manual_check,
        "source": source,
        "missing_reason": missing_reason,
    }


def get_cik_for_ticker(ticker: str) -> str | None:
    try:
        rows = requests.get(SEC_TICKERS_URL, headers=SEC_HEADERS, timeout=20).json()
        for _, row in rows.items():
            if row.get("ticker", "").upper() == ticker.upper():
                return str(row.get("cik_str", "")).zfill(10)
    except Exception:
        return None
    return None


def sec_intelligence(ticker: str) -> dict[str, Any]:
    out = {"latest_10k": None, "latest_10q": None, "latest_form4": [], "findings": [], "raw_excerpts": [], "source": "SEC Filing Text"}
    cik = get_cik_for_ticker(ticker)
    if not cik:
        return out
    try:
        sub = requests.get(f"https://data.sec.gov/submissions/CIK{cik}.json", headers=SEC_HEADERS, timeout=20).json()
        recent = sub.get("filings", {}).get("recent", {})
        forms = recent.get("form", [])
        dates = recent.get("filingDate", [])
        accs = recent.get("accessionNumber", [])
        docs = recent.get("primaryDocument", [])
        for i, form in enumerate(forms):
            acc = (accs[i] if i < len(accs) else "").replace("-", "")
            doc = docs[i] if i < len(docs) else ""
            url = f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/{acc}/{doc}" if acc and doc else None
            rec = {"date": dates[i] if i < len(dates) else None, "url": url, "form": form}
            if form == "10-K" and out["latest_10k"] is None:
                out["latest_10k"] = rec
            if form == "10-Q" and out["latest_10q"] is None:
                out["latest_10q"] = rec
            if form == "4" and len(out["latest_form4"]) < 6:
                out["latest_form4"].append(rec)
        out["findings"] = [
            {"area": "SEC", "severity": "Watch", "point": "Review latest annual and quarterly filings.", "evidence_short": f"10-K: {out['latest_10k']['date'] if out['latest_10k'] else 'Unavailable'}, 10-Q: {out['latest_10q']['date'] if out['latest_10q'] else 'Unavailable'}", "source": "SEC Filing Text"},
            {"area": "Insider", "severity": "Watch" if out["latest_form4"] else "Unknown", "point": "Inspect recent Form 4 transactions for patterns.", "evidence_short": f"Recent Form 4 count: {len(out['latest_form4'])}", "source": "SEC Filing Text"},
        ]
        out["raw_excerpts"] = list({f["evidence_short"][:300] for f in out["findings"]})
    except Exception:
        pass
    return out


def analyze_ticker(ticker: str, period: str) -> dict[str, Any]:
    period_map = {"1y": (1, 4), "3y": (3, 12), "5y": (5, 20)}
    years_req, q_req = period_map.get(period, (5, 20))
    t = yf.Ticker(ticker)
    fin_a, cf_a, bs_a = t.financials, t.cashflow, t.balance_sheet
    fin_q, cf_q = t.quarterly_financials, t.quarterly_cashflow

    annual_cols = list(fin_a.columns[:years_req]) if fin_a is not None and not fin_a.empty else []
    quarterly_cols = list(fin_q.columns[:q_req]) if fin_q is not None and not fin_q.empty else []

    rows = []
    for col in annual_cols:
        revenue = first_row(fin_a, ["Total Revenue"], col)
        ni = first_row(fin_a, ["Net Income"], col)
        cfo = first_row(cf_a, ["Operating Cash Flow"], col)
        capex = first_row(cf_a, ["Capital Expenditure"], col)
        debt = first_row(bs_a, ["Total Debt"], col)
        ie = abs_safe(first_row(fin_a, ["Interest Expense"], col))
        pretax = first_row(fin_a, ["Pretax Income", "Earnings Before Tax"], col)
        tax = first_row(fin_a, ["Tax Provision", "Income Tax Expense"], col)
        ar = first_row(bs_a, ["Accounts Receivable"], col)
        inv = first_row(bs_a, ["Inventory"], col)
        buybacks = abs_safe(first_row(cf_a, ["Repurchase Of Capital Stock"], col))
        divs = abs_safe(first_row(cf_a, ["Cash Dividends Paid"], col))
        fcf = safe_sub(cfo, abs_safe(capex))
        rows.append({
            "fiscal_year": str(col.year), "revenue": revenue, "net_income": ni, "cfo": cfo, "capex": capex, "fcf": fcf,
            "cfo_to_net_income": safe_div(cfo, ni), "fcf_margin": safe_div(fcf, revenue), "accrual_ratio": safe_div(safe_sub(ni, cfo), abs_safe(ni)),
            "pretax_income": pretax, "tax_expense": tax, "etr": safe_div(tax, pretax), "total_debt": debt, "interest_expense": ie,
            "debt_to_cfo": safe_div(debt, cfo), "interest_coverage": safe_div(first_row(fin_a, ["EBIT"], col), ie), "ar": ar, "inventory": inv,
            "ar_to_revenue": safe_div(ar, revenue), "inventory_to_revenue": safe_div(inv, revenue), "buybacks": buybacks, "dividends": divs,
            "buybacks_to_cfo": safe_div(buybacks, cfo), "dividends_to_net_income": safe_div(divs, ni),
        })

    latest = rows[0] if rows else {}
    sec = sec_intelligence(ticker)
    revenue, ni, cfo = latest.get("revenue"), latest.get("net_income"), latest.get("cfo")
    fcf = latest.get("fcf")
    debt = latest.get("total_debt")
    ie = latest.get("interest_expense")

    core_ratios = [
        ratio_row("Cash Flow", "CFO / Net Income", safe_div(cfo, ni), ">=1.0", "Checks earnings-to-cash conversion quality.", "Cash flow statement reconciliation.", "Derived"),
        ratio_row("Cash Flow", "CFO Margin", safe_div(cfo, revenue), ">10%", "Operating cash generation intensity.", "MD&A cash discussion.", "Derived"),
        ratio_row("Cash Flow", "FCF", fcf, ">0", "Cash left after CapEx.", "CapEx policy and maintenance vs growth split.", "Derived"),
        ratio_row("Cash Flow", "FCF Margin", safe_div(fcf, revenue), ">5%", "FCF productivity per dollar of sales.", "FCF bridge and CapEx mix.", "Derived"),
        ratio_row("Cash Flow", "FCF / Net Income", safe_div(fcf, ni), ">=1", "Sustainability of earnings after reinvestment.", "Reinvestment requirements.", "Derived"),
        ratio_row("Cash Flow", "Accrual Ratio", latest.get("accrual_ratio"), "<0.1", "High accruals may indicate weaker quality of earnings.", "Non-cash adjustments and reserves.", "Derived"),
        ratio_row("Debt", "Debt / CFO", safe_div(debt, cfo), "<3x", "Debt burden relative to internal cash generation.", "Maturity wall and covenant terms.", "Derived"),
        ratio_row("Debt", "Interest Coverage", safe_div(first_row(fin_a, ["EBIT"]), ie), ">2x", "Ability of operating profits to service interest.", "Floating-rate exposure.", "Derived"),
        ratio_row("Debt", "Cash Interest Coverage", safe_div(cfo, ie), ">2x", "Cash ability to service interest.", "Cash flow volatility stress test.", "Derived"),
        ratio_row("Tax", "ETR", latest.get("etr"), "10%-35%", "Book tax burden consistency.", "Tax footnote and jurisdiction mix.", "Derived"),
        ratio_row("Working Capital", "AR / Revenue", latest.get("ar_to_revenue"), "Stable", "Receivables pressure and collection quality.", "AR aging and credit policy.", "Derived"),
        ratio_row("Working Capital", "Inventory / Revenue", latest.get("inventory_to_revenue"), "Stable", "Inventory build risk vs demand.", "Write-down/obsolescence footnotes.", "Derived"),
        ratio_row("Capital Allocation", "Buybacks / CFO", latest.get("buybacks_to_cfo"), "<50%", "Buyback funding sustainability.", "Repurchase authorization and debt funding.", "Derived"),
        ratio_row("Capital Allocation", "Dividends / Net Income", latest.get("dividends_to_net_income"), "<80%", "Dividend payout sustainability.", "Dividend policy and liquidity tests.", "Derived"),
    ]

    debt_flags = [f for f in ["debt/CFO high" if safe_div(debt, cfo) and safe_div(debt, cfo) > 6 else None, "interest coverage below 2x" if safe_div(first_row(fin_a, ["EBIT"]), ie) and safe_div(first_row(fin_a, ["EBIT"]), ie) < 2 else None] if f]
    cash_flags = [f for f in ["CFO/NI < 1" if safe_div(cfo, ni) and safe_div(cfo, ni) < 1 else None, "FCF negative" if fcf is not None and fcf < 0 else None] if f]

    completeness_unavail = sum(1 for r in core_ratios if r["status"] == "Unavailable")
    if completeness_unavail > 8:
        view, risk, conf = "INCONCLUSIVE", "Unknown", "Low"
    elif debt_flags or cash_flags:
        view, risk, conf = "HOLD / WATCHLIST", "Medium", "Medium"
    else:
        view, risk, conf = "BUY / ACCUMULATE", "Low", "Medium"

    top = []
    for r in core_ratios:
        if r["status"] in {"Risk", "Unavailable"} and len(top) < 10:
            top.append({"severity": "High" if r["status"] == "Risk" else "Medium", "area": r["category"], "point": f"{r['name']} = {r['display_value']}", "evidence": r["interpretation"], "why_it_matters": r["explanation"], "where_to_verify": r["manual_check"], "source": r["source"]})

    insider = default_module()
    insider.update({"data_status": "Available" if sec.get("latest_form4") else "Unavailable", "flags": ["Insider selling is a signal, not proof of wrongdoing."], "findings": sec.get("latest_form4", [])})

    return {
        "ticker": ticker.upper(),
        "analysis_window": {
            "selected_period": period,
            "annual_years_requested": years_req,
            "annual_years_available": len(annual_cols),
            "quarterly_periods_requested": q_req,
            "quarterly_periods_available": len(quarterly_cols),
            "coverage_status": "Complete" if len(annual_cols) >= years_req else "Partial",
            "coverage_note": "Window based on available reported fiscal periods from yfinance.",
        },
        "executive_verdict": {"forensic_view": view, "risk_level": risk, "confidence": conf, "label": "Risk-based forensic opinion, not financial advice."},
        "top_attention_points": top,
        "annual_comparison": {"rows": rows},
        "core_ratios": core_ratios,
        "cash_flow_analysis": {"risk_level": "High" if cash_flags else "Low", "score": None, "data_status": "Available", "flags": cash_flags or ["No critical cash flow flags from available data."], "metrics": {"CFO": fmt_money(cfo), "Net Income": fmt_money(ni), "FCF": fmt_money(fcf), "CFO / NI": fmt_x(safe_div(cfo, ni)), "FCF Margin": fmt_pct(safe_div(fcf, revenue))}, "findings": []},
        "debt_analysis": {"risk_level": "High" if debt_flags else "Low", "score": None, "data_status": "Available", "flags": debt_flags or ["No critical debt flags from available data."], "metrics": {"total debt": fmt_money(debt), "debt/CFO": fmt_x(safe_div(debt, cfo)), "interest coverage": fmt_x(safe_div(first_row(fin_a, ["EBIT"]), ie))}, "findings": []},
        "tax_analysis": default_module(),
        "working_capital_analysis": default_module(),
        "capital_allocation_analysis": default_module(),
        "geographic_segment_analysis": default_module(),
        "insider_transactions_analysis": insider,
        "sbc_dilution_analysis": default_module(),
        "goodwill_intangibles_analysis": default_module(),
        "special_items_analysis": default_module(),
        "non_gaap_analysis": default_module(),
        "customer_concentration_analysis": default_module(),
        "auditor_internal_control_analysis": default_module(),
        "sec_filing_intelligence": sec,
    }


HTML = """
<!doctype html><html><head><meta charset='utf-8'><title>Forensic Dashboard</title>
<script src='https://cdn.jsdelivr.net/npm/chart.js'></script>
<style>body{font-family:Inter,Arial;background:#0b1220;color:#eaf2ff;margin:0}.wrap{max-width:1300px;margin:0 auto;padding:16px}.sticky{position:sticky;top:0;background:#0f182a;padding:12px;border-bottom:1px solid #294163;z-index:9;display:flex;gap:8px;align-items:center}.card{background:#121d31;border:1px solid #2d4669;border-radius:12px;padding:12px;margin:12px 0}.grid{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:12px}.kpi{background:#0f182a;padding:8px;border-radius:8px}table{width:100%;border-collapse:collapse}th,td{border-bottom:1px solid #28415f;padding:7px;text-align:left}.badge{background:#25456d;padding:2px 8px;border-radius:999px}.chart{height:260px}</style>
</head><body><div class='sticky'><input id='ticker' value='TSLA'><select id='period'><option value='1y'>1Y</option><option value='3y'>3Y</option><option value='5y' selected>5Y</option></select><button onclick='runAnalyze()'>Analyze</button></div><div class='wrap'>
<div id='statusMessage' class='card'></div>
<div id='executiveVerdict' class='card'></div><div id='analysisWindow' class='card'></div><div id='topAttentionPoints' class='card'></div>
<div id='annualComparison' class='card'><h3>Multi-Year Forensic Comparison</h3><div id='annualComparisonContent'></div></div>
<div class='grid'><div class='card'><canvas id='c1' class='chart'></canvas></div><div class='card'><canvas id='c2' class='chart'></canvas></div><div class='card'><canvas id='c3' class='chart'></canvas></div><div class='card'><canvas id='c4' class='chart'></canvas></div></div>
<div id='coreRatios' class='card'></div><div id='cashFlow' class='card'></div><div id='debtRisk' class='card'></div><div id='taxRisk' class='card'></div><div id='workingCapital' class='card'></div><div id='capitalAllocation' class='card'></div><div id='secIntel' class='card'></div><div id='screener' class='card'></div><div id='notes' class='card'></div></div>
<script>
const charts={}; function byId(i){return document.getElementById(i)}
function safeSet(id,html){const el=byId(id); if(el) el.innerHTML=html||''}
function draw(id,cfg){if(charts[id]) charts[id].destroy(); const el=byId(id); if(!el) return; charts[id]=new Chart(el,cfg)}
function num(v){return (v===null||v===undefined)?null:v}
function sectionUnavailable(){return "Section unavailable: missing API field"}
function normalizeApiResponse(data){
  const d = data || {};
  return {
    ...d,
    executive_verdict: d.executive_verdict ?? null,
    analysis_window: d.analysis_window ?? null,
    top_attention_points: d.top_attention_points ?? null,
    annual_comparison: d.annual_comparison ?? null,
    core_ratios: d.core_ratios ?? null,
    cash_flow_analysis: d.cash_flow_analysis ?? null,
    debt_analysis: d.debt_analysis ?? null,
    tax_analysis: d.tax_analysis ?? null,
    working_capital_analysis: d.working_capital_analysis ?? null,
    capital_allocation_analysis: d.capital_allocation_analysis ?? null,
    sec_filing_intelligence: d.sec_filing_intelligence ?? null
  };
}
async function runAnalyze(){const ticker=byId('ticker').value||'TSLA';const period=byId('period').value||'5y';
safeSet('statusMessage',"<b>Loading analysis...</b>");
try {
const res=await fetch(`/api/analyze?ticker=${ticker}&period=${period}`);
const data=await res.json();
console.log("API response", data);
if(!res.ok || data.error){throw new Error(data.error||"Failed to load analysis")}
const d=normalizeApiResponse(data);
safeSet('statusMessage', "");
try {const v=d.executive_verdict||{}; safeSet('executiveVerdict', d.executive_verdict?`<h2>Executive Verdict</h2><b>${v.forensic_view||'INCONCLUSIVE'}</b> <span class='badge'>Risk: ${v.risk_level||'Unknown'}</span> <span class='badge'>Confidence: ${v.confidence||'Low'}</span><div>${v.label||''}</div>`:sectionUnavailable());} catch(e){safeSet('executiveVerdict',sectionUnavailable())}
try {const w=d.analysis_window||{}; safeSet('analysisWindow', d.analysis_window?`<h3>Analysis Window</h3><div>Period: ${w.selected_period||'5y'}</div><div>Annual coverage: ${w.annual_years_available||0}/${w.annual_years_requested||0}</div><div>Quarterly coverage: ${w.quarterly_periods_available||0}/${w.quarterly_periods_requested||0}</div><div>${w.coverage_status||'Unknown'} — ${w.coverage_note||''}</div>`:sectionUnavailable());} catch(e){safeSet('analysisWindow',sectionUnavailable())}
try {safeSet('topAttentionPoints', d.top_attention_points?`<h3>Top Attention Points</h3>${(d.top_attention_points||[]).map(p=>`<div class='kpi'><b>${p.severity}</b> ${p.area}: ${p.point}<div>${p.why_it_matters}</div><i>${p.where_to_verify}</i></div>`).join('')||'Unavailable'}`:sectionUnavailable())} catch(e){safeSet('topAttentionPoints',sectionUnavailable())}
const rows=(d.annual_comparison||{}).rows||[]; const labels=rows.map(r=>r.fiscal_year).reverse();
try {safeSet('annualComparisonContent', d.annual_comparison?`<table><tr><th>Year</th><th>Revenue</th><th>NI</th><th>CFO</th><th>FCF</th><th>CFO/NI</th><th>Debt/CFO</th><th>ETR</th></tr>${rows.map(r=>`<tr><td>${r.fiscal_year}</td><td>${r.revenue??'Unavailable'}</td><td>${r.net_income??'Unavailable'}</td><td>${r.cfo??'Unavailable'}</td><td>${r.fcf??'Unavailable'}</td><td>${r.cfo_to_net_income??'Unavailable'}</td><td>${r.debt_to_cfo??'Unavailable'}</td><td>${r.etr??'Unavailable'}</td></tr>`).join('')}</table>`:sectionUnavailable())} catch(e){safeSet('annualComparisonContent',sectionUnavailable())}
draw('c1',{type:'line',data:{labels,datasets:[{label:'Net Income',data:rows.map(r=>num(r.net_income)).reverse()},{label:'CFO',data:rows.map(r=>num(r.cfo)).reverse()},{label:'FCF',data:rows.map(r=>num(r.fcf)).reverse()}]}})
draw('c2',{type:'line',data:{labels,datasets:[{label:'CFO/NI',data:rows.map(r=>num(r.cfo_to_net_income)).reverse()},{label:'Accrual Ratio',data:rows.map(r=>num(r.accrual_ratio)).reverse()},{label:'FCF Margin',data:rows.map(r=>num(r.fcf_margin)).reverse()}]}})
draw('c3',{type:'line',data:{labels,datasets:[{label:'Debt/CFO',data:rows.map(r=>num(r.debt_to_cfo)).reverse()},{label:'Interest Coverage',data:rows.map(r=>num(r.interest_coverage)).reverse()}]}})
draw('c4',{type:'line',data:{labels,datasets:[{label:'AR/Revenue',data:rows.map(r=>num(r.ar_to_revenue)).reverse()},{label:'Inventory/Revenue',data:rows.map(r=>num(r.inventory_to_revenue)).reverse()},{label:'ETR',data:rows.map(r=>num(r.etr)).reverse()}]}})
try {safeSet('coreRatios', d.core_ratios?`<h3>Core Ratio Matrix</h3><table><tr><th>Category</th><th>Name</th><th>Value</th><th>Status</th><th>Why it matters</th><th>10-K/10-Q verify</th><th>Source</th></tr>${(d.core_ratios||[]).map(r=>`<tr><td>${r.category}</td><td>${r.name}</td><td>${r.display_value}</td><td>${r.status}</td><td><details><summary>Explanation</summary>${r.explanation}</details></td><td>${r.manual_check}</td><td>${r.source}</td></tr>`).join('')}</table>`:sectionUnavailable())} catch(e){safeSet('coreRatios',sectionUnavailable())}
try {const c=d.cash_flow_analysis||{};safeSet('cashFlow', d.cash_flow_analysis?`<h3>Cash Flow</h3><div>Risk: ${c.risk_level||'Unknown'}</div><div>${(c.flags||[]).join(' ; ')||'Unavailable'}</div>`:sectionUnavailable())} catch(e){safeSet('cashFlow',sectionUnavailable())}
try {const db=d.debt_analysis||{};safeSet('debtRisk', d.debt_analysis?`<h3>Debt Risk</h3><div>Risk: ${db.risk_level||'Unknown'}</div><div>${(db.flags||[]).join(' ; ')||'Unavailable'}</div>`:sectionUnavailable())} catch(e){safeSet('debtRisk',sectionUnavailable())}
try {const tx=d.tax_analysis||{};safeSet('taxRisk', d.tax_analysis?`<h3>Tax Risk</h3><div>Risk: ${tx.risk_level||'Unknown'}</div><div>${(tx.flags||[]).join(' ; ')||'Unavailable'}</div>`:sectionUnavailable())} catch(e){safeSet('taxRisk',sectionUnavailable())}
try {const wc=d.working_capital_analysis||{};safeSet('workingCapital', d.working_capital_analysis?`<h3>Working Capital</h3><div>Risk: ${wc.risk_level||'Unknown'}</div><div>${(wc.flags||[]).join(' ; ')||'Unavailable'}</div>`:sectionUnavailable())} catch(e){safeSet('workingCapital',sectionUnavailable())}
try {const ca=d.capital_allocation_analysis||{};safeSet('capitalAllocation', d.capital_allocation_analysis?`<h3>Capital Allocation</h3><div>Risk: ${ca.risk_level||'Unknown'}</div><div>${(ca.flags||[]).join(' ; ')||'Unavailable'}</div>`:sectionUnavailable())} catch(e){safeSet('capitalAllocation',sectionUnavailable())}
try {const s=d.sec_filing_intelligence||{};safeSet('secIntel', d.sec_filing_intelligence?`<h3>SEC Filing Intelligence</h3><div>10-K: ${s.latest_10k?.url?`<a target='_blank' href='${s.latest_10k.url}'>${s.latest_10k.date}</a>`:'Unavailable'}</div><div>10-Q: ${s.latest_10q?.url?`<a target='_blank' href='${s.latest_10q.url}'>${s.latest_10q.date}</a>`:'Unavailable'}</div><div>Form 4: ${(s.latest_form4||[]).map(f=>`<a target='_blank' href='${f.url}'>${f.date}</a>`).join(' | ')||'Unavailable'}</div>${(s.raw_excerpts||[]).map(e=>`<details><summary>Evidence excerpt</summary>${e}</details>`).join('')}`:sectionUnavailable())} catch(e){safeSet('secIntel',sectionUnavailable())}
safeSet('screener', "<h3>Screener</h3>Section unavailable: missing API field");
const key='notes_'+ticker.toUpperCase(); safeSet('notes',`<h3>Notes Workspace</h3><textarea id='nb' rows='6' style='width:100%'></textarea><button onclick='localStorage.setItem("${'${key}'}",byId("nb").value)'>Save</button>`); byId('nb').value=localStorage.getItem(key)||'';
} catch(err) { safeSet('statusMessage', `<b>Error:</b> ${err.message||'Unable to load analysis'}`); }
}
runAnalyze();
</script></body></html>
"""


@app.route("/")
def index() -> str:
    return render_template_string(HTML)


@app.route("/api/analyze", methods=["GET", "POST"])
def api_analyze():
    payload = request.get_json(silent=True) or {}
    ticker = str(request.args.get("ticker") or payload.get("ticker") or "TSLA").strip().upper()
    period = str(request.args.get("period") or payload.get("period") or "5y").strip().lower()
    try:
        return jsonify(analyze_ticker(ticker, period))
    except Exception as exc:
        return jsonify({"error": str(exc), "ticker": ticker, "timestamp": datetime.utcnow().isoformat() + "Z"}), 500


@app.route("/api/screener", methods=["GET", "POST"])
def api_screener():
    payload = request.get_json(silent=True) or {}
    tickers = payload.get("tickers") or request.args.get("tickers") or "TSLA,MRK,AMZN,AAPL"
    period = str(payload.get("period") or request.args.get("period") or "5y").lower()
    risk_filter = str(payload.get("risk_filter") or request.args.get("risk_filter") or "").lower()
    rows = []
    for tkr in [t.strip().upper() for t in str(tickers).split(",") if t.strip()]:
        try:
            d = analyze_ticker(tkr, period)
            r = {
                "ticker": tkr, "selected_period": period, "overall_risk": d["executive_verdict"]["risk_level"], "forensic_view": d["executive_verdict"]["forensic_view"], "confidence": d["executive_verdict"]["confidence"],
                "cfo_ni": next((x["display_value"] for x in d["core_ratios"] if x["name"] == "CFO / Net Income"), "Unavailable"),
                "fcf_margin": next((x["display_value"] for x in d["core_ratios"] if x["name"] == "FCF Margin"), "Unavailable"),
                "debt_cfo": next((x["display_value"] for x in d["core_ratios"] if x["name"] == "Debt / CFO"), "Unavailable"),
                "interest_coverage": next((x["display_value"] for x in d["core_ratios"] if x["name"] == "Interest Coverage"), "Unavailable"),
                "etr": next((x["display_value"] for x in d["core_ratios"] if x["name"] == "ETR"), "Unavailable"),
                "ar_revenue": next((x["display_value"] for x in d["core_ratios"] if x["name"] == "AR / Revenue"), "Unavailable"),
                "inventory_revenue": next((x["display_value"] for x in d["core_ratios"] if x["name"] == "Inventory / Revenue"), "Unavailable"),
                "top_red_flag": d["top_attention_points"][0]["point"] if d["top_attention_points"] else "Unavailable", "data_completeness": d["analysis_window"]["coverage_status"],
            }
            rows.append(r)
        except Exception:
            continue
    rows.sort(key=lambda x: {"High": 0, "Medium": 1, "Low": 2, "Unknown": 3}.get(x["overall_risk"], 3))
    if risk_filter == "high risk":
        rows = [r for r in rows if r["overall_risk"] == "High"]
    elif risk_filter == "watchlist":
        rows = [r for r in rows if "WATCH" in r["forensic_view"]]
    elif risk_filter == "best quality":
        rows = [r for r in rows if r["overall_risk"] == "Low"]
    return jsonify({"rows": rows})


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5000)
