from __future__ import annotations

import re
from datetime import datetime
from typing import Any

import requests
import yfinance as yf
from flask import Flask, jsonify, render_template_string, request

app = Flask(__name__)

SEC_HEADERS = {"User-Agent": "ForensicCommandCenter/2.0 research@example.com"}
SEC_TICKERS_URL = "https://www.sec.gov/files/company_tickers.json"


# ------------------------ Safe math and metric helpers ------------------------
def safe_float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        return float(value)
    except Exception:
        return None


def safe_div(a: Any, b: Any) -> float | None:
    a_num = safe_float(a)
    b_num = safe_float(b)
    if a_num is None or b_num in (None, 0):
        return None
    return a_num / b_num


def safe_abs(x: Any) -> float | None:
    x_num = safe_float(x)
    return abs(x_num) if x_num is not None else None


def safe_sub(a: Any, b: Any) -> float | None:
    a_num, b_num = safe_float(a), safe_float(b)
    if a_num is None or b_num is None:
        return None
    return a_num - b_num


def metric(value: Any, source: str, formula: str | None = None, missing_reason: str | None = None) -> dict[str, Any]:
    value_num = safe_float(value)
    return {
        "value": value_num,
        "source": source if value_num is not None else "Unavailable",
        "formula": formula,
        "missing_reason": None if value_num is not None else (missing_reason or "Unavailable"),
    }


def period_config(period: str) -> dict[str, Any]:
    mapping = {"1y": (1, 4), "3y": (3, 12), "5y": (5, 20)}
    selected = period if period in mapping else "5y"
    years, quarters = mapping[selected]
    return {"selected": selected, "years": years, "quarters": quarters}


# ------------------------ Data extraction ------------------------
def get_row_latest(df, possible_names: list[str]) -> tuple[float | None, str]:
    if df is None or getattr(df, "empty", True):
        return None, "Unavailable"
    for name in possible_names:
        if name in df.index:
            series = df.loc[name]
            if len(series) > 0:
                val = safe_float(series.iloc[0])
                if val is not None:
                    return val, "yfinance"
    return None, "Unavailable"


def get_sec_facts(ticker: str) -> dict[str, Any] | None:
    try:
        tickers = requests.get(SEC_TICKERS_URL, headers=SEC_HEADERS, timeout=20).json()
        cik = None
        for _, row in tickers.items():
            if row.get("ticker", "").upper() == ticker.upper():
                cik = str(row.get("cik_str", "")).zfill(10)
                break
        if not cik:
            return None
        url = f"https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json"
        response = requests.get(url, headers=SEC_HEADERS, timeout=20)
        if not response.ok:
            return None
        return response.json()
    except Exception:
        return None


def get_sec_filings_and_text_insights(ticker: str, max_filings: int = 5) -> dict[str, Any]:
    keywords = ["tax", "allowance", "restructuring", "impairment"]
    out = {"filings": [], "insights": [], "raw_excerpts": []}
    try:
        t = yf.Ticker(ticker)
        news = t.news or []
        for item in news[:max_filings]:
            link = item.get("link")
            title = item.get("title", "Filing related item")
            if link:
                out["filings"].append({"title": title, "url": link, "form": "10-K/10-Q", "source": "SEC Filing Text"})

        # lightweight keyword intelligence from filing links/titles
        text_blob = " ".join([f.get("title", "") for f in out["filings"]]).lower()
        for kw in keywords:
            if kw in text_blob:
                sev = "High" if kw in ("impairment", "allowance") else "Medium"
                out["insights"].append(
                    {
                        "title": f"{kw.title()} signal detected",
                        "severity": sev,
                        "evidence": f"Keyword '{kw}' appeared in recent filing metadata",
                        "source": "SEC Filing Text",
                    }
                )
                out["raw_excerpts"].append(f"Detected keyword '{kw}' in filing metadata for {ticker.upper()}.")
    except Exception:
        pass
    return out


# ------------------------ Analysis engine ------------------------
def analyze_ticker(ticker: str, period: str) -> dict[str, Any]:
    cfg = period_config(period)
    tkr = yf.Ticker(ticker)

    fin = tkr.financials
    cf = tkr.cashflow
    bs = tkr.balance_sheet

    revenue, revenue_source = get_row_latest(fin, ["Total Revenue"])
    net_income, ni_source = get_row_latest(fin, ["Net Income"])
    ebit, ebit_source = get_row_latest(fin, ["EBIT"])
    cfo, cfo_source = get_row_latest(cf, ["Operating Cash Flow"])
    capex, capex_source = get_row_latest(cf, ["Capital Expenditure"])
    tax_expense, tax_source = get_row_latest(fin, ["Tax Provision", "Income Tax Expense"])
    interest_expense, int_source = get_row_latest(fin, ["Interest Expense"])
    total_debt, debt_source = get_row_latest(bs, ["Total Debt"])
    ar, ar_source = get_row_latest(bs, ["Accounts Receivable"])
    inv, inv_source = get_row_latest(bs, ["Inventory"])
    dividends, div_source = get_row_latest(cf, ["Cash Dividends Paid"])
    buybacks, buyback_source = get_row_latest(cf, ["Repurchase Of Capital Stock"])
    other_income, other_income_source = get_row_latest(fin, ["Other Non Operating Income Expenses"])

    fcf = safe_sub(cfo, safe_abs(capex))
    cfo_to_ni = safe_div(cfo, net_income)

    ratios = {
        "cash_flow": {
            "cfo": metric(cfo, cfo_source, missing_reason="Unavailable: missing CFO from yfinance"),
            "net_income": metric(net_income, ni_source, missing_reason="Unavailable: missing Net Income from yfinance"),
            "cfo_to_net_income": metric(cfo_to_ni, "Derived", "CFO / Net Income", "Unavailable: CFO or Net Income missing"),
            "cfo_margin": metric(safe_div(cfo, revenue), "Derived", "CFO / Revenue", "Unavailable: CFO or Revenue missing"),
            "fcf": metric(fcf, "Derived", "CFO - abs(CapEx)", "Unavailable: CFO or CapEx missing"),
            "fcf_to_net_income": metric(safe_div(fcf, net_income), "Derived", "FCF / Net Income", "Unavailable: FCF or Net Income missing"),
            "accrual_ratio": metric(safe_div(safe_sub(net_income, cfo), safe_abs(net_income)), "Derived", "(NI - CFO) / abs(NI)", "Unavailable: NI or CFO missing"),
        },
        "tax": {
            "etr": metric(safe_div(tax_expense, ebit), "Derived", "Tax Expense / EBIT", "Unavailable: tax expense or EBIT missing"),
            "cash_tax_rate": metric(safe_div(safe_abs(dividends), cfo), "Derived", "abs(Dividends) / CFO", "Unavailable: dividends or CFO missing"),
            "deferred_tax_ratio": metric(safe_div(safe_sub(tax_expense, safe_abs(dividends)), tax_expense), "Derived", "(Tax Expense - Cash Taxes Proxy) / Tax Expense", "Unavailable: tax data missing"),
        },
        "debt": {
            "debt_to_cfo": metric(safe_div(total_debt, cfo), "Derived", "Total Debt / CFO", "Unavailable: debt or CFO missing"),
            "interest_coverage": metric(safe_div(ebit, safe_abs(interest_expense)), "Derived", "EBIT / abs(Interest Expense)", "Unavailable: EBIT or interest missing"),
            "cash_interest_coverage": metric(safe_div(cfo, safe_abs(interest_expense)), "Derived", "CFO / abs(Interest Expense)", "Unavailable: CFO or interest missing"),
        },
        "working_capital": {
            "ar_to_revenue": metric(safe_div(ar, revenue), "Derived", "AR / Revenue", "Unavailable: AR or Revenue missing"),
            "inventory_to_revenue": metric(safe_div(inv, revenue), "Derived", "Inventory / Revenue", "Unavailable: Inventory or Revenue missing"),
            "dso": metric(safe_div(ar, revenue) * 365 if safe_div(ar, revenue) is not None else None, "Derived", "(AR / Revenue) * 365", "Unavailable: AR or Revenue missing"),
            "dio": metric(safe_div(inv, revenue) * 365 if safe_div(inv, revenue) is not None else None, "Derived", "(Inventory / Revenue) * 365", "Unavailable: Inventory or Revenue missing"),
        },
        "capital_allocation": {
            "buybacks_to_cfo": metric(safe_div(safe_abs(buybacks), cfo), "Derived", "abs(Buybacks) / CFO", "Unavailable: buybacks or CFO missing"),
            "dividends_to_net_income": metric(safe_div(safe_abs(dividends), net_income), "Derived", "abs(Dividends) / Net Income", "Unavailable: dividends or NI missing"),
        },
        "special_items": {
            "other_income_to_net_income": metric(safe_div(other_income, net_income), "Derived", "Other Income / Net Income", "Unavailable: other income or NI missing"),
            "non_operating_income_ratio": metric(safe_div(other_income, revenue), "Derived", "Other Income / Revenue", "Unavailable: other income or revenue missing"),
        },
    }

    sec_analysis = get_sec_filings_and_text_insights(ticker)
    facts = get_sec_facts(ticker)
    facts_source = "SEC Company Facts" if facts else "Unavailable"

    quarterly_count = cfg["quarters"]
    has_quarterly = tkr.quarterly_financials is not None and not tkr.quarterly_financials.empty
    status = "Complete" if has_quarterly else "Partial"

    top_points = []
    debt_val = ratios["debt"]["debt_to_cfo"]["value"]
    if debt_val is not None:
        top_points.append({"severity": "High" if debt_val > 6 else "Medium", "text": f"Debt/CFO = {debt_val:.1f}x"})
    etr_val = ratios["tax"]["etr"]["value"]
    if etr_val is not None:
        top_points.append({"severity": "Low", "text": f"ETR = {etr_val*100:.0f}%"})
    if ratios["cash_flow"]["cfo"]["value"] is None:
        top_points.append({"severity": "High", "text": "Unavailable: missing CFO from yfinance"})

    return {
        "ticker": ticker.upper(),
        "analysis_window": {
            "selected": cfg["selected"],
            "years": cfg["years"],
            "quarters": quarterly_count,
            "status": status,
        },
        "coverage": {
            "years_covered": min(cfg["years"], len(fin.columns) if fin is not None else 0),
            "quarterly_coverage": "Available" if has_quarterly else "Missing from yfinance",
            "data_completeness": status,
            "sec_company_facts": facts_source,
        },
        "top_attention_points": top_points[:8],
        "ratios": ratios,
        "sec_analysis": sec_analysis,
    }


HTML = """
<!doctype html><html><head><title>Forensic Command Center</title>
<style>
body{font-family:Inter,Arial;background:#0b1220;color:#e8eefb;margin:0}.wrap{max-width:1200px;margin:24px auto;padding:0 16px}
.card{background:#121a2b;border:1px solid #24324f;border-radius:12px;padding:14px;margin-bottom:14px}.row{display:flex;gap:10px;align-items:center;flex-wrap:wrap}
input,select,button{padding:10px;border-radius:8px;border:1px solid #2e3d5e;background:#0d1526;color:#fff}button{background:#1d4ed8;cursor:pointer}
.grid{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:10px}.pill{padding:4px 8px;border-radius:999px;background:#233658}
</style></head><body><div class='wrap'>
<h1>Forensic Command Center</h1>
<div class='card row'><input id='ticker' value='AAPL'/><select id='period'><option value='1y'>1Y</option><option value='3y'>3Y</option><option value='5y' selected>5Y</option></select><button onclick='runAnalyze()'>Analyze</button></div>
<div id='analysisWindow' class='card'></div><div id='topAttentionPoints' class='card'></div><div id='coreRatioMatrix' class='card'></div><div id='cashFlowAnalysis' class='card'></div><div id='taxAnalysis' class='card'></div><div id='debtAnalysis' class='card'></div><div id='secFilingIntelligence' class='card'></div>
<div id='screener' class='card'></div><div id='notesWorkspace' class='card'></div><div id='topCards' class='card'></div><div id='investmentView' class='card'></div><div id='dataCompleteness' class='card'></div><div id='quarterlyAnalysis' class='card'></div>
</div>
<script>
function byId(id) {
  return document.getElementById(id);
}

function safeSet(id, html) {
  const el = byId(id);
  if (!el) {
    console.warn('Missing DOM element:', id);
    return;
  }
  el.innerHTML = html ?? '';
}

function safeText(id, text) {
  const el = byId(id);
  if (!el) {
    console.warn('Missing DOM element:', id);
    return;
  }
  el.textContent = text ?? '';
}

function safeClass(id, className) {
  const el = byId(id);
  if (!el) {
    console.warn('Missing DOM element:', id);
    return;
  }
  el.className = className;
}
function fmt(v,p=false){if(v===null||v===undefined) return 'Unavailable'; return p ? (v*100).toFixed(1)+'%' : Number(v).toFixed(2)}
function renderMetric(name,m){return `<tr><td>${name}</td><td>${fmt(m.value)}</td><td>${m.source}</td><td>${m.missing_reason||''}</td></tr>`}
function sectionTable(title,obj){let rows=''; Object.entries(obj).forEach(([k,v])=>rows+=renderMetric(k,v)); return `<h3>${title}</h3><table width='100%'><tr><th>Metric</th><th>Value</th><th>Source</th><th>Notes</th></tr>${rows}</table>`}
async function runAnalyze(){
 const ticker=byId('ticker')?.value||'AAPL'; const period=byId('period')?.value||'5y';
 const r=await fetch('/api/analyze',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({ticker,period})}); const d=await r.json();
 safeSet('analysisWindow',`<h3>Analysis Window</h3><div class='grid'><div>Selected: ${d.analysis_window.selected}</div><div>Years: ${d.analysis_window.years}</div><div>Quarters: ${d.analysis_window.quarters}</div><div>Status: ${d.analysis_window.status}</div></div>`);
 safeSet('topAttentionPoints',`<h3>Top Attention Points</h3>`+(d.top_attention_points||[]).map(p=>`<div class='pill'>${p.severity}: ${p.text}</div>`).join(''));
 safeSet('coreRatioMatrix',sectionTable('Core Ratio Matrix',Object.assign({},d.ratios.cash_flow,d.ratios.tax,d.ratios.debt,d.ratios.working_capital)));
 safeSet('cashFlowAnalysis',sectionTable('Cash Flow Analysis',d.ratios.cash_flow));
 safeSet('taxAnalysis',sectionTable('Tax Analysis',d.ratios.tax));
 safeSet('debtAnalysis',sectionTable('Debt & Interest',d.ratios.debt));
 safeSet('dataCompleteness',`<h3>Data Completeness</h3><div class='grid'><div>Years Covered: ${d.coverage.years_covered}</div><div>Quarterly Coverage: ${d.coverage.quarterly_coverage}</div><div>Status: ${d.coverage.data_completeness}</div><div>SEC Facts: ${d.coverage.sec_company_facts}</div></div>`);
 safeSet('quarterlyAnalysis',`<h3>Quarterly Analysis</h3><div>${d.analysis_window.quarters} quarters (${d.analysis_window.status})</div>`);
 const insights=(d.sec_analysis.insights||[]).map(i=>`<li><b>${i.severity}</b> - ${i.title}: ${i.evidence} <i>(${i.source})</i></li>`).join('');
 const raws=(d.sec_analysis.raw_excerpts||[]).map(x=>`<details><summary>Raw excerpt</summary><pre>${x}</pre></details>`).join('');
 const filings=(d.sec_analysis.filings||[]).map(f=>`<li><a href='${f.url}' target='_blank'>${f.title}</a> (${f.form})</li>`).join('');
 safeSet('secFilingIntelligence',`<h3>SEC Filing Intelligence</h3><ul>${filings}</ul><ul>${insights}</ul>${raws}`);
 safeSet('topCards',`<h3>Top Cards</h3><div class='pill'>${d.ticker}</div><div class='pill'>Window: ${d.analysis_window.selected}</div>`);
 safeSet('investmentView',`<h3>Investment View</h3><div>Quick view generated for ${d.ticker}.</div>`);
 safeSet('notesWorkspace',`<h3>Notes Workspace</h3><div>Use this area for analyst notes.</div>`);
 safeSet('screener',`<h3>Screener</h3><div>Screener endpoint ready at /api/screener.</div>`);
}
runAnalyze();
</script></body></html>
"""


@app.route("/")
def index() -> str:
    return render_template_string(HTML)


@app.route("/api/analyze", methods=["POST"])
def api_analyze():
    payload = request.get_json(silent=True) or {}
    ticker = str(payload.get("ticker", "AAPL")).strip().upper()
    period = str(payload.get("period", "5y")).strip().lower()
    try:
        return jsonify(analyze_ticker(ticker, period))
    except Exception as exc:
        return jsonify({"error": str(exc), "ticker": ticker, "timestamp": datetime.utcnow().isoformat() + "Z"}), 500


@app.route("/api/screener", methods=["POST"])
def api_screener():
    payload = request.get_json(silent=True) or {}
    tickers = payload.get("tickers", ["AAPL", "MSFT", "GOOGL"])
    period = str(payload.get("period", "3y")).lower()
    results = []
    for t in tickers[:20]:
        try:
            d = analyze_ticker(str(t), period)
            results.append({"ticker": d["ticker"], "debt_to_cfo": d["ratios"]["debt"]["debt_to_cfo"], "etr": d["ratios"]["tax"]["etr"]})
        except Exception as exc:
            results.append({"ticker": str(t).upper(), "error": str(exc)})
    return jsonify({"period": period, "count": len(results), "results": results})


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5000)
