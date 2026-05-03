from __future__ import annotations

from datetime import datetime
from typing import Any

import requests
import yfinance as yf
from flask import Flask, jsonify, render_template_string, request

app = Flask(__name__)

SEC_HEADERS = {"User-Agent": "ForensicDashboard/3.0 analyst@example.com"}
SEC_TICKERS_URL = "https://www.sec.gov/files/company_tickers.json"


def safe_float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        return float(value)
    except Exception:
        return None


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


def get_row_latest(df, possible_names: list[str]) -> tuple[float | None, str]:
    if df is None or getattr(df, "empty", True):
        return None, "Unavailable"
    for name in possible_names:
        if name in df.index:
            v = safe_float(df.loc[name].iloc[0])
            if v is not None:
                return v, "yfinance"
    return None, "Unavailable"


def fmt_money(v: float | None) -> str:
    if v is None:
        return "Unavailable"
    return f"${v:,.0f}"


def status_for_ratio(name: str, value: float | None) -> str:
    if value is None:
        return "Unavailable"
    thresholds = {
        "CFO / Net Income": (1.0, 0.8),
        "CFO Margin": (0.12, 0.07),
        "FCF Margin": (0.08, 0.03),
        "Accrual Ratio": (0.10, 0.20),
        "ETR": (0.30, 0.40),
        "Debt / CFO": (3.0, 6.0),
        "Interest Coverage": (5.0, 2.0),
        "Cash Interest Coverage": (4.0, 2.0),
    }
    if name in ["Debt / CFO", "Accrual Ratio", "AR / Revenue", "Inventory / Revenue", "Buybacks / CFO", "Dividends / Net Income", "Other Income / Net Income"]:
        if name in thresholds:
            h, w = thresholds[name]
            return "Healthy" if value <= h else ("Watch" if value <= w else "Risk")
        return "Watch" if value < 0.2 else "Risk"
    if name in thresholds:
        h, w = thresholds[name]
        return "Healthy" if value >= h else ("Watch" if value >= w else "Risk")
    return "Watch"


def build_ratio(category: str, name: str, value: float | None, display: str, interpretation: str, missing: str | None, source: str) -> dict[str, Any]:
    if value is None:
        return {
            "category": category,
            "name": name,
            "value": None,
            "display_value": "Unavailable",
            "status": "Unavailable",
            "interpretation": missing or "Unavailable",
            "source": "Unavailable",
            "missing_reason": missing or "Unavailable",
        }
    return {
        "category": category,
        "name": name,
        "value": value,
        "display_value": display,
        "status": status_for_ratio(name, value),
        "interpretation": interpretation,
        "source": source,
        "missing_reason": None,
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


def get_sec_filing_intelligence(ticker: str) -> dict[str, Any]:
    out = {
        "latest_10k": None,
        "latest_10q": None,
        "risk_level": "Unknown",
        "summary": "SEC filing feed unavailable.",
        "findings": [],
        "raw_evidence": [],
    }
    cik = get_cik_for_ticker(ticker)
    if not cik:
        out["summary"] = "CIK not found for ticker."
        return out
    try:
        sub_url = f"https://data.sec.gov/submissions/CIK{cik}.json"
        sub = requests.get(sub_url, headers=SEC_HEADERS, timeout=20).json()
        recent = sub.get("filings", {}).get("recent", {})
        forms = recent.get("form", [])
        dates = recent.get("filingDate", [])
        accs = recent.get("accessionNumber", [])
        docs = recent.get("primaryDocument", [])

        for i, form in enumerate(forms):
            if form not in ("10-K", "10-Q"):
                continue
            date = dates[i] if i < len(dates) else None
            acc = (accs[i] if i < len(accs) else "").replace("-", "")
            doc = docs[i] if i < len(docs) else ""
            filing_url = f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/{acc}/{doc}" if acc and doc else None
            item = {"date": date, "url": filing_url}
            if form == "10-K" and out["latest_10k"] is None:
                out["latest_10k"] = item
            if form == "10-Q" and out["latest_10q"] is None:
                out["latest_10q"] = item
            if out["latest_10k"] and out["latest_10q"]:
                break

        findings = []
        if out["latest_10k"]:
            findings.append({
                "area": "SEC",
                "severity": "Low",
                "finding": "Latest 10-K located.",
                "why_it_matters": "Annual report should be checked for accounting policy updates and risk factors.",
                "evidence_short": f"10-K filing date: {out['latest_10k']['date']}",
                "source": "SEC Filing Text",
            })
        if out["latest_10q"]:
            findings.append({
                "area": "SEC",
                "severity": "Low",
                "finding": "Latest 10-Q located.",
                "why_it_matters": "Quarterly report can confirm whether trends are improving or deteriorating.",
                "evidence_short": f"10-Q filing date: {out['latest_10q']['date']}",
                "source": "SEC Filing Text",
            })

        evidence = []
        for f in findings:
            ev = f["evidence_short"][:250]
            if ev not in evidence:
                evidence.append(ev)

        out["findings"] = findings
        out["raw_evidence"] = evidence
        out["risk_level"] = "Low" if findings else "Unknown"
        out["summary"] = "SEC filing links available for direct analyst review." if findings else "No recent 10-K/10-Q identified in submissions feed."
    except Exception:
        out["summary"] = "SEC submissions endpoint unavailable."
    return out


def build_executive_verdict(core_ratios: list[dict[str, Any]], modules: dict[str, Any]) -> dict[str, Any]:
    unavailable = sum(1 for r in core_ratios if r["status"] == "Unavailable")
    cfo_ni = next((r for r in core_ratios if r["name"] == "CFO / Net Income"), None)
    debt_cfo = next((r for r in core_ratios if r["name"] == "Debt / CFO"), None)

    if unavailable >= 7:
        return {
            "forensic_view": "INCONCLUSIVE",
            "risk_level": "Unknown",
            "confidence": "Low",
            "summary": "Data gaps are too large for a reliable forensic conclusion.",
            "main_reasons": ["Multiple core ratios unavailable."],
            "main_risks": ["Accounting quality and solvency signals cannot be verified."],
        }

    cfo_ni_v = cfo_ni.get("value") if cfo_ni else None
    debt_v = debt_cfo.get("value") if debt_cfo else None

    if (cfo_ni_v is not None and cfo_ni_v < 1.0) and (debt_v is not None and debt_v > 6.0):
        view, risk = "AVOID", "High"
    elif (cfo_ni_v is not None and cfo_ni_v >= 1.1) and (debt_v is not None and debt_v <= 3.0):
        view, risk = "BUY", "Low"
    else:
        view, risk = "HOLD", "Medium"

    return {
        "forensic_view": view,
        "risk_level": risk,
        "confidence": "Medium" if unavailable < 4 else "Low",
        "summary": "Risk-based forensic view only, not investment advice.",
        "main_reasons": [f"CFO/NI: {cfo_ni.get('display_value', 'Unavailable')}", f"Debt/CFO: {debt_cfo.get('display_value', 'Unavailable')}"],
        "main_risks": modules.get("debt_flags", [])[:2] + modules.get("cash_flags", [])[:2],
    }


def analyze_ticker(ticker: str, period: str) -> dict[str, Any]:
    t = yf.Ticker(ticker)
    fin, cf, bs = t.financials, t.cashflow, t.balance_sheet

    revenue, _ = get_row_latest(fin, ["Total Revenue"])
    ni, _ = get_row_latest(fin, ["Net Income"])
    cfo, _ = get_row_latest(cf, ["Operating Cash Flow"])
    capex, _ = get_row_latest(cf, ["Capital Expenditure"])
    debt, _ = get_row_latest(bs, ["Total Debt"])
    ebit, _ = get_row_latest(fin, ["EBIT"])
    int_exp, _ = get_row_latest(fin, ["Interest Expense"])
    ar, _ = get_row_latest(bs, ["Accounts Receivable"])
    inv, _ = get_row_latest(bs, ["Inventory"])
    buybacks, _ = get_row_latest(cf, ["Repurchase Of Capital Stock"])
    dividends, _ = get_row_latest(cf, ["Cash Dividends Paid"])
    other_income, _ = get_row_latest(fin, ["Other Non Operating Income Expenses"])
    pretax, _ = get_row_latest(fin, ["Pretax Income", "Earnings Before Tax"])
    tax_expense, _ = get_row_latest(fin, ["Tax Provision", "Income Tax Expense"])
    cash_taxes, _ = get_row_latest(cf, ["Income Tax Paid Supplemental Data"])

    fcf = safe_sub(cfo, abs(capex) if capex is not None else None)
    accrual = safe_div((ni - cfo) if ni is not None and cfo is not None else None, abs(ni) if ni is not None else None)

    core_ratios = [
        build_ratio("Cash Flow", "CFO / Net Income", safe_div(cfo, ni), f"{safe_div(cfo, ni):.2f}x" if safe_div(cfo, ni) is not None else "", "Operating cash flow relative to reported earnings.", "Unavailable: CFO or Net Income missing", "Derived: yfinance"),
        build_ratio("Cash Flow", "CFO Margin", safe_div(cfo, revenue), f"{safe_div(cfo, revenue)*100:.1f}%" if safe_div(cfo, revenue) is not None else "", "Operating cash generation per revenue dollar.", "Unavailable: CFO or Revenue missing", "Derived: yfinance"),
        build_ratio("Cash Flow", "FCF", fcf, fmt_money(fcf), "Free cash flow after capital expenditure.", "Unavailable: CFO or CapEx missing", "Derived: yfinance"),
        build_ratio("Cash Flow", "FCF Margin", safe_div(fcf, revenue), f"{safe_div(fcf, revenue)*100:.1f}%" if safe_div(fcf, revenue) is not None else "", "FCF as a share of revenue.", "Unavailable: FCF or Revenue missing", "Derived: yfinance"),
        build_ratio("Cash Flow", "Accrual Ratio", accrual, f"{accrual:.2f}" if accrual is not None else "", "Higher values can imply weaker earnings quality.", "Unavailable: Net Income or CFO missing", "Derived: yfinance"),
        build_ratio("Tax", "ETR", safe_div(tax_expense, pretax), f"{safe_div(tax_expense, pretax)*100:.1f}%" if safe_div(tax_expense, pretax) is not None else "", "Effective tax rate from book tax expense.", "Unavailable: Tax expense or Pretax income missing", "Derived: yfinance"),
        build_ratio("Debt", "Debt / CFO", safe_div(debt, cfo), f"{safe_div(debt, cfo):.2f}x" if safe_div(debt, cfo) is not None else "", "Years of CFO needed to cover debt.", "Unavailable: Debt or CFO missing", "Derived: yfinance"),
        build_ratio("Debt", "Interest Coverage", safe_div(ebit, abs(int_exp) if int_exp is not None else None), f"{safe_div(ebit, abs(int_exp) if int_exp is not None else None):.2f}x" if safe_div(ebit, abs(int_exp) if int_exp is not None else None) is not None else "", "EBIT ability to cover interest.", "Unavailable: EBIT or Interest expense missing", "Derived: yfinance"),
        build_ratio("Debt", "Cash Interest Coverage", safe_div(cfo, abs(int_exp) if int_exp is not None else None), f"{safe_div(cfo, abs(int_exp) if int_exp is not None else None):.2f}x" if safe_div(cfo, abs(int_exp) if int_exp is not None else None) is not None else "", "CFO ability to cover cash interest burden.", "Unavailable: CFO or Interest expense missing", "Derived: yfinance"),
        build_ratio("Working Capital", "AR / Revenue", safe_div(ar, revenue), f"{safe_div(ar, revenue)*100:.1f}%" if safe_div(ar, revenue) is not None else "", "Receivables intensity versus revenue.", "Unavailable: AR or Revenue missing", "Derived: yfinance"),
        build_ratio("Working Capital", "Inventory / Revenue", safe_div(inv, revenue), f"{safe_div(inv, revenue)*100:.1f}%" if safe_div(inv, revenue) is not None else "", "Inventory load versus revenue.", "Unavailable: Inventory or Revenue missing", "Derived: yfinance"),
        build_ratio("Capital Allocation", "Buybacks / CFO", safe_div(abs(buybacks) if buybacks is not None else None, cfo), f"{safe_div(abs(buybacks) if buybacks is not None else None, cfo)*100:.1f}%" if safe_div(abs(buybacks) if buybacks is not None else None, cfo) is not None else "", "Share repurchases relative to operating cash.", "Unavailable: Buybacks or CFO missing", "Derived: yfinance"),
        build_ratio("Capital Allocation", "Dividends / Net Income", safe_div(abs(dividends) if dividends is not None else None, ni), f"{safe_div(abs(dividends) if dividends is not None else None, ni)*100:.1f}%" if safe_div(abs(dividends) if dividends is not None else None, ni) is not None else "", "Dividend payout relative to earnings.", "Unavailable: Dividends or Net Income missing", "Derived: yfinance"),
        build_ratio("Tax", "Other Income / Net Income", safe_div(other_income, ni), f"{safe_div(other_income, ni)*100:.1f}%" if safe_div(other_income, ni) is not None else "", "Non-operating impact on earnings.", "Unavailable: Other income or Net Income missing", "Derived: yfinance"),
    ]

    cash_flags = [
        "CFO/NI < 1" if safe_div(cfo, ni) is not None and safe_div(cfo, ni) < 1 else None,
        "FCF negative" if fcf is not None and fcf < 0 else None,
        "Accrual ratio high" if accrual is not None and accrual > 0.2 else None,
        "CFO weak vs NI" if cfo is not None and ni is not None and cfo < ni else None,
    ]
    cash_flags = [f for f in cash_flags if f]

    debt_flags = [
        "Debt/CFO above 6x" if safe_div(debt, cfo) is not None and safe_div(debt, cfo) > 6 else None,
        "Interest coverage below 2x" if safe_div(ebit, abs(int_exp) if int_exp is not None else None) is not None and safe_div(ebit, abs(int_exp) if int_exp is not None else None) < 2 else None,
    ]
    debt_flags = [f for f in debt_flags if f]

    etr = safe_div(tax_expense, pretax)
    tax_flags = ["ETR outside normal range" if etr is not None and (etr < 0.05 or etr > 0.40) else None, "Cash taxes unavailable" if cash_taxes is None else None]
    tax_flags = [f for f in tax_flags if f]

    modules = {"cash_flags": cash_flags, "debt_flags": debt_flags}
    verdict = build_executive_verdict(core_ratios, modules)

    top_attention_points = []
    for ratio in core_ratios:
        if ratio["status"] in ("Risk", "Unavailable") and len(top_attention_points) < 8:
            area = ratio["category"] if ratio["category"] in ["Cash Flow", "Debt", "Tax", "Working Capital"] else "Working Capital"
            top_attention_points.append({
                "severity": "High" if ratio["status"] == "Risk" else "Medium",
                "area": area,
                "point": f"{ratio['name']} = {ratio['display_value']}",
                "why_it_matters": ratio["interpretation"],
                "where_to_check": "Cash flow statement and MD&A" if area == "Cash Flow" else "Footnotes and risk disclosures",
                "source": ratio["source"],
            })

    sec_intel = get_sec_filing_intelligence(ticker)

    return {
        "ticker": ticker.upper(),
        "analysis_window": {"selected": period, "timestamp": datetime.utcnow().isoformat() + "Z"},
        "executive_verdict": verdict,
        "top_attention_points": top_attention_points,
        "core_ratios": core_ratios,
        "cash_flow_analysis": {
            "risk_level": "High" if cash_flags else "Low",
            "summary": "Cash conversion and free cash flow quality assessment.",
            "metrics": {"CFO": fmt_money(cfo), "Net Income": fmt_money(ni), "CFO/NI": next(r for r in core_ratios if r["name"] == "CFO / Net Income")["display_value"], "CapEx": fmt_money(capex), "FCF": fmt_money(fcf), "FCF margin": next(r for r in core_ratios if r["name"] == "FCF Margin")["display_value"], "Accrual ratio": next(r for r in core_ratios if r["name"] == "Accrual Ratio")["display_value"]},
            "flags": cash_flags,
            "chart_rows": [],
        },
        "debt_analysis": {
            "risk_level": "High" if debt_flags else "Low",
            "summary": "Debt service and interest burden check.",
            "metrics": {"total debt": fmt_money(debt), "debt/CFO": next(r for r in core_ratios if r["name"] == "Debt / CFO")["display_value"], "interest coverage": next(r for r in core_ratios if r["name"] == "Interest Coverage")["display_value"], "cash interest coverage": next(r for r in core_ratios if r["name"] == "Cash Interest Coverage")["display_value"]},
            "flags": debt_flags,
            "checklist": ["Review debt maturity schedule.", "Check variable-rate debt sensitivity."],
        },
        "tax_analysis": {
            "risk_level": "Medium" if tax_flags else "Low",
            "summary": "Book tax burden and cash tax visibility check.",
            "metrics": {"pretax income": fmt_money(pretax), "tax expense": fmt_money(tax_expense), "ETR": next(r for r in core_ratios if r["name"] == "ETR")["display_value"], "cash tax rate": f"{safe_div(cash_taxes, pretax)*100:.1f}%" if safe_div(cash_taxes, pretax) is not None else "Unavailable: cash taxes missing"},
            "flags": tax_flags,
            "source": "yfinance" if tax_expense is not None else "Unavailable",
        },
        "sec_filing_intelligence": sec_intel,
        "manual_review_checklist": [
            {"area": "Cash Flow", "item": "Reconcile NI to CFO for non-cash adjustments."},
            {"area": "Debt", "item": "Read debt covenants and maturity wall."},
            {"area": "Tax", "item": "Check valuation allowance and uncertain tax positions."},
            {"area": "SEC", "item": "Review latest 10-K and 10-Q risk factors."},
        ],
        "data_completeness": {"unavailable_ratio_count": sum(1 for r in core_ratios if r["status"] == "Unavailable"), "ratio_count": len(core_ratios)},
    }


HTML = """
<!doctype html><html><head><title>Forensic Dashboard</title>
<style>
body{font-family:Inter,Arial;background:#0c111b;color:#e7edf7;margin:0}.wrap{max-width:1200px;margin:20px auto;padding:0 16px}
.bar,.card{background:#121a27;border:1px solid #2a3a55;border-radius:12px;padding:14px;margin-bottom:14px}
.bar{display:flex;gap:10px;align-items:center}input,select,button,textarea{background:#0d1420;color:#e7edf7;border:1px solid #2a3a55;border-radius:8px;padding:10px}
button{background:#1e40af;cursor:pointer}.grid{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:10px}.badge{padding:3px 8px;border-radius:999px;background:#22324d}
table{width:100%;border-collapse:collapse}th,td{border-bottom:1px solid #2a3a55;padding:8px;text-align:left}.cards{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:8px}
</style></head><body><div class='wrap'>
<div class='bar'><input id='ticker' value='TSLA'/><select id='period'><option value='1y'>1Y</option><option value='3y'>3Y</option><option value='5y' selected>5Y</option></select><button onclick='runAnalyze()'>Analyze</button></div>
<div id='executiveVerdict' class='card'></div><div id='attentionPoints' class='card'></div><div id='coreRatios' class='card'></div><div id='cashFlow' class='card'></div>
<div id='debtRisk' class='card'></div><div id='taxRisk' class='card'></div><div id='secIntel' class='card'></div><div id='manualChecklist' class='card'></div><div id='screener' class='card'></div><div id='notes' class='card'></div>
</div><script>
function byId(id){return document.getElementById(id)}
function safeSet(id,html){const el=byId(id); if(!el){console.warn('Missing DOM element:',id); return;} el.innerHTML=html??''}
let ratioFilter='All';
function filteredRows(rows){return rows.filter(r=>ratioFilter==='All'||r.status===ratioFilter||r.category===ratioFilter)}
function renderRatios(rows){
 const filters=['All','Risk','Watch','Unavailable','Cash Flow','Debt','Tax'];
 const btns=filters.map(f=>`<button onclick="ratioFilter='${f}';renderCore(window.lastData)">${f}</button>`).join(' ');
 const body=filteredRows(rows).map(r=>`<tr><td>${r.category}</td><td>${r.name}</td><td>${r.display_value}</td><td><span class='badge'>${r.status}</span></td><td>${r.interpretation}</td><td>${r.source}</td></tr>`).join('');
 return `<h3>Core Ratios</h3><div>${btns}</div><table><tr><th>Category</th><th>Ratio</th><th>Value</th><th>Status</th><th>Interpretation</th><th>Source</th></tr>${body}</table>`
}
function renderCore(d){safeSet('coreRatios',renderRatios(d.core_ratios||[]))}
async function runAnalyze(){
 const ticker=byId('ticker').value||'TSLA'; const period=byId('period').value||'5y';
 const r=await fetch(`/api/analyze?ticker=${encodeURIComponent(ticker)}&period=${encodeURIComponent(period)}`); const d=await r.json(); window.lastData=d;
 const v=d.executive_verdict||{};
 safeSet('executiveVerdict',`<h2>Executive Verdict</h2><div class='grid'><div><b>${v.forensic_view||'INCONCLUSIVE'}</b> <span class='badge'>Risk: ${v.risk_level||'Unknown'}</span> <span class='badge'>Confidence: ${v.confidence||'Low'}</span></div><div>${v.summary||''}</div></div><div><b>Main reasons:</b> ${(v.main_reasons||[]).join('; ')}</div><div><b>Main risks:</b> ${(v.main_risks||[]).join('; ')}</div>`);
 safeSet('attentionPoints',`<h3>Top Attention Points</h3><div class='cards'>${(d.top_attention_points||[]).slice(0,8).map(p=>`<div class='card'><b>${p.severity} • ${p.area}</b><div>${p.point}</div><div>${p.why_it_matters}</div><div><i>${p.where_to_check}</i></div><div>${p.source}</div></div>`).join('')}</div>`);
 renderCore(d);
 const cf=d.cash_flow_analysis||{}; safeSet('cashFlow',`<h3>Cash Flow Quality</h3><div>${cf.summary||''} <span class='badge'>${cf.risk_level||'Unknown'}</span></div><div class='grid'>${Object.entries(cf.metrics||{}).map(([k,v])=>`<div><b>${k}</b><div>${v}</div></div>`).join('')}</div><div><b>Flags:</b> ${(cf.flags||[]).join('; ')||'None'}</div>`);
 const db=d.debt_analysis||{}; safeSet('debtRisk',`<h3>Debt & Interest Risk</h3><div>${db.summary||''} <span class='badge'>${db.risk_level||'Unknown'}</span></div><div class='grid'>${Object.entries(db.metrics||{}).map(([k,v])=>`<div><b>${k}</b><div>${v}</div></div>`).join('')}</div><div><b>Flags:</b> ${(db.flags||[]).join('; ')||'None'}</div>`);
 const tx=d.tax_analysis||{}; safeSet('taxRisk',`<h3>Tax / Book-vs-Tax</h3><div>${tx.summary||''} <span class='badge'>${tx.risk_level||'Unknown'}</span></div><div class='grid'>${Object.entries(tx.metrics||{}).map(([k,v])=>`<div><b>${k}</b><div>${v}</div></div>`).join('')}</div><div><b>Flags:</b> ${(tx.flags||[]).join('; ')||'None'}</div><div>Source: ${tx.source||'Unavailable'}</div>`);
 const si=d.sec_filing_intelligence||{};
 safeSet('secIntel',`<h3>SEC Filing Intelligence</h3><div>10-K: ${si.latest_10k?.url?`<a target='_blank' href='${si.latest_10k.url}'>${si.latest_10k.date}</a>`:'Unavailable'}</div><div>10-Q: ${si.latest_10q?.url?`<a target='_blank' href='${si.latest_10q.url}'>${si.latest_10q.date}</a>`:'Unavailable'}</div><div>${si.summary||''}</div><ul>${(si.findings||[]).map(f=>`<li><b>${f.severity}</b> ${f.area}: ${f.finding} — ${f.why_it_matters} <i>${f.source}</i></li>`).join('')}</ul>${(si.raw_evidence||[]).map(e=>`<details><summary>Raw evidence</summary><div>${e}</div></details>`).join('')}`);
 const grouped=(d.manual_review_checklist||[]).reduce((a,x)=>{(a[x.area]=a[x.area]||[]).push(x.item);return a},{});
 safeSet('manualChecklist',`<h3>Manual Review Checklist</h3>${Object.entries(grouped).map(([k,v])=>`<div><b>${k}</b><ul>${v.map(i=>`<li>${i}</li>`).join('')}</ul></div>`).join('')}`);
 safeSet('screener',`<h3>Screener</h3><table><tr><th>Ticker</th><th>Forensic View</th><th>Risk</th></tr><tr><td>${d.ticker}</td><td>${v.forensic_view||'INCONCLUSIVE'}</td><td>${v.risk_level||'Unknown'}</td></tr></table>`);
 const key='notes_'+ticker.toUpperCase();
 safeSet('notes',`<h3>Notes</h3><textarea id='notesBox' rows='6' style='width:100%'></textarea><div><button onclick='saveNotes()'>Save Notes</button></div>`);
 byId('notesBox').value=localStorage.getItem(key)||''; window.currentNotesKey=key;
}
function saveNotes(){const box=byId('notesBox'); if(box&&window.currentNotesKey){localStorage.setItem(window.currentNotesKey,box.value)}}
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


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5000)
