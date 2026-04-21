
from __future__ import annotations

import json
import os
import re
import ssl
import sqlite3
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Any

import pandas as pd
import requests
import yfinance as yf
from bs4 import BeautifulSoup
from flask import Flask, Response, jsonify, render_template_string, request

app = Flask(__name__)

USER_AGENT = "Vincenzo Iacovone vincenzo@email.com"
SEC_HEADERS = {
    "User-Agent": USER_AGENT,
    "Accept-Encoding": "gzip, deflate",
    "Host": "data.sec.gov",
}
DB_PATH = Path("forensic_dashboard.db")
DEFAULT_WATCHLIST = ["MRK", "MSFT", "JPM", "TSLA", "PFE", "JNJ", "AAPL", "GOOGL", "AMZN", "NVDA", "UNH", "ABBV"]
CORE_SCREENER_UNIVERSE = DEFAULT_WATCHLIST + [
    "META", "AVGO", "AMD", "BAC", "WMT", "COST", "XOM", "CVX", "NFLX", "CRM",
    "ORCL", "LLY", "ADBE", "TMO", "AMGN", "LIN", "QCOM", "TXN", "HON", "COP"
]
_SP500_CACHE: list[str] | None = None
FRED_API_KEY = os.getenv("FRED_API_KEY", "").strip()

APP_HTML = r"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Forensic Dashboard</title>
  <script src="https://cdn.plot.ly/plotly-2.35.2.min.js"></script>
  <style>
    :root {
      --bg: #050816;
      --bg2: #0a1022;
      --text: #edf2ff;
      --muted: #9fb0d0;
      --accent: #7dd3fc;
      --accent2: #a78bfa;
      --ok: #34d399;
      --warn: #fbbf24;
      --bad: #f87171;
      --border: rgba(255,255,255,0.09);
      --shadow: 0 24px 60px rgba(0,0,0,0.42);
      --shadow-soft: 0 12px 32px rgba(0,0,0,0.22);
    }
    * { box-sizing: border-box; }
    html { scroll-behavior: smooth; }
    body {
      margin: 0;
      background:
        radial-gradient(900px 520px at 0% -10%, rgba(125,211,252,0.17), transparent 58%),
        radial-gradient(1000px 560px at 100% 0%, rgba(167,139,250,0.14), transparent 55%),
        radial-gradient(800px 420px at 50% 100%, rgba(34,211,238,0.08), transparent 50%),
        linear-gradient(180deg, var(--bg) 0%, var(--bg2) 100%);
      color: var(--text);
      font-family: Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
    }
    .container { width: min(1620px, calc(100vw - 28px)); margin: 0 auto; padding: 18px 0 42px; }
    .topbar {
      position: sticky;
      top: 10px;
      z-index: 20;
      display: grid;
      grid-template-columns: 1.25fr 150px 190px repeat(5, 150px);
      gap: 12px;
      align-items: center;
      margin-bottom: 18px;
      padding: 10px;
      border-radius: 24px;
      background: rgba(7, 12, 26, 0.74);
      border: 1px solid rgba(255,255,255,0.08);
      backdrop-filter: blur(14px);
      box-shadow: var(--shadow-soft);
    }
    .input, .button, .textarea, .select {
      background: rgba(255,255,255,0.04);
      color: var(--text);
      border: 1px solid var(--border);
      border-radius: 18px;
      padding: 14px 16px;
      outline: none;
      width: 100%;
      backdrop-filter: blur(8px);
      transition: border-color .18s ease, transform .18s ease, box-shadow .18s ease, background .18s ease;
    }
    .input:focus, .select:focus, .textarea:focus {
      border-color: rgba(125,211,252,0.45);
      box-shadow: 0 0 0 4px rgba(125,211,252,0.08);
      background: rgba(255,255,255,0.05);
    }
    .button {
      cursor: pointer;
      font-weight: 800;
      letter-spacing: .01em;
      background: linear-gradient(135deg, rgba(125,211,252,0.18), rgba(167,139,250,0.18));
      box-shadow: var(--shadow-soft);
    }
    .button:hover { transform: translateY(-2px); box-shadow: 0 24px 56px rgba(0,0,0,0.45); }
    .panel {
      background: linear-gradient(180deg, rgba(255,255,255,0.055), rgba(255,255,255,0.025));
      border: 1px solid var(--border);
      border-radius: 28px;
      padding: 18px;
      box-shadow: var(--shadow);
      backdrop-filter: blur(12px);
      position: relative;
      overflow: hidden;
      transition: transform .18s ease, border-color .18s ease;
    }
    .panel:hover { transform: translateY(-2px); border-color: rgba(125,211,252,0.16); }
    .panel::before {
      content: "";
      position: absolute;
      inset: 0;
      background: linear-gradient(180deg, rgba(255,255,255,0.04), transparent 34%);
      pointer-events: none;
    }
    .hero { display: grid; grid-template-columns: 1.08fr .92fr; gap: 18px; margin-bottom: 18px; }
    .hero-left {
      padding: 26px;
      background:
        radial-gradient(700px 260px at 10% 0%, rgba(125,211,252,0.14), transparent 65%),
        linear-gradient(135deg, rgba(125,211,252,0.14), rgba(167,139,250,0.12)),
        linear-gradient(180deg, rgba(255,255,255,0.06), rgba(255,255,255,0.03));
    }
    .title { display: flex; align-items: center; justify-content: space-between; gap: 12px; }
    .title h1 { margin: 0; font-size: 40px; letter-spacing: -.035em; }
    .subtitle { color: var(--muted); margin-top: 10px; line-height: 1.5; max-width: 92%; }
    .badge { display: inline-flex; align-items: center; gap: 10px; padding: 10px 14px; border-radius: 999px; font-size: 13px; font-weight: 800; border: 1px solid var(--border); background: rgba(255,255,255,0.06); }
    .dot { width: 10px; height: 10px; border-radius: 999px; display: inline-block; box-shadow: 0 0 14px currentColor; }
    .dot.ok { background: var(--ok); color: var(--ok); }
    .dot.warn { background: var(--warn); color: var(--warn); }
    .dot.bad { background: var(--bad); color: var(--bad); }
    .stats { display: grid; grid-template-columns: repeat(5, 1fr); gap: 12px; margin-top: 20px; }
    .stat { border: 1px solid var(--border); background: linear-gradient(180deg, rgba(255,255,255,0.05), rgba(255,255,255,0.025)); border-radius: 22px; padding: 16px; }
    .stat .label { color: var(--muted); font-size: 12px; text-transform: uppercase; letter-spacing: .08em; }
    .stat .value { font-size: 29px; font-weight: 900; margin-top: 8px; letter-spacing: -.03em; }
    .metric-big { font-size: 30px; font-weight: 900; margin-top: 8px; }
    .hero-screener { display: grid; grid-template-columns: 1fr 1fr 1fr; gap: 12px; }
    .grid { display: grid; grid-template-columns: repeat(12, 1fr); gap: 18px; margin-top: 18px; }
    .span-12 { grid-column: span 12; } .span-8 { grid-column: span 8; } .span-6 { grid-column: span 6; } .span-4 { grid-column: span 4; } .span-3 { grid-column: span 3; }
    .section-title { display: flex; align-items: center; justify-content: space-between; margin-bottom: 12px; gap: 10px; }
    .section-title h2 { margin: 0; font-size: 20px; letter-spacing: -.02em; }
    .muted { color: var(--muted); }
    .flags { display: grid; gap: 10px; }
    .flag { border-radius: 18px; padding: 13px 15px; border: 1px solid var(--border); background: rgba(255,255,255,0.04); }
    .flag.bad { border-color: rgba(248,113,113,.25); background: rgba(248,113,113,.10); }
    .flag.warn { border-color: rgba(251,191,36,.25); background: rgba(251,191,36,.10); }
    .flag.ok { border-color: rgba(52,211,153,.25); background: rgba(52,211,153,.10); }
    .flag .t { font-weight: 800; margin-bottom: 4px; }
    .heat-grid { display: grid; grid-template-columns: repeat(6, 1fr); gap: 12px; }
    .heat-card { padding: 14px; border-radius: 20px; border: 1px solid var(--border); background: linear-gradient(180deg, rgba(255,255,255,0.05), rgba(255,255,255,0.025)); }
    .heat-label { color: var(--muted); font-size: 12px; text-transform: uppercase; letter-spacing: .07em; margin-bottom: 10px; }
    .heat-pill { display: inline-flex; align-items: center; justify-content: center; min-width: 92px; padding: 10px 14px; border-radius: 999px; font-weight: 900; border: 1px solid var(--border); background: rgba(255,255,255,0.05); }
    .heat-pill.ok { background: rgba(52,211,153,0.14); border-color: rgba(52,211,153,0.25); color: var(--ok); }
    .heat-pill.warn { background: rgba(251,191,36,0.14); border-color: rgba(251,191,36,0.25); color: var(--warn); }
    .heat-pill.bad { background: rgba(248,113,113,0.14); border-color: rgba(248,113,113,0.25); color: var(--bad); }
    .heat-pill.neutral { background: rgba(125,211,252,0.12); border-color: rgba(125,211,252,0.22); color: var(--accent); }
    table { width: 100%; border-collapse: collapse; border-radius: 18px; overflow: hidden; font-size: 14px; }
    th, td { padding: 12px 10px; border-bottom: 1px solid var(--border); text-align: left; vertical-align: top; }
    th { color: var(--muted); font-weight: 800; font-size: 12px; text-transform: uppercase; letter-spacing: .06em; background: rgba(255,255,255,0.02); }
    tr:last-child td { border-bottom: 0; }
    tbody tr:hover td { background: rgba(255,255,255,0.025); }
    .clickable-row { cursor: pointer; }
    .clickable-row:hover td { background: rgba(125,211,252,0.06) !important; }
    a { color: var(--accent); text-decoration: none; }
    a:hover { text-decoration: underline; }
    .pos { color: var(--ok); font-weight: 800; }
    .neg { color: var(--bad); font-weight: 800; }
    .score-glow { text-shadow: 0 0 14px rgba(125,211,252,0.18); }
    .loading { display: none; font-size: 14px; color: var(--muted); margin-left: 10px; }
    .footer-note { margin-top: 12px; color: var(--muted); font-size: 12px; }
    .note-box { min-height: 180px; }
    .small { font-size: 12px; }
    .chips { display: flex; flex-wrap: wrap; gap: 8px; }
    .chip { border: 1px solid var(--border); padding: 9px 11px; border-radius: 999px; font-size: 12px; color: var(--muted); background: rgba(255,255,255,0.04); }
    .evidence-wall { display: grid; grid-template-columns: 1fr 1fr; gap: 12px; }
    .evidence-card { border: 1px solid var(--border); border-radius: 18px; padding: 12px; background: rgba(255,255,255,0.04); }
    .evidence-kicker { font-size: 11px; text-transform: uppercase; letter-spacing: .08em; color: var(--muted); margin-bottom: 8px; }
    .evidence-title { font-weight: 800; margin-bottom: 6px; }
    .evidence-copy { color: var(--muted); font-size: 13px; line-height: 1.4; }
    .evidence-source { margin-top: 10px; color: var(--muted); font-size: 12px; }
    .geo-cards { display:grid; grid-template-columns:repeat(4,1fr); gap:10px; margin-top:10px; }
    .geo-card { border:1px solid var(--border); border-radius:16px; padding:12px; background:rgba(255,255,255,0.03); }
    .geo-kicker { font-size:11px; letter-spacing:.08em; text-transform:uppercase; color:var(--muted); margin-bottom:8px; }
    .geo-value { font-size:20px; font-weight:900; letter-spacing:-.02em; }
    .geo-note { color:var(--muted); font-size:12px; margin-top:6px; line-height:1.3; }
    .geo-diagnostic { border:1px solid rgba(251,191,36,.35); background:rgba(251,191,36,.10); border-radius:16px; padding:12px; margin-top:10px; }
    .geo-diagnostic h3 { margin:0 0 8px; font-size:16px; }
    .geo-diagnostic ul { margin:0; padding-left:18px; color:var(--muted); }
    .geo-interp { display:grid; grid-template-columns:repeat(2,1fr); gap:10px; }
    .geo-interp .flag { padding:12px 13px; border-radius:14px; }
    @media (max-width: 1380px) {
      .topbar { grid-template-columns: 1fr 1fr 1fr; }
      .hero { grid-template-columns: 1fr; }
      .stats { grid-template-columns: repeat(2, 1fr); }
      .heat-grid { grid-template-columns: repeat(3, 1fr); }
      .hero-screener { grid-template-columns: 1fr; }
      .evidence-wall { grid-template-columns: 1fr; }
      .geo-cards { grid-template-columns: repeat(2,1fr); }
      .geo-interp { grid-template-columns: 1fr; }
      .span-8, .span-6, .span-4, .span-3 { grid-column: span 12; }
    }
    @media (max-width: 720px) {
      .stats { grid-template-columns: 1fr; }
      .title { flex-direction: column; align-items: flex-start; }
      .title h1 { font-size: 30px; }
      .subtitle { max-width: 100%; }
      .heat-grid { grid-template-columns: 1fr 1fr; }
    }
  </style>
</head>
<body>
  <div class="container">
    <div class="topbar">
      <input id="tickerInput" class="input" value="MRK" placeholder="Enter ticker, e.g. MRK, MSFT, JPM" />
      <select id="periodSelect" class="select"><option value="1y" selected>1Y</option><option value="2y">2Y</option><option value="5y">5Y</option></select>
      <select id="universeSelect" class="select"><option value="core" selected>Core universe</option><option value="watchlist">Watchlist only</option><option value="sp100">Large caps</option></select>
      <button id="analyzeBtn" class="button">Run analysis</button>
      <button id="screenerBtn" class="button">Run screener</button>
      <button id="saveBtn" class="button">Save analysis</button>
      <button id="copyBtn" class="button">Copy notes</button>
      <button id="exportSavedBtn" class="button">Export saved CSV</button>
      <button id="exportWatchlistBtn" class="button">Export watchlist CSV</button>
      <button id="macroBtn" class="button" onclick="window.open('/macro', '_blank')">Global macro dashboard</button>
    </div>

    <div class="panel" style="margin-bottom:18px; padding:16px 18px;">
      <div class="section-title"><h2>Risk heatmap</h2><span class="muted">Fast anomaly radar</span></div>
      <div class="heat-grid">
        <div class="heat-card"><div class="heat-label">Cash conversion</div><div class="heat-pill" id="heatCfoNi">-</div></div>
        <div class="heat-card"><div class="heat-label">Beneish</div><div class="heat-pill" id="heatBeneish">-</div></div>
        <div class="heat-card"><div class="heat-label">Receivables</div><div class="heat-pill" id="heatDsri">-</div></div>
        <div class="heat-card"><div class="heat-label">Acquisitions</div><div class="heat-pill" id="heatAcq">-</div></div>
        <div class="heat-card"><div class="heat-label">Free cash flow</div><div class="heat-pill" id="heatFcf">-</div></div>
        <div class="heat-card"><div class="heat-label">Overall</div><div class="heat-pill" id="heatOverall">-</div></div>
      </div>
    </div>

    <div class="hero">
      <div class="panel hero-left">
        <div class="title">
          <div>
            <h1 id="companyTitle">Forensic Dashboard</h1>
            <div class="subtitle" id="companySubtitle">Quantitative and qualitative workflow for 10-K analysis</div>
          </div>
          <div class="badge" id="riskBadge"><span class="dot warn"></span><span>Awaiting analysis</span></div>
        </div>
        <div class="stats">
          <div class="stat"><div class="label">Price</div><div class="value" id="priceValue">-</div></div>
          <div class="stat"><div class="label">Market Cap</div><div class="value" id="mcapValue">-</div></div>
          <div class="stat"><div class="label">CFO / Net Income</div><div class="value score-glow" id="cfoNiValue">-</div></div>
          <div class="stat"><div class="label">Beneish M</div><div class="value" id="beneishValue">-</div></div>
          <div class="stat"><div class="label">Red Flags</div><div class="value" id="flagsCount">-</div></div>
        </div>
      </div>
      <div class="panel">
        <div class="section-title"><h2>Screener cockpit</h2><span class="loading" id="loadingLabel">Loading...</span></div>
        <div class="hero-screener">
          <div class="stat"><div class="label">Suspicious names</div><div class="metric-big" id="screenCount">-</div></div>
          <div class="stat"><div class="label">Worst score</div><div class="metric-big" id="screenWorst">-</div></div>
          <div class="stat"><div class="label">Most common pattern</div><div class="metric-big" id="screenTheme" style="font-size:18px; line-height:1.25;">-</div></div>
        </div>
        <div class="chips" style="margin-top:12px;">
          <div class="chip">1. Run screener on universe</div><div class="chip">2. Click suspicious ticker</div>
          <div class="chip">3. Open Item 7 and Item 8</div><div class="chip">4. Confirm cash quality</div>
          <div class="chip">5. Save thesis</div>
        </div>
      </div>
    </div>

    <div class="grid">
      <div class="panel span-8"><div class="section-title"><h2>Suspicious companies screener</h2><span class="muted">Click a row to load full analysis</span></div><div style="overflow:auto;"><table><thead><tr><th>Rank</th><th>Ticker</th><th>Score</th><th>Quality</th><th>Red flags</th><th>Risk</th><th>CFO/NI</th><th>Beneish</th><th>DSRI</th><th>Reason</th></tr></thead><tbody id="screenerBody"></tbody></table></div></div>
      <div class="panel span-4"><div class="section-title"><h2>10-K access</h2><span class="muted">Fast navigation</span></div><div id="filingsBox" class="flags"></div></div>

      <div class="panel span-8"><div class="section-title"><h2>Price & trend</h2><span class="muted">Market view</span></div><div id="priceChart" style="height:360px;"></div></div>
      <div class="panel span-4"><div class="section-title"><h2>Forensic summary</h2><span class="muted">Instant triage</span></div><div class="flags" id="insightContainer"></div><div class="flags" id="flagsContainer"></div></div>

      <div class="panel span-6"><div class="section-title"><h2>Cash flow breakdown</h2><span class="muted">Operating cash quality</span></div><div style="overflow:auto;"><table><thead><tr><th>Period</th><th>CFO</th><th>CapEx</th><th>FCF</th><th>Acquisitions</th><th>CFO/NI</th></tr></thead><tbody id="cashflowBody"></tbody></table></div></div>
      <div class="panel span-3"><div class="section-title"><h2>Acquisition analysis</h2><span class="muted">Roll-up risk</span></div><div style="overflow:auto;"><table><thead><tr><th>Metric</th><th>Value</th><th>Comment</th></tr></thead><tbody id="acqBody"></tbody></table></div></div>
      <div class="panel span-3"><div class="section-title"><h2>Cash flow red flags</h2><span class="muted">Automatic alerts</span></div><div class="flags" id="cashFlagsBox"></div></div>

      <div class="panel span-4"><div class="section-title"><h2>Working capital</h2><span class="muted">Balance sheet pressure</span></div><div style="overflow:auto;"><table><thead><tr><th>Period</th><th>AR</th><th>Inventory</th><th>Payables</th><th>AR growth</th><th>Inv growth</th><th>Payables growth</th></tr></thead><tbody id="workingCapitalBody"></tbody></table></div></div>
      <div class="panel span-4"><div class="section-title"><h2>CFO / NI trend</h2><span class="muted">Multi-period cash conversion</span></div><div id="cfoNiTrendChart" style="height:320px;"></div></div>
      <div class="panel span-4"><div class="section-title"><h2>DSRI & FCF trend</h2><span class="muted">Receivables and free cash flow</span></div><div id="dsriFcfTrendChart" style="height:320px;"></div></div>

      <div class="panel span-12"><div class="section-title"><h2>Trend signal board</h2><span class="muted">Automatic deterioration checks</span></div><div class="heat-grid"><div class="heat-card"><div class="heat-label">CFO / NI trend</div><div class="heat-pill" id="trendCfoNi">-</div></div><div class="heat-card"><div class="heat-label">DSRI trend</div><div class="heat-pill" id="trendDsri">-</div></div><div class="heat-card"><div class="heat-label">FCF trend</div><div class="heat-pill" id="trendFcf">-</div></div><div class="heat-card"><div class="heat-label">Trend verdict</div><div class="heat-pill" id="trendOverall">-</div></div><div class="heat-card" style="grid-column: span 2;"><div class="heat-label">Interpretation</div><div class="muted" id="trendNarrative">No trend narrative yet.</div></div></div></div>

      <div class="panel span-12"><div class="section-title"><h2>Fundamental quality table</h2><span class="muted">Latest periods</span></div><div style="overflow:auto;"><table><thead><tr><th>Period</th><th>Revenue</th><th>Net income</th><th>CFO</th><th>CFO/NI</th><th>Accruals</th><th>Revenue growth</th><th>AR growth</th><th>DSRI</th></tr></thead><tbody id="qualityTableBody"></tbody></table></div></div>

      <div class="panel span-4"><div class="section-title"><h2>Macro snapshot</h2><span class="muted">Risk context</span></div><div style="overflow:auto;"><table><thead><tr><th>Series</th><th>Last</th><th>5D change</th><th>Interpretation</th></tr></thead><tbody id="macroTableBody"></tbody></table></div></div>
      <div class="panel span-4"><div class="section-title"><h2>Top gainers</h2><span class="muted">S&P 500 snapshot</span></div><div style="overflow:auto;"><table><thead><tr><th>Ticker</th><th>Price</th><th>1D %</th></tr></thead><tbody id="gainersBody"></tbody></table></div></div>
      <div class="panel span-4"><div class="section-title"><h2>Top losers</h2><span class="muted">S&P 500 snapshot</span></div><div style="overflow:auto;"><table><thead><tr><th>Ticker</th><th>Price</th><th>1D %</th></tr></thead><tbody id="losersBody"></tbody></table></div></div>
      <div class="panel span-12"><div class="section-title"><h2>World news</h2><span class="muted">Company and macro headlines</span></div><div style="overflow:auto;"><table><thead><tr><th>Headline</th><th>Source</th><th>Published</th></tr></thead><tbody id="newsTableBody"></tbody></table></div></div>

      <div class="panel span-6"><div class="section-title"><h2>Forensic scorecard</h2><span class="muted">Key diagnostic metrics</span></div><div style="overflow:auto;"><table><thead><tr><th>Metric</th><th>Value</th><th>Comment</th></tr></thead><tbody id="scorecardBody"></tbody></table></div></div>
      <div class="panel span-6"><div class="section-title"><h2>Reading checklist</h2><span class="muted">Qualitative prompts</span></div><div class="flags" id="checklistBox"></div></div>

      <div class="panel span-4"><div class="section-title"><h2>10-K text signals</h2><span class="muted">Automatic filing scan</span></div><div style="overflow:auto;"><table><thead><tr><th>Signal</th><th>Hits</th><th>Interpretation</th></tr></thead><tbody id="textSignalsBody"></tbody></table></div></div>
      <div class="panel span-4"><div class="section-title"><h2>Item 7 excerpt</h2><span class="muted">MD&A sample</span></div><div class="flags" id="item7Box"></div></div>
      <div class="panel span-4"><div class="section-title"><h2>Item 9A excerpt</h2><span class="muted">Controls sample</span></div><div class="flags" id="item9aBox"></div></div>

      <div class="panel span-8"><div class="section-title"><h2>Non-operating & cost reduction evidence</h2><span class="muted">10-K / 10-Q text-based picture</span></div><div id="evidencePictureChart" style="height:320px;"></div></div>
      <div class="panel span-4"><div class="section-title"><h2>Filing evidence snippets</h2><span class="muted">Primary-text extracts</span></div><div id="evidenceWall" class="evidence-wall"></div></div>

      <div class="panel span-12">
        <div class="section-title"><h2>Geographic & segment intelligence</h2><span class="muted">Table-first SEC extraction + forensic interpretation</span></div>
        <div id="geoSummaryCards" class="geo-cards"></div>
        <div id="geoDiagnosticPanel"></div>
      </div>
      <div class="panel span-6"><div class="section-title"><h2>Revenue by region trend</h2><span class="muted">Multi-year regional revenue</span></div><div id="geoTrendChart" style="height:320px;"></div></div>
      <div class="panel span-6"><div class="section-title"><h2>Regional mix evolution</h2><span class="muted">Stacked concentration profile</span></div><div id="geoMixChart" style="height:320px;"></div></div>
      <div class="panel span-6"><div class="section-title"><h2>Current-period mix donuts</h2><span class="muted">Geography and segments</span></div><div id="geoDonutChart" style="height:280px;"></div><div id="segmentMixChart" style="height:280px; margin-top:12px;"></div></div>
      <div class="panel span-6"><div class="section-title"><h2>Segment revenue trend</h2><span class="muted">Business line trajectory</span></div><div id="segmentTrendChart" style="height:320px;"></div></div>
      <div class="panel span-6"><div class="section-title"><h2>Revenue by region table</h2><span class="muted">SEC disclosure extraction</span></div><div style="overflow:auto;"><table><thead><tr><th>Year</th><th>Region</th><th>Revenue</th><th>Mix</th><th>YoY</th></tr></thead><tbody id="geoRevenueBody"></tbody></table></div></div>
      <div class="panel span-6"><div class="section-title"><h2>Segment revenue table</h2><span class="muted">Business segment disclosure</span></div><div style="overflow:auto;"><table><thead><tr><th>Year</th><th>Segment</th><th>Revenue</th><th>Mix</th><th>YoY</th></tr></thead><tbody id="segmentRevenueBody"></tbody></table></div></div>
      <div class="panel span-12"><div class="section-title"><h2>Geographic and segment interpretation</h2><span class="muted">Dominance, growth leadership, concentration, mismatch checks</span></div><div class="geo-interp" id="geoSegmentSummaryBox"></div></div>

      <div class="panel span-6"><div class="section-title"><h2>10-K excerpts</h2><span class="muted">Keyword context</span></div><div class="flags" id="excerptBox"></div></div>
      <div class="panel span-6"><div class="section-title"><h2>Decision engine</h2><span class="muted">Hedge Fund mode</span></div><div style="overflow:auto;"><table><thead><tr><th>Metric</th><th>Value</th><th>Meaning</th></tr></thead><tbody id="decisionBody"></tbody></table></div></div>
      <div class="panel span-6"><div class="section-title"><h2>Peer comparison</h2><span class="muted">Automatic sector-style peers</span></div><div style="overflow:auto;"><table><thead><tr><th>Ticker</th><th>Price</th><th>Market cap</th><th>CFO/NI</th><th>Beneish</th><th>Flags</th></tr></thead><tbody id="peersBody"></tbody></table></div></div>
      <div class="panel span-6"><div class="section-title"><h2>Cash flow trend</h2><span class="muted">CFO vs FCF vs acquisitions</span></div><div id="cashTrendChart" style="height:320px;"></div></div>
      <div class="panel span-12"><div class="section-title"><h2>Watchlist ranking</h2><span class="muted">Multi-ticker triage</span></div><div style="overflow:auto;"><table><thead><tr><th>Rank</th><th>Ticker</th><th>Score</th><th>Risk</th><th>Verdict</th><th>CFO/NI</th><th>Beneish</th><th>Flags</th></tr></thead><tbody id="watchlistBody"></tbody></table></div></div>

      <div class="panel span-12"><div class="section-title"><h2>Qualitative analysis workspace</h2><span class="muted">Your O'Glove layer</span></div><textarea id="qualNotes" class="textarea note-box" placeholder="Examples: management uses temporary explanations repeatedly; cash conversion weak despite revenue growth; control language is generic; restructuring keeps recurring; AR up faster than sales; unusual non-GAAP reliance."></textarea><div class="footer-note small">Tip: write a brief thesis, 3 concerns, 3 confirming points, and the next pages to inspect.</div></div>

      <div class="panel span-6"><div class="section-title"><h2>Saved analyses</h2><span class="muted">Local SQLite history</span></div><div style="overflow:auto;"><table><thead><tr><th>Saved at</th><th>Ticker</th><th>Risk</th><th>CFO/NI</th><th>Beneish</th><th>Notes</th></tr></thead><tbody id="savedBody"></tbody></table></div></div>
      <div class="panel span-6"><div class="section-title"><h2>Ticker history</h2><span class="muted">Stored history for current ticker</span></div><div style="overflow:auto;"><table><thead><tr><th>Saved at</th><th>Risk</th><th>CFO/NI</th><th>Beneish</th><th>Notes</th></tr></thead><tbody id="tickerHistoryBody"></tbody></table></div></div>
    </div>
  </div>

<script>
  let currentPayload = null;

  function moneyFmt(v) {
    if (v === null || v === undefined || Number.isNaN(v)) return '-';
    const abs = Math.abs(v);
    if (abs >= 1e12) return '$' + (v / 1e12).toFixed(2) + 'T';
    if (abs >= 1e9) return '$' + (v / 1e9).toFixed(2) + 'B';
    if (abs >= 1e6) return '$' + (v / 1e6).toFixed(2) + 'M';
    return '$' + Number(v).toFixed(2);
  }

  function signedMoneyFmt(v) {
    if (v === null || v === undefined || Number.isNaN(v)) return '-';
    const sign = v < 0 ? '-' : '+';
    return sign + moneyFmt(Math.abs(v));
  }

  function numFmt(v, digits = 2) {
    if (v === null || v === undefined || Number.isNaN(v)) return '-';
    return Number(v).toFixed(digits);
  }

  function pctFmt(v) {
    if (v === null || v === undefined || Number.isNaN(v)) return '-';
    return (v * 100).toFixed(1) + '%';
  }

  function setRiskBadge(level, text) {
    const badge = document.getElementById('riskBadge');
    const dotClass = level === 'High' ? 'bad' : (level === 'Medium' ? 'warn' : 'ok');
    badge.innerHTML = `<span class="dot ${dotClass}"></span><span>${text}</span>`;
  }

  function setHeat(id, label, cls) {
    const el = document.getElementById(id);
    if (!el) return;
    el.textContent = label;
    el.className = `heat-pill ${cls}`;
  }

  function renderHeatmap(data) {
    const cfoNi = data.latest_cfo_ni;
    const beneish = data.beneish_m;
    const lastQuality = (data.quality_rows || []).length ? data.quality_rows[data.quality_rows.length - 1] : {};
    const dsri = lastQuality.dsri;
    const cashRows = data.cashflow_rows || [];
    const lastCash = cashRows.length ? cashRows[cashRows.length - 1] : {};
    const fcf = lastCash.fcf;
    const acqRatio = parseFloat((data.acquisition_table || []).find(r => r.metric === 'Acquisitions / CFO')?.value || 'NaN');

    if (cfoNi === null || cfoNi === undefined || Number.isNaN(cfoNi)) setHeat('heatCfoNi', '-', 'neutral');
    else if (cfoNi < 0.8) setHeat('heatCfoNi', 'High risk', 'bad');
    else if (cfoNi < 1.0) setHeat('heatCfoNi', 'Watch', 'warn');
    else setHeat('heatCfoNi', 'Healthy', 'ok');

    if (beneish === null || beneish === undefined || Number.isNaN(beneish)) setHeat('heatBeneish', '-', 'neutral');
    else if (beneish > -1.78) setHeat('heatBeneish', 'High risk', 'bad');
    else if (beneish > -2.2) setHeat('heatBeneish', 'Watch', 'warn');
    else setHeat('heatBeneish', 'Clean', 'ok');

    if (dsri === null || dsri === undefined || Number.isNaN(dsri)) setHeat('heatDsri', '-', 'neutral');
    else if (dsri > 1.15) setHeat('heatDsri', 'High risk', 'bad');
    else if (dsri > 1.0) setHeat('heatDsri', 'Watch', 'warn');
    else setHeat('heatDsri', 'Healthy', 'ok');

    if (Number.isNaN(acqRatio)) setHeat('heatAcq', '-', 'neutral');
    else if (acqRatio > 0.8) setHeat('heatAcq', 'Heavy', 'bad');
    else if (acqRatio > 0.5) setHeat('heatAcq', 'Watch', 'warn');
    else setHeat('heatAcq', 'Light', 'ok');

    if (fcf === null || fcf === undefined || Number.isNaN(fcf)) setHeat('heatFcf', '-', 'neutral');
    else if (fcf < 0) setHeat('heatFcf', 'Negative', 'bad');
    else setHeat('heatFcf', 'Positive', 'ok');

    const overall = data.risk_level || 'Medium';
    if (overall === 'High') setHeat('heatOverall', 'High', 'bad');
    else if (overall === 'Medium') setHeat('heatOverall', 'Medium', 'warn');
    else setHeat('heatOverall', 'Low', 'ok');
  }

  function renderFlags(flags) {
    const el = document.getElementById('flagsContainer');
    if (!flags.length) {
      el.innerHTML = '<div class="flag ok"><div class="t">No critical flags</div><div class="muted">Available data did not trigger major forensic alerts.</div></div>';
      return;
    }
    el.innerHTML = flags.map(f => `<div class="flag ${f.severity.toLowerCase()}"><div class="t">${f.title}</div><div class="muted">${f.detail}</div></div>`).join('');
  }

  function renderInsights(data) {
    const el = document.getElementById('insightContainer');
    if (!el) return;
    const quality = data.earnings_quality_classification || '';
    const cls = quality === 'HIGH QUALITY' ? 'ok' : (quality === 'MODERATE QUALITY' ? 'warn' : 'bad');
    const topFlags = (data.red_flag_highlights || []).slice(0, 3);
    const flagHtml = topFlags.map(f => `<div class="muted small">• ${f.title} (${f.severity}, ${f.persistence})</div>`).join('');
    el.innerHTML = `
      <div class="flag ${cls}">
        <div class="t">${data.earnings_quality_classification || 'QUALITY N/A'}</div>
        <div class="muted">${data.earnings_quality_explanation || data.forensic_summary || 'No explanation available.'}</div>
        ${flagHtml}
      </div>
    `;
  }

  function renderFilings(rows) {
    const el = document.getElementById('filingsBox');
    if (!el) return;
    if (!rows || !rows.length) {
      el.innerHTML = '<div class="flag warn"><div class="t">No SEC filings found</div><div class="muted">Recent 10-K / 10-Q links were not available for this ticker.</div></div>';
      return;
    }
    el.innerHTML = rows.map(r => `
      <div class="flag ok">
        <div class="t">${r.form} - ${r.filing_date}</div>
        <div class="muted">
          <a href="${r.url}" target="_blank">Open filing</a> |
          <a href="${r.index_url}" target="_blank">Index</a> |
          <a href="${r.company_url}" target="_blank">Company filings</a>
        </div>
      </div>`).join('');
  }

  function renderWorkingCapital(rows) {
    document.getElementById('workingCapitalBody').innerHTML = rows.map(r => `<tr><td>${r.period}</td><td>${moneyFmt(r.ar)}</td><td>${moneyFmt(r.inventory)}</td><td>${moneyFmt(r.payables)}</td><td>${pctFmt(r.ar_growth)}</td><td>${pctFmt(r.inventory_growth)}</td><td>${pctFmt(r.payables_growth)}</td></tr>`).join('');
  }

  function renderQualityTable(rows) {
    document.getElementById('qualityTableBody').innerHTML = rows.map(r => `<tr><td>${r.period}</td><td>${moneyFmt(r.revenue)}</td><td>${moneyFmt(r.net_income)}</td><td>${moneyFmt(r.cfo)}</td><td>${numFmt(r.cfo_ni)}</td><td>${signedMoneyFmt(r.accruals)}</td><td>${pctFmt(r.revenue_growth)}</td><td>${pctFmt(r.ar_growth)}</td><td>${numFmt(r.dsri)}</td></tr>`).join('');
  }

  function renderCashflow(rows) {
    document.getElementById('cashflowBody').innerHTML = rows.map(r => `<tr><td>${r.period}</td><td>${moneyFmt(r.cfo)}</td><td class="neg">-${moneyFmt(Math.abs(r.capex))}</td><td class="${(r.fcf ?? 0) >= 0 ? 'pos' : 'neg'}">${signedMoneyFmt(r.fcf)}</td><td class="${(r.acquisitions ?? 0) <= 0 ? 'neg' : 'pos'}">${signedMoneyFmt(r.acquisitions)}</td><td>${numFmt(r.cfo_ni)}</td></tr>`).join('');
  }

  function renderAcqTable(rows) {
    document.getElementById('acqBody').innerHTML = rows.map(r => `<tr><td>${r.metric}</td><td>${r.value}</td><td>${r.comment}</td></tr>`).join('');
  }

  function renderCashFlags(rows) {
    const el = document.getElementById('cashFlagsBox');
    if (!rows || !rows.length) {
      el.innerHTML = '<div class="flag ok"><div class="t">No major cash flow flags</div><div class="muted">Cash flow diagnostics do not show major automatic warnings.</div></div>';
      return;
    }
    el.innerHTML = rows.map(r => `<div class="flag ${r.severity.toLowerCase()}"><div class="t">${r.title}</div><div class="muted">${r.detail}</div></div>`).join('');
  }

  function renderMacro(rows) {
    document.getElementById('macroTableBody').innerHTML = rows.map(r => `<tr><td>${r.name}</td><td>${numFmt(r.last)}</td><td class="${(r.change_5d ?? 0) >= 0 ? 'pos' : 'neg'}">${r.change_5d === null ? '-' : numFmt(r.change_5d) + '%'}</td><td>${r.interpretation}</td></tr>`).join('');
  }

  function renderMovers(elId, rows) {
    document.getElementById(elId).innerHTML = rows.map(r => `<tr><td>${r.ticker}</td><td>${moneyFmt(r.price)}</td><td class="${r.change_pct >= 0 ? 'pos' : 'neg'}">${numFmt(r.change_pct)}%</td></tr>`).join('');
  }

  function renderNews(rows) {
    document.getElementById('newsTableBody').innerHTML = rows.map(r => `<tr><td><a href="${r.link}" target="_blank">${r.title}</a></td><td>${r.source || '-'}</td><td>${r.published || '-'}</td></tr>`).join('');
  }

  function renderScorecard(rows) {
    document.getElementById('scorecardBody').innerHTML = rows.map(r => `<tr><td>${r.metric}</td><td>${r.value}</td><td>${r.comment}</td></tr>`).join('');
  }

  function renderChecklist(rows) {
    document.getElementById('checklistBox').innerHTML = rows.map(r => `<div class="flag warn"><div class="t">${r.title}</div><div class="muted">${r.detail}</div></div>`).join('');
  }

  function renderTextSignals(rows) {
    document.getElementById('textSignalsBody').innerHTML = rows.map(r => `<tr><td>${r.signal}</td><td>${r.hits}</td><td>${r.interpretation}</td></tr>`).join('');
  }

  function renderExcerpts(rows) {
    const el = document.getElementById('excerptBox');
    if (!rows || !rows.length) {
      el.innerHTML = '<div class="flag ok"><div class="t">No excerpts found</div><div class="muted">No monitored keyword excerpt was captured from the latest filing.</div></div>';
      return;
    }
    el.innerHTML = rows.map(r => `<div class="flag ${r.severity.toLowerCase()}"><div class="t">${r.keyword}</div><div class="muted">${r.excerpt}</div></div>`).join('');
  }

  function renderSingleExcerpt(elId, title, excerpt) {
    const el = document.getElementById(elId);
    if (!excerpt) {
      el.innerHTML = `<div class="flag warn"><div class="t">${title}</div><div class="muted">Section not extracted from filing text.</div></div>`;
      return;
    }
    el.innerHTML = `<div class="flag ok"><div class="t">${title}</div><div class="muted">${excerpt}</div></div>`;
  }

  function renderDecision(rows) {
    document.getElementById('decisionBody').innerHTML = rows.map(r => `<tr><td>${r.metric}</td><td>${r.value}</td><td>${r.comment}</td></tr>`).join('');
  }

  function renderEvidenceBoard(evidence) {
    const wall = document.getElementById('evidenceWall');
    if (!wall) return;
    if (!evidence) {
      wall.innerHTML = '<div class="evidence-card"><div class="evidence-title">No filing evidence available</div><div class="evidence-copy">Run analysis to extract non-operating and cost reduction language from filings.</div></div>';
      return;
    }
    const source = evidence.source || {};
    const sourceHtml = source.url ? `<a href="${source.url}" target="_blank">${source.form || 'Filing'} ${source.filing_date || ''}</a>` : `${source.form || 'Filing source unavailable'}`;
    const nonOp = (evidence.non_operating_evidence || [])[0];
    const cost = (evidence.cost_reduction_evidence || [])[0];
    wall.innerHTML = `
      <div class="evidence-card">
        <div class="evidence-kicker">Non-operating / non-recurring</div>
        <div class="evidence-title">${nonOp ? nonOp.keyword : 'No explicit trigger found'}</div>
        <div class="evidence-copy">${nonOp ? nonOp.excerpt : 'No non-operating / non-recurring wording was automatically extracted from the latest filing text.'}</div>
      </div>
      <div class="evidence-card">
        <div class="evidence-kicker">Cost reduction / efficiency</div>
        <div class="evidence-title">${cost ? cost.keyword : 'No explicit trigger found'}</div>
        <div class="evidence-copy">${cost ? cost.excerpt : 'No direct cost reduction wording was extracted from the latest filing text.'}</div>
      </div>
      <div class="evidence-card" style="grid-column: span 2;">
        <div class="evidence-kicker">Source</div>
        <div class="evidence-source">${sourceHtml}</div>
        <div class="evidence-copy" style="margin-top:8px;">${evidence.summary || 'Evidence summary unavailable.'}</div>
      </div>
    `;
  }

  function renderEvidencePicture(evidence) {
    if (!evidence || !evidence.chart_rows || !evidence.chart_rows.length) {
      return;
    }
    const rows = evidence.chart_rows;
    const colors = rows.map(r => r.group === 'non_operating' ? '#f87171' : '#34d399');
    const trace = {
      x: rows.map(r => r.hits),
      y: rows.map(r => r.label),
      type: 'bar',
      orientation: 'h',
      marker: { color: colors, line: { color: 'rgba(255,255,255,0.25)', width: 1 } },
      text: rows.map(r => `${r.hits} hit${r.hits === 1 ? '' : 's'}`),
      textposition: 'outside',
      cliponaxis: false,
    };
    const layout = {
      paper_bgcolor: 'rgba(0,0,0,0)',
      plot_bgcolor: 'rgba(0,0,0,0)',
      margin: { l: 150, r: 20, t: 10, b: 35 },
      font: { color: '#edf2ff' },
      xaxis: { title: 'Keyword hits in latest filing text', gridcolor: 'rgba(255,255,255,0.07)' },
      yaxis: { automargin: true },
    };
    Plotly.newPlot('evidencePictureChart', [trace], layout, { responsive: true, displayModeBar: false });
  }

  function renderWatchlist(rows) {
    document.getElementById('watchlistBody').innerHTML = rows.map(r => `<tr><td>${r.rank}</td><td>${r.ticker}</td><td>${numFmt(r.score)}</td><td>${r.risk}</td><td>${r.verdict}</td><td>${numFmt(r.cfo_ni)}</td><td>${numFmt(r.beneish)}</td><td>${r.flag_count}</td></tr>`).join('');
  }

  function renderGeoRevenue(rows) {
    const el = document.getElementById('geoRevenueBody');
    if (!el) return;
    if (!rows || !rows.length) {
      el.innerHTML = '<tr><td colspan="5" class="muted">Geographic revenue disclosure not available for this filing.</td></tr>';
      return;
    }
    el.innerHTML = rows.map(r => `<tr><td>${r.year || '-'}</td><td>${r.region || '-'}</td><td>${moneyFmt(r.revenue)}</td><td>${pctFmt(r.share_of_total)}</td><td class="${(r.yoy_growth ?? 0) >= 0 ? 'pos' : 'neg'}">${pctFmt(r.yoy_growth)}</td></tr>`).join('');
  }

  function renderSegmentRevenue(rows) {
    const el = document.getElementById('segmentRevenueBody');
    if (!el) return;
    if (!rows || !rows.length) {
      el.innerHTML = '<tr><td colspan="5" class="muted">Segment revenue disclosure not available for this filing.</td></tr>';
      return;
    }
    el.innerHTML = rows.map(r => `<tr><td>${r.year || '-'}</td><td>${r.segment || '-'}</td><td>${moneyFmt(r.revenue)}</td><td>${pctFmt(r.share_of_total)}</td><td class="${(r.yoy_growth ?? 0) >= 0 ? 'pos' : 'neg'}">${pctFmt(r.yoy_growth)}</td></tr>`).join('');
  }

  function renderGeoTrendChart(rows) {
    const host = document.getElementById('geoTrendChart');
    if (!host) return;
    if (!rows || !rows.length) {
      host.innerHTML = '<div class="muted">Geographic revenue disclosure not available for this filing.</div>';
      return;
    }
    const byRegion = {};
    rows.forEach(r => {
      if (!r.region || !r.year || r.revenue === null || r.revenue === undefined) return;
      if (!byRegion[r.region]) byRegion[r.region] = [];
      byRegion[r.region].push(r);
    });
    const traces = Object.keys(byRegion).slice(0, 8).map(region => {
      const sorted = byRegion[region].sort((a,b) => String(a.year).localeCompare(String(b.year)));
      return { x: sorted.map(x => x.year), y: sorted.map(x => x.revenue), type: 'scatter', mode: 'lines+markers', name: region };
    });
    if (!traces.length) {
      host.innerHTML = '<div class="muted">Geographic revenue disclosure not available for this filing.</div>';
      return;
    }
    const layout = { paper_bgcolor: 'rgba(0,0,0,0)', plot_bgcolor: 'rgba(0,0,0,0)', margin: { l: 40, r: 10, t: 10, b: 40 }, font: { color: '#edf2ff' }, xaxis: { gridcolor: 'rgba(255,255,255,0.06)' }, yaxis: { gridcolor: 'rgba(255,255,255,0.06)' } };
    Plotly.newPlot('geoTrendChart', traces, layout, { responsive: true, displayModeBar: false });
  }

  function renderSegmentTrendChart(rows) {
    const host = document.getElementById('segmentTrendChart');
    if (!host) return;
    if (!rows || !rows.length) {
      host.innerHTML = '<div class="muted">Segment revenue disclosure not available for this filing.</div>';
      return;
    }
    const bySegment = {};
    rows.forEach(r => {
      if (!r.segment || !r.year || r.revenue === null || r.revenue === undefined) return;
      if (!bySegment[r.segment]) bySegment[r.segment] = [];
      bySegment[r.segment].push(r);
    });
    const traces = Object.keys(bySegment).slice(0, 8).map(segment => {
      const sorted = bySegment[segment].sort((a,b) => String(a.year).localeCompare(String(b.year)));
      return { x: sorted.map(x => x.year), y: sorted.map(x => x.revenue), type: 'scatter', mode: 'lines+markers', name: segment };
    });
    if (!traces.length) {
      host.innerHTML = '<div class="muted">Segment revenue disclosure not available for this filing.</div>';
      return;
    }
    Plotly.newPlot('segmentTrendChart', traces, { paper_bgcolor: 'rgba(0,0,0,0)', plot_bgcolor: 'rgba(0,0,0,0)', margin: { l: 40, r: 10, t: 10, b: 40 }, font: { color: '#edf2ff' }, xaxis: { gridcolor: 'rgba(255,255,255,0.06)' }, yaxis: { gridcolor: 'rgba(255,255,255,0.06)' } }, { responsive: true, displayModeBar: false });
  }

  function renderMixCharts(geoMixRows, segmentMixRows) {
    const geoHost = document.getElementById('geoMixChart');
    const geoDonutHost = document.getElementById('geoDonutChart');
    const segHost = document.getElementById('segmentMixChart');
    if (geoHost) {
      if (geoMixRows && geoMixRows.length) {
        const years = [...new Set(geoMixRows.map(r => r.year))].sort();
        const regions = [...new Set(geoMixRows.map(r => r.region))].slice(0, 8);
        const traces = regions.map(region => ({
          x: years,
          y: years.map(y => ((geoMixRows.find(r => r.year === y && r.region === region)?.share_of_total || 0) * 100)),
          type: 'bar',
          name: region
        }));
        Plotly.newPlot('geoMixChart', traces, { barmode: 'stack', paper_bgcolor: 'rgba(0,0,0,0)', plot_bgcolor: 'rgba(0,0,0,0)', font: { color: '#edf2ff' }, margin: { l: 35, r: 10, t: 10, b: 35 }, yaxis: { title: 'Mix %', gridcolor: 'rgba(255,255,255,0.06)' }, xaxis: { gridcolor: 'rgba(255,255,255,0.06)' } }, { responsive: true, displayModeBar: false });
        if (geoDonutHost) {
          const latestYear = years[years.length - 1];
          const latestRows = geoMixRows.filter(r => r.year === latestYear);
          const trace = { labels: latestRows.map(r => r.region), values: latestRows.map(r => (r.share_of_total || 0) * 100), type: 'pie', hole: 0.5 };
          Plotly.newPlot('geoDonutChart', [trace], { paper_bgcolor: 'rgba(0,0,0,0)', font: { color: '#edf2ff' }, margin: { l: 10, r: 10, t: 20, b: 10 }, title: `Geographic mix (${latestYear})` }, { responsive: true, displayModeBar: false });
        }
      } else {
        geoHost.innerHTML = '<div class="muted">Geographic mix not available.</div>';
        if (geoDonutHost) geoDonutHost.innerHTML = '<div class="muted">Geographic mix not available.</div>';
      }
    }
    if (segHost) {
      if (segmentMixRows && segmentMixRows.length) {
        const latestYear = segmentMixRows.map(r => r.year).sort().slice(-1)[0];
        const latestRows = segmentMixRows.filter(r => r.year === latestYear);
        const trace = { labels: latestRows.map(r => r.segment), values: latestRows.map(r => (r.share_of_total || 0) * 100), type: 'pie', hole: 0.45 };
        Plotly.newPlot('segmentMixChart', [trace], { paper_bgcolor: 'rgba(0,0,0,0)', font: { color: '#edf2ff' }, margin: { l: 10, r: 10, t: 20, b: 10 }, title: `Segment mix (${latestYear})` }, { responsive: true, displayModeBar: false });
      } else {
        segHost.innerHTML = '<div class="muted">Segment mix not available.</div>';
      }
    }
  }

  function renderGeoSegmentSummary(data) {
    const el = document.getElementById('geoSegmentSummaryBox');
    if (!el) return;
    const items = [];
    if (data.geographic_summary && data.geographic_summary.summary) {
      items.push({ title: 'Geographic summary', detail: data.geographic_summary.summary, severity: data.geographic_summary.severity || 'Warn' });
    } else {
      items.push({ title: 'Geographic disclosure gap', detail: 'Geographic revenue disclosure not available for this filing.', severity: 'Warn' });
    }
    if (data.segment_summary && data.segment_summary.summary) {
      items.push({ title: 'Segment summary', detail: data.segment_summary.summary, severity: data.segment_summary.severity || 'Warn' });
    } else {
      items.push({ title: 'Segment disclosure gap', detail: 'Segment revenue disclosure not available for this filing.', severity: 'Warn' });
    }
    (data.geographic_summary?.warnings || []).forEach(w => items.push({ title: 'Geographic warning', detail: w, severity: 'Warn' }));
    (data.segment_summary?.warnings || []).forEach(w => items.push({ title: 'Segment warning', detail: w, severity: 'Warn' }));
    const forensic = data.geo_segment_forensic || {};
    (forensic.signals || []).forEach(s => items.push({ title: s.title || 'Forensic signal', detail: s.detail || '', severity: s.severity || 'Warn' }));
    el.innerHTML = items.map(x => `<div class="flag ${String(x.severity || 'Warn').toLowerCase()}"><div class="t">${x.title}</div><div class="muted">${x.detail}</div></div>`).join('');
  }

  function renderGeoSummaryCards(data) {
    const el = document.getElementById('geoSummaryCards');
    if (!el) return;
    const geoRows = data.geographic_revenue_rows || [];
    const segRows = data.segment_revenue_rows || [];
    const latestGeoYear = geoRows.map(r => r.year).sort().slice(-1)[0];
    const latestSegYear = segRows.map(r => r.year).sort().slice(-1)[0];
    const latestGeoRows = geoRows.filter(r => r.year === latestGeoYear);
    const latestSegRows = segRows.filter(r => r.year === latestSegYear);
    const domGeo = latestGeoRows.sort((a,b) => (b.share_of_total || 0) - (a.share_of_total || 0))[0];
    const domSeg = latestSegRows.sort((a,b) => (b.share_of_total || 0) - (a.share_of_total || 0))[0];
    const growthLeader = geoRows.filter(r => r.year === latestGeoYear && r.yoy_growth !== null && r.yoy_growth !== undefined).sort((a,b) => (b.yoy_growth || -999) - (a.yoy_growth || -999))[0];
    const extractionMethod = data.extraction_method_used || 'not_available';
    el.innerHTML = `
      <div class="geo-card"><div class="geo-kicker">Dominant region</div><div class="geo-value">${domGeo ? domGeo.region : '-'}</div><div class="geo-note">${domGeo ? pctFmt(domGeo.share_of_total) + ' of revenue' : 'No validated region mix'}</div></div>
      <div class="geo-card"><div class="geo-kicker">Growth leader</div><div class="geo-value">${growthLeader ? growthLeader.region : '-'}</div><div class="geo-note">${growthLeader ? 'YoY ' + pctFmt(growthLeader.yoy_growth) : 'No multi-year geographic trend'}</div></div>
      <div class="geo-card"><div class="geo-kicker">Segment dependence</div><div class="geo-value">${domSeg ? domSeg.segment : '-'}</div><div class="geo-note">${domSeg ? pctFmt(domSeg.share_of_total) + ' of segment revenue' : 'No validated segment mix'}</div></div>
      <div class="geo-card"><div class="geo-kicker">Extraction method</div><div class="geo-value" style="font-size:16px;">${String(extractionMethod).replaceAll('_',' ')}</div><div class="geo-note">Candidate tables: ${data.candidate_tables_found || 0}</div></div>
    `;
  }

  function renderGeoDiagnostics(data) {
    const el = document.getElementById('geoDiagnosticPanel');
    if (!el) return;
    const geoRows = data.geographic_revenue_rows || [];
    const segRows = data.segment_revenue_rows || [];
    if (geoRows.length || segRows.length) {
      el.innerHTML = '';
      return;
    }
    const notes = (data.extraction_debug_notes || []).slice(0, 5);
    const failure = data.extraction_failure_reason || 'No validated region/segment table passed scoring and validation checks.';
    el.innerHTML = `
      <div class="geo-diagnostic">
        <h3>Geographic disclosure not extracted</h3>
        <ul>
          <li><strong>Method attempted:</strong> ${data.extraction_method_used || 'unknown'}</li>
          <li><strong>Probable reason:</strong> ${failure}</li>
          <li><strong>Candidate table detected:</strong> ${(data.candidate_tables_found || 0) > 0 ? 'Yes (rejected)' : 'No'}</li>
          ${notes.map(n => `<li>${n}</li>`).join('')}
        </ul>
      </div>
    `;
  }

  function renderPeers(rows) {
    document.getElementById('peersBody').innerHTML = rows.map(r => `<tr><td>${r.ticker}</td><td>${moneyFmt(r.price)}</td><td>${moneyFmt(r.market_cap)}</td><td>${numFmt(r.cfo_ni)}</td><td>${numFmt(r.beneish)}</td><td>${r.flag_count ?? '-'}</td></tr>`).join('');
  }

  function renderSaved(rows) {
    document.getElementById('savedBody').innerHTML = rows.map(r => `<tr><td>${r.saved_at}</td><td>${r.ticker}</td><td>${r.risk}</td><td>${numFmt(r.cfo_ni)}</td><td>${numFmt(r.beneish)}</td><td>${r.notes || ''}</td></tr>`).join('');
  }

  function renderTickerHistory(rows) {
    document.getElementById('tickerHistoryBody').innerHTML = rows.map(r => `<tr><td>${r.saved_at}</td><td>${r.risk}</td><td>${numFmt(r.cfo_ni)}</td><td>${numFmt(r.beneish)}</td><td>${r.notes || ''}</td></tr>`).join('');
  }

  function renderScreener(rows) {
    const body = document.getElementById('screenerBody');
    if (!body) return;
    body.innerHTML = rows.map((r, idx) => `<tr class="clickable-row" onclick="loadTickerFromScreener('${r.ticker}')"><td>${idx + 1}</td><td>${r.ticker}</td><td>${numFmt(r.score)}</td><td>${r.quality_classification || '-'}</td><td>${r.red_flag_count ?? '-'}</td><td>${r.risk || '-'}</td><td>${numFmt(r.cfo_ni)}</td><td>${numFmt(r.beneish)}</td><td>${numFmt(r.dsri)}</td><td>${r.short_reason || r.reason || '-'}</td></tr>`).join('');
    document.getElementById('screenCount').textContent = String(rows.length || 0);
    document.getElementById('screenWorst').textContent = rows.length ? numFmt(rows[0].score) : '-';
    const text = rows.flatMap(r => (r.short_reason || r.reason || '').split(',').map(x => x.trim())).filter(Boolean);
    const freq = {};
    text.forEach(x => { freq[x] = (freq[x] || 0) + 1; });
    const top = Object.entries(freq).sort((a, b) => b[1] - a[1])[0];
    document.getElementById('screenTheme').textContent = top ? top[0] : '-';
  }

  function loadTickerFromScreener(ticker) {
    const input = document.getElementById('tickerInput');
    if (input) input.value = ticker;
    runAnalysis();
    window.scrollTo({ top: 0, behavior: 'smooth' });
  }

  function renderPriceChart(chart) {
    if (!chart || !chart.dates || !chart.prices) return;
    const trace = { x: chart.dates, y: chart.prices, type: 'scatter', mode: 'lines', line: { width: 3 }, fill: 'tozeroy' };
    const layout = { paper_bgcolor: 'rgba(0,0,0,0)', plot_bgcolor: 'rgba(0,0,0,0)', margin: { l: 40, r: 10, t: 10, b: 40 }, font: { color: '#edf2ff' }, xaxis: { gridcolor: 'rgba(255,255,255,0.06)' }, yaxis: { gridcolor: 'rgba(255,255,255,0.06)' } };
    Plotly.newPlot('priceChart', [trace], layout, { responsive: true, displayModeBar: false });
  }

  function renderCashTrend(rows) {
    if (!rows || !rows.length) return;
    const x = rows.map(r => r.period);
    const t1 = { x, y: rows.map(r => r.cfo), type: 'bar', name: 'CFO' };
    const t2 = { x, y: rows.map(r => r.fcf), type: 'bar', name: 'FCF' };
    const t3 = { x, y: rows.map(r => r.acquisitions), type: 'scatter', mode: 'lines+markers', name: 'Acquisitions' };
    const layout = { barmode: 'group', paper_bgcolor: 'rgba(0,0,0,0)', plot_bgcolor: 'rgba(0,0,0,0)', margin: { l: 40, r: 10, t: 10, b: 40 }, font: { color: '#edf2ff' }, xaxis: { gridcolor: 'rgba(255,255,255,0.06)' }, yaxis: { gridcolor: 'rgba(255,255,255,0.06)' } };
    Plotly.newPlot('cashTrendChart', [t1, t2, t3], layout, { responsive: true, displayModeBar: false });
  }

  function renderCfoNiTrend(trend) {
    if (!trend || !trend.years || !trend.values || !trend.years.length) return;
    const trace = { x: trend.years, y: trend.values, type: 'scatter', mode: 'lines+markers', name: 'CFO / NI', line: { width: 3 }, marker: { size: 8 } };
    const layout = { paper_bgcolor: 'rgba(0,0,0,0)', plot_bgcolor: 'rgba(0,0,0,0)', margin: { l: 40, r: 10, t: 10, b: 40 }, font: { color: '#edf2ff' }, xaxis: { gridcolor: 'rgba(255,255,255,0.06)' }, yaxis: { gridcolor: 'rgba(255,255,255,0.06)' } };
    Plotly.newPlot('cfoNiTrendChart', [trace], layout, { responsive: true, displayModeBar: false });
  }

  function renderDsriFcfTrend(trend) {
    if (!trend || !trend.years || !trend.years.length) return;
    const t1 = { x: trend.years, y: trend.dsri, type: 'scatter', mode: 'lines+markers', name: 'DSRI', yaxis: 'y1', line: { width: 3 } };
    const t2 = { x: trend.years, y: trend.fcf, type: 'bar', name: 'FCF', yaxis: 'y2', opacity: 0.55 };
    const layout = { paper_bgcolor: 'rgba(0,0,0,0)', plot_bgcolor: 'rgba(0,0,0,0)', margin: { l: 40, r: 40, t: 10, b: 40 }, font: { color: '#edf2ff' }, xaxis: { gridcolor: 'rgba(255,255,255,0.06)' }, yaxis: { title: 'DSRI', gridcolor: 'rgba(255,255,255,0.06)' }, yaxis2: { title: 'FCF', overlaying: 'y', side: 'right', showgrid: false } };
    Plotly.newPlot('dsriFcfTrendChart', [t1, t2], layout, { responsive: true, displayModeBar: false });
  }

  function renderTrendSignals(trends) {
    const setPill = (id, label, cls) => {
      const el = document.getElementById(id);
      if (!el) return;
      el.textContent = label;
      el.className = `heat-pill ${cls}`;
    };
    if (!trends) {
      setPill('trendCfoNi', '-', 'neutral');
      setPill('trendDsri', '-', 'neutral');
      setPill('trendFcf', '-', 'neutral');
      setPill('trendOverall', '-', 'neutral');
      document.getElementById('trendNarrative').textContent = 'No trend narrative yet.';
      return;
    }
    const clsFor = (status) => status === 'Deteriorating' || status === 'Negative' ? 'bad' : status === 'Watch' || status === 'Volatile' ? 'warn' : status === 'Improving' || status === 'Stable' || status === 'Positive' ? 'ok' : 'neutral';
    setPill('trendCfoNi', trends.cfo_ni_status || '-', clsFor(trends.cfo_ni_status));
    setPill('trendDsri', trends.dsri_status || '-', clsFor(trends.dsri_status));
    setPill('trendFcf', trends.fcf_status || '-', clsFor(trends.fcf_status));
    setPill('trendOverall', trends.overall_status || '-', clsFor(trends.overall_status));
    document.getElementById('trendNarrative').textContent = trends.narrative || 'No trend narrative yet.';
  }

  async function loadSaved() {
    const res = await fetch('/api/saved');
    const data = await res.json();
    renderSaved(data.rows || []);
  }

  async function loadTickerHistory(ticker) {
    const res = await fetch(`/api/history?ticker=${encodeURIComponent(ticker)}`);
    const data = await res.json();
    renderTickerHistory(data.rows || []);
  }

  async function loadScreener() {
    const universe = document.getElementById('universeSelect').value;
    try {
      const res = await fetch(`/api/screener?universe=${encodeURIComponent(universe)}`);
      if (!res.ok) {
        const txt = await res.text();
        alert('Screener backend error: ' + txt);
        return;
      }
      const data = await res.json();
      renderScreener(data.rows || []);
    } catch (e) {
      alert('Error loading screener: ' + e);
    }
  }

  async function runAnalysis() {
    const ticker = document.getElementById('tickerInput').value.trim().toUpperCase();
    const period = document.getElementById('periodSelect').value;
    if (!ticker) return;
    document.getElementById('loadingLabel').style.display = 'inline';
    try {
      const url = `/api/analyze?ticker=${encodeURIComponent(ticker)}&period=${encodeURIComponent(period)}`;
      const res = await fetch(url);
      if (!res.ok) {
        const txt = await res.text();
        alert('Backend error: ' + txt);
        return;
      }
      const data = await res.json();
      if (data.error) {
        alert(data.error);
        return;
      }
      currentPayload = data;
      document.getElementById('companyTitle').textContent = `${data.company_name} (${data.ticker})`;
      document.getElementById('companySubtitle').textContent = data.company_summary || 'No summary available';
      document.getElementById('priceValue').textContent = moneyFmt(data.price);
      document.getElementById('mcapValue').textContent = moneyFmt(data.market_cap);
      document.getElementById('cfoNiValue').textContent = numFmt(data.latest_cfo_ni);
      document.getElementById('beneishValue').textContent = data.beneish_m !== null ? numFmt(data.beneish_m) : '-';
      document.getElementById('flagsCount').textContent = String(data.flags.length);
      setRiskBadge(data.risk_level, `${data.earnings_quality_classification || 'N/A'} | Risk: ${data.risk_level}`);
      renderHeatmap(data);
      renderInsights(data);
      renderFlags(data.flags || []);
      renderFilings(data.filings || []);
      renderWorkingCapital(data.working_capital_rows || []);
      renderCfoNiTrend(data.cfo_ni_trend || { years: [], values: [] });
      renderDsriFcfTrend(data.dsri_fcf_trend || { years: [], dsri: [], fcf: [] });
      renderTrendSignals(data.trend_signals || null);
      renderQualityTable(data.quality_rows || []);
      renderCashflow(data.cashflow_rows || []);
      renderAcqTable(data.acquisition_table || []);
      renderCashFlags(data.cashflow_flags || []);
      renderMacro(data.macro || []);
      renderMovers('gainersBody', data.top_gainers || []);
      renderMovers('losersBody', data.top_losers || []);
      renderNews(data.news || []);
      renderScorecard(data.scorecard || []);
      renderChecklist(data.reading_checklist || []);
      renderTextSignals(data.text_signals || []);
      renderExcerpts(data.text_excerpts || []);
      renderSingleExcerpt('item7Box', 'Item 7', data.item7_excerpt);
      renderSingleExcerpt('item9aBox', 'Item 9A', data.item9a_excerpt);
      renderEvidencePicture(data.filing_evidence || null);
      renderEvidenceBoard(data.filing_evidence || null);
      renderGeoRevenue(data.geographic_revenue_rows || []);
      renderSegmentRevenue(data.segment_revenue_rows || []);
      renderGeoTrendChart(data.geographic_revenue_rows || []);
      renderSegmentTrendChart(data.segment_revenue_rows || []);
      renderMixCharts(data.geographic_mix_rows || [], data.segment_mix_rows || []);
      renderGeoSummaryCards(data);
      renderGeoDiagnostics(data);
      renderGeoSegmentSummary(data);
      renderDecision(data.decision_table || []);
      renderWatchlist(data.watchlist || []);
      renderPeers(data.peers || []);
      renderPriceChart(data.price_chart || {});
      renderCashTrend(data.cashflow_rows || []);
      loadTickerHistory(data.ticker);
    } catch (e) {
      alert('Error loading analysis: ' + e);
    } finally {
      document.getElementById('loadingLabel').style.display = 'none';
    }
  }

  document.getElementById('analyzeBtn').addEventListener('click', runAnalysis);
  document.getElementById('screenerBtn').addEventListener('click', loadScreener);
  document.getElementById('tickerInput').addEventListener('keydown', (e) => { if (e.key === 'Enter') runAnalysis(); });
  document.getElementById('copyBtn').addEventListener('click', async () => {
    const txt = document.getElementById('qualNotes').value;
    try {
      await navigator.clipboard.writeText(txt);
      alert('Notes copied');
    } catch (_) {
      alert('Unable to copy notes');
    }
  });
  document.getElementById('saveBtn').addEventListener('click', async () => {
    const notes = document.getElementById('qualNotes').value;
    if (!currentPayload) {
      alert('Run an analysis first');
      return;
    }
    const res = await fetch('/api/save', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        ticker: currentPayload.ticker,
        risk: currentPayload.risk_level,
        cfo_ni: currentPayload.latest_cfo_ni,
        beneish: currentPayload.beneish_m,
        notes
      })
    });
    const data = await res.json();
    if (data.ok) {
      alert('Analysis saved');
      loadSaved();
      loadTickerHistory(currentPayload.ticker);
    } else {
      alert(data.error || 'Save failed');
    }
  });
  document.getElementById('exportSavedBtn').addEventListener('click', () => { window.open('/api/export/saved', '_blank'); });
  document.getElementById('exportWatchlistBtn').addEventListener('click', () => { window.open('/api/export/watchlist', '_blank'); });

  runAnalysis();
  loadSaved();
  loadScreener();
</script>
</body>
</html>"""

MACRO_HTML = r"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Global Macro Dashboard</title>
  <script src="https://cdn.plot.ly/plotly-2.35.2.min.js"></script>
  <style>
    :root { --bg:#060b1a; --panel:#0f1a32; --border:rgba(255,255,255,.12); --text:#e7edff; --muted:#a3b2d8; --ok:#34d399; --warn:#fbbf24; --bad:#f87171; --neutral:#7dd3fc; }
    body{ margin:0; background:linear-gradient(180deg,#060b1a,#0c1328); color:var(--text); font-family:Inter,Segoe UI,sans-serif;}
    .container{ width:min(1600px,calc(100vw - 28px)); margin:auto; padding:18px 0 30px;}
    .top{ display:flex; align-items:center; justify-content:space-between; gap:10px; margin-bottom:12px;}
    .btn{ border:1px solid var(--border); border-radius:12px; color:var(--text); background:rgba(255,255,255,.06); padding:10px 14px; cursor:pointer; text-decoration:none;}
    .grid{ display:grid; grid-template-columns:repeat(12,1fr); gap:12px;}
    .card{ grid-column:span 12; border:1px solid var(--border); border-radius:18px; background:rgba(255,255,255,.05); padding:14px;}
    .span-6{ grid-column:span 6;} .span-4{ grid-column:span 4;} .span-8{ grid-column:span 8;}
    h1,h2{ margin:0 0 8px;} .muted{ color:var(--muted);}
    .regimes{ display:grid; grid-template-columns: repeat(5,1fr); gap:8px;}
    .pill{ padding:8px 10px; border-radius:999px; border:1px solid var(--border); display:inline-block; font-weight:700;}
    .ok{ background:rgba(52,211,153,.14); color:var(--ok);} .warn{ background:rgba(251,191,36,.14); color:var(--warn);} .bad{ background:rgba(248,113,113,.14); color:var(--bad);} .neutral{ background:rgba(125,211,252,.14); color:var(--neutral);}
    table{ width:100%; border-collapse:collapse; font-size:14px;} th,td{ padding:8px; border-bottom:1px solid var(--border); text-align:left; vertical-align:top;} th{ color:var(--muted); font-size:12px; text-transform:uppercase;}
    .tabs{ display:flex; gap:8px; flex-wrap:wrap; margin:10px 0;} .tab{ padding:8px 12px; border-radius:999px; border:1px solid var(--border); cursor:pointer;}
    .tab.active{ background:rgba(125,211,252,.18); color:#fff;}
    .section{ display:none;} .section.active{ display:block;}
    .notice{ margin-top:8px; color:var(--warn); font-size:13px;}
    @media (max-width: 1200px){ .span-6,.span-4,.span-8{grid-column:span 12;} .regimes{grid-template-columns:1fr 1fr;} }
  </style>
</head>
<body>
<div class="container">
  <div class="top">
    <h1>Global Macro Dashboard</h1>
    <div>
      <button class="btn" id="refreshBtn">Refresh</button>
      <a class="btn" href="/">Forensic dashboard</a>
    </div>
  </div>
  <div class="tabs" id="tabs"></div>
  <div class="grid">
    <div class="card"><h2>How the world is doing</h2><div id="summaryText" class="muted">Loading...</div><div class="regimes" id="regimePanel"></div><div id="notices" class="notice"></div></div>
    <div class="card section active" data-section="Global Overview"><h2>Global Overview</h2><div id="overviewInterpretation" class="muted"></div><div id="overviewChart" style="height:300px;"></div></div>
    <div class="card section" data-section="Markets"><h2>Equity markets</h2><div class="muted" id="equityInterpretation"></div><div style="overflow:auto;"><table><thead><tr><th>Index</th><th>Last</th><th>5D%</th><th>Status</th><th>Interpretation</th></tr></thead><tbody id="equityBody"></tbody></table></div></div>
    <div class="card section" data-section="Rates & FX"><h2>Rates & FX</h2><div class="muted" id="ratesFxInterpretation"></div><div class="grid"><div class="span-6"><table><thead><tr><th>Rate</th><th>Last</th><th>5D</th><th>Interpretation</th></tr></thead><tbody id="ratesBody"></tbody></table></div><div class="span-6"><table><thead><tr><th>FX</th><th>Last</th><th>5D%</th><th>Interpretation</th></tr></thead><tbody id="fxBody"></tbody></table></div></div></div>
    <div class="card section" data-section="Commodities"><h2>Commodities & Crypto</h2><div class="muted" id="commoditiesInterpretation"></div><div class="grid"><div class="span-8"><table><thead><tr><th>Asset</th><th>Last</th><th>5D%</th><th>Interpretation</th></tr></thead><tbody id="commodityBody"></tbody></table></div><div class="span-4"><table><thead><tr><th>Crypto</th><th>Last</th><th>5D%</th></tr></thead><tbody id="cryptoBody"></tbody></table></div></div></div>
    <div class="card section" data-section="Economy"><h2>Economy</h2><div class="muted" id="economyInterpretation"></div><table><thead><tr><th>Indicator</th><th>Latest</th><th>Prior</th><th>Trend</th><th>Notes</th></tr></thead><tbody id="economyBody"></tbody></table></div>
    <div class="card section" data-section="News"><h2>Most important news</h2><div class="grid"><div class="span-4"><h3>Macro</h3><table><tbody id="newsMacro"></tbody></table></div><div class="span-4"><h3>Markets</h3><table><tbody id="newsMarkets"></tbody></table></div><div class="span-4"><h3>Geopolitical / risk</h3><table><tbody id="newsGeo"></tbody></table></div></div></div>
  </div>
</div>
<script>
const sections = ['Global Overview','Markets','Rates & FX','Commodities','Economy','News'];
function fmt(v,d=2){ return (v===null||v===undefined||Number.isNaN(Number(v))) ? '-' : Number(v).toFixed(d); }
function rowCls(v){ if (v===null||v===undefined) return 'neutral'; if (v>0.7) return 'ok'; if (v<-0.7) return 'bad'; return 'warn'; }
function toNewsRows(rows){ return (rows||[]).map(r=>`<tr><td><a href="${r.link||'#'}" target="_blank">${r.title||'-'}</a><div class="muted">${r.source||'-'} | ${r.published||'-'}</div></td></tr>`).join(''); }
function initTabs(){ const host=document.getElementById('tabs'); host.innerHTML=sections.map((s,i)=>`<div class="tab ${i===0?'active':''}" data-tab="${s}">${s}</div>`).join(''); host.querySelectorAll('.tab').forEach(t=>t.onclick=()=>activateTab(t.dataset.tab)); }
function activateTab(name){ document.querySelectorAll('.tab').forEach(t=>t.classList.toggle('active', t.dataset.tab===name)); document.querySelectorAll('.section').forEach(s=>s.classList.toggle('active', s.dataset.section===name)); }
function renderRegimes(reg){ const panel=document.getElementById('regimePanel'); const items=[['Risk',reg.global_risk_regime],['Growth',reg.growth_regime],['Inflation',reg.inflation_regime],['Liquidity',reg.liquidity_regime],['Stress',reg.market_stress_regime]]; panel.innerHTML=items.map(([k,v])=>`<div><div class="muted">${k}</div><span class="pill ${v.class||'neutral'}">${v.label||'-'}</span></div>`).join(''); }
function renderTable(id, rows, mapper){ document.getElementById(id).innerHTML=(rows||[]).map(mapper).join(''); }
function fmtIndicator(v, unit=null, d=2){ const base = fmt(v,d); return base==='-' ? base : (unit==='pct' ? `${base}%` : base); }
async function loadMacro(){
  const r=await fetch('/api/macro/dashboard'); const d=await r.json();
  document.getElementById('summaryText').textContent = d.summary?.human_summary || 'No summary.';
  document.getElementById('overviewInterpretation').textContent = d.summary?.overview_interpretation || '';
  renderRegimes(d.summary?.regimes || {});
  document.getElementById('notices').textContent = (d.runtime_notices||[]).join(' | ');
  renderTable('equityBody', d.markets?.equities, x=>`<tr><td>${x.name}</td><td>${fmt(x.last)}</td><td>${fmt(x.change_5d_pct)}</td><td><span class="pill ${rowCls(x.change_5d_pct)}">${x.signal||'-'}</span></td><td>${x.interpretation||''}</td></tr>`);
  renderTable('ratesBody', d.rates?.major, x=>`<tr><td>${x.name}</td><td>${fmt(x.last)}</td><td>${fmt(x.change_5d)}</td><td>${x.interpretation||''}</td></tr>`);
  renderTable('fxBody', d.fx?.major, x=>`<tr><td>${x.name}</td><td>${fmt(x.last,4)}</td><td>${fmt(x.change_5d_pct)}</td><td>${x.interpretation||''}</td></tr>`);
  renderTable('commodityBody', d.commodities?.major, x=>`<tr><td>${x.name}</td><td>${fmt(x.last)}</td><td>${fmt(x.change_5d_pct)}</td><td>${x.interpretation||''}</td></tr>`);
  renderTable('cryptoBody', d.crypto?.major, x=>`<tr><td>${x.name}</td><td>${fmt(x.last)}</td><td>${fmt(x.change_5d_pct)}</td></tr>`);
  renderTable('economyBody', d.economy?.indicators, x=>`<tr><td>${x.name}</td><td>${fmtIndicator(x.latest, x.unit)}</td><td>${fmtIndicator(x.prior, x.unit)}</td><td>${x.trend||'-'}</td><td>${x.interpretation||''}</td></tr>`);
  document.getElementById('equityInterpretation').textContent = d.markets?.interpretation || '';
  document.getElementById('ratesFxInterpretation').textContent = d.rates_fx_interpretation || '';
  document.getElementById('commoditiesInterpretation').textContent = d.commodities?.interpretation || '';
  document.getElementById('economyInterpretation').textContent = d.economy?.interpretation || '';
  document.getElementById('newsMacro').innerHTML = toNewsRows(d.news?.macro);
  document.getElementById('newsMarkets').innerHTML = toNewsRows(d.news?.markets);
  document.getElementById('newsGeo').innerHTML = toNewsRows(d.news?.geopolitical);
  const heat = d.markets?.equities?.slice(0,10)||[];
  Plotly.newPlot('overviewChart',[{type:'bar',x:heat.map(x=>x.name),y:heat.map(x=>x.change_5d_pct),marker:{color:heat.map(x=>x.change_5d_pct>=0?'#34d399':'#f87171')}}],{paper_bgcolor:'rgba(0,0,0,0)',plot_bgcolor:'rgba(0,0,0,0)',font:{color:'#e7edff'}},{displayModeBar:false,responsive:true});
}
initTabs(); loadMacro(); document.getElementById('refreshBtn').onclick=loadMacro;
</script>
</body>
</html>"""


def init_db() -> None:
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS saved_analyses (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            saved_at TEXT NOT NULL,
            ticker TEXT NOT NULL,
            risk TEXT,
            cfo_ni REAL,
            beneish REAL,
            notes TEXT
        )
        """
    )
    conn.commit()
    conn.close()


def safe_float(x: Any) -> float | None:
    try:
        if x is None or pd.isna(x):
            return None
        return float(x)
    except Exception:
        return None


def fmt_value(v: Any, digits: int = 2) -> str:
    n = safe_float(v)
    return "-" if n is None else f"{n:.{digits}f}"


def get_companyfacts_ticker_map() -> dict[str, dict[str, Any]]:
    url = "https://www.sec.gov/files/company_tickers.json"
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=30) as resp:
        data = json.loads(resp.read().decode("utf-8"))
    return {item["ticker"].upper(): item for _, item in data.items()}


def get_sp500_tickers(limit: int | None = 300) -> list[str]:
    global _SP500_CACHE
    if _SP500_CACHE is None:
        try:
            tables = pd.read_html("https://en.wikipedia.org/wiki/List_of_S%26P_500_companies")
            _SP500_CACHE = [str(x).replace('.', '-') for x in tables[0]["Symbol"].tolist()]
        except Exception:
            _SP500_CACHE = DEFAULT_WATCHLIST.copy()
    return _SP500_CACHE[:limit] if limit else _SP500_CACHE


def choose_universe(mode: str) -> list[str]:
    if mode == "watchlist":
        return DEFAULT_WATCHLIST
    if mode == "sp100":
        return get_sp500_tickers(limit=100)
    return CORE_SCREENER_UNIVERSE


def get_sec_recent_filings(ticker: str, max_items: int = 4) -> list[dict[str, str]]:
    try:
        tmap = get_companyfacts_ticker_map()
        info = tmap.get(ticker.upper())
        if not info:
            return []
        cik = str(info["cik_str"]).zfill(10)
        url = f"https://data.sec.gov/submissions/CIK{cik}.json"
        r = requests.get(url, headers=SEC_HEADERS, timeout=30)
        r.raise_for_status()
        data = r.json()
        recent = data.get("filings", {}).get("recent", {})
        rows = []
        for form, dt, acc, doc in zip(
            recent.get("form", []),
            recent.get("filingDate", []),
            recent.get("accessionNumber", []),
            recent.get("primaryDocument", []),
        ):
            if form not in {"10-K", "10-Q", "8-K", "20-F"}:
                continue
            acc_clean = acc.replace("-", "")
            rows.append({
                "form": form,
                "filing_date": dt,
                "url": f"https://www.sec.gov/Archives/edgar/data/{int(info['cik_str'])}/{acc_clean}/{doc}",
                "index_url": f"https://www.sec.gov/Archives/edgar/data/{int(info['cik_str'])}/{acc_clean}/{acc}-index.htm",
                "company_url": f"https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK={ticker}&type={form}&count=10",
            })
            if len(rows) >= max_items:
                break
        return rows
    except Exception:
        return []


def get_news_google_rss(query: str, limit: int = 8) -> list[dict[str, str]]:
    try:
        q = urllib.parse.quote(query)
        url = f"https://news.google.com/rss/search?q={q}&hl=en-US&gl=US&ceid=US:en"
        req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
        with urllib.request.urlopen(req, context=ssl.create_default_context(), timeout=30) as resp:
            root = ET.fromstring(resp.read())
        items = []
        for item in root.findall(".//item")[:limit]:
            source_el = item.find("source")
            items.append({
                "title": (item.findtext("title") or "").strip(),
                "link": (item.findtext("link") or "").strip(),
                "published": (item.findtext("pubDate") or "").strip(),
                "source": source_el.text.strip() if source_el is not None and source_el.text else "Google News",
            })
        return items
    except Exception:
        return []


def history_last_and_5d_change(symbol: str, period: str = "1mo") -> tuple[float | None, float | None]:
    try:
        hist = yf.Ticker(symbol).history(period=period, auto_adjust=False)
        close = hist.get("Close", pd.Series(dtype=float)).dropna()
        if close.empty:
            return None, None
        last = safe_float(close.iloc[-1])
        change = None
        if len(close) >= 6:
            prev = safe_float(close.iloc[-6])
            if prev not in (None, 0):
                change = ((last / prev) - 1.0) * 100.0
        return last, change
    except Exception:
        return None, None


def get_macro_snapshot() -> list[dict[str, Any]]:
    series = {
        "US 10Y Yield": ("^TNX", "Higher yields can pressure valuation multiples."),
        "S&P 500": ("^GSPC", "Broad equity risk appetite benchmark."),
        "VIX": ("^VIX", "Higher volatility implies risk-off conditions."),
        "Dollar Index": ("DX-Y.NYB", "Stronger dollar can affect multinationals and liquidity."),
    }
    rows = []
    for name, (symbol, interp) in series.items():
        last, chg = history_last_and_5d_change(symbol)
        rows.append({"name": name, "last": last, "change_5d": chg, "interpretation": interp})
    return rows


def get_top_gainers_losers() -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    universe = get_sp500_tickers(limit=220)
    rows = []
    for ticker in universe:
        try:
            hist = yf.Ticker(ticker).history(period="5d", auto_adjust=False)
            close = hist.get("Close", pd.Series(dtype=float)).dropna()
            if len(close) >= 2:
                last = safe_float(close.iloc[-1])
                prev = safe_float(close.iloc[-2])
                if last is not None and prev not in (None, 0):
                    rows.append({"ticker": ticker, "price": last, "change_pct": ((last / prev) - 1.0) * 100.0})
        except Exception:
            continue
    rows.sort(key=lambda x: x["change_pct"], reverse=True)
    return rows[:10], sorted(rows, key=lambda x: x["change_pct"])[:10]


def get_price_chart_and_info(ticker: str, period: str = "1y") -> dict[str, Any]:
    tk = yf.Ticker(ticker)
    info = tk.info or {}
    try:
        hist = tk.history(period=period, auto_adjust=False)
    except Exception:
        hist = pd.DataFrame(columns=["Close"])
    if hist is None or hist.empty or "Close" not in hist.columns:
        hist = pd.DataFrame(columns=["Close"])
    return {
        "chart": {
            "dates": [d.strftime("%Y-%m-%d") for d in hist.index.to_pydatetime()] if not hist.empty else [],
            "prices": [safe_float(x) for x in hist["Close"].tolist()] if not hist.empty else [],
        },
        "price": safe_float(info.get("currentPrice") or info.get("regularMarketPrice")),
        "market_cap": safe_float(info.get("marketCap")),
        "long_name": info.get("longName") or ticker.upper(),
        "summary": info.get("longBusinessSummary") or "",
    }


def pick_series(df: pd.DataFrame, candidates: list[str]) -> pd.Series | None:
    for c in candidates:
        if c in df.columns:
            return df[c]
    return None


def build_quality_rows(ticker: str) -> tuple[list[dict[str, Any]], dict[str, float | None], list[dict[str, Any]], dict[str, Any], list[dict[str, Any]]]:
    tk = yf.Ticker(ticker)
    fin = tk.financials.T.copy()
    cf = tk.cashflow.T.copy()
    bs = tk.balance_sheet.T.copy()
    if fin.empty:
        return [], {}, [], {}, []

    rev = pick_series(fin, ["Total Revenue", "Operating Revenue", "Revenue"])
    ni = pick_series(fin, ["Net Income", "Net Income Common Stockholders"])
    cfo = pick_series(cf, ["Operating Cash Flow", "Cash Flow From Continuing Operating Activities", "Total Cash From Operating Activities"])
    capex = pick_series(cf, ["Capital Expenditure", "Capital Expenditures"])
    acquisitions = pick_series(cf, ["Acquisitions Net", "Net Business Purchase And Sale", "Purchase Of Business", "Acquisition Of Business"])
    ar = pick_series(bs, ["Accounts Receivable", "Receivables", "Net Receivables"])
    inv = pick_series(bs, ["Inventory", "Inventories"])
    payables = pick_series(bs, ["Accounts Payable", "Payables And Accrued Expenses", "Current Payables"])
    ca = pick_series(bs, ["Current Assets"])
    ppe = pick_series(bs, ["Net PPE", "Property Plant Equipment", "Gross PPE"])
    ta = pick_series(bs, ["Total Assets"])
    dep = pick_series(cf, ["Depreciation And Amortization", "Depreciation Amortization Depletion", "Depreciation"])
    sga = pick_series(fin, ["Selling General And Administration", "Selling And Marketing Expense", "Selling General Administrative"])
    cl = pick_series(bs, ["Current Liabilities"])
    ltd = pick_series(bs, ["Long Term Debt", "Long Term Debt And Capital Lease Obligation"])
    other_income = pick_series(fin, ["Other Income Expense", "Other Non Operating Income Expenses", "Total Other Finance Cost"])
    non_operating_income = pick_series(fin, ["Non Operating Income Expenses", "Total Unusual Items Excluding Goodwill"])
    non_recurring_income = pick_series(fin, ["Special Income Charges", "Other Special Charges"])
    asset_sale_gain = pick_series(fin, ["Gain On Sale Of Business", "Gain On Sale Of Security", "Gain Loss On Sale Of Assets"])
    equity_investment_gain = pick_series(fin, ["Earnings From Equity Interest", "Gain Loss On Investment Securities"])
    fair_value_gain = pick_series(fin, ["Unrealized Gain Loss", "Gain Loss On Fair Value Adjustments"])
    tax_benefit = pick_series(fin, ["Tax Effect Of Unusual Items", "Deferred Tax", "Provision For Doubtful Accounts"])
    one_time_gain = pick_series(fin, ["Gain On Sale Of Ppe", "Extraordinary Items"])
    litigation_settlement_gain = pick_series(fin, ["Litigation Expense"])
    if rev is None or ni is None or cfo is None:
        return [], {}, [], {}, []

    df = pd.DataFrame({
        "revenue": rev, "net_income": ni, "cfo": cfo, "capex": capex, "acquisitions": acquisitions,
        "ar": ar, "inventory": inv, "payables": payables, "current_assets": ca, "ppe": ppe,
        "total_assets": ta, "dep": dep, "sga": sga, "current_liabilities": cl, "long_term_debt": ltd,
        "other_income": other_income,
        "non_operating_income": non_operating_income,
        "non_recurring_income": non_recurring_income,
        "asset_sale_gain": asset_sale_gain,
        "equity_investment_gain": equity_investment_gain,
        "fair_value_gain": fair_value_gain,
        "tax_benefit": tax_benefit,
        "one_time_gain": one_time_gain,
        "litigation_settlement_gain": litigation_settlement_gain,
    }).dropna(how="all")
    if df.empty:
        return [], {}, [], {}, []

    df = df.sort_index()
    df["cfo_ni"] = df.apply(lambda r: (r["cfo"] / r["net_income"]) if safe_float(r["net_income"]) not in (None, 0) else None, axis=1)
    df["accruals"] = df["net_income"] - df["cfo"]
    df["revenue_growth"] = df["revenue"].pct_change()
    df["ar_growth"] = df["ar"].pct_change() if "ar" in df else None
    df["inventory_growth"] = df["inventory"].pct_change() if "inventory" in df else None
    df["payables_growth"] = df["payables"].pct_change() if "payables" in df else None
    df["dsri"] = ((df["ar"] / df["revenue"]) / (df["ar"].shift(1) / df["revenue"].shift(1))) if "ar" in df else None
    df["aqi"] = None
    if all(c in df.columns for c in ["current_assets", "ppe", "total_assets"]):
        num = 1 - ((df["current_assets"] + df["ppe"]) / df["total_assets"])
        den = 1 - ((df["current_assets"].shift(1) + df["ppe"].shift(1)) / df["total_assets"].shift(1))
        df["aqi"] = num / den
    df["sgi"] = df["revenue"] / df["revenue"].shift(1)
    df["depi"] = (df["dep"].shift(1) / (df["dep"].shift(1) + df["ppe"].shift(1))) / (df["dep"] / (df["dep"] + df["ppe"])) if all(c in df.columns for c in ["dep", "ppe"]) else None
    df["sgai"] = (df["sga"] / df["revenue"]) / (df["sga"].shift(1) / df["revenue"].shift(1)) if "sga" in df else None
    df["lvgi"] = (((df["current_liabilities"] + df["long_term_debt"]) / df["total_assets"]) / (((df["current_liabilities"].shift(1) + df["long_term_debt"].shift(1)) / df["total_assets"].shift(1)))) if all(c in df.columns for c in ["current_liabilities", "long_term_debt", "total_assets"]) else None
    avg_assets = (df["total_assets"] + df["total_assets"].shift(1)) / 2 if "total_assets" in df else None
    df["tata"] = ((df["net_income"] - df["cfo"]) / avg_assets) if avg_assets is not None else None
    df["fcf"] = df["cfo"] - df["capex"].abs() if "capex" in df else None

    def z(x: Any) -> float:
        val = safe_float(x)
        return 0.0 if val is None else val

    df["beneish_m"] = df.apply(lambda r: -4.84 + 0.92 * z(r.get("dsri")) + 0.404 * z(r.get("aqi")) + 0.892 * z(r.get("sgi")) + 0.115 * z(r.get("depi")) - 0.172 * z(r.get("sgai")) + 4.679 * z(r.get("tata")) - 0.327 * z(r.get("lvgi")), axis=1)

    rows, cash_rows, wc_rows = [], [], []
    for idx, row in df.tail(6).iterrows():
        period = idx.strftime("%Y-%m-%d") if hasattr(idx, "strftime") else str(idx)
        rows.append({
            "period": period,
            "revenue": safe_float(row.get("revenue")),
            "net_income": safe_float(row.get("net_income")),
            "cfo": safe_float(row.get("cfo")),
            "cfo_ni": safe_float(row.get("cfo_ni")),
            "accruals": safe_float(row.get("accruals")),
            "revenue_growth": safe_float(row.get("revenue_growth")),
            "ar_growth": safe_float(row.get("ar_growth")),
            "dsri": safe_float(row.get("dsri")),
            "beneish_m": safe_float(row.get("beneish_m")),
            "tata": safe_float(row.get("tata")),
            "other_income": safe_float(row.get("other_income")),
            "non_operating_income": safe_float(row.get("non_operating_income")),
            "non_recurring_income": safe_float(row.get("non_recurring_income")),
            "asset_sale_gain": safe_float(row.get("asset_sale_gain")),
            "equity_investment_gain": safe_float(row.get("equity_investment_gain")),
            "fair_value_gain": safe_float(row.get("fair_value_gain")),
            "tax_benefit": safe_float(row.get("tax_benefit")),
            "one_time_gain": safe_float(row.get("one_time_gain")),
            "litigation_settlement_gain": safe_float(row.get("litigation_settlement_gain")),
        })
        cash_rows.append({
            "period": period,
            "cfo": safe_float(row.get("cfo")),
            "capex": safe_float(row.get("capex")),
            "fcf": safe_float(row.get("fcf")),
            "acquisitions": safe_float(row.get("acquisitions")),
            "cfo_ni": safe_float(row.get("cfo_ni")),
        })
        wc_rows.append({
            "period": period,
            "ar": safe_float(row.get("ar")),
            "inventory": safe_float(row.get("inventory")),
            "payables": safe_float(row.get("payables")),
            "ar_growth": safe_float(row.get("ar_growth")),
            "inventory_growth": safe_float(row.get("inventory_growth")),
            "payables_growth": safe_float(row.get("payables_growth")),
        })

    latest = rows[-1] if rows else {}
    acq_metrics = {
        "latest_acquisitions": safe_float(df["acquisitions"].iloc[-1]) if "acquisitions" in df.columns and len(df) else None,
        "avg_acquisitions": safe_float(df["acquisitions"].tail(4).mean()) if "acquisitions" in df.columns else None,
        "acq_to_cfo": safe_float((abs(df["acquisitions"].iloc[-1]) / abs(df["cfo"].iloc[-1])) if "acquisitions" in df.columns and safe_float(df["cfo"].iloc[-1]) not in (None, 0) else None),
        "latest_fcf": safe_float(df["fcf"].iloc[-1]) if "fcf" in df.columns and len(df) else None,
    }
    return rows, latest, cash_rows, acq_metrics, wc_rows


def get_cfo_ni_trend_from_quality_rows(quality_rows: list[dict[str, Any]]) -> dict[str, list[Any]]:
    if not quality_rows:
        return {"years": [], "values": []}
    years, values = [], []
    for row in quality_rows:
        year = str(row.get("period", ""))[:4]
        years.append(year)
        values.append(safe_float(row.get("cfo_ni")))
    return {"years": years, "values": values}


def get_dsri_fcf_trend(quality_rows: list[dict[str, Any]], cashflow_rows: list[dict[str, Any]]) -> dict[str, list[Any]]:
    if not quality_rows:
        return {"years": [], "dsri": [], "fcf": []}
    fcf_map = {}
    for row in cashflow_rows:
        year = str(row.get("period", ""))[:4]
        fcf_map[year] = safe_float(row.get("fcf"))
    years, dsri_vals, fcf_vals = [], [], []
    for row in quality_rows:
        year = str(row.get("period", ""))[:4]
        years.append(year)
        dsri_vals.append(safe_float(row.get("dsri")))
        fcf_vals.append(fcf_map.get(year))
    return {"years": years, "dsri": dsri_vals, "fcf": fcf_vals}


def build_trend_signals(cfo_ni_trend: dict[str, list[Any]], dsri_fcf_trend: dict[str, list[Any]]) -> dict[str, str]:
    cfo_vals = [v for v in cfo_ni_trend.get("values", []) if v is not None]
    dsri_vals = [v for v in dsri_fcf_trend.get("dsri", []) if v is not None]
    fcf_vals = [v for v in dsri_fcf_trend.get("fcf", []) if v is not None]

    def cfo_status(vals: list[float]) -> str:
        if len(vals) < 2:
            return "Neutral"
        if vals[-1] < 0.8:
            return "Deteriorating"
        if vals[-1] < vals[0] - 0.15:
            return "Watch"
        if vals[-1] >= 1.0 and vals[-1] >= vals[0]:
            return "Stable"
        return "Neutral"

    def dsri_status(vals: list[float]) -> str:
        if len(vals) < 2:
            return "Neutral"
        if vals[-1] > 1.15:
            return "Deteriorating"
        if vals[-1] > vals[0] + 0.08:
            return "Watch"
        if vals[-1] <= 1.0:
            return "Stable"
        return "Neutral"

    def fcf_status(vals: list[float]) -> str:
        if len(vals) < 2:
            return "Neutral"
        if vals[-1] < 0:
            return "Negative"
        negatives = sum(1 for v in vals if v < 0)
        if negatives >= max(2, len(vals) // 2):
            return "Volatile"
        if vals[-1] > 0:
            return "Positive"
        return "Neutral"

    c_status = cfo_status(cfo_vals)
    d_status = dsri_status(dsri_vals)
    f_status = fcf_status(fcf_vals)
    bads = sum(1 for s in [c_status, d_status, f_status] if s in {"Deteriorating", "Negative"})
    warns = sum(1 for s in [c_status, d_status, f_status] if s in {"Watch", "Volatile"})
    if bads >= 2:
        overall = "Deteriorating"
    elif bads >= 1 or warns >= 2:
        overall = "Watch"
    elif c_status == "Stable" and d_status == "Stable" and f_status == "Positive":
        overall = "Stable"
    else:
        overall = "Neutral"
    return {
        "cfo_ni_status": c_status,
        "dsri_status": d_status,
        "fcf_status": f_status,
        "overall_status": overall,
        "narrative": f"CFO/NI trend: {c_status}. DSRI trend: {d_status}. FCF trend: {f_status}.",
    }


def _severity_to_points(severity: str) -> int:
    return 2 if severity == "Bad" else 1 if severity == "Warn" else 0


def _ratio_change(curr_num: float | None, curr_den: float | None, prev_num: float | None, prev_den: float | None) -> float | None:
    curr_num_v = safe_float(curr_num)
    curr_den_v = safe_float(curr_den)
    prev_num_v = safe_float(prev_num)
    prev_den_v = safe_float(prev_den)
    if curr_num_v is None or curr_den_v in (None, 0) or prev_num_v is None or prev_den_v in (None, 0):
        return None
    return (curr_num_v / curr_den_v) - (prev_num_v / prev_den_v)


def classify_persistence(hit_count: int) -> str:
    """
    Classify signal persistence for explainability.
    One-off = 1 period, repeated = 2 periods, persistent = 3+ periods.
    """
    if hit_count >= 3:
        return "persistent"
    if hit_count == 2:
        return "repeated"
    if hit_count == 1:
        return "one-off"
    return "none"


def detect_non_operating_support(quality_rows: list[dict[str, Any]]) -> dict[str, Any]:
    """
    Detect earnings support from non-core/non-recurring sources.
    This is intentionally rules-based (no black-box model) so each score effect is auditable.
    """
    result: dict[str, Any] = {
        "penalty": 0,
        "signal_count": 0,
        "signals": [],
        "latest_support_ratio": None,
        "persistence": "none",
    }
    if not quality_rows:
        return result

    latest = quality_rows[-1]
    ni = safe_float(latest.get("net_income"))
    cfo = safe_float(latest.get("cfo"))
    if ni in (None, 0):
        return result

    support_items = [
        ("non_operating_income", "non-operating income"),
        ("non_recurring_income", "non-recurring income"),
        ("asset_sale_gain", "asset sale gain"),
        ("one_time_gain", "one-time gain"),
        ("equity_investment_gain", "equity investment gain"),
        ("fair_value_gain", "fair value gain"),
        ("litigation_settlement_gain", "litigation settlement gain"),
        ("tax_benefit", "tax benefit"),
        ("other_income", "other income"),
    ]

    latest_support_total = 0.0
    yearly_support_hits = 0
    for row in quality_rows[-5:]:
        row_total = 0.0
        for key, _ in support_items:
            val = safe_float(row.get(key))
            if val is not None and val > 0:
                row_total += val
        if row_total > 0:
            yearly_support_hits += 1
        if row is latest:
            latest_support_total = row_total

    support_ratio = abs(latest_support_total) / abs(ni) if ni not in (None, 0) else None
    result["latest_support_ratio"] = support_ratio
    result["persistence"] = classify_persistence(yearly_support_hits)

    for key, label in support_items:
        val = safe_float(latest.get(key))
        if val is None or val <= 0:
            continue
        contribution = abs(val) / abs(ni) if ni not in (None, 0) else 0.0
        if contribution >= 0.1:
            result["signals"].append({
                "signal": label,
                "value": val,
                "ratio_to_net_income": contribution,
            })

    # Severity increases if earnings look strong while operating cash support is weak.
    cfo_support_mismatch = cfo is not None and cfo < ni * 0.8
    if support_ratio is not None:
        if support_ratio >= 0.35:
            result["penalty"] += 12
        elif support_ratio >= 0.2:
            result["penalty"] += 7
        elif support_ratio >= 0.1:
            result["penalty"] += 4
    if cfo_support_mismatch and (support_ratio or 0) >= 0.1:
        result["penalty"] += 5

    if result["persistence"] == "persistent":
        result["penalty"] += 5
    elif result["persistence"] == "repeated":
        result["penalty"] += 2

    result["signal_count"] = len(result["signals"])
    return result


def build_forensic_components(
    quality_rows: list[dict[str, Any]],
    text_signals: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """
    Build explainable forensic components used by flags and scoring.
    Every component explicitly maps a business signal (cash/revenue quality, persistence,
    and working-capital stress) to a score effect so the UI remains interpretable.
    """
    comps: dict[str, Any] = {
        "penalties": 0,
        "persistence_penalty": 0,
        "text_alignment_boost": 0,
        "reason_tags": [],
        "major_reasons": [],
        "persistent_events": 0,
        "mismatch_classification": "none",
        "working_capital_anomalies": [],
        "non_operating_analysis": {},
    }
    if len(quality_rows) < 2:
        return comps

    last = quality_rows[-1]
    prev = quality_rows[-2]

    # 1) Revenue growth versus margin quality divergence.
    rev_growth = safe_float(last.get("revenue_growth"))
    net_margin_delta = _ratio_change(last.get("net_income"), last.get("revenue"), prev.get("net_income"), prev.get("revenue"))
    cfo_margin_delta = _ratio_change(last.get("cfo"), last.get("revenue"), prev.get("cfo"), prev.get("revenue"))
    rev_margin_divergence = False
    if rev_growth is not None and rev_growth > 0.08:
        if (net_margin_delta is not None and net_margin_delta < -0.015) or (cfo_margin_delta is not None and cfo_margin_delta < -0.015):
            rev_margin_divergence = True
            comps["penalties"] += 9
            comps["reason_tags"].append("margin divergence")
            comps["major_reasons"].append("Revenue growth is outpacing margin quality")

    # 2) Earnings versus CFO mismatch persistence (one-off vs repeated).
    mismatch_years = 0
    mismatch_last = False
    for row in quality_rows[-5:]:
        ni = safe_float(row.get("net_income"))
        cfo = safe_float(row.get("cfo"))
        cfo_ni = safe_float(row.get("cfo_ni"))
        if ni in (None, 0) or cfo is None:
            continue
        mismatch = (abs(ni - cfo) / abs(ni) > 0.30) or (cfo_ni is not None and cfo_ni < 0.9)
        if mismatch:
            mismatch_years += 1
            if row is quality_rows[-1]:
                mismatch_last = True
    mismatch_class = classify_persistence(mismatch_years)
    comps["mismatch_classification"] = mismatch_class
    if mismatch_years >= 3:
        comps["penalties"] += 14
        comps["persistence_penalty"] += 7
        comps["persistent_events"] += 1
        comps["reason_tags"].append("persistent earnings/cash mismatch")
        comps["major_reasons"].append("Earnings and operating cash flow diverge across multiple years")
    elif mismatch_years == 2:
        comps["penalties"] += 8
        comps["persistence_penalty"] += 3
        comps["reason_tags"].append("repeated earnings/cash mismatch")
    elif mismatch_last:
        comps["penalties"] += 4
        comps["reason_tags"].append("one-off earnings/cash mismatch")

    # 3) Working-capital shock detection using own-history baseline.
    wc_metrics = [
        ("ar_growth", "receivables"),
        ("inventory_growth", "inventory"),
        ("payables_growth", "payables"),
    ]
    wc_shocks = 0
    wc_anomalies: list[dict[str, Any]] = []
    for key, label in wc_metrics:
        vals = [safe_float(r.get(key)) for r in quality_rows[-6:]]
        clean = [v for v in vals if v is not None]
        if len(clean) < 3:
            continue
        latest = clean[-1]
        baseline = clean[:-1]
        mean = pd.Series(baseline).mean()
        std = pd.Series(baseline).std(ddof=0) or 0.0
        shock_threshold = abs(mean) + max(0.20, 1.5 * std)
        is_shock = abs(latest) > shock_threshold
        deterioration = (label in {"receivables", "inventory"} and latest > 0) or (label == "payables" and latest < 0)
        if is_shock and deterioration:
            wc_shocks += 1
            comps["penalties"] += 5
            comps["reason_tags"].append(f"{label} shock")
            comps["major_reasons"].append(f"Unusual {label} move is pressuring cash conversion")
            wc_anomalies.append({
                "metric": label,
                "latest_change": latest,
                "baseline_mean": safe_float(mean),
                "shock_threshold": safe_float(shock_threshold),
                "why_it_matters": (
                    "Higher receivables can signal weaker collections or aggressive recognition." if label == "receivables"
                    else "Inventory spikes can indicate demand softness or inventory overbuild." if label == "inventory"
                    else "Payables contraction can consume cash and weaken liquidity buffer."
                ),
            })
    if wc_shocks >= 2:
        comps["persistence_penalty"] += 3
        comps["persistent_events"] += 1
    comps["working_capital_anomalies"] = wc_anomalies

    # 4) Persistence scaling: repeated anomalies should weigh more than isolated anomalies.
    if rev_margin_divergence and mismatch_years >= 2:
        comps["persistence_penalty"] += 4
        comps["persistent_events"] += 1

    # 4b) Explicit non-operating/non-recurring earnings support analysis.
    non_op = detect_non_operating_support(quality_rows)
    comps["non_operating_analysis"] = non_op
    if non_op.get("penalty", 0) > 0:
        comps["penalties"] += non_op["penalty"]
        if non_op.get("persistence") in {"repeated", "persistent"}:
            comps["persistent_events"] += 1
        comps["reason_tags"].append("non-operating earnings support")
        comps["major_reasons"].append("Reported earnings appear supported by non-core or non-recurring items")

    # 5) Text + numeric alignment boost (when both point to same direction).
    text_hits = {str(s.get("signal", "")).lower() for s in (text_signals or [])}
    alignment = 0
    if rev_margin_divergence and ({"allowance", "channel", "one-time", "one time"} & text_hits):
        alignment += 3
    if mismatch_years >= 2 and ({"material weakness", "restatement", "temporary"} & text_hits):
        alignment += 4
    if wc_shocks > 0 and ({"restructure", "restructuring", "litigation"} & text_hits):
        alignment += 2
    if non_op.get("penalty", 0) > 0 and ({"one-time", "one time", "fair value", "gain on sale", "tax benefit", "other income"} & text_hits):
        alignment += 4
    comps["text_alignment_boost"] = alignment
    comps["penalties"] += alignment

    # Keep concise reason text for screener and preserve deterministic ordering.
    unique_major = []
    for reason in comps["major_reasons"]:
        if reason not in unique_major:
            unique_major.append(reason)
    comps["major_reasons"] = unique_major[:4]
    comps["reason_tags"] = list(dict.fromkeys(comps["reason_tags"]))[:6]
    return comps


def generate_flags(
    quality_rows: list[dict[str, Any]],
    text_signals: list[dict[str, Any]] | None = None,
) -> tuple[list[dict[str, str]], str, float | None, float | None, dict[str, Any]]:
    if not quality_rows:
        return (
            [{"severity": "Warn", "title": "Limited fundamentals", "detail": "Available structured data is incomplete. Manual filing review becomes more important."}],
            "Medium",
            None,
            None,
            {"penalties": 0, "persistence_penalty": 0, "text_alignment_boost": 0, "reason_tags": [], "major_reasons": [], "persistent_events": 0},
        )
    flags: list[dict[str, str]] = []
    last = quality_rows[-1]
    latest_cfo_ni = last.get("cfo_ni")
    beneish = last.get("beneish_m")
    components = build_forensic_components(quality_rows, text_signals=text_signals)
    if latest_cfo_ni is not None:
        if latest_cfo_ni < 0.8:
            flags.append({"severity": "Bad", "title": "Weak cash conversion", "detail": f"Latest CFO/NI is {latest_cfo_ni:.2f}. Earnings are not converting well into cash."})
        elif latest_cfo_ni < 1.0:
            flags.append({"severity": "Warn", "title": "CFO below net income", "detail": f"Latest CFO/NI is {latest_cfo_ni:.2f}. Monitor whether this is temporary or structural."})
        else:
            flags.append({"severity": "Ok", "title": "Cash conversion acceptable", "detail": f"Latest CFO/NI is {latest_cfo_ni:.2f}. Cash conversion is not currently signaling distress."})
    accruals, ni = last.get("accruals"), last.get("net_income")
    if accruals is not None and ni not in (None, 0):
        accrual_ratio = accruals / abs(ni)
        if accrual_ratio > 0.4:
            flags.append({"severity": "Bad", "title": "High accrual dependence", "detail": "A large share of earnings is not backed by operating cash flow."})
        elif accrual_ratio > 0.2:
            flags.append({"severity": "Warn", "title": "Moderate accrual pressure", "detail": "Accruals deserve attention versus earnings quality."})
    latest_rg, latest_arg = last.get("revenue_growth"), last.get("ar_growth")
    if latest_rg is not None and latest_arg is not None and latest_arg > latest_rg + 0.10:
        flags.append({"severity": "Warn", "title": "Receivables outpacing sales", "detail": "Accounts receivable are growing faster than revenue. Review revenue recognition and collections."})
    dsri = last.get("dsri")
    if dsri is not None and dsri > 1.15:
        flags.append({"severity": "Warn", "title": "Elevated DSRI", "detail": f"DSRI is {dsri:.2f}. Sales may be converting more slowly into cash or receivables may be stretched."})
    if beneish is not None and beneish > -1.78:
        flags.append({"severity": "Bad", "title": "Beneish M-Score elevated", "detail": f"Latest Beneish M is {beneish:.2f}. This is a classic manipulation warning threshold."})
    elif beneish is not None and beneish > -2.20:
        flags.append({"severity": "Warn", "title": "Beneish M-Score watch zone", "detail": f"Latest Beneish M is {beneish:.2f}. Not decisive, but worth deeper reading."})

    if "margin divergence" in components.get("reason_tags", []):
        flags.append({
            "severity": "Warn",
            "title": "Revenue quality divergence",
            "detail": "Revenue is growing while net or CFO margin is deteriorating. Growth quality may be weakening.",
        })
    if "persistent earnings/cash mismatch" in components.get("reason_tags", []):
        flags.append({
            "severity": "Bad",
            "title": "Persistent earnings vs cash mismatch",
            "detail": "Net income and operating cash flow diverge across multiple periods, which raises earnings quality risk.",
        })
    elif "repeated earnings/cash mismatch" in components.get("reason_tags", []):
        flags.append({
            "severity": "Warn",
            "title": "Repeated earnings vs cash mismatch",
            "detail": "The NI vs CFO gap appears in multiple years, suggesting more than one-off timing noise.",
        })
    elif "one-off earnings/cash mismatch" in components.get("reason_tags", []):
        flags.append({
            "severity": "Warn",
            "title": "One-off earnings vs cash mismatch",
            "detail": "Current-period NI and CFO diverge, but persistence is limited so far.",
        })
    if any(tag.endswith("shock") for tag in components.get("reason_tags", [])):
        flags.append({
            "severity": "Warn",
            "title": "Working-capital shock",
            "detail": "Receivables/inventory/payables moved unusually versus history, which can absorb cash and pressure future margins.",
        })
    for anomaly in components.get("working_capital_anomalies", [])[:2]:
        metric = str(anomaly.get("metric", "working capital")).title()
        latest_change = safe_float(anomaly.get("latest_change"))
        detail = anomaly.get("why_it_matters", "Working-capital volatility can weaken cash quality.")
        flags.append({
            "severity": "Warn",
            "title": f"{metric} anomaly",
            "detail": f"Latest change is {fmt_value(latest_change)}. {detail}",
        })
    non_op = components.get("non_operating_analysis", {}) or {}
    if non_op.get("penalty", 0) > 0:
        support_ratio = safe_float(non_op.get("latest_support_ratio"))
        persistence = str(non_op.get("persistence", "none"))
        severity = "Bad" if (support_ratio or 0) >= 0.2 or persistence == "persistent" else "Warn"
        ratio_txt = f"{support_ratio:.2f}" if support_ratio is not None else "n/a"
        flags.append({
            "severity": severity,
            "title": "Non-operating / non-recurring earnings support",
            "detail": (
                f"Estimated non-core support is {ratio_txt}x net income with {persistence} pattern. "
                "Strong reported earnings may be less sustainable when support is non-operating."
            ),
        })
        if non_op.get("signal_count", 0):
            signal_labels = [str(s.get("signal")) for s in non_op.get("signals", [])[:3]]
            flags.append({
                "severity": "Warn",
                "title": "Potential one-time gain contributors",
                "detail": "Detected contributors: " + ", ".join(signal_labels) + ". These can reduce repeatability of earnings quality.",
            })
    if components.get("text_alignment_boost", 0) >= 3:
        flags.append({
            "severity": "Warn",
            "title": "Text and numeric risk alignment",
            "detail": "Filing language warning signals align with the financial anomalies, increasing confidence in the red flags.",
        })

    score = sum(_severity_to_points(f["severity"]) for f in flags)
    score += min(components.get("persistent_events", 0), 3)
    risk = "High" if score >= 5 else "Medium" if score >= 2 else "Low"
    return flags, risk, latest_cfo_ni, beneish, components


def build_cashflow_flags(cashflow_rows: list[dict[str, Any]], acq_metrics: dict[str, Any]) -> list[dict[str, str]]:
    flags: list[dict[str, str]] = []
    if not cashflow_rows:
        return flags
    last = cashflow_rows[-1]
    fcf = safe_float(last.get("fcf"))
    cfo = safe_float(last.get("cfo"))
    capex = safe_float(last.get("capex"))
    acquisitions = safe_float(last.get("acquisitions"))
    cfo_ni = safe_float(last.get("cfo_ni"))
    acq_to_cfo = safe_float(acq_metrics.get("acq_to_cfo"))
    if fcf is not None and fcf < 0:
        flags.append({"severity": "Warn", "title": "Negative free cash flow", "detail": "Operating cash flow does not cover capital expenditures in the latest period."})
    if acq_to_cfo is not None and acq_to_cfo > 0.5:
        flags.append({"severity": "Warn", "title": "Acquisitions large relative to CFO", "detail": "Acquisition cash outlays are a large share of operating cash flow. Watch for roll-up distortion."})
    if cfo is not None and acquisitions is not None and abs(acquisitions) > abs(cfo):
        flags.append({"severity": "Bad", "title": "Acquisitions exceed CFO", "detail": "The company is spending more on acquisitions than it generates in operating cash flow."})
    if cfo_ni is not None and cfo_ni > 1.8 and fcf is not None and fcf < 0:
        flags.append({"severity": "Warn", "title": "Strong CFO but weak free cash flow", "detail": "Cash conversion looks strong, but reinvestment needs or deal spending absorb the benefit."})
    if capex is not None and cfo is not None and abs(capex) > abs(cfo) * 0.8:
        flags.append({"severity": "Warn", "title": "Heavy capital intensity", "detail": "CapEx consumes most of operating cash flow. Check sustainability of free cash flow."})
    return flags


def build_scorecard(latest: dict[str, Any]) -> list[dict[str, str]]:
    return [
        {"metric": "CFO / Net Income", "value": fmt_value(latest.get("cfo_ni")), "comment": "Below 1.0 often deserves follow-up. Persistent sub-1.0 is more concerning."},
        {"metric": "Beneish M-Score", "value": fmt_value(latest.get("beneish_m")), "comment": "Above -1.78 is a classic warning line. Use as screening, not proof."},
        {"metric": "DSRI", "value": fmt_value(latest.get("dsri")), "comment": "Above 1.0 means receivables are rising faster than sales; above ~1.15 is stronger warning."},
        {"metric": "TATA", "value": fmt_value(latest.get("tata")), "comment": "Higher positive accruals relative to assets can indicate lower earnings quality."},
    ]


def fetch_latest_10k_text(ticker: str) -> str:
    filings = get_sec_recent_filings(ticker, max_items=6)
    target = next((f for f in filings if f.get("form") in {"10-K", "20-F"}), None)
    if not target:
        return ""
    try:
        r = requests.get(target["url"], headers=SEC_HEADERS, timeout=40)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "lxml")
        return re.sub(r"\s+", " ", soup.get_text(" ", strip=True))[:1200000]
    except Exception:
        return ""


def fetch_latest_annual_filing_table_frames(ticker: str, max_filings: int = 3) -> tuple[list[dict[str, Any]], list[dict[str, str]], list[str]]:
    filings = get_sec_recent_filings(ticker, max_items=max_filings + 3)
    annuals = [f for f in filings if f.get("form") in {"10-K", "20-F"}][:max_filings]
    frames: list[dict[str, Any]] = []
    used_sources: list[dict[str, str]] = []
    warnings: list[str] = []
    for filing in annuals:
        try:
            r = requests.get(filing["url"], headers=SEC_HEADERS, timeout=40)
            r.raise_for_status()
            soup = BeautifulSoup(r.text, "lxml")
            html_tables = soup.find_all("table")
            for table in html_tables:
                parsed_frames: list[pd.DataFrame] = []
                try:
                    parsed_frames.extend(pd.read_html(str(table)))
                except Exception:
                    pass
                robust_frame = _parse_html_table_to_frame(table)
                if robust_frame is not None:
                    parsed_frames.append(robust_frame)
                if not parsed_frames:
                    continue
                context_text = _table_context_text(table)
                for t in parsed_frames:
                    if t.empty or t.shape[0] < 2 or t.shape[1] < 2:
                        continue
                    frames.append({"frame": t, "context": context_text, "source_url": filing.get("url", "")})
            used_sources.append(filing)
        except Exception as exc:
            warnings.append(f"Table parsing failed for {filing.get('form', 'annual filing')} {filing.get('filing_date', '')}: {exc}")
            continue
    return frames, used_sources, warnings


def _normalize_bucket_label(raw: str) -> str:
    txt = str(raw or "").strip()
    low = txt.lower()
    region_map = [
        (r"(united states and canada|u\.s\.\s*&\s*canada|us\s*&\s*canada|ucan)", "UCAN"),
        (r"(united states|u\.s\.|^us$|north america)", "United States / North America"),
        (r"(emea|europe|middle east|africa)", "EMEA"),
        (r"(apac|asia[\s\-]?pacific|asia pacific)", "APAC"),
        (r"(latin america|latam|south america)", "LATAM"),
        (r"(china)", "China"),
        (r"(japan)", "Japan"),
        (r"(other|rest of world|international)", "International / Other"),
    ]
    for pattern, normalized in region_map:
        if re.search(pattern, low):
            return normalized
    return txt[:80]


def _normalize_segment_label(raw: str) -> str:
    txt = str(raw or "").strip()
    low = txt.lower()
    segment_map = [
        (r"(streaming)", "Streaming"),
        (r"(products?)", "Products"),
        (r"(cloud)", "Cloud"),
        (r"(advertising|ads?)", "Advertising"),
        (r"(services?)", "Services"),
        (r"(hardware)", "Hardware"),
        (r"(enterprise)", "Enterprise"),
        (r"(consumer)", "Consumer"),
    ]
    for pattern, normalized in segment_map:
        if re.search(pattern, low):
            return normalized
    return txt[:80]


def _extract_year_tokens(cols: list[str]) -> list[tuple[int, int]]:
    year_pos: list[tuple[int, int]] = []
    for idx, c in enumerate(cols):
        m = re.search(r"(20\d{2})", str(c))
        if m:
            year_pos.append((idx, int(m.group(1))))
    return year_pos


def _parse_numeric(v: Any) -> float | None:
    if v is None:
        return None
    txt = str(v).strip()
    if not txt:
        return None
    neg = "(" in txt and ")" in txt
    cleaned = re.sub(r"[^0-9.\-]", "", txt.replace(",", ""))
    if cleaned in {"", "-", ".", "-."}:
        return None
    try:
        val = float(cleaned)
        return -abs(val) if neg else val
    except Exception:
        return None


def _table_kind_for_geo_segment(cols: list[str], first_col_samples: list[str], context_text: str = "") -> str | None:
    low_cols = " ".join(str(c).lower() for c in cols)
    low_rows = " ".join(str(x).lower() for x in first_col_samples[:35])
    context = str(context_text or "").lower()
    hay = low_cols + " " + low_rows + " " + context
    geo_signals = [
        "geographic", "geography", "region", "international", "domestic", "united states", "north america",
        "us & canada", "u.s. & canada", "ucan", "emea", "latin america", "latam", "asia pacific", "apac",
        "revenues by geography", "revenue by region", "regional streaming revenues",
        "streaming revenue by region", "streaming revenues by region", "revenues by region",
    ]
    seg_signals = ["segment", "business unit", "operating segment", "by segment", "product line", "reportable segment"]
    geo_score = sum(1 for s in geo_signals if s in hay)
    seg_score = sum(1 for s in seg_signals if s in hay)
    if ("item 8" in hay or "notes to consolidated financial statements" in hay or "item 7" in hay) and geo_score >= 1:
        geo_score += 1
    if ("item 8" in hay or "notes to consolidated financial statements" in hay) and seg_score >= 1:
        seg_score += 1
    if geo_score >= 2 and geo_score >= seg_score:
        return "geography"
    if seg_score >= 1 and seg_score > geo_score:
        return "segment"
    return None


def _looks_revenue_related(text: str) -> bool:
    low = str(text or "").lower()
    return any(x in low for x in ["revenue", "revenues", "sales", "streaming", "net sales", "turnover"])


def _is_year_cell(raw: Any) -> bool:
    return bool(re.search(r"20\d{2}", str(raw or "")))


def _candidate_heading_signals() -> list[str]:
    return [
        "streaming revenues by region",
        "revenues by region",
        "geographic information",
        "segment information",
        "reportable segments",
        "international revenues",
        "domestic and international",
        "revenue by geography",
        "regional revenue",
    ]


def _score_table_candidate(df: pd.DataFrame, context_text: str) -> dict[str, Any]:
    cols = [str(c).strip() for c in df.columns]
    first_col = str(cols[0]) if cols else ""
    sample_labels = [str(x) for x in df[first_col].head(40).tolist()] if first_col in df.columns else []
    kind = _table_kind_for_geo_segment(cols, sample_labels, context_text=context_text)
    low_context = str(context_text or "").lower()
    heading_bonus = sum(1 for h in _candidate_heading_signals() if h in low_context)
    year_pos = _extract_year_tokens(cols)
    if len(year_pos) < 1 and len(df) > 0:
        first_row = df.iloc[0].tolist()
        year_pos = [(idx, int(m.group(1))) for idx, cell in enumerate(first_row) if (m := re.search(r"(20\d{2})", str(cell)))]
    labels = sample_labels[:30]
    region_hits = sum(1 for x in labels if _normalize_bucket_label(x) != x[:80])
    segment_hits = sum(1 for x in labels if _normalize_segment_label(x) != x[:80])
    numeric_cells = 0
    numeric_total = 0
    for _, row in df.head(50).iterrows():
        for pos, _ in year_pos[:4]:
            if pos >= len(row):
                continue
            numeric_total += 1
            if _parse_numeric(row.iloc[pos]) is not None:
                numeric_cells += 1
    numeric_consistency = (numeric_cells / numeric_total) if numeric_total else 0.0
    row_total_score = 0
    for _, row in df.iterrows():
        low_label = str(row.iloc[0]).lower().strip() if len(row) else ""
        if "total" not in low_label:
            continue
        vals = []
        for pos, _ in year_pos[:3]:
            v = _parse_numeric(row.iloc[pos] if pos < len(row) else None)
            if v and v > 0:
                vals.append(v)
        if vals:
            row_total_score = 1
            break
    revenue_context = 1 if _looks_revenue_related(" ".join(cols) + " " + " ".join(labels) + " " + context_text) else 0
    relevance_score = (
        (8 if kind == "geography" else 6 if kind == "segment" else 0)
        + min(region_hits, 4)
        + min(segment_hits, 3)
        + (4 if len(year_pos) >= 2 else 1 if len(year_pos) == 1 else 0)
        + int(numeric_consistency * 5)
        + heading_bonus
        + (3 if revenue_context else 0)
        + row_total_score
    )
    return {
        "kind": kind,
        "score": relevance_score,
        "year_pos": year_pos,
        "numeric_consistency": round(numeric_consistency, 3),
        "revenue_context": bool(revenue_context),
        "region_hits": region_hits,
        "segment_hits": segment_hits,
        "heading_bonus": heading_bonus,
    }


def _parse_html_table_to_frame(table: Any) -> pd.DataFrame | None:
    try:
        rows: list[list[str]] = []
        for tr in table.find_all("tr"):
            cells = tr.find_all(["th", "td"])
            if not cells:
                continue
            values = [re.sub(r"\s+", " ", c.get_text(" ", strip=True)) for c in cells]
            if any(v for v in values):
                rows.append(values)
        if len(rows) < 2:
            return None
        max_cols = max(len(r) for r in rows)
        norm_rows = [r + [""] * (max_cols - len(r)) for r in rows]
        header_row = norm_rows[0]
        has_year_header = sum(1 for c in header_row if re.search(r"20\d{2}", c)) >= 2
        if has_year_header:
            return pd.DataFrame(norm_rows[1:], columns=header_row)
        return pd.DataFrame(norm_rows)
    except Exception:
        return None


def _table_context_text(table: Any) -> str:
    context_nodes: list[str] = []
    caption = table.find("caption")
    if caption:
        cap = re.sub(r"\s+", " ", caption.get_text(" ", strip=True))
        if cap:
            context_nodes.append(cap)
    prev = table
    for _ in range(6):
        prev = prev.find_previous(["p", "div", "h1", "h2", "h3", "h4", "strong", "span"])
        if not prev:
            break
        txt = re.sub(r"\s+", " ", prev.get_text(" ", strip=True))
        if txt:
            context_nodes.append(txt)
    return " | ".join(context_nodes[:6])


def extract_geographic_and_segment_disclosures(ticker: str) -> dict[str, Any]:
    frames, sources, fetch_warnings = fetch_latest_annual_filing_table_frames(ticker)
    geo_raw: dict[tuple[int, str], float] = {}
    seg_raw: dict[tuple[int, str], float] = {}
    totals_by_year: dict[int, float] = {}
    warnings: list[str] = list(fetch_warnings)
    debug_notes: list[str] = []
    candidate_tables: list[dict[str, Any]] = []
    extraction_method = "none"
    extraction_failure_reason = ""
    for idx, frame_entry in enumerate(frames):
        try:
            df = frame_entry["frame"].copy()
            context_text = str(frame_entry.get("context", ""))
            df.columns = [str(c).strip() for c in df.columns]
            if df.empty or df.shape[1] < 2:
                continue
            score = _score_table_candidate(df, context_text)
            candidate_tables.append({
                "table_index": idx,
                "kind": score.get("kind") or "unknown",
                "score": score.get("score", 0),
                "numeric_consistency": score.get("numeric_consistency", 0),
                "revenue_context": score.get("revenue_context", False),
                "context_excerpt": context_text[:180],
            })
            kind = score.get("kind")
            if kind is None:
                continue
            year_pos = score.get("year_pos", [])
            if len(year_pos) < 1:
                continue
            if len(year_pos) == 1:
                debug_notes.append(f"Table {idx} had only one year column; accepted for partial extraction")
            for _, row in df.iterrows():
                raw_label = str(row.iloc[0]).strip()
                if len(raw_label) < 2:
                    continue
                low_label = raw_label.lower()
                if any(skip in low_label for skip in ["elimination", "consolidated", "intersegment"]):
                    continue
                is_total_row = "total" in low_label
                bucket = _normalize_bucket_label(raw_label) if kind == "geography" else _normalize_segment_label(raw_label)
                for pos, year in year_pos:
                    value = _parse_numeric(row.iloc[pos] if pos < len(row) else None)
                    if value is None or value <= 0:
                        continue
                    if is_total_row:
                        totals_by_year[year] = max(totals_by_year.get(year, 0.0), value)
                        continue
                    key = (year, bucket)
                    if kind == "geography":
                        geo_raw[key] = max(geo_raw.get(key, 0.0), value)
                    else:
                        seg_raw[key] = max(seg_raw.get(key, 0.0), value)
            if geo_raw or seg_raw:
                extraction_method = "table_first_scored"
        except Exception as exc:
            warnings.append(f"Disclosure parsing warning: {exc}")
            continue

    def _validate_raw(raw_map: dict[tuple[int, str], float], label_key: str) -> tuple[bool, str]:
        if not raw_map:
            return False, f"No {label_key} rows parsed"
        years = sorted({y for (y, _) in raw_map.keys()})
        labels = sorted({l for (_, l) in raw_map.keys()})
        if len(labels) < 2:
            return False, f"Only one {label_key} bucket found"
        if not years:
            return False, "No year labels detected"
        if not any(isinstance(y, int) and y > 1990 for y in years):
            return False, "No numeric filing year extracted"
        return True, "ok"

    geo_valid, geo_reason = _validate_raw(geo_raw, "region")
    seg_valid, seg_reason = _validate_raw(seg_raw, "segment")
    if not geo_valid:
        debug_notes.append(f"Geographic validation failed: {geo_reason}")
    if not seg_valid and seg_raw:
        debug_notes.append(f"Segment validation failed: {seg_reason}")
    if not geo_valid:
        geo_raw = {}
    if not seg_valid:
        seg_raw = {}

    if not geo_raw:
        try:
            filing_text = fetch_latest_10k_text(ticker)
            item8 = extract_section_excerpt(filing_text, "item 8", ["item 9", "item 9a"], max_len=8000)
            notes = extract_section_excerpt(filing_text, "notes to consolidated financial statements", ["item 9", "item 8"], max_len=8000)
            item7 = extract_section_excerpt(filing_text, "item 7", ["item 7a", "item 8"], max_len=8000)
            fallback_zone = " ".join([item8, notes, item7])
            region_hints = [
                "us & canada", "u.s. & canada", "united states and canada", "ucan", "united states", "domestic", "emea",
                "latin america", "latam", "asia pacific", "asia-pacific", "apac", "international",
            ]
            year_hints = [int(y) for y in re.findall(r"(20\d{2})", fallback_zone)]
            fallback_year = max(year_hints) if year_hints else 0
            for hint in region_hints:
                rx = re.compile(rf"{re.escape(hint)}[^0-9]{{0,30}}([0-9][0-9,]{{3,}}(?:\.\d+)?)", re.IGNORECASE)
                for m in rx.finditer(fallback_zone):
                    value = _parse_numeric(m.group(1))
                    if value is None or value <= 0:
                        continue
                    label = _normalize_bucket_label(hint)
                    key = (fallback_year, label)
                    geo_raw[key] = max(geo_raw.get(key, 0.0), value)
            if geo_raw:
                extraction_method = "text_fallback"
                debug_notes.append("Table parsing did not validate; text fallback produced geographic candidates")
            if geo_raw and all(year == 0 for year, _ in geo_raw.keys()):
                warnings.append("Geographic rows were extracted from text fallback without explicit year headers")
        except Exception as exc:
            warnings.append(f"Text fallback extraction failed: {exc}")

    if not geo_raw and sources:
        annual = sources[0]
        try:
            r = requests.get(annual["url"], headers=SEC_HEADERS, timeout=40)
            r.raise_for_status()
            soup = BeautifulSoup(r.text, "lxml")
            heading_patterns = _candidate_heading_signals()
            for node in soup.find_all(["h1", "h2", "h3", "h4", "p", "div", "strong"]):
                text = re.sub(r"\s+", " ", node.get_text(" ", strip=True)).lower()
                if not text:
                    continue
                if not any(p in text for p in heading_patterns):
                    continue
                table = node.find_next("table")
                if table is None:
                    continue
                frame = _parse_html_table_to_frame(table)
                if frame is None or frame.empty:
                    continue
                cols = [str(c).strip() for c in frame.columns]
                year_pos = _extract_year_tokens(cols)
                if len(year_pos) < 1:
                    continue
                first_col = str(cols[0])
                for _, row in frame.iterrows():
                    raw_label = str(row.iloc[0]).strip()
                    if len(raw_label) < 2:
                        continue
                    low_label = raw_label.lower()
                    if any(skip in low_label for skip in ["total", "consolidated", "elimination", "intersegment"]):
                        continue
                    for pos, year in year_pos:
                        value = _parse_numeric(row.iloc[pos] if pos < len(row) else None)
                        if value is None or value <= 0:
                            continue
                        label = _normalize_bucket_label(raw_label)
                        geo_raw[(year, label)] = max(geo_raw.get((year, label), 0.0), value)
                if geo_raw:
                    extraction_method = "heading_to_nearest_table"
                    warnings.append("Geographic rows recovered using heading-to-nearest-table fallback")
                    break
        except Exception as exc:
            warnings.append(f"Heading/table neighborhood fallback failed: {exc}")

    unique_geo_buckets = {bucket for (_, bucket) in geo_raw.keys()}
    years_with_multiple_regions = {
        year for year in {y for (y, _) in geo_raw.keys()} if sum(1 for (y, _) in geo_raw.keys() if y == year) >= 3
    }
    if geo_raw and (len(unique_geo_buckets) < 3 or not years_with_multiple_regions):
        warnings.append("Geographic extraction failed validation: insufficient regional rows in a revenue context")
        extraction_failure_reason = "Parsed rows did not have enough distinct regions or multi-region yearly coverage"
        geo_raw = {}

    def build_rows(raw_map: dict[tuple[int, str], float], label_key: str) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, Any]]:
        if not raw_map:
            return [], [], {"summary": "", "warnings": [f"{label_key.title()} extraction attempted but no valid rows were parsed"], "severity": "Warn"}
        rows = [{"year": y, label_key: bucket, "revenue": val} for (y, bucket), val in raw_map.items()]
        rows.sort(key=lambda x: (x["year"], str(x[label_key])))
        totals: dict[int, float] = {}
        for r in rows:
            totals[r["year"]] = totals.get(r["year"], 0.0) + float(r["revenue"])
        yoy_lookup: dict[tuple[int, str], float | None] = {}
        for r in rows:
            y = r["year"]
            prev = next((z for z in sorted(totals.keys()) if z < y), None)
            prev_val = raw_map.get((prev, r[label_key])) if prev is not None else None
            yoy_lookup[(y, r[label_key])] = ((r["revenue"] / prev_val) - 1.0) if prev_val not in (None, 0) else None
        full_rows = []
        mix_rows = []
        years = sorted(totals.keys())
        for r in rows:
            share = (r["revenue"] / totals[r["year"]]) if totals.get(r["year"]) else None
            enriched = {**r, "share_of_total": share, "yoy_growth": yoy_lookup.get((r["year"], r[label_key]))}
            full_rows.append(enriched)
            mix_rows.append({"year": r["year"], label_key: r[label_key], "share_of_total": share, "revenue": r["revenue"]})
        latest_year = years[-1]
        latest_rows = [r for r in full_rows if r["year"] == latest_year and r.get("share_of_total") is not None]
        dominant = max(latest_rows, key=lambda x: x["share_of_total"]) if latest_rows else None
        dominance = dominant.get("share_of_total") if dominant else None
        sev = "Warn" if dominance is not None and dominance >= 0.5 else "Ok"
        if dominance is not None and dominance >= 0.6:
            sev = "Bad"
        summary = (
            f"Latest {label_key} concentration: {dominant[label_key]} at {dominance * 100:.1f}% of disclosed revenue."
            if dominant and dominance is not None
            else f"{label_key.title()} disclosure parsed with {len(full_rows)} rows."
        )
        local_warnings: list[str] = []
        if len(years) < 2:
            local_warnings.append(f"Only one disclosed year was available for {label_key} analysis")
        if dominance is not None and dominance >= 0.6:
            local_warnings.append(f"Concentration risk is high: {dominant[label_key]} exceeds 60% of disclosed revenue")
        elif dominance is not None and dominance >= 0.5:
            local_warnings.append(f"Concentration risk watch: {dominant[label_key]} exceeds 50% of disclosed revenue")
        negative_yoy = [r for r in full_rows if r.get("yoy_growth") is not None and r["yoy_growth"] < -0.05]
        if negative_yoy:
            local_warnings.append("At least one disclosed bucket shows material YoY deterioration")
        if label_key == "region":
            for year in years:
                disclosed_total = totals.get(year)
                filing_total = totals_by_year.get(year)
                if disclosed_total and filing_total and filing_total > 0:
                    gap = abs(disclosed_total - filing_total) / filing_total
                    if gap > 0.2:
                        local_warnings.append(
                            f"Regional rows for {year} differ materially from disclosed total revenue (gap {gap * 100:.1f}%)"
                        )
        return full_rows, mix_rows, {"summary": summary, "warnings": local_warnings, "severity": sev, "dominant_bucket": dominant[label_key] if dominant else None}

    geo_rows, geo_mix, geo_summary = build_rows(geo_raw, "region")
    seg_rows, seg_mix, seg_summary = build_rows(seg_raw, "segment")
    if not extraction_failure_reason and not geo_rows and not seg_rows:
        extraction_failure_reason = "No high-confidence geography/segment table passed scoring + validation filters"
    if extraction_method == "none":
        extraction_method = "failed_all_methods"

    forensic_signals: list[dict[str, str]] = []
    if geo_rows:
        latest_year = max((r.get("year") for r in geo_rows if r.get("year")), default=None)
        latest_rows = [r for r in geo_rows if r.get("year") == latest_year]
        dom = max(latest_rows, key=lambda x: safe_float(x.get("share_of_total")) or -1) if latest_rows else None
        if dom and (safe_float(dom.get("share_of_total")) or 0) >= 0.6:
            forensic_signals.append({"title": "Concentration risk", "detail": f"{dom.get('region')} exceeds 60% of disclosed revenue", "severity": "Bad"})
        elif dom and (safe_float(dom.get("share_of_total")) or 0) >= 0.5:
            forensic_signals.append({"title": "Concentration watch", "detail": f"{dom.get('region')} exceeds 50% of disclosed revenue", "severity": "Warn"})
        slow_dom = dom and safe_float(dom.get("yoy_growth")) is not None and safe_float(dom.get("yoy_growth")) < -0.03
        if slow_dom:
            forensic_signals.append({"title": "Dominant market slowdown", "detail": f"{dom.get('region')} YoY trend is negative", "severity": "Warn"})
    if seg_rows:
        latest_year = max((r.get("year") for r in seg_rows if r.get("year")), default=None)
        latest_rows = [r for r in seg_rows if r.get("year") == latest_year]
        dom = max(latest_rows, key=lambda x: safe_float(x.get("share_of_total")) or -1) if latest_rows else None
        if dom and (safe_float(dom.get("share_of_total")) or 0) >= 0.6:
            forensic_signals.append({"title": "Segment dependence", "detail": f"{dom.get('segment')} is over 60% of disclosed segment revenue", "severity": "Bad"})

    if not sources:
        warnings.append("No annual filing tables were parsed from recent 10-K/20-F links")
    return {
        "geographic_revenue_rows": geo_rows,
        "geographic_mix_rows": geo_mix,
        "geographic_summary": geo_summary,
        "segment_revenue_rows": seg_rows,
        "segment_mix_rows": seg_mix,
        "segment_summary": seg_summary,
        "extraction_method_used": extraction_method,
        "extraction_debug_notes": debug_notes + warnings[:6],
        "candidate_tables_found": len(candidate_tables),
        "candidate_table_details": sorted(candidate_tables, key=lambda x: x.get("score", 0), reverse=True)[:20],
        "extraction_failure_reason": extraction_failure_reason,
        "geo_segment_forensic": {"signals": forensic_signals},
        "disclosure_warnings": warnings,
        "sources": sources,
    }


def extract_section_excerpt(text: str, section_label: str, next_candidates: list[str], max_len: int = 700) -> str:
    if not text:
        return ""
    lower = text.lower()
    start = lower.find(section_label.lower())
    if start < 0:
        return ""
    ends = [lower.find(c.lower(), start + len(section_label)) for c in next_candidates if lower.find(c.lower(), start + len(section_label)) > start]
    end = min(ends) if ends else min(len(text), start + 2500)
    excerpt = text[start:end].strip()
    return excerpt[:max_len] + ("..." if len(excerpt) > max_len else "")


def analyze_filing_text(text: str) -> tuple[list[dict[str, Any]], list[dict[str, str]]]:
    if not text:
        return [], []
    keyword_map = {
        "material weakness": ("Bad", "Internal control weakness disclosed."),
        "restatement": ("Bad", "Possible revision of prior reported numbers."),
        "restructure": ("Warn", "Recurring restructuring may distort comparability."),
        "restructuring": ("Warn", "Recurring restructuring may distort comparability."),
        "non-gaap": ("Warn", "Heavy adjusted-metric framing can require closer scrutiny."),
        "adjusted": ("Warn", "Frequent adjusted language may indicate earnings framing."),
        "one-time": ("Warn", "Management may classify recurring costs as exceptional."),
        "one time": ("Warn", "Management may classify recurring costs as exceptional."),
        "temporary": ("Warn", "Repeated temporary explanations deserve skepticism."),
        "impairment": ("Warn", "Asset write-down may affect true earnings power."),
        "goodwill": ("Warn", "Goodwill-related discussion may indicate acquisition quality issues."),
        "allowance": ("Warn", "Allowance language can matter for reserve adequacy."),
        "channel": ("Warn", "Channel references may matter for sell-in vs sell-through quality."),
        "litigation": ("Warn", "Legal exposure may affect earnings quality and reserves."),
        "non-operating": ("Warn", "Non-operating earnings support can be less sustainable."),
        "non recurring": ("Warn", "Non-recurring items may inflate current-period earnings."),
        "non-recurring": ("Warn", "Non-recurring items may inflate current-period earnings."),
        "gain on sale": ("Warn", "Gain on sale can boost earnings without core operating improvement."),
        "asset sale": ("Warn", "Asset sale gains are often non-repeatable earnings support."),
        "fair value": ("Warn", "Fair value gains can be volatile and less cash-backed."),
        "equity method": ("Warn", "Equity investment gains may not reflect core operating momentum."),
        "tax benefit": ("Warn", "Tax benefit can materially inflate reported net income."),
        "settlement gain": ("Warn", "Settlement gains are often one-time and non-operating."),
        "other income": ("Warn", "Other income increases may warrant reconciliation to core earnings."),
    }
    lower, signals, excerpts = text.lower(), [], []
    for key, (severity, interpretation) in keyword_map.items():
        hits = len(re.findall(re.escape(key), lower))
        if hits > 0:
            signals.append({"signal": key, "hits": hits, "interpretation": interpretation})
            if len(excerpts) < 8:
                idx = lower.find(key)
                start, end = max(0, idx - 180), min(len(text), idx + len(key) + 220)
                excerpt = text[start:end].strip()
                excerpts.append({"keyword": key, "severity": severity, "excerpt": excerpt[:420] + ("..." if len(excerpt) > 420 else "")})
    signals.sort(key=lambda x: x["hits"], reverse=True)
    return signals, excerpts


def build_filing_evidence_snapshot(text: str, filings: list[dict[str, str]] | None = None) -> dict[str, Any]:
    filings = filings or []
    source = filings[0] if filings else {}
    if not text:
        return {
            "source": source,
            "non_operating_evidence": [],
            "cost_reduction_evidence": [],
            "chart_rows": [],
            "summary": "No filing text available for evidence extraction.",
        }

    def collect_hits(terms: list[str], group: str, max_rows: int = 4) -> list[dict[str, Any]]:
        lower = text.lower()
        hits: list[dict[str, Any]] = []
        for term in terms:
            matches = list(re.finditer(re.escape(term), lower))
            if not matches:
                continue
            idx = matches[0].start()
            start, end = max(0, idx - 190), min(len(text), idx + len(term) + 240)
            excerpt = text[start:end].strip()
            hits.append(
                {
                    "group": group,
                    "keyword": term,
                    "hits": len(matches),
                    "excerpt": excerpt[:420] + ("..." if len(excerpt) > 420 else ""),
                    "label": term.replace("-", " ").title(),
                }
            )
        hits.sort(key=lambda x: x["hits"], reverse=True)
        return hits[:max_rows]

    non_operating_terms = [
        "non-operating", "non recurring", "non-recurring", "gain on sale", "asset sale",
        "fair value", "tax benefit", "settlement gain", "other income",
    ]
    cost_terms = [
        "cost reduction", "reduced costs", "cost savings", "efficiency program", "productivity",
        "restructuring", "headcount reduction", "lower operating expenses",
    ]
    non_operating_evidence = collect_hits(non_operating_terms, "non_operating")
    cost_reduction_evidence = collect_hits(cost_terms, "cost_reduction")
    chart_rows = sorted(
        [
            *[
                {"group": "non_operating", "label": r["label"], "hits": r["hits"]}
                for r in non_operating_evidence
            ],
            *[
                {"group": "cost_reduction", "label": r["label"], "hits": r["hits"]}
                for r in cost_reduction_evidence
            ],
        ],
        key=lambda x: x["hits"],
        reverse=True,
    )[:8]
    summary = (
        f"Detected {sum(r['hits'] for r in non_operating_evidence)} non-operating/non-recurring hits and "
        f"{sum(r['hits'] for r in cost_reduction_evidence)} cost-reduction/efficiency hits in the latest filing text."
    )
    return {
        "source": source,
        "non_operating_evidence": non_operating_evidence,
        "cost_reduction_evidence": cost_reduction_evidence,
        "chart_rows": chart_rows,
        "summary": summary,
    }


def build_reading_checklist() -> list[dict[str, str]]:
    return [
        {"title": "Item 7 - MD&A", "detail": "Is management blaming temporary factors every year? Are explanations specific or generic?"},
        {"title": "Revenue quality", "detail": "Check if receivables, deferred revenue, rebates, returns, and channel language support reported growth."},
        {"title": "Cash flow quality", "detail": "Did working capital absorb cash? Is operating cash supported by sustainable activity?"},
        {"title": "Special items", "detail": "Look for restructuring, asset sales, litigation releases, tax benefits, or recurring 'one-time' adjustments."},
        {"title": "Controls and audit signals", "detail": "Read Item 9A and note any weaknesses, remediations, unusual turnover, or vague control language."},
    ]


def compute_forensic_score(
    cfo_ni: float | None,
    beneish: float | None,
    flag_count: int,
    components: dict[str, Any] | None = None,
    text_signals: list[dict[str, Any]] | None = None,
) -> tuple[float, str, str]:
    """
    Explainable score where 100 is cleaner accounting quality.
    Persistence penalties are additive but bounded, so temporary anomalies do not dominate.
    """
    score = 100.0
    if cfo_ni is not None:
        if cfo_ni < 0.6:
            score -= 30
        elif cfo_ni < 0.8:
            score -= 20
        elif cfo_ni < 1.0:
            score -= 10
        elif cfo_ni > 1.2:
            score += 5
    if beneish is not None:
        if beneish > -1.78:
            score -= 35
        elif beneish > -2.20:
            score -= 18
        elif beneish < -2.8:
            score += 5
    score -= min(flag_count * 6, 30)
    if components:
        score -= min(safe_float(components.get("penalties")) or 0.0, 25.0)
        score -= min(safe_float(components.get("persistence_penalty")) or 0.0, 14.0)
        # Reward if signals are internally consistent and clean.
        if (safe_float(components.get("penalties")) or 0.0) <= 1 and (safe_float(components.get("persistent_events")) or 0.0) == 0:
            score += 4
    if text_signals:
        severe_words = {"material weakness", "restatement"}
        if any(str(s.get("signal", "")).lower() in severe_words for s in text_signals):
            score -= 4
    score = max(0.0, min(100.0, score))
    if score >= 80:
        return score, "Monitor / Possible long candidate", "Low"
    if score >= 60:
        return score, "Neutral / Needs deeper reading", "Medium"
    return score, "Avoid / High forensic attention", "High"


def classify_earnings_quality(
    forensic_score: float,
    risk_level: str,
    flags: list[dict[str, Any]],
    components: dict[str, Any] | None = None,
    text_signals: list[dict[str, Any]] | None = None,
) -> str:
    components = components or {}
    high_severity_flags = sum(1 for f in flags if str(f.get("severity")) == "Bad")
    persistent_events = int(safe_float(components.get("persistent_events")) or 0)
    severe_text_signals = {"material weakness", "restatement"}
    has_severe_text = any(str(s.get("signal", "")).lower() in severe_text_signals for s in (text_signals or []))

    if forensic_score < 35 or high_severity_flags >= 3 or persistent_events >= 2 or has_severe_text:
        return "HIGH RISK"
    if forensic_score < 55 or risk_level == "High" or high_severity_flags >= 2:
        return "LOW QUALITY"
    if forensic_score < 75 or risk_level == "Medium":
        return "MODERATE QUALITY"
    return "HIGH QUALITY"


def _component_signal_from_score(score: float) -> str:
    if score >= 75:
        return "LOW"
    if score >= 45:
        return "MEDIUM"
    return "HIGH"


def _map_severity_label(severity: str) -> str:
    return "HIGH" if severity == "Bad" else "MEDIUM" if severity == "Warn" else "LOW"


def _map_persistence_label(persistence: str) -> str:
    normalized = persistence.lower()
    if normalized == "one-off":
        return "temporary"
    return normalized if normalized in {"temporary", "repeated", "persistent"} else "temporary"


def build_grouped_signals(
    components: dict[str, Any],
    text_signals: list[dict[str, Any]],
    flags: list[dict[str, Any]],
) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = {
        "earnings_quality": [],
        "cash_flow_quality": [],
        "working_capital_risk": [],
        "non_operating_non_recurring_risk": [],
        "narrative_risk": [],
    }
    for flag in flags:
        title = str(flag.get("title", "")).lower()
        signal = {"signal": flag.get("title"), "severity": _map_severity_label(str(flag.get("severity", "Warn"))), "detail": flag.get("detail")}
        if "cash" in title or "accrual" in title or "cfo" in title:
            grouped["cash_flow_quality"].append(signal)
        if "revenue" in title or "earnings" in title or "beneish" in title:
            grouped["earnings_quality"].append(signal)
        if "receivable" in title or "working-capital" in title or "inventory" in title or "payables" in title or "dsri" in title:
            grouped["working_capital_risk"].append(signal)
        if "non-operating" in title or "one-time" in title or "gain" in title:
            grouped["non_operating_non_recurring_risk"].append(signal)

    mismatch = components.get("mismatch_classification")
    if mismatch and mismatch != "none":
        grouped["earnings_quality"].append(
            {
                "signal": "NI/CFO persistence pattern",
                "severity": "HIGH" if mismatch == "persistent" else "MEDIUM",
                "persistence": _map_persistence_label(str(mismatch)),
                "detail": "Mismatch between net income and operating cash flow is recurring.",
            }
        )
    for anomaly in components.get("working_capital_anomalies", []):
        grouped["working_capital_risk"].append(
            {
                "signal": f"{str(anomaly.get('metric', 'working capital')).title()} anomaly",
                "severity": "MEDIUM",
                "detail": anomaly.get("why_it_matters"),
            }
        )
    non_op = components.get("non_operating_analysis", {}) or {}
    if non_op.get("penalty", 0) > 0:
        grouped["non_operating_non_recurring_risk"].append(
            {
                "signal": "Earnings supported by non-core items",
                "severity": "HIGH" if (safe_float(non_op.get("latest_support_ratio")) or 0) >= 0.2 else "MEDIUM",
                "persistence": _map_persistence_label(str(non_op.get("persistence", "temporary"))),
                "detail": "Reported profit appears supported by non-recurring or non-operating income.",
            }
        )
    for sig in text_signals:
        grouped["narrative_risk"].append(
            {
                "signal": sig.get("signal"),
                "severity": "HIGH" if str(sig.get("signal", "")).lower() in {"material weakness", "restatement"} else "MEDIUM",
                "hits": sig.get("hits"),
                "detail": sig.get("interpretation"),
            }
        )
    return grouped


def build_forensic_breakdown(
    quality_rows: list[dict[str, Any]],
    components: dict[str, Any],
    flags: list[dict[str, Any]],
    text_signals: list[dict[str, Any]],
    forensic_score: float,
) -> dict[str, Any]:
    latest = quality_rows[-1] if quality_rows else {}
    latest_cfo_ni = safe_float(latest.get("cfo_ni"))
    latest_beneish = safe_float(latest.get("beneish_m"))
    dsri = safe_float(latest.get("dsri"))
    mismatch = str(components.get("mismatch_classification", "none"))
    non_op = components.get("non_operating_analysis", {}) or {}
    non_op_ratio = safe_float(non_op.get("latest_support_ratio"))
    mismatch_component_score = 85.0
    if mismatch == "persistent":
        mismatch_component_score = 30.0
    elif mismatch == "repeated":
        mismatch_component_score = 50.0
    elif mismatch == "one-off":
        mismatch_component_score = 65.0

    revenue_score = 88.0 if "margin divergence" not in components.get("reason_tags", []) else 42.0
    cashflow_score = 90.0 if (latest_cfo_ni is not None and latest_cfo_ni >= 1.0) else 65.0 if (latest_cfo_ni is not None and latest_cfo_ni >= 0.8) else 38.0
    working_capital_score = 88.0 if not components.get("working_capital_anomalies") and (dsri is None or dsri <= 1.05) else 60.0 if (dsri is None or dsri <= 1.15) else 35.0
    non_operating_score = 90.0 if non_op.get("penalty", 0) <= 0 else 62.0 if (non_op_ratio or 0) < 0.2 else 30.0
    text_score = 90.0 if not text_signals else 60.0 if len(text_signals) <= 4 else 40.0
    if latest_beneish is not None and latest_beneish > -1.78:
        revenue_score = min(revenue_score, 32.0)
        cashflow_score = min(cashflow_score, 35.0)

    score_components = [
        {"name": "revenue_quality", "score": revenue_score, "severity": _component_signal_from_score(revenue_score), "persistence": _map_persistence_label("repeated" if "margin divergence" in components.get("reason_tags", []) else "temporary")},
        {"name": "cash_flow_quality", "score": cashflow_score, "severity": _component_signal_from_score(cashflow_score), "persistence": _map_persistence_label(mismatch if mismatch != "none" else "temporary")},
        {"name": "working_capital", "score": working_capital_score, "severity": _component_signal_from_score(working_capital_score), "persistence": _map_persistence_label("repeated" if len(components.get("working_capital_anomalies", [])) >= 2 else "temporary")},
        {"name": "non_operating_income", "score": non_operating_score, "severity": _component_signal_from_score(non_operating_score), "persistence": _map_persistence_label(str(non_op.get("persistence", "temporary")))},
        {"name": "text_signals", "score": text_score, "severity": _component_signal_from_score(text_score), "persistence": _map_persistence_label("persistent" if len(text_signals) >= 6 else "repeated" if len(text_signals) >= 2 else "temporary")},
        {"name": "ni_cfo_persistence", "score": mismatch_component_score, "severity": _component_signal_from_score(mismatch_component_score), "persistence": _map_persistence_label(mismatch if mismatch != "none" else "temporary")},
    ]
    component_summary = {
        c["name"]: {"score": c["score"], "severity": c["severity"], "persistence": c["persistence"]}
        for c in score_components
    }
    return {
        "overall_score": forensic_score,
        "score_components": score_components,
        "component_summary": component_summary,
        "persistent_event_count": int(safe_float(components.get("persistent_events")) or 0),
    }


def build_red_flag_highlights(flags: list[dict[str, Any]], components: dict[str, Any]) -> list[dict[str, Any]]:
    weighted = []
    for flag in flags:
        sev = str(flag.get("severity", "Warn"))
        score = 3 if sev == "Bad" else 2 if sev == "Warn" else 1
        title = str(flag.get("title", ""))
        persistence = "temporary"
        if "Persistent" in title or "persistent" in title:
            persistence = "persistent"
            score += 2
        elif "Repeated" in title or "repeated" in title:
            persistence = "repeated"
            score += 1
        weighted.append(
            {
                "title": title,
                "detail": flag.get("detail"),
                "severity": _map_severity_label(sev),
                "persistence": persistence,
                "importance_score": score,
            }
        )
    non_op = components.get("non_operating_analysis", {}) or {}
    if non_op.get("penalty", 0) > 0:
        weighted.append(
            {
                "title": "Non-operating earnings support",
                "detail": "Part of net income appears to come from non-recurring or non-operating sources.",
                "severity": "HIGH" if (safe_float(non_op.get("latest_support_ratio")) or 0) >= 0.2 else "MEDIUM",
                "persistence": _map_persistence_label(str(non_op.get("persistence", "temporary"))),
                "importance_score": 5 if str(non_op.get("persistence")) == "persistent" else 4,
            }
        )
    weighted.sort(key=lambda x: (-x["importance_score"], x["title"]))
    return weighted[:6]


def build_quality_explanation(
    quality_classification: str,
    breakdown: dict[str, Any],
    red_flags: list[dict[str, Any]],
) -> str:
    top = red_flags[:3]
    if not top:
        return f"{quality_classification}: reported earnings quality currently screens as stable with no major forensic red flags."
    reasons = []
    for item in top:
        title = str(item.get("title", "")).lower()
        if "revenue" in title:
            reasons.append("revenue growth appears to diverge from margin quality")
        elif "cash" in title or "cfo" in title:
            reasons.append("operating cash flow does not fully support accounting earnings")
        elif "working" in title or "receivable" in title or "inventory" in title or "dsri" in title:
            reasons.append("working capital trends suggest weaker collections or cash absorption")
        elif "non-operating" in title or "one-time" in title:
            reasons.append("non-recurring or non-operating items may be supporting earnings")
    if not reasons:
        reasons = [str(item.get("title", "key forensic issue")).lower() for item in top]
    concise_reasons = ", ".join(list(dict.fromkeys(reasons))[:3])
    return f"{quality_classification}: {concise_reasons}."


def build_decision_table(
    latest_metrics: dict[str, Any],
    flags: list[dict[str, str]],
    risk: str,
    company_name: str = "",
    components: dict[str, Any] | None = None,
) -> list[dict[str, str]]:
    cfo_ni = safe_float(latest_metrics.get("cfo_ni"))
    beneish = safe_float(latest_metrics.get("beneish_m"))
    score, verdict, derived_risk = compute_forensic_score(cfo_ni, beneish, len(flags), components=components)
    persistence_note = "Repeated anomaly clusters increase score impact." if (components or {}).get("persistent_events", 0) else "No strong multi-year persistence penalty."
    return [
        {"metric": "Forensic score", "value": f"{score:.1f}/100", "comment": "Higher is cleaner. This is a screening score, not proof."},
        {"metric": "Verdict", "value": verdict, "comment": "Use this only after checking Item 7, Item 8, and controls."},
        {"metric": "Risk alignment", "value": f"API risk: {risk} / Score risk: {derived_risk}", "comment": "If these disagree, read the filing more carefully."},
        {"metric": "Main focus", "value": "Cash conversion, working capital, acquisitions", "comment": f"Start with receivables, free cash flow, and roll-up intensity. {persistence_note}"},
    ]


def build_watchlist_snapshot() -> list[dict[str, Any]]:
    rows = []
    for ticker in DEFAULT_WATCHLIST:
        try:
            quality_rows, _, _, _, _ = build_quality_rows(ticker)
            flags, risk, cfo_ni, beneish, components = generate_flags(quality_rows)
            score, verdict, _ = compute_forensic_score(cfo_ni, beneish, len(flags), components=components)
            quality_classification = classify_earnings_quality(score, risk, flags, components=components, text_signals=[])
            red_flag_highlights = build_red_flag_highlights(flags, components)
            rows.append(
                {
                    "ticker": ticker,
                    "score": score,
                    "risk": risk,
                    "verdict": verdict,
                    "cfo_ni": cfo_ni,
                    "beneish": beneish,
                    "flag_count": len(flags),
                    "quality_classification": quality_classification,
                    "red_flag_count": len(red_flag_highlights),
                }
            )
        except Exception:
            rows.append(
                {
                    "ticker": ticker,
                    "score": 0.0,
                    "risk": "NA",
                    "verdict": "NA",
                    "cfo_ni": None,
                    "beneish": None,
                    "flag_count": 0,
                    "quality_classification": "HIGH RISK",
                    "red_flag_count": 0,
                }
            )
    rows.sort(key=lambda x: (-x["score"], x["flag_count"]))
    for i, row in enumerate(rows, start=1):
        row["rank"] = i
    return rows


def _safe_pct_change(curr: float | None, prev: float | None) -> float | None:
    if curr is None or prev in (None, 0):
        return None
    return ((curr / prev) - 1.0) * 100.0


def _series_signal(change: float | None, positive_label: str = "Improving", negative_label: str = "Deteriorating") -> str:
    if change is None:
        return "Stable"
    if change > 0.7:
        return positive_label
    if change < -0.7:
        return negative_label
    return "Stable"


def history_last_and_prev(symbol: str, period: str = "3mo", value_col: str = "Close") -> tuple[float | None, float | None]:
    try:
        hist = yf.Ticker(symbol).history(period=period, auto_adjust=False)
        vals = hist.get(value_col, pd.Series(dtype=float)).dropna()
        if len(vals) < 2:
            return None, None
        return safe_float(vals.iloc[-1]), safe_float(vals.iloc[-2])
    except Exception:
        return None, None


def get_fred_indicator(series_id: str) -> tuple[float | None, float | None]:
    if FRED_API_KEY:
        try:
            resp = requests.get(
                "https://api.stlouisfed.org/fred/series/observations",
                params={
                    "series_id": series_id,
                    "api_key": FRED_API_KEY,
                    "file_type": "json",
                    "sort_order": "desc",
                    "limit": 6,
                },
                timeout=12,
            )
            resp.raise_for_status()
            observations = resp.json().get("observations", [])
            values = [safe_float(obs.get("value")) for obs in observations]
            values = [v for v in values if v is not None]
            if len(values) < 2:
                return (values[0] if values else None), None
            return values[0], values[1]
        except Exception:
            pass
    try:
        url = f"https://fred.stlouisfed.org/graph/fredgraph.csv?id={series_id}"
        df = pd.read_csv(url)
        if "VALUE" not in df.columns:
            return None, None
        ser = pd.to_numeric(df["VALUE"], errors="coerce").dropna()
        if len(ser) < 2:
            return (safe_float(ser.iloc[-1]) if len(ser) else None), None
        return safe_float(ser.iloc[-1]), safe_float(ser.iloc[-2])
    except Exception:
        return None, None


def get_fred_yoy_pct(series_id: str, periods_back: int = 12) -> tuple[float | None, float | None]:
    if FRED_API_KEY:
        try:
            resp = requests.get(
                "https://api.stlouisfed.org/fred/series/observations",
                params={
                    "series_id": series_id,
                    "api_key": FRED_API_KEY,
                    "file_type": "json",
                    "sort_order": "asc",
                },
                timeout=12,
            )
            resp.raise_for_status()
            observations = resp.json().get("observations", [])
            ser = pd.Series([safe_float(obs.get("value")) for obs in observations], dtype=float).dropna()
            yoy = (ser / ser.shift(periods_back) - 1.0) * 100.0
            yoy = yoy.dropna()
            if len(yoy) < 2:
                return (safe_float(yoy.iloc[-1]) if len(yoy) else None), None
            return safe_float(yoy.iloc[-1]), safe_float(yoy.iloc[-2])
        except Exception:
            pass
    try:
        url = f"https://fred.stlouisfed.org/graph/fredgraph.csv?id={series_id}"
        df = pd.read_csv(url)
        if "VALUE" not in df.columns:
            return None, None
        ser = pd.to_numeric(df["VALUE"], errors="coerce").dropna()
        yoy = (ser / ser.shift(periods_back) - 1.0) * 100.0
        yoy = yoy.dropna()
        if len(yoy) < 2:
            return (safe_float(yoy.iloc[-1]) if len(yoy) else None), None
        return safe_float(yoy.iloc[-1]), safe_float(yoy.iloc[-2])
    except Exception:
        return None, None


def build_market_block(symbol_map: dict[str, tuple[str, str]], pct: bool = True) -> tuple[list[dict[str, Any]], list[str]]:
    rows, notices = [], []
    for name, (symbol, interp) in symbol_map.items():
        last, prev = history_last_and_prev(symbol)
        if last is None:
            notices.append(f"{name} unavailable")
        change = _safe_pct_change(last, prev) if pct else (None if last is None or prev is None else last - prev)
        rows.append({
            "name": name,
            "symbol": symbol,
            "last": last,
            "prev": prev,
            "change_5d_pct": change if pct else None,
            "change_5d": None if pct else change,
            "signal": _series_signal(change),
            "interpretation": interp,
        })
    return rows, notices


def build_macro_dashboard_payload() -> dict[str, Any]:
    notices: list[str] = []
    equities_map = {
        "S&P 500": ("^GSPC", "US large-cap risk benchmark."),
        "Nasdaq 100": ("^NDX", "Growth and duration-sensitive equity benchmark."),
        "Dow Jones": ("^DJI", "Blue-chip industrial benchmark."),
        "Russell 2000": ("^RUT", "Small-cap domestic growth sensitivity."),
        "VIX": ("^VIX", "Higher implied volatility reflects stress."),
        "Euro Stoxx 50": ("^STOXX50E", "Eurozone blue-chip equity pulse."),
        "DAX": ("^GDAXI", "German cyclical and exporter exposure."),
        "CAC 40": ("^FCHI", "French large-cap benchmark."),
        "FTSE 100": ("^FTSE", "UK large-cap/global commodity tilt."),
        "IBEX 35": ("^IBEX", "Spanish equity benchmark."),
        "Nikkei 225": ("^N225", "Japan equity benchmark."),
        "TOPIX": ("^TOPX", "Broader Japanese market breadth."),
        "Hang Seng": ("^HSI", "Hong Kong / China risk sentiment."),
        "Shanghai Composite": ("000001.SS", "Mainland China benchmark."),
        "Shenzhen Composite": ("399001.SZ", "China growth/tech sensitivity."),
        "Sensex": ("^BSESN", "India benchmark."),
        "Nifty 50": ("^NSEI", "India large-cap benchmark."),
        "ASX 200": ("^AXJO", "Australia benchmark with commodity tilt."),
        "EM Equities (EEM)": ("EEM", "Emerging markets risk appetite proxy."),
    }
    rates_market_map = {
        "US 2Y Treasury": ("^UST2Y", "Short-end policy sensitivity."),
        "US 10Y Treasury": ("^TNX", "Long-end growth/inflation expectations."),
        "US 30Y Treasury": ("^TYX", "Long-duration term premium signal."),
        "German 10Y Bund": ("^DE10YB", "Eurozone sovereign core benchmark."),
        "UK 10Y Gilt": ("^UK10YT", "UK sovereign benchmark."),
        "Japan 10Y JGB": ("^JP10Y", "Japan rate control and inflation shift."),
    }
    fx_map = {
        "DXY": ("DX-Y.NYB", "A rising dollar tightens global financial conditions."),
        "EUR/USD": ("EURUSD=X", "Core developed-market FX gauge."),
        "USD/JPY": ("JPY=X", "Rate differential and BoJ regime sensitivity."),
        "GBP/USD": ("GBPUSD=X", "Sterling growth/rates signal."),
        "USD/CNY": ("CNY=X", "China currency and policy pressure proxy."),
        "USD/CHF": ("CHF=X", "Safe-haven flow gauge."),
    }
    commodities_map = {
        "WTI Crude": ("CL=F", "Oil strength can reflect demand and inflation pressure."),
        "Brent Crude": ("BZ=F", "Global oil benchmark."),
        "Natural Gas": ("NG=F", "Energy volatility and supply stress signal."),
        "Gold": ("GC=F", "Real yield and risk-hedge proxy."),
        "Silver": ("SI=F", "Mixed industrial/precious metal signal."),
        "Copper": ("HG=F", "Global industrial activity pulse."),
        "Corn": ("ZC=F", "Agricultural inflation proxy."),
    }
    crypto_map = {"BTC": ("BTC-USD", "Alternative risk appetite proxy."), "ETH": ("ETH-USD", "Broader crypto-beta gauge.")}
    credit_map = {
        "High Yield (HYG)": ("HYG", "High-yield spread/risk proxy."),
        "Investment Grade (LQD)": ("LQD", "Investment-grade credit condition proxy."),
        "VIX": ("^VIX", "Equity volatility stress proxy."),
    }
    equities, eq_notices = build_market_block(equities_map, pct=True)
    rates, rate_notices = build_market_block(rates_market_map, pct=False)
    fx, fx_notices = build_market_block(fx_map, pct=True)
    commodities, com_notices = build_market_block(commodities_map, pct=True)
    crypto, cry_notices = build_market_block(crypto_map, pct=True)
    credit, cred_notices = build_market_block(credit_map, pct=True)
    notices.extend(eq_notices + rate_notices + fx_notices + com_notices + cry_notices + cred_notices)

    # Reliable FRED fallback for sovereign curve and key macro.
    dgs2, dgs2_prev = get_fred_indicator("DGS2")
    dgs10, dgs10_prev = get_fred_indicator("DGS10")
    dgs30, dgs30_prev = get_fred_indicator("DGS30")
    t102y, t102y_prev = get_fred_indicator("T10Y2Y")
    if any(x is not None for x in [dgs2, dgs10, dgs30]):
        rates = [
            {"name": "US 2Y Treasury", "last": dgs2, "prior": dgs2_prev, "change_5d": None if dgs2 is None or dgs2_prev is None else dgs2 - dgs2_prev, "interpretation": "Short-end policy sensitivity."},
            {"name": "US 10Y Treasury", "last": dgs10, "prior": dgs10_prev, "change_5d": None if dgs10 is None or dgs10_prev is None else dgs10 - dgs10_prev, "interpretation": "Long-end growth/inflation expectations."},
            {"name": "US 30Y Treasury", "last": dgs30, "prior": dgs30_prev, "change_5d": None if dgs30 is None or dgs30_prev is None else dgs30 - dgs30_prev, "interpretation": "Long-duration term premium signal."},
            {"name": "10Y-2Y Curve Spread", "last": t102y, "prior": t102y_prev, "change_5d": None if t102y is None or t102y_prev is None else t102y - t102y_prev, "interpretation": "Inversion typically signals growth concerns."},
        ]

    econ_specs = [
        {"name": "CPI (YoY)", "series_id": "CPIAUCSL", "interpretation": "Inflation level; direction matters for policy.", "calc": "yoy_pct", "unit": "pct"},
        {"name": "Core CPI", "series_id": "CPILFESL", "interpretation": "Underlying inflation pressure.", "calc": "yoy_pct", "unit": "pct"},
        {"name": "Unemployment", "series_id": "UNRATE", "interpretation": "Labor slack and recession risk signal."},
        {"name": "Payrolls", "series_id": "PAYEMS", "interpretation": "Labor demand pulse."},
        {"name": "GDP", "series_id": "GDP", "interpretation": "Aggregate growth backdrop."},
        {"name": "ISM Manufacturing PMI", "series_id": "NAPM", "interpretation": "Factory momentum signal."},
        {"name": "Services PMI", "series_id": "NAPMS", "interpretation": "Service-sector activity."},
        {"name": "Retail Sales", "series_id": "RSAFS", "interpretation": "Consumer demand proxy."},
        {"name": "Housing Permits", "series_id": "PERMIT", "interpretation": "Forward housing activity indicator."},
        {"name": "Consumer Confidence", "series_id": "UMCSENT", "interpretation": "Household sentiment proxy."},
        {"name": "Fed Funds Rate", "series_id": "FEDFUNDS", "interpretation": "Federal Reserve policy stance."},
        {"name": "ECB Deposit Rate", "series_id": "ECBDFR", "interpretation": "ECB policy stance."},
        {"name": "BoE Policy Rate", "series_id": "IR3TIB01GBM156N", "interpretation": "BoE policy proxy."},
        {"name": "BoJ Policy Rate", "series_id": "IRSTCI01JPM156N", "interpretation": "BoJ policy proxy."},
    ]
    econ_rows = []
    for spec in econ_specs:
        name = spec["name"]
        series_id = spec["series_id"]
        interp = spec["interpretation"]
        calc = spec.get("calc")
        latest, prior = get_fred_yoy_pct(series_id) if calc == "yoy_pct" else get_fred_indicator(series_id)
        if latest is None:
            notices.append(f"{name} unavailable")
        trend = "stable"
        if latest is not None and prior is not None:
            if latest > prior:
                trend = "rising"
            elif latest < prior:
                trend = "falling"
        econ_rows.append({"name": name, "latest": latest, "prior": prior, "trend": trend, "interpretation": interp, "unit": spec.get("unit")})

    risk_score = sum((x.get("change_5d_pct") or 0) for x in equities if x["name"] in {"S&P 500", "Nasdaq 100", "EM Equities (EEM)"}) / 3.0
    vix_row = next((x for x in equities if x["name"] == "VIX"), {})
    vix_change = vix_row.get("change_5d_pct")
    inflation_row = next((x for x in econ_rows if x["name"] == "CPI (YoY)"), {})
    growth_row = next((x for x in econ_rows if x["name"] == "GDP"), {})
    curve_row = next((x for x in rates if x["name"] == "10Y-2Y Curve Spread"), {})
    credit_score = sum((x.get("change_5d_pct") or 0) for x in credit if x["name"] in {"High Yield (HYG)", "Investment Grade (LQD)"}) / 2.0

    regimes = {
        "global_risk_regime": {"label": "risk-on" if risk_score > 0 else "risk-off", "class": "ok" if risk_score > 0 else "bad"},
        "growth_regime": {"label": "growth strengthening" if (growth_row.get("trend") == "rising") else ("growth weakening" if growth_row.get("trend") == "falling" else "stable"), "class": "ok" if growth_row.get("trend") == "rising" else ("bad" if growth_row.get("trend") == "falling" else "neutral")},
        "inflation_regime": {"label": "inflation pressure rising" if inflation_row.get("trend") == "rising" else ("inflation pressure easing" if inflation_row.get("trend") == "falling" else "stable"), "class": "bad" if inflation_row.get("trend") == "rising" else ("ok" if inflation_row.get("trend") == "falling" else "neutral")},
        "liquidity_regime": {"label": "dollar tightening" if (next((x.get("change_5d_pct") for x in fx if x["name"] == "DXY"), 0) or 0) > 0 else "liquidity easing", "class": "warn" if (next((x.get("change_5d_pct") for x in fx if x["name"] == "DXY"), 0) or 0) > 0 else "ok"},
        "market_stress_regime": {"label": "deteriorating" if (vix_change or 0) > 0 else "improving", "class": "bad" if (vix_change or 0) > 0 else "ok"},
    }

    summary_sentence = (
        f"Global growth is {regimes['growth_regime']['label'].replace('growth ', '')} while risk assets are "
        f"{'resilient' if regimes['global_risk_regime']['label']=='risk-on' else 'fragile'}. "
        f"Inflation is {('easing' if regimes['inflation_regime']['label'].endswith('easing') else 'still a pressure point')}, "
        f"and curve spread at {fmt_value(curve_row.get('last'))} keeps recession debate active."
    )

    news_macro = get_news_google_rss("global economy inflation central bank recession risk", limit=8)
    news_markets = get_news_google_rss("equities bonds yields commodities market volatility", limit=8)
    news_geo = get_news_google_rss("geopolitics sanctions conflict oil supply market risk", limit=8)

    return {
        "summary": {
            "regimes": regimes,
            "human_summary": summary_sentence,
            "overview_interpretation": "Risk, growth, inflation, liquidity and stress are synthesized so major macro inflections are visible in one decision panel.",
            "risk_on_off_composite": risk_score,
            "credit_composite": credit_score,
        },
        "markets": {
            "equities": equities,
            "interpretation": "Equity breadth and volatility jointly indicate whether the market is absorbing or rejecting macro risk."
        },
        "rates": {"major": rates},
        "fx": {"major": fx},
        "rates_fx_interpretation": "Higher yields and a stronger dollar can tighten global conditions and pressure duration-sensitive assets.",
        "commodities": {
            "major": commodities,
            "interpretation": "Oil and copper indicate growth and inflation cross-currents; gold helps track defensive demand."
        },
        "credit_risk": {
            "major": credit,
            "interpretation": "Credit ETFs and VIX together provide a practical stress proxy when full spread data is delayed."
        },
        "economy": {
            "indicators": econ_rows,
            "interpretation": "Economic releases are displayed with trend direction to clarify whether growth and inflation momentum is improving or deteriorating."
        },
        "crypto": {"major": crypto},
        "news": {"macro": news_macro[:5], "markets": news_markets[:5], "geopolitical": news_geo[:5]},
        "runtime_notices": sorted(set(notices))[:18],
    }


def build_screener_snapshot(mode: str = "core") -> list[dict[str, Any]]:
    rows = []
    for ticker in choose_universe(mode):
        try:
            quality_rows, latest_metrics, cashflow_rows, acq_metrics, _ = build_quality_rows(ticker)
            flags, risk, cfo_ni, beneish, components = generate_flags(quality_rows)
            cashflow_flags = build_cashflow_flags(cashflow_rows, acq_metrics)
            score, _, _ = compute_forensic_score(cfo_ni, beneish, len(flags) + len(cashflow_flags), components=components)
            quality_classification = classify_earnings_quality(score, risk, flags, components=components, text_signals=[])
            red_flag_highlights = build_red_flag_highlights(flags + cashflow_flags, components)
            dsri = safe_float(latest_metrics.get("dsri"))
            reasons = []
            if cfo_ni is not None and cfo_ni < 0.9:
                reasons.append("weak cash conversion")
            if beneish is not None and beneish > -2.2:
                reasons.append("Beneish watch")
            if dsri is not None and dsri > 1.1:
                reasons.append("AR pressure")
            if safe_float(acq_metrics.get("acq_to_cfo")) is not None and safe_float(acq_metrics.get("acq_to_cfo")) > 0.5:
                reasons.append("acquisition heavy")
            if (components.get("non_operating_analysis", {}) or {}).get("penalty", 0) > 0:
                reasons.append("non-operating support")
            if components.get("mismatch_classification") == "persistent":
                reasons.append("persistent NI/CFO divergence")
            reasons.extend(components.get("reason_tags", [])[:3])
            reasons.extend(components.get("major_reasons", [])[:2])
            if reasons:
                distress_rank = (
                    (100.0 - score)
                    + min(len(cashflow_flags) * 3, 12)
                    + min(sum(_severity_to_points(f["severity"]) for f in flags), 12)
                    + min(safe_float(components.get("penalties")) or 0.0, 15.0)
                    + min(safe_float(components.get("persistence_penalty")) or 0.0, 10.0)
                    + min((safe_float(components.get("persistent_events")) or 0.0) * 3.0, 9.0)
                    + min(safe_float(components.get("text_alignment_boost")) or 0.0, 6.0)
                )
                rows.append({
                    "ticker": ticker,
                    "score": score,
                    "risk": risk,
                    "quality_classification": quality_classification,
                    "red_flag_count": len(red_flag_highlights),
                    "cfo_ni": cfo_ni,
                    "beneish": beneish,
                    "dsri": dsri,
                    "reason": ", ".join(list(dict.fromkeys(reasons))[:3]),
                    "short_reason": ", ".join(list(dict.fromkeys(reasons))[:1]) if reasons else "no major anomaly cluster",
                    "distress_rank": distress_rank,
                })
        except Exception:
            continue
    rows.sort(key=lambda x: (-x.get("distress_rank", 0), x.get("score", 100)))
    for row in rows:
        row.pop("distress_rank", None)
    return rows[:20]


def build_peer_snapshot(base_ticker: str) -> list[dict[str, Any]]:
    try:
        base_info = yf.Ticker(base_ticker).info or {}
        base_sector = base_info.get("sector")
    except Exception:
        base_sector = None
    peers = []
    candidates = get_sp500_tickers(limit=80)
    fallback = [t for t in DEFAULT_WATCHLIST if t != base_ticker]
    for ticker in candidates + fallback:
        if ticker == base_ticker:
            continue
        try:
            y = yf.Ticker(ticker)
            info = y.info or {}
            if base_sector and info.get("sector") != base_sector:
                continue
            price_info = get_price_chart_and_info(ticker, period="1y")
            quality_rows, _, _, _, _ = build_quality_rows(ticker)
            flags, _, cfo_ni, beneish, _ = generate_flags(quality_rows)
            peers.append({"ticker": ticker, "price": price_info.get("price"), "market_cap": price_info.get("market_cap"), "cfo_ni": cfo_ni, "beneish": beneish, "flag_count": len(flags)})
            if len(peers) >= 6:
                break
        except Exception:
            continue
    if not peers:
        for ticker in fallback[:6]:
            try:
                price_info = get_price_chart_and_info(ticker, period="1y")
                quality_rows, _, _, _, _ = build_quality_rows(ticker)
                flags, _, cfo_ni, beneish, _ = generate_flags(quality_rows)
                peers.append({"ticker": ticker, "price": price_info.get("price"), "market_cap": price_info.get("market_cap"), "cfo_ni": cfo_ni, "beneish": beneish, "flag_count": len(flags)})
            except Exception:
                continue
    return peers


@app.route("/")
def home() -> str:
    init_db()
    return render_template_string(APP_HTML)


@app.route("/macro")
def macro_dashboard() -> str:
    return render_template_string(MACRO_HTML)


@app.route("/api/analyze")
def analyze() -> Any:
    ticker = (request.args.get("ticker") or "MRK").strip().upper()
    period = (request.args.get("period") or "1y").strip().lower()
    if period not in {"1y", "2y", "5y"}:
        period = "1y"
    if not re.fullmatch(r"[A-Z.\-]{1,10}", ticker):
        return jsonify({"error": "Invalid ticker format."}), 400
    try:
        info = get_price_chart_and_info(ticker, period=period)
        quality_rows, latest_metrics, cashflow_rows, acq_metrics, working_capital_rows = build_quality_rows(ticker)
        filings = get_sec_recent_filings(ticker)
        macro = get_macro_snapshot()
        news = get_news_google_rss(f"{ticker} stock OR global economy OR inflation OR interest rates", limit=8)
        scorecard = build_scorecard(latest_metrics)
        reading_checklist = build_reading_checklist()
        filing_text = fetch_latest_10k_text(ticker)
        text_signals, text_excerpts = analyze_filing_text(filing_text)
        geo_segment = extract_geographic_and_segment_disclosures(ticker)
        flags, risk, latest_cfo_ni, beneish, forensic_components = generate_flags(quality_rows, text_signals=text_signals)
        item7_excerpt = extract_section_excerpt(filing_text, "item 7", ["item 7a", "item 8"])
        item9a_excerpt = extract_section_excerpt(filing_text, "item 9a", ["item 9b", "item 10"])
        filing_evidence = build_filing_evidence_snapshot(filing_text, filings)
        watchlist = build_watchlist_snapshot()
        peers = build_peer_snapshot(ticker)
        cfo_ni_trend = get_cfo_ni_trend_from_quality_rows(quality_rows)
        dsri_fcf_trend = get_dsri_fcf_trend(quality_rows, cashflow_rows)
        trend_signals = build_trend_signals(cfo_ni_trend, dsri_fcf_trend)
        geo_rows = geo_segment.get("geographic_revenue_rows", []) or []
        seg_rows = geo_segment.get("segment_revenue_rows", []) or []
        if geo_rows and quality_rows:
            latest_company_growth = safe_float(quality_rows[-1].get("revenue_growth"))
            latest_geo_year = max((r.get("year") for r in geo_rows if r.get("year") is not None), default=None)
            dominant_geo = None
            if latest_geo_year is not None:
                latest_geo_rows = [r for r in geo_rows if r.get("year") == latest_geo_year]
                if latest_geo_rows:
                    dominant_geo = max(latest_geo_rows, key=lambda x: safe_float(x.get("share_of_total")) or -1)
            dom_yoy = safe_float(dominant_geo.get("yoy_growth")) if dominant_geo else None
            if latest_company_growth is not None and dom_yoy is not None and latest_company_growth > 0.05 and dom_yoy < 0:
                geo_segment["geographic_summary"]["warnings"].append(
                    "Company-level growth is positive while the dominant region is shrinking; investigate narrative mismatch."
                )
                geo_segment["geographic_summary"]["severity"] = "Warn"
        if seg_rows:
            latest_seg_year = max((r.get("year") for r in seg_rows if r.get("year") is not None), default=None)
            if latest_seg_year is not None:
                latest_seg_rows = [r for r in seg_rows if r.get("year") == latest_seg_year]
                dominant_seg = max(latest_seg_rows, key=lambda x: safe_float(x.get("share_of_total")) or -1) if latest_seg_rows else None
                if dominant_seg and (safe_float(dominant_seg.get("share_of_total")) or 0) >= 0.6:
                    geo_segment["segment_summary"]["warnings"].append(
                        f"Segment concentration is high in {dominant_seg.get('segment')} (>60% of disclosed segment revenue)."
                    )
                    geo_segment["segment_summary"]["severity"] = "Bad"
        decision_table = build_decision_table(latest_metrics, flags, risk, info["long_name"], components=forensic_components)
        forensic_score, _, _ = compute_forensic_score(
            latest_cfo_ni,
            beneish,
            len(flags),
            components=forensic_components,
            text_signals=text_signals,
        )
        earnings_quality_classification = classify_earnings_quality(
            forensic_score,
            risk,
            flags,
            components=forensic_components,
            text_signals=text_signals,
        )
        forensic_breakdown = build_forensic_breakdown(
            quality_rows,
            forensic_components,
            flags,
            text_signals,
            forensic_score,
        )
        grouped_signals = build_grouped_signals(forensic_components, text_signals, flags)
        acquisition_table = [
            {"metric": "Latest acquisitions cash", "value": fmt_value(abs(acq_metrics.get("latest_acquisitions")) if safe_float(acq_metrics.get("latest_acquisitions")) is not None else None), "comment": "Cash outflow for acquisitions. Displayed as absolute size for readability."},
            {"metric": "Average acquisitions (4 periods)", "value": fmt_value(abs(acq_metrics.get("avg_acquisitions")) if safe_float(acq_metrics.get("avg_acquisitions")) is not None else None), "comment": "Useful to spot serial acquirers and roll-up patterns."},
            {"metric": "Acquisitions / CFO", "value": fmt_value(acq_metrics.get("acq_to_cfo")), "comment": "High ratios mean acquisitions materially affect the cash story."},
            {"metric": "Latest free cash flow", "value": fmt_value(acq_metrics.get("latest_fcf")), "comment": "FCF = CFO - CapEx. Check whether strength remains after reinvestment."},
        ]
        cashflow_flags = build_cashflow_flags(cashflow_rows, acq_metrics)
        red_flag_highlights = build_red_flag_highlights(flags + cashflow_flags, forensic_components)
        quality_explanation = build_quality_explanation(earnings_quality_classification, forensic_breakdown, red_flag_highlights)
        top_gainers, top_losers = get_top_gainers_losers()
        return jsonify({
            "ticker": ticker,
            "company_name": info["long_name"],
            "company_summary": info["summary"][:340] + ("..." if len(info["summary"]) > 340 else ""),
            "price": info["price"],
            "market_cap": info["market_cap"],
            "price_chart": info["chart"],
            "working_capital_rows": working_capital_rows,
            "quality_rows": quality_rows,
            "cfo_ni_trend": cfo_ni_trend,
            "dsri_fcf_trend": dsri_fcf_trend,
            "trend_signals": trend_signals,
            "cashflow_rows": cashflow_rows,
            "acquisition_table": acquisition_table,
            "cashflow_flags": cashflow_flags,
            "flags": flags,
            "risk_level": risk,
            "latest_cfo_ni": latest_cfo_ni,
            "beneish_m": beneish,
            "filings": filings,
            "macro": macro,
            "news": news,
            "scorecard": scorecard,
            "reading_checklist": reading_checklist,
            "text_signals": text_signals,
            "text_excerpts": text_excerpts,
            "item7_excerpt": item7_excerpt,
            "item9a_excerpt": item9a_excerpt,
            "filing_evidence": filing_evidence,
            "geographic_revenue_rows": geo_segment.get("geographic_revenue_rows", []),
            "geographic_mix_rows": geo_segment.get("geographic_mix_rows", []),
            "geographic_summary": geo_segment.get("geographic_summary", {}),
            "segment_revenue_rows": geo_segment.get("segment_revenue_rows", []),
            "segment_mix_rows": geo_segment.get("segment_mix_rows", []),
            "segment_summary": geo_segment.get("segment_summary", {}),
            "extraction_method_used": geo_segment.get("extraction_method_used", ""),
            "extraction_debug_notes": geo_segment.get("extraction_debug_notes", []),
            "candidate_tables_found": geo_segment.get("candidate_tables_found", 0),
            "candidate_table_details": geo_segment.get("candidate_table_details", []),
            "extraction_failure_reason": geo_segment.get("extraction_failure_reason", ""),
            "geo_segment_forensic": geo_segment.get("geo_segment_forensic", {}),
            "decision_table": decision_table,
            "forensic_score": forensic_score,
            "forensic_components": forensic_components,
            "forensic_summary": "; ".join(forensic_components.get("major_reasons", [])[:3]) if forensic_components.get("major_reasons") else "No additional persistent forensic anomalies detected.",
            "forensic_breakdown": forensic_breakdown,
            "earnings_quality_classification": earnings_quality_classification,
            "earnings_quality_explanation": quality_explanation,
            "red_flag_highlights": red_flag_highlights,
            "grouped_signals": grouped_signals,
            "new_signals_added": [
                "Revenue growth vs net-margin deterioration",
                "Revenue growth vs CFO-margin deterioration",
                "NI vs CFO mismatch classified as one-off/repeated/persistent",
                "Working-capital shock detection (AR/inventory/payables)",
                "Persistence weighting across multi-year anomalies",
                "Text and numeric anomaly alignment weighting",
                "Non-operating and non-recurring earnings support detection",
                "Gain-on-sale / fair-value / tax-benefit support checks",
            ],
            "peers": peers,
            "watchlist": watchlist,
            "top_gainers": top_gainers,
            "top_losers": top_losers,
        })
    except Exception as e:
        return jsonify({"error": f"Failed to analyze {ticker}: {e}"}), 500


@app.route("/api/screener")
def screener() -> Any:
    mode = (request.args.get("universe") or "core").strip().lower()
    return jsonify({"rows": build_screener_snapshot(mode)})


@app.route("/api/macro/dashboard")
def macro_dashboard_api() -> Any:
    return jsonify(build_macro_dashboard_payload())


@app.route("/api/save", methods=["POST"])
def save_analysis() -> Any:
    init_db()
    data = request.get_json(silent=True) or {}
    ticker = (data.get("ticker") or "").strip().upper()
    if not ticker:
        return jsonify({"error": "Ticker missing"}), 400
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO saved_analyses (saved_at, ticker, risk, cfo_ni, beneish, notes) VALUES (datetime('now'), ?, ?, ?, ?, ?)",
        (ticker, data.get("risk"), data.get("cfo_ni"), data.get("beneish"), data.get("notes")),
    )
    conn.commit()
    conn.close()
    return jsonify({"ok": True})


@app.route("/api/saved")
def list_saved() -> Any:
    init_db()
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.execute("SELECT saved_at, ticker, risk, cfo_ni, beneish, notes FROM saved_analyses ORDER BY id DESC LIMIT 50")
    rows = [dict(r) for r in cur.fetchall()]
    conn.close()
    return jsonify({"rows": rows})


@app.route("/api/history")
def ticker_history() -> Any:
    init_db()
    ticker = (request.args.get("ticker") or "").strip().upper()
    if not ticker:
        return jsonify({"rows": []})
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.execute("SELECT saved_at, risk, cfo_ni, beneish, notes FROM saved_analyses WHERE ticker = ? ORDER BY id DESC LIMIT 30", (ticker,))
    rows = [dict(r) for r in cur.fetchall()]
    conn.close()
    return jsonify({"rows": rows})


@app.route("/api/export/saved")
def export_saved_csv() -> Response:
    init_db()
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql_query("SELECT saved_at, ticker, risk, cfo_ni, beneish, notes FROM saved_analyses ORDER BY id DESC", conn)
    conn.close()
    csv_data = df.to_csv(index=False)
    return Response(csv_data, mimetype="text/csv", headers={"Content-Disposition": "attachment; filename=saved_analyses.csv"})


@app.route("/api/export/watchlist")
def export_watchlist_csv() -> Response:
    rows = build_watchlist_snapshot()
    df = pd.DataFrame(rows)
    csv_data = df.to_csv(index=False)
    return Response(csv_data, mimetype="text/csv", headers={"Content-Disposition": "attachment; filename=watchlist_snapshot.csv"})


if __name__ == "__main__":
    init_db()
    app.run(host="127.0.0.1", port=5000, debug=False, use_reloader=False)
