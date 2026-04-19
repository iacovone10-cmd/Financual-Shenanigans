
from __future__ import annotations

import json
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
    @media (max-width: 1380px) {
      .topbar { grid-template-columns: 1fr 1fr 1fr; }
      .hero { grid-template-columns: 1fr; }
      .stats { grid-template-columns: repeat(2, 1fr); }
      .heat-grid { grid-template-columns: repeat(3, 1fr); }
      .hero-screener { grid-template-columns: 1fr; }
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
      <div class="panel span-8"><div class="section-title"><h2>Suspicious companies screener</h2><span class="muted">Click a row to load full analysis</span></div><div style="overflow:auto;"><table><thead><tr><th>Rank</th><th>Ticker</th><th>Score</th><th>Risk</th><th>CFO/NI</th><th>Beneish</th><th>DSRI</th><th>Reason</th></tr></thead><tbody id="screenerBody"></tbody></table></div></div>
      <div class="panel span-4"><div class="section-title"><h2>10-K access</h2><span class="muted">Fast navigation</span></div><div id="filingsBox" class="flags"></div></div>

      <div class="panel span-8"><div class="section-title"><h2>Price & trend</h2><span class="muted">Market view</span></div><div id="priceChart" style="height:360px;"></div></div>
      <div class="panel span-4"><div class="section-title"><h2>Forensic summary</h2><span class="muted">Instant triage</span></div><div class="flags" id="flagsContainer"></div></div>

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

  function renderWatchlist(rows) {
    document.getElementById('watchlistBody').innerHTML = rows.map(r => `<tr><td>${r.rank}</td><td>${r.ticker}</td><td>${numFmt(r.score)}</td><td>${r.risk}</td><td>${r.verdict}</td><td>${numFmt(r.cfo_ni)}</td><td>${numFmt(r.beneish)}</td><td>${r.flag_count}</td></tr>`).join('');
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
    body.innerHTML = rows.map((r, idx) => `<tr class="clickable-row" onclick="loadTickerFromScreener('${r.ticker}')"><td>${idx + 1}</td><td>${r.ticker}</td><td>${numFmt(r.score)}</td><td>${r.risk || '-'}</td><td>${numFmt(r.cfo_ni)}</td><td>${numFmt(r.beneish)}</td><td>${numFmt(r.dsri)}</td><td>${r.reason || '-'}</td></tr>`).join('');
    document.getElementById('screenCount').textContent = String(rows.length || 0);
    document.getElementById('screenWorst').textContent = rows.length ? numFmt(rows[0].score) : '-';
    const text = rows.flatMap(r => (r.reason || '').split(',').map(x => x.trim())).filter(Boolean);
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
      setRiskBadge(data.risk_level, `Risk level: ${data.risk_level}`);
      renderHeatmap(data);
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
    if rev is None or ni is None or cfo is None:
        return [], {}, [], {}, []

    df = pd.DataFrame({
        "revenue": rev, "net_income": ni, "cfo": cfo, "capex": capex, "acquisitions": acquisitions,
        "ar": ar, "inventory": inv, "payables": payables, "current_assets": ca, "ppe": ppe,
        "total_assets": ta, "dep": dep, "sga": sga, "current_liabilities": cl, "long_term_debt": ltd,
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
    if wc_shocks >= 2:
        comps["persistence_penalty"] += 3
        comps["persistent_events"] += 1

    # 4) Persistence scaling: repeated anomalies should weigh more than isolated anomalies.
    if rev_margin_divergence and mismatch_years >= 2:
        comps["persistence_penalty"] += 4
        comps["persistent_events"] += 1

    # 5) Text + numeric alignment boost (when both point to same direction).
    text_hits = {str(s.get("signal", "")).lower() for s in (text_signals or [])}
    alignment = 0
    if rev_margin_divergence and ({"allowance", "channel", "one-time", "one time"} & text_hits):
        alignment += 3
    if mismatch_years >= 2 and ({"material weakness", "restatement", "temporary"} & text_hits):
        alignment += 4
    if wc_shocks > 0 and ({"restructure", "restructuring", "litigation"} & text_hits):
        alignment += 2
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
            rows.append({"ticker": ticker, "score": score, "risk": risk, "verdict": verdict, "cfo_ni": cfo_ni, "beneish": beneish, "flag_count": len(flags)})
        except Exception:
            rows.append({"ticker": ticker, "score": 0.0, "risk": "NA", "verdict": "NA", "cfo_ni": None, "beneish": None, "flag_count": 0})
    rows.sort(key=lambda x: (-x["score"], x["flag_count"]))
    for i, row in enumerate(rows, start=1):
        row["rank"] = i
    return rows


def build_screener_snapshot(mode: str = "core") -> list[dict[str, Any]]:
    rows = []
    for ticker in choose_universe(mode):
        try:
            quality_rows, latest_metrics, cashflow_rows, acq_metrics, _ = build_quality_rows(ticker)
            flags, risk, cfo_ni, beneish, components = generate_flags(quality_rows)
            cashflow_flags = build_cashflow_flags(cashflow_rows, acq_metrics)
            score, _, _ = compute_forensic_score(cfo_ni, beneish, len(flags) + len(cashflow_flags), components=components)
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
            reasons.extend(components.get("reason_tags", [])[:3])
            if reasons:
                distress_rank = (
                    (100.0 - score)
                    + min(len(cashflow_flags) * 3, 12)
                    + min(sum(_severity_to_points(f["severity"]) for f in flags), 12)
                    + min(safe_float(components.get("persistence_penalty")) or 0.0, 10.0)
                    + min(safe_float(components.get("text_alignment_boost")) or 0.0, 6.0)
                )
                rows.append({
                    "ticker": ticker,
                    "score": score,
                    "risk": risk,
                    "cfo_ni": cfo_ni,
                    "beneish": beneish,
                    "dsri": dsri,
                    "reason": ", ".join(list(dict.fromkeys(reasons))[:3]),
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
        flags, risk, latest_cfo_ni, beneish, forensic_components = generate_flags(quality_rows, text_signals=text_signals)
        item7_excerpt = extract_section_excerpt(filing_text, "item 7", ["item 7a", "item 8"])
        item9a_excerpt = extract_section_excerpt(filing_text, "item 9a", ["item 9b", "item 10"])
        watchlist = build_watchlist_snapshot()
        peers = build_peer_snapshot(ticker)
        cfo_ni_trend = get_cfo_ni_trend_from_quality_rows(quality_rows)
        dsri_fcf_trend = get_dsri_fcf_trend(quality_rows, cashflow_rows)
        trend_signals = build_trend_signals(cfo_ni_trend, dsri_fcf_trend)
        decision_table = build_decision_table(latest_metrics, flags, risk, info["long_name"], components=forensic_components)
        forensic_score, _, _ = compute_forensic_score(
            latest_cfo_ni,
            beneish,
            len(flags),
            components=forensic_components,
            text_signals=text_signals,
        )
        acquisition_table = [
            {"metric": "Latest acquisitions cash", "value": fmt_value(abs(acq_metrics.get("latest_acquisitions")) if safe_float(acq_metrics.get("latest_acquisitions")) is not None else None), "comment": "Cash outflow for acquisitions. Displayed as absolute size for readability."},
            {"metric": "Average acquisitions (4 periods)", "value": fmt_value(abs(acq_metrics.get("avg_acquisitions")) if safe_float(acq_metrics.get("avg_acquisitions")) is not None else None), "comment": "Useful to spot serial acquirers and roll-up patterns."},
            {"metric": "Acquisitions / CFO", "value": fmt_value(acq_metrics.get("acq_to_cfo")), "comment": "High ratios mean acquisitions materially affect the cash story."},
            {"metric": "Latest free cash flow", "value": fmt_value(acq_metrics.get("latest_fcf")), "comment": "FCF = CFO - CapEx. Check whether strength remains after reinvestment."},
        ]
        cashflow_flags = build_cashflow_flags(cashflow_rows, acq_metrics)
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
            "decision_table": decision_table,
            "forensic_score": forensic_score,
            "forensic_components": forensic_components,
            "forensic_summary": "; ".join(forensic_components.get("major_reasons", [])[:3]) if forensic_components.get("major_reasons") else "No additional persistent forensic anomalies detected.",
            "new_signals_added": [
                "Revenue growth vs net-margin deterioration",
                "Revenue growth vs CFO-margin deterioration",
                "Persistent net income vs CFO divergence",
                "Working-capital shock detection (AR/inventory/payables)",
                "Persistence weighting across multi-year anomalies",
                "Text and numeric anomaly alignment weighting",
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
