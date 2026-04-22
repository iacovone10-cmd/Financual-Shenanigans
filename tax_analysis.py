from __future__ import annotations

import statistics
from typing import Any


def _safe_float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        if isinstance(value, str) and not value.strip():
            return None
        out = float(value)
        if out != out:
            return None
        return out
    except Exception:
        return None


def _clamp(value: float, lower: float, upper: float) -> float:
    return max(lower, min(upper, value))


def analyze_tax_quality(
    quality_rows: list[dict[str, Any]],
    statutory_rate: float = 0.21,
) -> dict[str, Any]:
    tax_rows: list[dict[str, Any]] = []
    etr_values: list[float] = []
    cash_gap_hits = 0
    low_etr_hits = 0
    deferred_heavy_hits = 0
    negative_tax_positive_earnings_hits = 0
    reasons: list[str] = []
    notes_normal: list[str] = []
    notes_suspicious: list[str] = []

    for row in quality_rows[-6:]:
        pre_tax_income = _safe_float(row.get("pretax_income"))
        tax_expense = _safe_float(row.get("income_tax_expense"))
        cash_taxes_paid = _safe_float(row.get("cash_taxes_paid"))
        deferred_tax_total = _safe_float(row.get("deferred_tax_total"))
        deferred_tax_assets = _safe_float(row.get("deferred_tax_assets"))
        deferred_tax_liabilities = _safe_float(row.get("deferred_tax_liabilities"))

        etr = None
        if pre_tax_income not in (None, 0) and tax_expense is not None:
            etr = tax_expense / pre_tax_income
            etr_values.append(etr)

        cash_to_expense = None
        if cash_taxes_paid is not None and tax_expense not in (None, 0):
            cash_to_expense = abs(cash_taxes_paid) / abs(tax_expense)

        deferred_ratio_to_pretax = None
        if deferred_tax_total is not None and pre_tax_income not in (None, 0):
            deferred_ratio_to_pretax = abs(deferred_tax_total) / abs(pre_tax_income)

        tax_rows.append(
            {
                "period": row.get("period"),
                "pre_tax_income": pre_tax_income,
                "income_tax_expense": tax_expense,
                "cash_taxes_paid": cash_taxes_paid,
                "deferred_tax_total": deferred_tax_total,
                "deferred_tax_assets": deferred_tax_assets,
                "deferred_tax_liabilities": deferred_tax_liabilities,
                "etr": etr,
                "cash_tax_to_expense": cash_to_expense,
                "deferred_ratio_to_pretax": deferred_ratio_to_pretax,
            }
        )

        if etr is not None and pre_tax_income is not None and pre_tax_income > 0:
            if etr < 0.10:
                low_etr_hits += 1
            if etr <= 0.02 and tax_expense is not None and tax_expense <= 0:
                negative_tax_positive_earnings_hits += 1
        if pre_tax_income is not None and pre_tax_income > 0 and cash_to_expense is not None and cash_to_expense < 0.6:
            cash_gap_hits += 1
        if deferred_ratio_to_pretax is not None and deferred_ratio_to_pretax > 0.2:
            deferred_heavy_hits += 1

    etr_volatility = statistics.pstdev(etr_values) if len(etr_values) >= 2 else None
    latest = tax_rows[-1] if tax_rows else {}
    latest_etr = _safe_float(latest.get("etr"))
    latest_cash_gap = _safe_float(latest.get("cash_tax_to_expense"))
    latest_deferred_ratio = _safe_float(latest.get("deferred_ratio_to_pretax"))

    etr_deviation = None
    if latest_etr is not None:
        etr_deviation = statutory_rate - latest_etr
    etr_score = _clamp((abs(etr_deviation or 0.0) / 0.21) * 100.0, 0.0, 100.0)
    cash_gap_score = _clamp(((1.0 - (latest_cash_gap if latest_cash_gap is not None else 1.0)) / 0.6) * 100.0, 0.0, 100.0)
    deferred_score = _clamp(((latest_deferred_ratio or 0.0) / 0.35) * 100.0, 0.0, 100.0)
    divergence_score = round(0.4 * etr_score + 0.35 * cash_gap_score + 0.25 * deferred_score, 1)
    risk_score = round(
        _clamp(
            divergence_score
            + (18 if low_etr_hits >= 2 else 0)
            + (15 if (etr_volatility or 0.0) > 0.08 else 0)
            + (12 if cash_gap_hits >= 2 else 0)
            + (12 if deferred_heavy_hits >= 2 else 0)
            + (20 if negative_tax_positive_earnings_hits >= 1 else 0),
            0.0,
            100.0,
        ),
        1,
    )
    risk_level = "High" if risk_score >= 70 else "Medium" if risk_score >= 40 else "Low"

    if low_etr_hits >= 2 or (latest_etr is not None and latest_etr < statutory_rate * 0.6):
        reasons.append("Low effective tax rate vs statutory baseline")
        notes_suspicious.append("ETR is materially below the statutory anchor; reconcile permanent differences and tax planning effects.")
    elif latest_etr is not None:
        notes_normal.append("ETR is not far from statutory baseline after considering expected mix effects.")

    if (etr_volatility or 0.0) > 0.08:
        reasons.append("Highly volatile effective tax rate")
        notes_suspicious.append("ETR swings are large year-to-year; check footnote reconciling items and one-off tax impacts.")
    elif etr_volatility is not None:
        notes_normal.append("ETR volatility is limited across recent periods.")

    if negative_tax_positive_earnings_hits >= 1:
        reasons.append("Negative or near-zero tax despite positive pre-tax earnings")
        notes_suspicious.append("Positive pre-tax income paired with near-zero or negative tax may indicate non-recurring tax benefits.")

    if cash_gap_hits >= 2 or (latest_cash_gap is not None and latest_cash_gap < 0.6):
        reasons.append("High earnings but low cash taxes")
        notes_suspicious.append("Cash taxes paid are persistently below tax expense; assess deferrals and uncertain tax positions.")
    elif latest_cash_gap is not None and latest_cash_gap >= 0.8:
        notes_normal.append("Cash taxes are broadly aligned with booked tax expense.")

    if deferred_heavy_hits >= 2 or (latest_deferred_ratio is not None and latest_deferred_ratio > 0.2):
        reasons.append("Heavy reliance on deferred tax benefits")
        notes_suspicious.append("Deferred tax movements are large relative to pre-tax income.")

    if divergence_score >= 55:
        reasons.append("Potential book vs tax divergence")

    if not reasons:
        reasons.append("Tax profile appears broadly normal")

    guidance = [
        "Check income tax footnote for effective tax rate reconciliation.",
        "Look for deferred tax reconciliation and drivers of DTA/DTL changes.",
        "Review valuation allowance changes and management justification.",
        "Inspect cash flow statement line for income taxes paid.",
    ]

    return {
        "statutory_rate_used": statutory_rate,
        "tax_rows": tax_rows,
        "etr_trend": {"periods": [r.get("period") for r in tax_rows], "values": [r.get("etr") for r in tax_rows]},
        "cash_vs_reported_trend": {
            "periods": [r.get("period") for r in tax_rows],
            "tax_expense": [r.get("income_tax_expense") for r in tax_rows],
            "cash_taxes_paid": [r.get("cash_taxes_paid") for r in tax_rows],
            "cash_tax_to_expense": [r.get("cash_tax_to_expense") for r in tax_rows],
        },
        "deferred_tax_trend": {
            "periods": [r.get("period") for r in tax_rows],
            "deferred_tax_total": [r.get("deferred_tax_total") for r in tax_rows],
            "deferred_tax_assets": [r.get("deferred_tax_assets") for r in tax_rows],
            "deferred_tax_liabilities": [r.get("deferred_tax_liabilities") for r in tax_rows],
        },
        "book_tax_divergence_score": divergence_score,
        "tax_quality_score": round(100.0 - risk_score, 1),
        "tax_risk_level": risk_level,
        "reason_codes": reasons,
        "interpretation": {
            "normal_signals": notes_normal,
            "suspicious_signals": notes_suspicious,
            "ten_k_checks": guidance,
        },
    }
