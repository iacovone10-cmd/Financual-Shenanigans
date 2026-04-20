import unittest
from unittest.mock import patch

from forensic_dashboard_app import (
    build_macro_dashboard_payload,
    build_forensic_breakdown,
    build_forensic_components,
    build_grouped_signals,
    build_quality_explanation,
    build_red_flag_highlights,
    build_screener_snapshot,
    classify_earnings_quality,
    classify_persistence,
    compute_forensic_score,
    generate_flags,
)


class ForensicLogicTests(unittest.TestCase):
    def test_macro_dashboard_payload_structure(self):
        def fake_market_block(symbol_map, pct=True):
            rows = []
            for i, name in enumerate(symbol_map.keys()):
                rows.append(
                    {
                        "name": name,
                        "symbol": "X",
                        "last": 100 + i,
                        "prev": 99 + i,
                        "change_5d_pct": 1.0 if pct else None,
                        "change_5d": 0.1 if not pct else None,
                        "signal": "Improving",
                        "interpretation": "ok",
                    }
                )
            return rows, []

        with patch("forensic_dashboard_app.build_market_block", side_effect=fake_market_block), patch(
            "forensic_dashboard_app.get_fred_indicator", return_value=(2.0, 1.9)
        ), patch("forensic_dashboard_app.get_news_google_rss", return_value=[{"title": "t", "source": "s", "published": "d", "link": "l"}]):
            payload = build_macro_dashboard_payload()

        self.assertIn("summary", payload)
        self.assertIn("markets", payload)
        self.assertIn("rates", payload)
        self.assertIn("fx", payload)
        self.assertIn("commodities", payload)
        self.assertIn("economy", payload)
        self.assertIn("news", payload)
        self.assertIn("runtime_notices", payload)
        self.assertIn("human_summary", payload["summary"])
        self.assertTrue(payload["markets"]["equities"])

    def test_revenue_growth_with_margin_deterioration(self):
        rows = [
            {"period": "2024-12-31", "revenue": 1000, "net_income": 140, "cfo": 150, "revenue_growth": 0.05, "cfo_ni": 1.07},
            {"period": "2025-12-31", "revenue": 1140, "net_income": 130, "cfo": 120, "revenue_growth": 0.14, "cfo_ni": 0.92},
        ]
        comps = build_forensic_components(rows, text_signals=[])
        self.assertIn("margin divergence", comps["reason_tags"])

    def test_one_off_repeated_persistent_mismatch(self):
        one_off_rows = [
            {"period": "2021", "net_income": 100, "cfo": 105, "cfo_ni": 1.05},
            {"period": "2022", "net_income": 120, "cfo": 125, "cfo_ni": 1.04},
            {"period": "2023", "net_income": 130, "cfo": 70, "cfo_ni": 0.54},
        ]
        repeated_rows = one_off_rows + [{"period": "2024", "net_income": 110, "cfo": 70, "cfo_ni": 0.64}]
        persistent_rows = repeated_rows + [{"period": "2025", "net_income": 120, "cfo": 75, "cfo_ni": 0.62}]

        self.assertEqual(build_forensic_components(one_off_rows)["mismatch_classification"], "one-off")
        self.assertEqual(build_forensic_components(repeated_rows)["mismatch_classification"], "repeated")
        self.assertEqual(build_forensic_components(persistent_rows)["mismatch_classification"], "persistent")

    def test_working_capital_shock_detection(self):
        rows = [
            {"period": "2021", "net_income": 100, "cfo": 95, "cfo_ni": 0.95, "ar_growth": 0.03, "inventory_growth": 0.02, "payables_growth": 0.01},
            {"period": "2022", "net_income": 102, "cfo": 96, "cfo_ni": 0.94, "ar_growth": 0.02, "inventory_growth": 0.03, "payables_growth": 0.01},
            {"period": "2023", "net_income": 106, "cfo": 99, "cfo_ni": 0.93, "ar_growth": 0.04, "inventory_growth": 0.04, "payables_growth": 0.02},
            {"period": "2024", "net_income": 110, "cfo": 100, "cfo_ni": 0.91, "ar_growth": 0.55, "inventory_growth": 0.52, "payables_growth": -0.30},
        ]
        comps = build_forensic_components(rows)
        self.assertTrue(any(tag.endswith("shock") for tag in comps["reason_tags"]))
        self.assertGreaterEqual(len(comps["working_capital_anomalies"]), 1)

    def test_non_recurring_non_operating_support_detection(self):
        rows = [
            {"period": "2023", "net_income": 100, "cfo": 80, "cfo_ni": 0.8, "non_operating_income": 20, "one_time_gain": 8},
            {"period": "2024", "net_income": 100, "cfo": 70, "cfo_ni": 0.7, "non_operating_income": 30, "asset_sale_gain": 12, "tax_benefit": 5},
        ]
        comps = build_forensic_components(rows)
        non_op = comps["non_operating_analysis"]
        self.assertGreater(non_op.get("penalty", 0), 0)
        self.assertIn("non-operating earnings support", comps["reason_tags"])

    def test_alignment_weighting_text_numeric(self):
        rows = [
            {"period": "2024", "revenue": 1000, "net_income": 130, "cfo": 120, "revenue_growth": 0.05, "cfo_ni": 0.92},
            {"period": "2025", "revenue": 1180, "net_income": 120, "cfo": 95, "revenue_growth": 0.18, "cfo_ni": 0.79, "non_operating_income": 25},
        ]
        comps_plain = build_forensic_components(rows, text_signals=[])
        comps_aligned = build_forensic_components(rows, text_signals=[{"signal": "one-time"}, {"signal": "gain on sale"}])
        self.assertGreater(comps_aligned["text_alignment_boost"], comps_plain["text_alignment_boost"])

    def test_screener_ranking_consistency(self):
        sample_universe = ["AAA", "BBB"]

        def fake_quality_rows(ticker):
            if ticker == "AAA":
                rows = [
                    {"period": "2024", "net_income": 100, "cfo": 95, "cfo_ni": 0.95, "beneish_m": -2.5, "dsri": 0.95, "accruals": 5, "revenue_growth": 0.03},
                    {"period": "2025", "net_income": 105, "cfo": 98, "cfo_ni": 0.93, "beneish_m": -2.4, "dsri": 1.0, "accruals": 7, "revenue_growth": 0.04},
                ]
                latest = rows[-1]
                cash = [{"period": "2025", "cfo": 98, "capex": -30, "fcf": 68, "acquisitions": -5, "cfo_ni": 0.93}]
                return rows, latest, cash, {"acq_to_cfo": 0.05}, []
            rows = [
                {"period": "2022", "revenue": 1000, "net_income": 150, "cfo": 90, "cfo_ni": 0.6, "beneish_m": -1.7, "dsri": 1.25, "accruals": 60, "revenue_growth": 0.10, "ar_growth": 0.35, "non_operating_income": 20},
                {"period": "2023", "revenue": 1100, "net_income": 160, "cfo": 95, "cfo_ni": 0.59, "beneish_m": -1.6, "dsri": 1.3, "accruals": 65, "revenue_growth": 0.12, "ar_growth": 0.45, "non_operating_income": 25},
                {"period": "2024", "revenue": 1210, "net_income": 170, "cfo": 100, "cfo_ni": 0.58, "beneish_m": -1.5, "dsri": 1.33, "accruals": 70, "revenue_growth": 0.10, "ar_growth": 0.58, "inventory_growth": 0.50, "payables_growth": -0.25, "non_operating_income": 30, "asset_sale_gain": 12},
                {"period": "2025", "revenue": 1331, "net_income": 180, "cfo": 90, "cfo_ni": 0.5, "beneish_m": -1.4, "dsri": 1.4, "accruals": 90, "revenue_growth": 0.10, "ar_growth": 0.62, "inventory_growth": 0.56, "payables_growth": -0.30, "non_operating_income": 40, "one_time_gain": 20},
            ]
            latest = rows[-1]
            cash = [{"period": "2025", "cfo": 90, "capex": -80, "fcf": 10, "acquisitions": -70, "cfo_ni": 0.5}]
            return rows, latest, cash, {"acq_to_cfo": 0.77}, []

        with patch("forensic_dashboard_app.choose_universe", return_value=sample_universe), patch(
            "forensic_dashboard_app.build_quality_rows", side_effect=fake_quality_rows
        ):
            rows = build_screener_snapshot("core")
        self.assertGreaterEqual(len(rows), 1)
        self.assertEqual(rows[0]["ticker"], "BBB")
        self.assertTrue(rows[0]["reason"])
        self.assertIn("quality_classification", rows[0])
        self.assertIn("red_flag_count", rows[0])

    def test_quality_classification_and_explanation(self):
        quality_rows = [
            {"period": "2023", "revenue": 1000, "net_income": 140, "cfo": 75, "cfo_ni": 0.54, "beneish_m": -1.62, "dsri": 1.28, "revenue_growth": 0.14, "ar_growth": 0.42, "non_operating_income": 20},
            {"period": "2024", "revenue": 1160, "net_income": 145, "cfo": 70, "cfo_ni": 0.48, "beneish_m": -1.55, "dsri": 1.35, "revenue_growth": 0.16, "ar_growth": 0.53, "non_operating_income": 28, "asset_sale_gain": 8},
            {"period": "2025", "revenue": 1280, "net_income": 150, "cfo": 68, "cfo_ni": 0.45, "beneish_m": -1.48, "dsri": 1.41, "revenue_growth": 0.12, "ar_growth": 0.6, "inventory_growth": 0.5, "payables_growth": -0.22, "non_operating_income": 32, "one_time_gain": 10},
        ]
        text_signals = [{"signal": "material weakness", "hits": 2, "interpretation": "Internal control weakness disclosed."}]
        flags, risk, latest_cfo_ni, beneish, components = generate_flags(quality_rows, text_signals=text_signals)

        score, _, _ = compute_forensic_score(latest_cfo_ni, beneish, len(flags), components=components, text_signals=text_signals)
        classification = classify_earnings_quality(score, risk, flags, components=components, text_signals=text_signals)
        breakdown = build_forensic_breakdown(quality_rows, components, flags, text_signals, score)
        highlights = build_red_flag_highlights(flags, components)
        explanation = build_quality_explanation(classification, breakdown, highlights)
        grouped = build_grouped_signals(components, text_signals, flags)

        self.assertIn(classification, {"HIGH RISK", "LOW QUALITY"})
        self.assertTrue(explanation.startswith(classification))
        self.assertTrue(any(x["severity"] in {"HIGH", "MEDIUM"} for x in highlights))
        self.assertIn("cash_flow_quality", grouped)
        self.assertIn("score_components", breakdown)

    def test_classify_persistence(self):
        self.assertEqual(classify_persistence(0), "none")
        self.assertEqual(classify_persistence(1), "one-off")
        self.assertEqual(classify_persistence(2), "repeated")
        self.assertEqual(classify_persistence(3), "persistent")


if __name__ == "__main__":
    unittest.main()
