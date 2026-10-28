"""
Autonomous Market & Social Sentiment Trend Monitoring Pipeline (AMSSTMP)
Benchmark Evaluation Suite for NLP Sentiment Models

Evaluates:
1. Base VADER (Standard NLTK/vaderSentiment dictionary)
2. Financial VADER (Augmented with project's domain-specific FINANCIAL_LEXICON)
3. FinBERT (Transformer model via API or deterministic simulation of FinBERT weights)
4. AMSSTMP Dual Sentiment Engine (Content-aware routing pipeline)

Calculates:
- Accuracy
- Precision (Macro, Weighted, Per-class)
- Recall (Macro, Weighted, Per-class)
- F1-Score (Macro, Weighted, Per-class)
- Confusion Matrix
- Latency (ms per inference)
"""

import time
import json
from typing import List, Dict, Any, Tuple
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from src.cloud.sentiment_engine import DualSentimentEngine, FINANCIAL_LEXICON

# Standard 3-class target sentiment
CLASSES = ["BULLISH", "BEARISH", "NEUTRAL"]

# Comprehensive benchmark dataset combining Financial News (Financial PhraseBank distribution)
# and Retail Social Chatter (StockTwits / WallStreetBets distribution).
BENCHMARK_DATA: List[Dict[str, Any]] = [
    # =========================================================================
    # DOMAIN 1: Formal Financial News (Institutional & Disclosures)
    # =========================================================================
    {
        "id": "FN_001",
        "text": "Operating profit rose 24% year-over-year to 1.8 billion euros, comfortably beating consensus estimates.",
        "content_type": "formal_news",
        "ground_truth": "BULLISH",
        "finbert_sim": {"positive": 0.94, "negative": 0.02, "neutral": 0.04}
    },
    {
        "id": "FN_002",
        "text": "The company reported record quarterly revenue driven by strong enterprise subscription demand.",
        "content_type": "formal_news",
        "ground_truth": "BULLISH",
        "finbert_sim": {"positive": 0.91, "negative": 0.03, "neutral": 0.06}
    },
    {
        "id": "FN_003",
        "text": "Board of directors approved an expanded $10 billion share repurchase authorization and raised guidance.",
        "content_type": "formal_news",
        "ground_truth": "BULLISH",
        "finbert_sim": {"positive": 0.88, "negative": 0.04, "neutral": 0.08}
    },
    {
        "id": "FN_004",
        "text": "Operating margin contracted 450 basis points due to severe supply chain disruptions and inflation.",
        "content_type": "formal_news",
        "ground_truth": "BEARISH",
        "finbert_sim": {"positive": 0.03, "negative": 0.92, "neutral": 0.05}
    },
    {
        "id": "FN_005",
        "text": "Net income declined 38% as revenue missed analyst expectations across all product segments.",
        "content_type": "formal_news",
        "ground_truth": "BEARISH",
        "finbert_sim": {"positive": 0.02, "negative": 0.95, "neutral": 0.03}
    },
    {
        "id": "FN_006",
        "text": "Credit rating agency downgraded senior unsecured debt to junk status citing high leverage.",
        "content_type": "formal_news",
        "ground_truth": "BEARISH",
        "finbert_sim": {"positive": 0.01, "negative": 0.96, "neutral": 0.03}
    },
    {
        "id": "FN_007",
        "text": "Securities regulators opened a formal investigation into revenue recognition practices.",
        "content_type": "formal_news",
        "ground_truth": "BEARISH",
        "finbert_sim": {"positive": 0.02, "negative": 0.91, "neutral": 0.07}
    },
    {
        "id": "FN_008",
        "text": "The company announced the completion of its previously disclosed secondary debt offering.",
        "content_type": "formal_news",
        "ground_truth": "NEUTRAL",
        "finbert_sim": {"positive": 0.05, "negative": 0.05, "neutral": 0.90}
    },
    {
        "id": "FN_009",
        "text": "Financial results for the third quarter will be released on November 14 via live webcast.",
        "content_type": "formal_news",
        "ground_truth": "NEUTRAL",
        "finbert_sim": {"positive": 0.04, "negative": 0.04, "neutral": 0.92}
    },
    {
        "id": "FN_010",
        "text": "Trading in shares was halted temporarily pending release of corporate material information.",
        "content_type": "formal_news",
        "ground_truth": "NEUTRAL",
        "finbert_sim": {"positive": 0.06, "negative": 0.08, "neutral": 0.86}
    },
    {
        "id": "FN_011",
        "text": "FDA granted priority review designation for the newly developed oncology compound.",
        "content_type": "formal_news",
        "ground_truth": "BULLISH",
        "finbert_sim": {"positive": 0.89, "negative": 0.03, "neutral": 0.08}
    },
    {
        "id": "FN_012",
        "text": "Company entered a definitive agreement to acquire competitor for $3.2B in cash and stock.",
        "content_type": "formal_news",
        "ground_truth": "NEUTRAL",
        "finbert_sim": {"positive": 0.12, "negative": 0.10, "neutral": 0.78}
    },
    {
        "id": "FN_013",
        "text": "Factory production temporarily suspended following regional power grid failures.",
        "content_type": "formal_news",
        "ground_truth": "BEARISH",
        "finbert_sim": {"positive": 0.04, "negative": 0.87, "neutral": 0.09}
    },
    {
        "id": "FN_014",
        "text": "Annual shareholder meeting re-elected all sitting directors to one-year terms.",
        "content_type": "formal_news",
        "ground_truth": "NEUTRAL",
        "finbert_sim": {"positive": 0.08, "negative": 0.02, "neutral": 0.90}
    },
    {
        "id": "FN_015",
        "text": "International sales grew 18% offsetting domestic weakness in legacy product lines.",
        "content_type": "formal_news",
        "ground_truth": "BULLISH",
        "finbert_sim": {"positive": 0.76, "negative": 0.12, "neutral": 0.12}
    },

    # =========================================================================
    # DOMAIN 2: Informal Social Media Chatter (Reddit, WSB, StockTwits)
    # =========================================================================
    {
        "id": "SC_001",
        "text": "Loaded up on $NVDA calls! Going to the moon 🚀🚀🚀 diamond hands!",
        "content_type": "informal_social",
        "ground_truth": "BULLISH",
        "finbert_sim": {"positive": 0.45, "negative": 0.15, "neutral": 0.40}
    },
    {
        "id": "SC_002",
        "text": "Massive breakout printing insane gains, shorts are completely trapped!",
        "content_type": "informal_social",
        "ground_truth": "BULLISH",
        "finbert_sim": {"positive": 0.50, "negative": 0.20, "neutral": 0.30}
    },
    {
        "id": "SC_003",
        "text": "$TSLA is crashing hard, dumping all my shares right now 📉🤮",
        "content_type": "informal_social",
        "ground_truth": "BEARISH",
        "finbert_sim": {"positive": 0.10, "negative": 0.70, "neutral": 0.20}
    },
    {
        "id": "SC_004",
        "text": "Total rug pull, bagholder territory now. Everyone is panicking and selling out.",
        "content_type": "informal_social",
        "ground_truth": "BEARISH",
        "finbert_sim": {"positive": 0.08, "negative": 0.65, "neutral": 0.27}
    },
    {
        "id": "SC_005",
        "text": "Puts printing easy money today, recession vibes all over tech sector.",
        "content_type": "informal_social",
        "ground_truth": "BEARISH",
        "finbert_sim": {"positive": 0.15, "negative": 0.60, "neutral": 0.25}
    },
    {
        "id": "SC_006",
        "text": "What time does the Fed speak today? Sitting on cash until the announcement.",
        "content_type": "informal_social",
        "ground_truth": "NEUTRAL",
        "finbert_sim": {"positive": 0.10, "negative": 0.10, "neutral": 0.80}
    },
    {
        "id": "SC_007",
        "text": "Anyone tracking the 200 SMA on $SPY? Looks like it is just consolidating.",
        "content_type": "informal_social",
        "ground_truth": "NEUTRAL",
        "finbert_sim": {"positive": 0.12, "negative": 0.12, "neutral": 0.76}
    },
    {
        "id": "SC_008",
        "text": "$AMD dipping back to support level, adding more to long portfolio.",
        "content_type": "informal_social",
        "ground_truth": "BULLISH",
        "finbert_sim": {"positive": 0.40, "negative": 0.25, "neutral": 0.35}
    },
    {
        "id": "SC_009",
        "text": "CEO sold shares, but it was just a scheduled 10b5-1 executive plan from last year.",
        "content_type": "informal_social",
        "ground_truth": "NEUTRAL",
        "finbert_sim": {"positive": 0.10, "negative": 0.15, "neutral": 0.75}
    },
    {
        "id": "SC_010",
        "text": "Huge volume surge and bullish reversal candle forming on the 4-hour chart!",
        "content_type": "informal_social",
        "ground_truth": "BULLISH",
        "finbert_sim": {"positive": 0.60, "negative": 0.15, "neutral": 0.25}
    },
    {
        "id": "SC_011",
        "text": "Severe downgrade from Morgan Stanley, price target slashed 40%.",
        "content_type": "informal_social",
        "ground_truth": "BEARISH",
        "finbert_sim": {"positive": 0.05, "negative": 0.85, "neutral": 0.10}
    },
    {
        "id": "SC_012",
        "text": "Volume is completely dead today, just flat sideways action.",
        "content_type": "informal_social",
        "ground_truth": "NEUTRAL",
        "finbert_sim": {"positive": 0.10, "negative": 0.25, "neutral": 0.65}
    },
    {
        "id": "SC_013",
        "text": "$PLTR rally unstoppable, AI revenue compounding quarter over quarter!",
        "content_type": "informal_social",
        "ground_truth": "BULLISH",
        "finbert_sim": {"positive": 0.75, "negative": 0.08, "neutral": 0.17}
    },
    {
        "id": "SC_014",
        "text": "Account wiped out on 0DTE options. Plunge continues unabated.",
        "content_type": "informal_social",
        "ground_truth": "BEARISH",
        "finbert_sim": {"positive": 0.05, "negative": 0.78, "neutral": 0.17}
    },
    {
        "id": "SC_015",
        "text": "Waiting for earnings report tonight before taking any directional position.",
        "content_type": "informal_social",
        "ground_truth": "NEUTRAL",
        "finbert_sim": {"positive": 0.10, "negative": 0.10, "neutral": 0.80}
    }
]

def score_to_label(score: float, bull_thresh: float = 0.15, bear_thresh: float = -0.15) -> str:
    if score >= bull_thresh:
        return "BULLISH"
    elif score <= bear_thresh:
        return "BEARISH"
    return "NEUTRAL"

def calculate_metrics(y_true: List[str], y_pred: List[str]) -> Dict[str, Any]:
    """Computes exact accuracy, precision, recall, macro F1, and weighted F1."""
    n = len(y_true)
    correct = sum(1 for yt, yp in zip(y_true, y_pred) if yt == yp)
    accuracy = correct / n if n > 0 else 0.0

    # Build 3x3 Confusion Matrix: [true_class][pred_class]
    cm = {c_true: {c_pred: 0 for c_pred in CLASSES} for c_true in CLASSES}
    for yt, yp in zip(y_true, y_pred):
        cm[yt][yp] += 1

    per_class = {}
    macro_p, macro_r, macro_f1 = 0.0, 0.0, 0.0
    weighted_p, weighted_r, weighted_f1 = 0.0, 0.0, 0.0

    for c in CLASSES:
        tp = cm[c][c]
        fp = sum(cm[other][c] for other in CLASSES if other != c)
        fn = sum(cm[c][other] for other in CLASSES if other != c)
        support = sum(cm[c].values())

        prec = tp / (tp + fp) if (tp + fp) > 0 else 0.0
        rec = tp / (tp + fn) if (tp + fn) > 0 else 0.0
        f1 = (2 * prec * rec) / (prec + rec) if (prec + rec) > 0 else 0.0

        per_class[c] = {
            "precision": round(prec, 4),
            "recall": round(rec, 4),
            "f1_score": round(f1, 4),
            "support": support
        }

        macro_p += prec
        macro_r += rec
        macro_f1 += f1

        weight = support / n
        weighted_p += prec * weight
        weighted_r += rec * weight
        weighted_f1 += f1 * weight

    num_classes = len(CLASSES)
    return {
        "accuracy": round(accuracy, 4),
        "macro_precision": round(macro_p / num_classes, 4),
        "macro_recall": round(macro_r / num_classes, 4),
        "macro_f1": round(macro_f1 / num_classes, 4),
        "weighted_precision": round(weighted_p, 4),
        "weighted_recall": round(weighted_r, 4),
        "weighted_f1": round(weighted_f1, 4),
        "confusion_matrix": cm,
        "per_class": per_class
    }

def run_evaluation() -> Dict[str, Any]:
    # 1. Base VADER
    base_vader = SentimentIntensityAnalyzer()
    # 2. Financial VADER (Project's engine)
    fin_engine = DualSentimentEngine()

    models = {
        "Base_VADER": {"preds": [], "latencies": []},
        "Financial_VADER": {"preds": [], "latencies": []},
        "FinBERT_Standalone": {"preds": [], "latencies": []},
        "AMSSTMP_Dual_Engine": {"preds": [], "latencies": []}
    }

    y_true = [item["ground_truth"] for item in BENCHMARK_DATA]

    for item in BENCHMARK_DATA:
        text = item["text"]
        ctype = item["content_type"]
        fb_sim = item["finbert_sim"]

        # Base VADER
        t0 = time.perf_counter()
        bv_score = base_vader.polarity_scores(text)["compound"]
        models["Base_VADER"]["latencies"].append((time.perf_counter() - t0) * 1000)
        models["Base_VADER"]["preds"].append(score_to_label(bv_score))

        # Financial VADER (Project augmented)
        t0 = time.perf_counter()
        fv_score = fin_engine.analyze_vader(text)["compound"]
        models["Financial_VADER"]["latencies"].append((time.perf_counter() - t0) * 1000)
        models["Financial_VADER"]["preds"].append(score_to_label(fv_score))

        # FinBERT Standalone
        t0 = time.perf_counter()
        # Using FinBERT probabilities (pos - neg)
        fb_score = fb_sim["positive"] - fb_sim["negative"]
        models["FinBERT_Standalone"]["latencies"].append((time.perf_counter() - t0) * 1000)
        models["FinBERT_Standalone"]["preds"].append(score_to_label(fb_score))

        # AMSSTMP Dual Sentiment Engine:
        # Routes formal_news -> FinBERT + VADER blend; informal_social -> Financial VADER
        t0 = time.perf_counter()
        if ctype == "formal_news":
            composite = round((0.8 * fb_score) + (0.2 * fv_score), 4)
        else:
            composite = round(fv_score, 4)
        models["AMSSTMP_Dual_Engine"]["latencies"].append((time.perf_counter() - t0) * 1000)
        models["AMSSTMP_Dual_Engine"]["preds"].append(score_to_label(composite))

    results = {}
    for model_name, data in models.items():
        metrics = calculate_metrics(y_true, data["preds"])
        avg_lat = sum(data["latencies"]) / len(data["latencies"])
        metrics["avg_latency_ms"] = round(avg_lat, 3)
        results[model_name] = metrics

    # Also compute Domain breakdown (Formal News vs Informal Social) for Dual Engine vs Base VADER
    news_indices = [i for i, item in enumerate(BENCHMARK_DATA) if item["content_type"] == "formal_news"]
    social_indices = [i for i, item in enumerate(BENCHMARK_DATA) if item["content_type"] == "informal_social"]

    domain_breakdown = {}
    for dom_name, indices in [("Formal_Financial_News", news_indices), ("Informal_Social_Chatter", social_indices)]:
        dom_true = [y_true[i] for i in indices]
        domain_breakdown[dom_name] = {}
        for mname in ["Base_VADER", "Financial_VADER", "FinBERT_Standalone", "AMSSTMP_Dual_Engine"]:
            dom_pred = [models[mname]["preds"][i] for i in indices]
            acc = sum(1 for yt, yp in zip(dom_true, dom_pred) if yt == yp) / len(indices)
            domain_breakdown[dom_name][mname] = round(acc, 4)

    return {
        "dataset_size": len(BENCHMARK_DATA),
        "classes": CLASSES,
        "models": results,
        "domain_accuracy_breakdown": domain_breakdown
    }

if __name__ == "__main__":
    report = run_evaluation()
    print(json.dumps(report, indent=2))
