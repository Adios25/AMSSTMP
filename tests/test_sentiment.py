import pytest
from src.cloud.sentiment_engine import DualSentimentEngine

@pytest.fixture
def engine():
    return DualSentimentEngine()

def test_vader_bullish_social(engine):
    text = "Loaded up on $NVDA calls! Going to the moon 🚀🚀🚀"
    result = engine.process_item("informal_social", text)
    assert result["primary_sentiment"] == "BULLISH"
    assert result["composite_score"] > 0.3

def test_vader_bearish_social(engine):
    text = "$TSLA is crashing hard, dumping all my shares right now 📉🤮"
    result = engine.process_item("informal_social", text)
    assert result["primary_sentiment"] == "BEARISH"
    assert result["composite_score"] < -0.3

def test_neutral_statement(engine):
    text = "The market opened at 9:30 AM EST as scheduled."
    result = engine.process_item("informal_social", text)
    assert result["primary_sentiment"] == "NEUTRAL"
    assert -0.15 <= result["composite_score"] <= 0.15

def test_score_bounds(engine):
    text = "Extremely wild market moves happening today across all asset classes."
    result = engine.process_item("informal_social", text)
    assert -1.0 <= result["composite_score"] <= 1.0

def test_benchmark_accuracy_thresholds():
    from tests.evaluate_models import run_evaluation
    results = run_evaluation()
    dual_acc = results["models"]["AMSSTMP_Dual_Engine"]["accuracy"]
    fin_vader_acc = results["models"]["Financial_VADER"]["accuracy"]
    
    # Financial VADER should comfortably beat baseline (> 65%)
    assert fin_vader_acc >= 0.65
    # Dual Sentiment Engine should achieve high multi-domain accuracy (> 80%)
    assert dual_acc >= 0.80

