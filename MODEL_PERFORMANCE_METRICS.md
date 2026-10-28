# 📊 NLP Model Performance & Accuracy Evaluation

This document presents empirical benchmark evaluation metrics for the sentiment analysis models deployed in the **Autonomous Market & Social Sentiment Trend Monitoring Pipeline (AMSSTMP)**.

---

## 🎯 Executive Summary

The evaluation was executed across a standardized 3-class test suite (`BULLISH`, `BEARISH`, `NEUTRAL`) spanning two primary operational channels:
1. **Formal Financial News & Disclosures** (Institutional earnings, SEC releases, regulatory filings, analyst ratings)
2. **Informal Retail Social Chatter** (Reddit r/wallstreetbets, StockTwits, Twitter/X retail traders with slang & emojis)

Four model configurations were benchmarked:
- **Base VADER**: Vanilla NLTK/vaderSentiment lexicon.
- **Financial VADER**: VADER enhanced with AMSSTMP's domain-specific `FINANCIAL_LEXICON` (`moon`, `puts`, `calls`, `diamond hands`, `breakout`, etc.).
- **FinBERT (Standalone)**: Transformer (`ProsusAI/finbert`) fine-tuned on Financial PhraseBank.
- **AMSSTMP Dual Engine**: Content-type routed production architecture (`formal_news` $\rightarrow$ FinBERT + VADER; `informal_social` $\rightarrow$ Financial VADER).

---

## 📈 Global Performance Metrics

| Model Configuration | Accuracy | Macro Precision | Macro Recall | Macro F1 | Weighted F1 | Avg Latency / Item |
|---|:---:|:---:|:---:|:---:|:---:|:---:|
| **Base VADER** | 56.67% | 56.67% | 56.67% | 55.95% | 55.95% | ~0.037 ms |
| **Financial VADER (Custom)** | **70.00%** | **72.34%** | **70.00%** | **68.17%** | **68.17%** | ~0.032 ms |
| **FinBERT (Standalone)** | **96.67%** | **96.97%** | **96.67%** | **96.66%** | **96.66%** | ~120 - 250 ms (API) |
| **AMSSTMP Dual Engine** | **86.67%** | **86.94%** | **86.67%** | **86.23%** | **86.23%** | **< 1 ms (Social) / Hybrid** |

---

## 🌐 Channel & Domain Breakdown

How each model behaves when partitioned by data source category:

| Model Configuration | Formal Financial News (Accuracy) | Informal Retail Social Chatter (Accuracy) |
|---|:---:|:---:|
| **Base VADER** | 66.67% | 46.67% |
| **Financial VADER (AMSSTMP)** | 66.67% | **73.33%** (+26.66% gain) |
| **FinBERT (Standalone)** | **100.00%** | 93.33% |
| **AMSSTMP Dual Engine** | **100.00%** | **73.33%** |

### Key Takeaways:
1. **The Custom Lexicon Fixes Social Media**: Standard VADER fails on retail trading slang (46.67% accuracy), misclassifying calls/puts/mooning as neutral or wrong polarities. Adding `FINANCIAL_LEXICON` elevates social sentiment accuracy to **73.33%**.
2. **FinBERT Dominates Formal News**: Achieving **100% accuracy** on earnings disclosures and institutional filings, while standard VADER only reaches 66.67% due to financial nuance (e.g. *contracted 450 bps*, *debt downgrade*).
3. **Dual Engine Balance**: Achieves **86.67% global accuracy** while keeping streaming latency near-instantaneous for high-volume social queues.

---

## 🧮 Detailed Per-Class Breakdown

### 1. AMSSTMP Dual Sentiment Engine (Production)
```
Overall Accuracy: 86.67%
Macro F1-Score:   86.23%
```
- **BULLISH**: Precision `83.33%` | Recall `100.00%` | F1 `90.91%` (Support: 10)
- **BEARISH**: Precision `90.00%` | Recall `90.00%`  | F1 `90.00%` (Support: 10)
- **NEUTRAL**: Precision `87.50%` | Recall `70.00%`  | F1 `77.78%` (Support: 10)

#### Confusion Matrix:
| Ground Truth \ Predicted | Predicted BULLISH | Predicted BEARISH | Predicted NEUTRAL |
|---|:---:|:---:|:---:|
| **Actual BULLISH** | **10** | 0 | 0 |
| **Actual BEARISH** | 0 | **9** | 1 |
| **Actual NEUTRAL** | 2 | 1 | **7** |

---

### 2. Financial VADER (Augmented Lexicon)
```
Overall Accuracy: 70.00%
Macro F1-Score:   68.17%
```
- **BULLISH**: Precision `64.29%` | Recall `90.00%` | F1 `75.00%`
- **BEARISH**: Precision `72.73%` | Recall `80.00%` | F1 `76.19%`
- **NEUTRAL**: Precision `80.00%` | Recall `40.00%` | F1 `53.33%`

#### Confusion Matrix:
| Ground Truth \ Predicted | Predicted BULLISH | Predicted BEARISH | Predicted NEUTRAL |
|---|:---:|:---:|:---:|
| **Actual BULLISH** | **9** | 1 | 0 |
| **Actual BEARISH** | 1 | **8** | 1 |
| **Actual NEUTRAL** | 4 | 2 | **4** |

---

### 3. Base VADER (Unaugmented Baseline)
```
Overall Accuracy: 56.67%
Macro F1-Score:   55.95%
```
- **BULLISH**: Precision `58.33%` | Recall `70.00%` | F1 `63.64%`
- **BEARISH**: Precision `54.55%` | Recall `60.00%` | F1 `57.14%`
- **NEUTRAL**: Precision `57.14%` | Recall `40.00%` | F1 `47.06%`

#### Confusion Matrix:
| Ground Truth \ Predicted | Predicted BULLISH | Predicted BEARISH | Predicted NEUTRAL |
|---|:---:|:---:|:---:|
| **Actual BULLISH** | **7** | 3 | 0 |
| **Actual BEARISH** | 1 | **6** | 3 |
| **Actual NEUTRAL** | 4 | 2 | **4** |

---

## 🔬 How to Reproduce / Re-run the Evaluation

You can execute the evaluation suite anytime directly from the virtual environment:

```bash
# Activate Python environment
source .venv/bin/activate

# Execute the evaluation benchmark
python tests/evaluate_models.py
```

The script will re-calculate and print the real-time JSON metrics payload with exact precision, recall, F1, and confusion matrix tables.
