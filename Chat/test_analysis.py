"""
Test script for the improved keyword analysis module.

Features:
1. Runs a keyword + topic analysis for a sample query
2. Compares scoring methods (improved vs multi-layer)
3. Evaluates threshold sensitivity and visualizes results
"""

import matplotlib.pyplot as plt
from analysis import (
    analyze_keywords,
    compare_scoring_methods,
    evaluate_threshold_sensitivity
)
from config import LIGHTRAG_KEYWORDS, LIGHTRAG_TOPICS

print("\n--- DEBUG CONFIG ---")
print(f"Loaded {len(LIGHTRAG_KEYWORDS)} keywords")
print(f"Sample: {LIGHTRAG_KEYWORDS[:10]}")
print(f"Loaded {len(LIGHTRAG_TOPICS)} topics")
print(f"Topics: {list(LIGHTRAG_TOPICS.keys())}")
print("--------------------\n")


# ---------------------------------------------------------------------------
# 1️⃣  Query de exemplo (você pode trocar por qualquer pergunta real)
# ---------------------------------------------------------------------------
user_question = "What's the minimum wage for agricultural workers in California?"

# ---------------------------------------------------------------------------
# 2️⃣  Rodar análise principal
# ---------------------------------------------------------------------------
result = analyze_keywords(user_question, use_multi_layer=False, verbose=True)
print("\n=== IMPROVED ANALYSIS ===")
print(f"Has LightRAG Keywords: {result.has_lightrag_keywords}")
print(f"Matched Keywords: {result.matched_keywords}")
print(f"Suggested Topic: {result.suggested_topic}")
print(f"Confidence: {result.confidence:.2f}")
print("Match Scores:")
for kw, sc in result.match_scores.items():
    print(f"  - {kw:<40} {sc:.3f}")

# ---------------------------------------------------------------------------
# 3️⃣  Comparar métodos de pontuação
# ---------------------------------------------------------------------------
comparison = compare_scoring_methods(user_question)
print("\n=== COMPARISON ===")
for name, res in comparison.items():
    print(f"\nMethod: {name.upper()}")
    print(f"  Matched: {res.matched_keywords}")
    print(f"  Topic: {res.suggested_topic}")
    print(f"  Confidence: {res.confidence:.2f}")

# ---------------------------------------------------------------------------
# 4️⃣  Sensibilidade de Threshold
# ---------------------------------------------------------------------------
thresholds = [round(x, 2) for x in [i * 0.05 for i in range(5, 21)]]  # 0.25 → 1.00
sensitivity = evaluate_threshold_sensitivity(user_question, thresholds)

print("\n=== THRESHOLD SENSITIVITY ===")
for th, count in sensitivity.items():
    print(f"Threshold {th:.2f}: {count} matches")
