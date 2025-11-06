"""
Keyword analysis utilities for routing and topic classification.

This module provides reusable functions and a small data class used to
analyze user queries for topic signals. It implements Jaccard and cosine
similarity metrics and a combined analysis method suitable for routing.
"""
import logging
import json
from dataclasses import dataclass
from typing import Dict, List, Optional, Set

from config import LIGHTRAG_KEYWORDS, LIGHTRAG_TOPICS

logger = logging.getLogger(__name__)


@dataclass
class KeywordAnalysis:
    has_lightrag_keywords: bool
    matched_keywords: List[str]
    suggested_topic: Optional[str]
    confidence: float


def calculate_jaccard_similarity(set1: Set[str], set2: Set[str]) -> float:
    """Return Jaccard similarity between two sets of tokens."""
    if not set1 or not set2:
        return 0.0
    intersection = len(set1 & set2)
    union = len(set1 | set2)
    return intersection / union if union > 0 else 0.0


def calculate_cosine_similarity(vec1: Dict[str, int], vec2: Dict[str, int]) -> float:
    """Return cosine similarity between two frequency dictionaries."""
    common = set(vec1.keys()) & set(vec2.keys())
    if not common:
        return 0.0
    dot = sum(vec1[w] * vec2[w] for w in common)
    mag1 = sum(v * v for v in vec1.values()) ** 0.5
    mag2 = sum(v * v for v in vec2.values()) ** 0.5
    return dot / (mag1 * mag2) if mag1 * mag2 > 0 else 0.0


def analyze_keywords(user_question: str) -> KeywordAnalysis:
    """
    Analyze a user question and return keyword/topic analysis.

    The function combines set- and frequency-based similarity metrics to
    improve robustness over plain substring checks.
    """
    try:
        # Tunable parameters (can be extended to accept from caller)
        match_threshold = 0.35  # combined similarity threshold to consider a keyword match
        jaccard_weight = 0.6
        cosine_weight = 0.4

        q_lower = user_question.lower()
        tokens = [w for w in q_lower.split() if len(w) > 2]
        token_set = set(tokens)
        freqs: Dict[str, int] = {}
        for w in tokens:
            freqs[w] = freqs.get(w, 0) + 1

        matched: Set[str] = set()
        # Match LIGHTRAG keywords using combined similarity (weighted Jaccard + Cosine)
        for kw in LIGHTRAG_KEYWORDS:
            kw_lower = kw.lower()
            kw_tokens = set(t for t in kw_lower.split() if len(t) > 2)
            j = calculate_jaccard_similarity(token_set, kw_tokens)
            # build small freq vector for keyword
            kw_freq: Dict[str, int] = {}
            for w in kw_tokens:
                kw_freq[w] = kw_freq.get(w, 0) + 1
            c = calculate_cosine_similarity(freqs, kw_freq)
            combined = j * jaccard_weight + c * cosine_weight
            if combined > match_threshold:
                matched.add(kw)

        # Topic classification (pattern-driven)
        TOPIC_PATTERNS = {}
        # build from LIGHTRAG_TOPICS when available (fallback to static patterns)
        try:
            for category, topics in LIGHTRAG_TOPICS.items():
                for t in topics:
                    TOPIC_PATTERNS[t] = {'keywords': [t], 'threshold': 1}
        except Exception:
            # fallback basic patterns
            TOPIC_PATTERNS = {
                'Agricultural Employment': {'keywords': ['agricultural', 'farm', 'agriculture'], 'threshold': 1},
                'Entertainment': {'keywords': ['entertainment', 'performer', 'actor', 'musician'], 'threshold': 1},
                'Payday Requirements': {'keywords': ['payday', 'pay frequency', 'payment schedule'], 'threshold': 1},
            }

        best_topic: Optional[str] = None
        best_conf = 0.0

        for topic, pattern in TOPIC_PATTERNS.items():
            topic_score = 0.0
            topic_matches = 0
            # prepare pattern freq
            pat_freq: Dict[str, int] = {}
            for pkw in pattern.get('keywords', []):
                for w in pkw.lower().split():
                    if len(w) > 2:
                        pat_freq[w] = pat_freq.get(w, 0) + 1

                for pkw in pattern.get('keywords', []):
                    kw_tokens = set(w for w in pkw.lower().split() if len(w) > 2)
                    j = calculate_jaccard_similarity(token_set, kw_tokens)
                    kw_freq: Dict[str, int] = {}
                    for w in kw_tokens:
                        kw_freq[w] = kw_freq.get(w, 0) + 1
                    c = calculate_cosine_similarity(freqs, kw_freq)
                    combined = j * jaccard_weight + c * cosine_weight
                    if combined > match_threshold:
                        topic_score += combined
                        topic_matches += 1
                        matched.add(pkw)

            if topic_matches >= pattern.get('threshold', 1):
                # Normalize confidence by pattern size and weight
                conf = min(1.0, topic_score / max(1, len(pattern.get('keywords', []))))
                if conf > best_conf:
                    best_conf = conf
                    best_topic = topic

        logger.debug("analyze_keywords -> matched=%s topic=%s conf=%.2f", list(matched), best_topic, best_conf)
        return KeywordAnalysis(
            has_lightrag_keywords=bool(matched),
            matched_keywords=list(matched),
            suggested_topic=best_topic,
            confidence=best_conf
        )

    except Exception as e:
        logger.error("analyze_keywords failed: %s", str(e))
        raise
