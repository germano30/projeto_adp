"""
Keyword analysis utilities for routing and topic classification.

This module provides reusable functions and a small data class used to
analyze user queries for topic signals. It implements Jaccard and cosine
similarity metrics and a combined analysis method suitable for routing.
"""
import logging
import re
import unicodedata
from dataclasses import dataclass
from typing import Dict, List, Optional, Set, Tuple
from nltk.corpus import stopwords
from nltk.stem import PorterStemmer
import nltk
try:
    nltk.download('stopwords')
    STOPWORDS = set(stopwords.words('english'))
    STEMMER = PorterStemmer()
    USE_NLTK = True
except ImportError:
    STOPWORDS = {
        'the', 'a', 'an', 'and', 'or', 'but', 'in', 'on', 'at', 'to', 'for',
        'of', 'with', 'by', 'from', 'as', 'is', 'was', 'are', 'were', 'been',
        'be', 'have', 'has', 'had', 'do', 'does', 'did', 'will', 'would',
        'could', 'should', 'may', 'might', 'can', 'this', 'that', 'these',
        'those', 'i', 'you', 'he', 'she', 'it', 'we', 'they', 'what', 'which',
        'who', 'when', 'where', 'why', 'how'
    }
    USE_NLTK = False

from config import LIGHTRAG_KEYWORDS, LIGHTRAG_TOPICS

logger = logging.getLogger(__name__)


@dataclass
class KeywordAnalysis:
    has_lightrag_keywords: bool
    matched_keywords: List[str]
    suggested_topic: Optional[str]
    confidence: float


def remove_accents(text: str) -> str:
    """Remove accents from unicode string."""
    nfkd = unicodedata.normalize('NFKD', text)
    return ''.join([c for c in nfkd if not unicodedata.combining(c)])


def simple_stem(word: str) -> str:
    """Basic suffix removal for stemming."""
    suffixes = ['iness', 'ation', 'ement', 'ment', 'ness', 'tion', 
                'able', 'ible', 'ing', 'ed', 'er', 'ly', 'al', 's']
    
    word = word.lower()
    for suffix in suffixes:
        if len(word) > len(suffix) + 2 and word.endswith(suffix):
            return word[:-len(suffix)]
    return word


def normalize_text(text: str, remove_stopwords: bool = True) -> List[str]:
    """
    Normalize text: lowercase, remove accents, punctuation, stopwords, and stem.
    
    Args:
        text: Input text to normalize
        remove_stopwords: Whether to filter out stopwords
    
    Returns:
        List of normalized tokens
    """
    text = remove_accents(text)
    text = text.lower()
    text = re.sub(r'[^\w\s-]', ' ', text)
    
    tokens = text.split()
    
    if remove_stopwords:
        tokens = [t for t in tokens if len(t) > 2 and t not in STOPWORDS]
    else:
        tokens = [t for t in tokens if len(t) > 2]
    
    if USE_NLTK:
        stemmed = [STEMMER.stem(t) for t in tokens]
    else:
        stemmed = [simple_stem(t) for t in tokens]
    
    return stemmed


def generate_ngrams(tokens: List[str], n: int = 2) -> List[str]:
    """Generate n-grams from token list."""
    if len(tokens) < n:
        return []
    return [' '.join(tokens[i:i+n]) for i in range(len(tokens) - n + 1)]


def extract_features(text: str) -> Tuple[Set[str], Dict[str, int]]:
    """
    Extract unigrams and bigrams with frequencies.
    
    Returns:
        Tuple of (token_set, frequency_dict) combining unigrams and bigrams
    """
    tokens = normalize_text(text, remove_stopwords=True)
    
    unigrams = tokens
    bigrams = generate_ngrams(tokens, n=2)
    
    all_features = unigrams + bigrams
    
    feature_set = set(all_features)
    freq_dict: Dict[str, int] = {}
    for f in all_features:
        freq_dict[f] = freq_dict.get(f, 0) + 1
    
    return feature_set, freq_dict


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
    improve robustness over plain substring checks. Uses normalized text,
    stopword removal, stemming, and n-grams for better matching.
    """
    try:
        match_threshold = 0.25
        jaccard_weight = 0.6
        cosine_weight = 0.4

        token_set, freqs = extract_features(user_question)

        matched: Set[str] = set()
        
        for kw in LIGHTRAG_KEYWORDS:
            kw_set, kw_freq = extract_features(kw)
            
            j = calculate_jaccard_similarity(token_set, kw_set)
            c = calculate_cosine_similarity(freqs, kw_freq)
            combined = j * jaccard_weight + c * cosine_weight
            
            if combined > match_threshold:
                matched.add(kw)

        TOPIC_PATTERNS = {}
        
        try:
            for category, topics in LIGHTRAG_TOPICS.items():
                for t in topics:
                    TOPIC_PATTERNS[t] = {'keywords': [t], 'threshold': 1}
        except Exception:
            TOPIC_PATTERNS = {
                'Agricultural Employment': {
                    'keywords': ['agricultural', 'farm', 'agriculture'], 
                    'threshold': 1
                },
                'Entertainment': {
                    'keywords': ['entertainment', 'performer', 'actor', 'musician'], 
                    'threshold': 1
                },
                'Payday Requirements': {
                    'keywords': ['payday', 'pay frequency', 'payment schedule'], 
                    'threshold': 1
                },
            }

        best_topic: Optional[str] = None
        best_conf = 0.0

        for topic, pattern in TOPIC_PATTERNS.items():
            topic_score = 0.0
            topic_matches = 0

            for pkw in pattern.get('keywords', []):
                pkw_set, pkw_freq = extract_features(pkw)
                
                j = calculate_jaccard_similarity(token_set, pkw_set)
                c = calculate_cosine_similarity(freqs, pkw_freq)
                combined = j * jaccard_weight + c * cosine_weight
                
                if combined > match_threshold:
                    topic_score += combined
                    topic_matches += 1
                    matched.add(pkw)

            if topic_matches >= pattern.get('threshold', 1):
                conf = min(1.0, topic_score / max(1, len(pattern.get('keywords', []))))
                if conf > best_conf:
                    best_conf = conf
                    best_topic = topic

        logger.debug(
            "analyze_keywords -> matched=%s topic=%s conf=%.2f", 
            list(matched), best_topic, best_conf
        )
        
        return KeywordAnalysis(
            has_lightrag_keywords=bool(matched),
            matched_keywords=list(matched),
            suggested_topic=best_topic,
            confidence=best_conf
        )

    except Exception as e:
        logger.error("analyze_keywords failed: %s", str(e))
        raise