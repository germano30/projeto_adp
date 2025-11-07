import logging
import re
import unicodedata
from dataclasses import dataclass
from typing import Dict, List, Optional, Set, Tuple
from nltk.corpus import stopwords
from nltk.stem import PorterStemmer
import nltk

try:
    nltk.download('stopwords', quiet=True)
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
    USE_NLTK = True

from config import LIGHTRAG_KEYWORDS, LIGHTRAG_TOPICS

logger = logging.getLogger(__name__)

# ============================================================================
# CONFIGURATION - Baseado em literatura acadêmica
# ============================================================================

# Thresholds melhorados (Rekabsaz et al., 2017; Ferragina et al., 2015)
BASE_THRESHOLD = 0.4  
SHORT_KEYWORD_THRESHOLD = 0.5  # Keywords curtas precisam match mais forte
EXACT_MATCH_BOOST = 0.30  # Bonus para matches literais

# Pesos otimizados (Alatrista-Salas et al., 2016; Zahrotun, 2016)
# Jaccard privilegiado para keyword matching (presença > frequência)
JACCARD_WEIGHT = 0.65 
COSINE_WEIGHT = 0.35  

# Threshold de similaridade de tamanho
MIN_SIZE_RATIO = 0.3  # Razão mínima entre tamanhos para penalizar disparidades


@dataclass
class KeywordAnalysis:
    """Keywords analyzis result."""
    has_lightrag_keywords: bool
    matched_keywords: List[str]
    suggested_topic: Optional[str]
    confidence: float
    match_scores: Dict[str, float]


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
    """
    Calculate Jaccard similarity between two sets.
    
    References:
        - Jaccard, P. (1912). The Distribution of the Flora in the Alpine Zone
        - Zahrotun, L. (2016). Comparison of Jaccard and Cosine Similarity
    """
    if not set1 or not set2:
        return 0.0
    intersection = len(set1 & set2)
    union = len(set1 | set2)
    return intersection / union if union > 0 else 0.0


def calculate_cosine_similarity(vec1: Dict[str, int], vec2: Dict[str, int]) -> float:
    """
    Calculate cosine similarity between two frequency dictionaries.
    
    References:
        - Salton, G. & McGill, M. J. (1983). Introduction to Modern Information Retrieval
        - Singhal, A. (2001). Modern Information Retrieval: A Brief Overview
    """
    common = set(vec1.keys()) & set(vec2.keys())
    if not common:
        return 0.0
    dot = sum(vec1[w] * vec2[w] for w in common)
    mag1 = sum(v * v for v in vec1.values()) ** 0.5
    mag2 = sum(v * v for v in vec2.values()) ** 0.5
    return dot / (mag1 * mag2) if mag1 * mag2 > 0 else 0.0


def calculate_size_penalty(set1: Set[str], set2: Set[str]) -> float:
    """
    Calculate penalty based on size disparity between sets.
    
    Prevents matching very short keywords with very long queries.
    
    References:
        - Rekabsaz et al. (2017). Exploration of Threshold for Similarity
    """
    if not set1 or not set2:
        return 0.0
    
    size_ratio = min(len(set1), len(set2)) / max(len(set1), len(set2))
    
    if size_ratio < MIN_SIZE_RATIO:
        return 0.5 + 0.5 * (size_ratio / MIN_SIZE_RATIO)
    elif size_ratio < 0.5:
        return 0.8 + 0.2 * size_ratio
    else:
        return 1.0


def check_exact_match(query: str, keyword: str) -> float:
    """
    Check for exact or substring matches before normalization.
    
    Returns bonus score if found.
    
    References:
        - Mihalcea et al. (2006). Corpus-based and Knowledge-based Measures
    """
    query_lower = query.lower()
    keyword_lower = keyword.lower()
    
    # Match exato completo
    if query_lower == keyword_lower:
        return EXACT_MATCH_BOOST * 1.5
    
    # Substring em ambas direções
    if keyword_lower in query_lower or query_lower in keyword_lower:
        return EXACT_MATCH_BOOST
    
    return 0.0


def calculate_combined_score(
    token_set: Set[str],
    freqs: Dict[str, int],
    kw_set: Set[str],
    kw_freq: Dict[str, int],
    original_query: str,
    original_keyword: str,
    keyword_length: int
) -> Tuple[float, float]:
    """
    Calculate combined similarity score with multiple improvements.
    
    Returns:
        Tuple of (combined_score, adaptive_threshold)
    
    References:
        - Alatrista-Salas et al. (2016). Combinations of Jaccard with Numerical Measures
        - Ferragina et al. (2015). Optimal Threshold Determination
    """
    # 1. Detectar match exato primeiro
    exact_bonus = check_exact_match(original_query, original_keyword)
    # 2. Calcular similaridades base
    jaccard_sim = calculate_jaccard_similarity(token_set, kw_set)
    cosine_sim = calculate_cosine_similarity(freqs, kw_freq)
    
    # 3. Aplicar penalidade de tamanho
    size_penalty = calculate_size_penalty(token_set, kw_set)
    
    # 4. Combinar com pesos otimizados
    weighted_sim = (jaccard_sim * JACCARD_WEIGHT + 
                   cosine_sim * COSINE_WEIGHT)
    
    # 5. Aplicar penalidade e bonus
    combined = weighted_sim * size_penalty + exact_bonus
    
    # 6. Determinar threshold adaptativo
    # Keywords curtas precisam de match mais forte
    if keyword_length <= 2:
        adaptive_threshold = SHORT_KEYWORD_THRESHOLD
    else:
        adaptive_threshold = BASE_THRESHOLD
    
    return min(1.0, combined), adaptive_threshold


def calculate_multi_layer_score(
    token_set: Set[str],
    freqs: Dict[str, int],
    kw_set: Set[str],
    kw_freq: Dict[str, int],
    original_query: str,
    original_keyword: str
) -> float:
    """
    Alternative scoring: Multi-layer approach.
    
    Gives highest priority to exact matches, then subset matches,
    then similarity metrics.
    
    References:
        - Mihalcea et al. (2006). Measuring Semantic Similarity
    """
    if original_keyword.lower() in original_query.lower():
        return 1.0
    
    score = 0.0
    
    # Camada 2: Todos os tokens da keyword presentes (subset)
    if kw_set and kw_set.issubset(token_set):
        score += 0.6
    
    # Camada 3: Jaccard (peso médio)
    jaccard_sim = calculate_jaccard_similarity(token_set, kw_set)
    score += jaccard_sim * 0.3
    
    # Camada 4: Cosseno (peso baixo)
    cosine_sim = calculate_cosine_similarity(freqs, kw_freq)
    score += cosine_sim * 0.1
    
    return min(1.0, score)


def analyze_keywords(
    user_question: str, 
    use_multi_layer: bool = False,
    verbose: bool = False
) -> KeywordAnalysis:
    """
    Analyze a user question with improved keyword/topic matching.
    
    Args:
        user_question: The user's input query
        use_multi_layer: Use alternative multi-layer scoring (experimental)
        verbose: Print debug information
    
    Returns:
        KeywordAnalysis with matched keywords and suggested topic
    
    Key Improvements:
        1. Higher base threshold (0.40 vs 0.25) reduces false positives
        2. Jaccard-dominant weighting (0.65/0.35) better for keyword presence
        3. Adaptive thresholds for short vs long keywords
        4. Size normalization prevents mismatches between very different lengths
        5. Exact match detection with bonus scoring
    
    References:
        - Zahrotun, L. (2016). Comparison Jaccard/Cosine Similarity
        - Rekabsaz et al. (2017). Exploration of Threshold for Similarity
        - Alatrista-Salas et al. (2016). Combinations of Jaccard
        - Ferragina et al. (2015). Optimal Threshold Determination, PLOS ONE
    """
    try:
        token_set, freqs = extract_features(user_question)
        
        matched: Set[str] = set()
        match_scores: Dict[str, float] = {}
        if verbose:
            print(f"Query tokens: {token_set}")
            print(f"Query freqs: {freqs}")
        # Análise de keywords
        for kw in LIGHTRAG_KEYWORDS:
            kw_set, kw_freq = extract_features(kw)
            keyword_length = len(kw_set)
            if use_multi_layer:
                combined = calculate_multi_layer_score(
                    token_set, freqs, kw_set, kw_freq,
                    user_question, kw
                )
                adaptive_threshold = BASE_THRESHOLD
            else:
                combined, adaptive_threshold = calculate_combined_score(
                    token_set, freqs, kw_set, kw_freq,
                    user_question, kw, keyword_length
                )
            
            if verbose:
                print(
                    f"Keyword '{kw}': score={combined:.3f}, "
                    f"threshold={adaptive_threshold:.3f}"
                )
            
            if combined > adaptive_threshold:
                matched.add(kw)
                match_scores[kw] = combined
        
        if verbose:
            print(f"Matched keywords: {matched}")
        # Análise de tópicos
        TOPIC_PATTERNS = {}
        try:
            for category, topics in LIGHTRAG_TOPICS.items():
                if isinstance(topics, dict):
                    for topic_name, topic_data in topics.items():
                        if isinstance(topic_data, dict) and "keywords" in topic_data:
                            TOPIC_PATTERNS[topic_name] = {
                                "keywords": topic_data["keywords"],
                                "threshold": topic_data.get("threshold", 0.35)
                            }
                        elif category == "general_indicators" and "keywords" in topics:
                            TOPIC_PATTERNS[category] = {
                                "keywords": topics["keywords"],
                                "threshold": topics.get("threshold", 0.35)
                            }
                            break
                else:
                    # fallback para listas simples
                    TOPIC_PATTERNS[category] = {"keywords": topics, "threshold": 0.35}
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
                pkw_set, pkw_freq = extract_features(pkw.lower())
                pkw_length = len(pkw_set)
                
                if use_multi_layer:
                    combined = calculate_multi_layer_score(
                        token_set, freqs, pkw_set, pkw_freq,
                        user_question, pkw
                    )
                    adaptive_threshold = BASE_THRESHOLD
                else:
                    combined, adaptive_threshold = calculate_combined_score(
                        token_set, freqs, pkw_set, pkw_freq,
                        user_question, pkw, pkw_length
                    )
                if combined > adaptive_threshold:
                    topic_score += combined
                    topic_matches += 1
                    matched.add(pkw)
                    match_scores[pkw] = combined
            if topic_matches >= pattern.get('threshold', 1):
                conf = min(1.0, topic_score / max(1, len(pattern.get('keywords', []))))
                if conf > best_conf:
                    best_conf = conf
                    best_topic = topic
                    
        return KeywordAnalysis(
            has_lightrag_keywords=bool(matched),
            matched_keywords=sorted(list(matched)),
            suggested_topic=best_topic,
            confidence=best_conf,
            match_scores=match_scores
        )
    
    except Exception as e:
        logger.error("analyze_keywords failed: %s", str(e), exc_info=True)
        raise


# ============================================================================
# UTILIDADES ADICIONAIS
# ============================================================================

def compare_scoring_methods(user_question: str) -> Dict[str, KeywordAnalysis]:
    """
    Compare both scoring methods for analysis.
    
    Useful for evaluation and choosing the best approach.
    """
    return {
        'improved': analyze_keywords(user_question, use_multi_layer=False),
        'multi_layer': analyze_keywords(user_question, use_multi_layer=True)
    }


def evaluate_threshold_sensitivity(
    user_question: str,
    thresholds: List[float]
) -> Dict[float, int]:
    """
    Evaluate how many matches occur at different thresholds.
    
    Useful for tuning BASE_THRESHOLD for your specific dataset.
    
    References:
        - Rekabsaz et al. (2017). Exploration of Threshold for Similarity
    """
    token_set, freqs = extract_features(user_question)
    results = {}
    
    for threshold in thresholds:
        matched_count = 0
        for kw in LIGHTRAG_KEYWORDS:
            kw_set, kw_freq = extract_features(kw)
            jaccard = calculate_jaccard_similarity(token_set, kw_set)
            cosine = calculate_cosine_similarity(freqs, kw_freq)
            combined = jaccard * JACCARD_WEIGHT + cosine * COSINE_WEIGHT
            
            if combined > threshold:
                matched_count += 1
        
        results[threshold] = matched_count
    
    return results