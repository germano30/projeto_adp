"""
Query routing system for minimum wage information retrieval.

This module implements a sophisticated routing system that determines the optimal
processing path for user queries about minimum wage and labor laws. It combines
keyword analysis, topic classification, and LLM-based decision making to route
queries to either SQL, RAG, or hybrid processing pipelines.
"""

import logging
import json
from dataclasses import dataclass
from enum import Enum
from typing import Dict, List, Optional, Set, Tuple

from config import LIGHTRAG_KEYWORDS, LIGHTRAG_TOPICS, ROUTING_EXAMPLES
from llm_client import get_llm_client

logger = logging.getLogger(__name__)


class RoutingError(Exception):
    """Base exception for routing errors."""
    pass

class TopicClassificationError(RoutingError):
    """Raised when topic classification fails."""
    pass

class LLMRoutingError(RoutingError):
    """Raised when LLM-based routing fails."""
    pass


from analysis import analyze_keywords, KeywordAnalysis


class QueryRoute(Enum):
    """Tipos de rota para processar a query"""
    SQL = "sql"
    LIGHTRAG = "lightrag"
    HYBRID = "hybrid"  # Para casos que precisam de ambos


class QueryRouter:
    """Decide qual sistema usar para responder a pergunta do usuário"""
    
    def __init__(self):
        self.llm_client = get_llm_client()
        logger.info("QueryRouter inicializado")
    
    def route_question(self, user_question: str) -> Dict[str, any]:
        """
        Analyze and route user questions to the appropriate processing pipeline.

        Multi-stage strategy:
        1. Keyword analysis for quick routing of clear cases
        2. LLM-based analysis for complex queries
        3. Fallback logic based on keywords

        Returns a dict with keys: 'route', 'reason', 'topic', 'confidence'.
        """
        try:
            # Stage 1: Keyword analysis (fast path)
            keyword_analysis = self._analyze_keywords(user_question)

            if not keyword_analysis.has_lightrag_keywords:
                logger.info("Fast routing: SQL (no LightRAG keywords)")
                return {
                    'route': QueryRoute.SQL,
                    'reason': 'Direct minimum wage query without specialized terms',
                    'topic': None,
                    'confidence': 0.9
                }

            # Stage 2: LLM-based routing for ambiguous/complex cases
            llm_routing = self._llm_route_decision(user_question)
            if llm_routing:
                if keyword_analysis.suggested_topic:
                    llm_topic = llm_routing.get('topic')
                    if llm_topic and llm_topic == keyword_analysis.suggested_topic:
                        llm_routing['confidence'] = min(1.0, llm_routing['confidence'] + 0.1)
                return llm_routing

            # Stage 3: Fallback to keyword-based topic if LLM fails
            logger.warning("LLM routing failed, using keyword analysis fallback")
            if keyword_analysis.suggested_topic:
                return {
                    'route': QueryRoute.LIGHTRAG,
                    'reason': f"Topic classification via keywords: {', '.join(keyword_analysis.matched_keywords[:3])}",
                    'topic': keyword_analysis.suggested_topic,
                    'confidence': max(0.6, keyword_analysis.confidence)
                }

            # Default fallback
            return {
                'route': QueryRoute.SQL,
                'reason': 'Default fallback to SQL query',
                'topic': None,
                'confidence': 0.5
            }

        except Exception as e:
            logger.error("Query routing failed: %s", str(e))
            raise RoutingError(f"Failed to route query: {str(e)}") from e
    
    # Topic classification patterns
    TOPIC_PATTERNS = {
        'Agricultural Employment': {
            'keywords': ['agricultural', 'farm', 'agriculture', 'farming', 'crop', 'harvest'],
            'threshold': 1
        },
        'Non-farm Employment': {
            'keywords': ['non-farm', 'non farm', 'nonfarm', 'general employment'],
            'threshold': 1
        },
        'Entertainment': {
            'keywords': ['entertainment', 'performer', 'actor', 'musician', 'artist', 'theatrical',
                        'stage', 'performance', 'concert', 'show', 'production'],
            'threshold': 1
        },
        'Door-to-Door Sales': {
            'keywords': ['door-to-door', 'door to door', 'sales', 'salesperson', 'commission',
                        'direct selling', 'outside sales'],
            'threshold': 1
        },
        'Minimum Paid Rest Periods': {
            'keywords': ['rest period', 'break', 'rest break', 'work period', 'rest time',
                        'break period', 'rest requirement'],
            'threshold': 2
        },
        'Minimum Meal Periods': {
            'keywords': ['meal period', 'lunch', 'dinner break', 'meal break', 'meal time',
                        'lunch period', 'dining period'],
            'threshold': 2
        },
        'Prevailing Wages': {
            'keywords': ['prevailing wage', 'davis bacon', 'government contract', 'federal contract',
                        'public works', 'prevailing rate'],
            'threshold': 1
        },
        'Payday Requirements': {
            'keywords': ['payday', 'pay frequency', 'payment schedule', 'pay period',
                        'wage payment', 'pay timing', 'paycheck frequency'],
            'threshold': 1
        }
    }

    @staticmethod
    def _calculate_jaccard_similarity(set1: Set[str], set2: Set[str]) -> float:
        """
        Calculate Jaccard similarity between two sets of words.
        
        Parameters
        ----------
        set1 : Set[str]
            First set of words
        set2 : Set[str]
            Second set of words
            
        Returns
        -------
        float
            Jaccard similarity score (0-1)
        """
        if not set1 or not set2:
            return 0.0
        intersection = len(set1.intersection(set2))
        union = len(set1.union(set2))
        return intersection / union if union > 0 else 0.0

    @staticmethod
    def _calculate_cosine_similarity(vec1: Dict[str, int], vec2: Dict[str, int]) -> float:
        """
        Calculate cosine similarity between two word frequency vectors.
        
        Parameters
        ----------
        vec1 : Dict[str, int]
            First word frequency vector
        vec2 : Dict[str, int]
            Second word frequency vector
            
        Returns
        -------
        float
            Cosine similarity score (0-1)
        """
        # Get common words
        common_words = set(vec1.keys()) & set(vec2.keys())
        if not common_words:
            return 0.0
            
        # Calculate dot product
        dot_product = sum(vec1[word] * vec2[word] for word in common_words)
        
        # Calculate magnitudes
        mag1 = sum(val * val for val in vec1.values()) ** 0.5
        mag2 = sum(val * val for val in vec2.values()) ** 0.5
        
        return dot_product / (mag1 * mag2) if mag1 * mag2 > 0 else 0.0

    def _analyze_keywords(self, user_question: str) -> KeywordAnalysis:
        """
        Analyze keywords in user query using multiple similarity metrics.
        
        This method implements a sophisticated keyword analysis system that:
        1. Uses Jaccard similarity for set-based matching
        2. Uses cosine similarity for frequency-based matching
        3. Combines multiple similarity metrics for robust matching
        4. Calculates confidence scores based on similarity scores
        
        Parameters
        ----------
        user_question : str
            The user's question to analyze
            
        Returns
        -------
        KeywordAnalysis
            Complete analysis results including matched keywords and topic
        
        Raises
        ------
        TopicClassificationError
            If topic classification fails due to invalid patterns
        """
            
        return analyze_keywords(user_question)
    
    def _llm_route_decision(self, user_question: str) -> Optional[Dict[str, any]]:
        """
        Use LLM for intelligent query routing decisions.
        
        This method implements LLM-based query analysis to determine the optimal
        processing route. It handles:
        1. Query complexity assessment
        2. Topic classification
        3. Hybrid processing requirements
        
        The implementation includes:
        - Structured prompt engineering
        - Response validation
        - Error handling and logging
        - Confidence scoring
        
        Parameters
        ----------
        user_question : str
            The user's question to analyze
            
        Returns
        -------
        Optional[Dict[str, any]]
            Routing decision if successful, None if routing fails
            
        Raises
        ------
        LLMRoutingError
            If LLM processing fails
        json.JSONDecodeError
            If LLM response parsing fails
        """
        try:
            system_prompt = self._get_routing_prompt()
            
            # Get routing decision from LLM
            result_json_string = self.llm_client.generate_sql_conditions(
                user_question=user_question,
                system_prompt=system_prompt
            )
            
            if not result_json_string:
                raise LLMRoutingError("Empty response from LLM client")
            
            logger.debug("LLM routing raw response: %s", result_json_string)
            
            # Parse and validate response
            try:
                routing_data = json.loads(result_json_string)
            except json.JSONDecodeError as e:
                raise LLMRoutingError(f"Invalid JSON response from LLM: {str(e)}") from e
            
            if not routing_data or 'route' not in routing_data:
                raise LLMRoutingError("Missing required 'route' field in LLM response")
            
            # Normalize and validate route
            route_str = routing_data['route'].lower()
            try:
                route = {
                    'sql': QueryRoute.SQL,
                    'lightrag': QueryRoute.LIGHTRAG,
                    'hybrid': QueryRoute.HYBRID
                }[route_str]
            except KeyError:
                raise LLMRoutingError(f"Invalid route type: {route_str}")
            
            # Calculate confidence based on response quality
            confidence = 0.85  # Base confidence
            if routing_data.get('reason'):
                confidence += 0.05
            if route != QueryRoute.SQL and routing_data.get('topic'):
                confidence += 0.05
            
            return {
                'route': route,
                'reason': routing_data.get('reason', 'LLM-based classification'),
                'topic': routing_data.get('topic'),
                'confidence': min(1.0, confidence)
            }
            
        except LLMRoutingError as e:
            logger.error("LLM routing failed: %s", str(e))
            return None
        except Exception as e:
            logger.error("Unexpected error in LLM routing: %s", str(e))
            logger.debug("Detailed error information:", exc_info=True)
            return None

        except json.JSONDecodeError as e:
            logger.error(f"Erro ao decodificar JSON do LLM: {e}")
            return None
        except Exception as e:
            logger.error(f"Erro no LLM routing: {e}")
            return None
    
    def _get_routing_prompt(self) -> str:
        """
        Generate the system prompt for LLM routing decisions.
        
        This method constructs a detailed prompt that guides the LLM in:
        1. Query classification
        2. Route selection
        3. Topic identification
        4. Confidence assessment
        
        The prompt includes:
        - Available topics and categories
        - Routing rules and criteria
        - Example queries and decisions
        - Response format specifications
        
        Returns
        -------
        str
            Complete system prompt for LLM
        """
        # Build hierarchical topic list
        topic_sections = []
        for category, topics in LIGHTRAG_TOPICS.items():
            topic_sections.append(f"\n{category}:")
            topic_sections.extend([f"  - {topic}" for topic in topics])
        topics_list = "\n".join(topic_sections)
        
        return f"""You are a query routing assistant that specializes in labor law and wage regulation queries.

                    You must decide between three routes:
                    1. "sql" - Direct queries about minimum wage amounts that can be answered from structured database
                    2. "lightrag" - Questions about specific labor law topics, special employment types, or regulatory requirements
                    3. "hybrid" - Questions that need both wage data AND additional context from labor laws

                    AVAILABLE LIGHTRAG TOPICS:
                    {topics_list}

                    ROUTING RULES:
                    - Use "sql" for: specific wage amounts, wage comparisons, historical wage data, tipped vs standard wages
                    - Use "lightrag" for: labor law requirements, special employment categories, regulatory details, compliance questions
                    - Use "hybrid" for: questions that explicitly ask about wages AND special rules/laws

                    {ROUTING_EXAMPLES}

                    IMPORTANT: 
                    - Return ONLY a JSON object with: {{"route": "sql|lightrag|hybrid", "reason": "brief explanation", "topic": "specific topic name or null"}}
                    - Be conservative: if unsure between sql and lightrag, prefer "sql" for wage-related questions
                    - topic field is required ONLY for lightrag and hybrid routes

                    Analyze the user's question and provide routing decision:"""


def get_query_router() -> QueryRouter:
    """
    Retorna uma instância singleton do QueryRouter
    
    Returns:
        Instância do QueryRouter
    """
    global _query_router
    if '_query_router' not in globals():
        globals()['_query_router'] = QueryRouter()
    return globals()['_query_router']