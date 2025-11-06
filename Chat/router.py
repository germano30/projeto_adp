"""Sistema de roteamento para decidir entre SQL direto ou LightRAG"""

import logging
from typing import Dict, Optional
from enum import Enum
import json

from config import LIGHTRAG_KEYWORDS, LIGHTRAG_TOPICS, ROUTING_EXAMPLES
from llm_client import get_llm_client

logger = logging.getLogger(__name__)


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
    
    def route_question(self, user_question: str) -> Dict:
        """
        Analisa a pergunta e decide qual rota usar
        
        Args:
            user_question: Pergunta do usuário
            
        Returns:
            Dict com:
                - route: QueryRoute (sql, lightrag, ou hybrid)
                - reason: Explicação da decisão
                - topic: Tópico específico do LightRAG (se aplicável)
                - confidence: Confiança na decisão (0-1)
        """
        keyword_analysis = self._analyze_keywords(user_question)
        
        if not keyword_analysis['has_lightrag_keywords']:
            logger.info("Roteamento rápido: SQL (sem keywords do LightRAG)")
            return {
                'route': QueryRoute.SQL,
                'reason': 'Direct minimum wage query',
                'topic': None,
                'confidence': 0.9
            }
        
        llm_routing = self._llm_route_decision(user_question)
        
        if llm_routing:
            return llm_routing
        
        logger.warning("LLM routing falhou, usando fallback de keywords")
        if keyword_analysis['suggested_topic']:
            return {
                'route': QueryRoute.LIGHTRAG,
                'reason': f"Keyword match: {keyword_analysis['matched_keywords']}",
                'topic': keyword_analysis['suggested_topic'],
                'confidence': 0.6
            }
        else:
            return {
                'route': QueryRoute.SQL,
                'reason': 'Default fallback',
                'topic': None,
                'confidence': 0.5
            }
    
    def _analyze_keywords(self, user_question: str) -> Dict:
        """
        Analisa keywords na pergunta
        
        Args:
            user_question: Pergunta do usuário
            
        Returns:
            Dict com análise de keywords
        """
        question_lower = user_question.lower()
        matched_keywords = []
        
        for keyword in LIGHTRAG_KEYWORDS:
            if keyword.lower() in question_lower:
                matched_keywords.append(keyword)
        
        suggested_topic = None
        
        # Employment types
        if any(k in question_lower for k in ['agricultural', 'farm', 'agriculture']):
            suggested_topic = 'Agricultural Employment'
        elif any(k in question_lower for k in ['non-farm']):
            suggested_topic = 'Non-farm Employment'
        elif any(k in question_lower for k in ['entertainment', 'performer', 'actor', 'musician']):
            suggested_topic = 'Entertainment'
        elif any(k in question_lower for k in ['door-to-door', 'sales', 'salesperson']):
            suggested_topic = 'Door-to-Door Sales'
        
        # Labor laws
        elif any(k in question_lower for k in ['rest period', 'break', 'rest break']):
            suggested_topic = 'Minimum Paid Rest Periods'
        elif any(k in question_lower for k in ['meal period', 'lunch', 'dinner break']):
            suggested_topic = 'Minimum Meal Periods'
        elif any(k in question_lower for k in ['prevailing wage', 'davis bacon']):
            suggested_topic = 'Prevailing Wages'
        elif any(k in question_lower for k in ['payday', 'pay frequency', 'payment schedule']):
            suggested_topic = 'Payday Requirements'
        
        return {
            'has_lightrag_keywords': len(matched_keywords) > 0,
            'matched_keywords': matched_keywords,
            'suggested_topic': suggested_topic
        }
    
    def _llm_route_decision(self, user_question: str) -> Optional[Dict]:
        """
        Usa LLM (Gemini) para decidir a rota de forma mais inteligente

        Args:
            user_question: Pergunta do usuário

        Returns:
            Dict com decisão de roteamento ou None se falhar
        """
        try:
            system_prompt = self._get_routing_prompt()

            # --- MUDANÇA PRINCIPAL AQUI ---
            # Chamamos nosso novo método que GARANTE JSON
            result_json_string = self.llm_client.generate_sql_conditions(
                user_question=user_question,
                system_prompt=system_prompt
            )

            if not result_json_string:
                logger.error("LLM routing falhou (resposta vazia do cliente)")
                return None

            logger.debug(f"LLM routing response: {result_json_string}")

            # Agora apenas carregamos o JSON, sem precisar "extrair"
            routing_data = json.loads(result_json_string)
            # --- FIM DA MUDANÇA ---

            if not routing_data or 'route' not in routing_data:
                logger.error("Invalid routing response from LLM")
                return None

            route_str = routing_data['route'].lower()
            if route_str == 'sql':
                route = QueryRoute.SQL
            elif route_str == 'lightrag':
                route = QueryRoute.LIGHTRAG
            elif route_str == 'hybrid':
                route = QueryRoute.HYBRID
            else:
                logger.error(f"Unknown route type: {route_str}")
                return None

            return {
                'route': route,
                'reason': routing_data.get('reason', 'LLM decision'),
                'topic': routing_data.get('topic'),
                'confidence': 0.85
            }

        except json.JSONDecodeError as e:
            logger.error(f"Erro ao decodificar JSON do LLM: {e}")
            return None
        except Exception as e:
            logger.error(f"Erro no LLM routing: {e}")
            return None
    
    def _get_routing_prompt(self) -> str:
        """Retorna o prompt para decisão de roteamento"""
        
        topics_list = "\n".join([
            f"- {topic}" 
            for category in LIGHTRAG_TOPICS.values() 
            for topic in category
        ])
        
        return f"""You are a query routing assistant that decides how to process user questions about labor laws and wages.

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