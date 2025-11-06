"""
Core pipeline module for processing minimum wage queries.

This module implements the main processing pipeline that handles natural language queries
about minimum wage information. It orchestrates the interaction between different components:
- Query routing (SQL vs RAG vs Hybrid)
- Database operations
- Language model interactions
- Response generation and formatting
"""

import asyncio
import inspect
import logging
from typing import Dict, List, Optional, Tuple

from config import BASE_QUERY
from database import get_db_manager
from lightrag_client import get_lightrag_client
from llm_client import get_llm_client
from prompts import (
    get_hybrid_response_prompt,
    get_lightrag_response_prompt,
    get_response_generation_prompt,
    get_sql_generation_prompt,
)
from router import QueryRoute, get_query_router
from utils import (
    build_sql_query,
    create_error_response,
    extract_json_from_response,
    format_query_results,
    log_conversation,
    sanitize_user_input,
    validate_sql_conditions,
)
from analysis import analyze_keywords
import os

logger = logging.getLogger(__name__)

# Hybrid override threshold (can be adjusted via env var HYBRID_CONFIDENCE)
HYBRID_CONFIDENCE_THRESHOLD = float(os.environ.get('HYBRID_CONFIDENCE', '0.55'))


class MinimumWagePipeline:
    """
    Core pipeline for processing minimum wage related queries.
    
    This class orchestrates the entire query processing workflow, from initial
    query routing to final response generation. It handles both structured data
    queries through SQL and unstructured data through RAG (Retrieval Augmented Generation).
    """
    
    def __init__(self, use_mock_lightrag: bool = False):
        """
        Initialize the pipeline with required components.
        
        Parameters
        ----------
        use_mock_lightrag : bool, optional
            If True, uses a mock LightRAG implementation for development and testing,
            defaults to False
        """
        self.db_manager = get_db_manager()
        self.llm_client = get_llm_client()
        self.router = get_query_router()
        self.lightrag_client = get_lightrag_client(use_mock=use_mock_lightrag)
        logger.info("Pipeline initialized with all components")

    def analyze_keywords(self, user_question: str):
        """Convenience wrapper to use shared keyword analysis utilities.

        This avoids importing analysis logic directly in callers and keeps a
        single integration point if we later need to adapt or mock behavior.
        """
        return analyze_keywords(user_question)

    def _call_lightrag_query(self, topic: str, user_prompt: str, state: Optional[str] = None):
        """
        Execute a LightRAG query with support for both async and sync implementations.
        - Se query_topic for coroutine function -> usa asyncio.run(...)
        - Caso contrário, chama diretamente e tenta adaptar argumentos.
        """
        fn = getattr(self.lightrag_client, "query_topic", None)
        if fn is None:
            logger.error("LightRAG client não possui método query_topic")
            return None

        if inspect.iscoroutinefunction(fn):
            try:
                return asyncio.run(fn(topic, state))
            except Exception as e:
                logger.error(f"Erro ao executar query_topic (async): {e}")
                return None

        try:
            return fn(topic, user_prompt, state)
        except TypeError:
            try:
                return fn(topic, user_prompt)
            except Exception as e:
                logger.error(f"Erro ao executar query_topic (sync fallback): {e}")
                return None
        except Exception as e:
            logger.error(f"Erro inesperado ao chamar query_topic: {e}")
            return None

    def process_question(self, user_question: str) -> Dict:
        """
        Processa uma pergunta do usuário do início ao fim
        
        Args:
            user_question: Pergunta do usuário
            
        Returns:
            Dict com:
                - success (bool): Se o processamento foi bem-sucedido
                - response (str): Resposta final ou mensagem de erro
                - route (str): Rota usada (sql, lightrag, hybrid)
                - sql_query (str): Query SQL gerada (se aplicável)
                - results_count (int): Número de resultados (se aplicável)
                - topic (str): Tópico do LightRAG (se aplicável)
        """
        logger.info(f"Processando pergunta: '{user_question}'")
        
        clean_question = sanitize_user_input(user_question)

        # Lightweight keyword/topic analysis to inform routing heuristics
        try:
            analysis = self.analyze_keywords(clean_question)
            logger.debug("Keyword analysis: suggested_topic=%s confidence=%.2f matched=%s",
                         analysis.suggested_topic, analysis.confidence, analysis.matched_keywords)
        except Exception:
            analysis = None
            logger.debug("Keyword analysis unavailable or failed; continuing without it")

        # Passo 1: Roteamento - decidir qual sistema usar
        routing_decision = self.router.route_question(clean_question)

        # Heuristic override logic:
        # - If analysis suggests a LightRAG topic AND the question also contains wage-related tokens,
        #   prefer HYBRID (both SQL + LightRAG).
        # - Otherwise, if router picked SQL but analysis strongly suggests LightRAG, switch to LightRAG.
        try:
            question_lower = clean_question.lower()
            wage_tokens = ['wage', 'minimum', 'minimum wage', 'tipped', 'tip', 'cash', 'rate', 'salary']

            has_wage_token = any(tok in question_lower for tok in wage_tokens)

            # Check if LLM routing (separate) suggests LightRAG/hybrid as well
            llm_hint = None
            try:
                llm_hint = self.router._llm_route_decision(clean_question)
            except Exception:
                llm_hint = None

            if has_wage_token and (analysis and analysis.suggested_topic or (llm_hint and llm_hint.get('route') != QueryRoute.SQL)):
                suggested_topic = (analysis.suggested_topic if analysis and analysis.suggested_topic else (llm_hint.get('topic') if llm_hint else None))
                confidence = (analysis.confidence if analysis else (llm_hint.get('confidence') if llm_hint else 0.6))
                logger.info("Overriding routing decision to HYBRID based on wage tokens + LightRAG signal: %s (%.2f)",
                            suggested_topic, confidence)
                routing_decision = {
                    'route': QueryRoute.HYBRID,
                    'reason': 'Hybrid override (wage + LightRAG signal)',
                    'topic': suggested_topic,
                    'confidence': confidence
                }
            elif (routing_decision and routing_decision.get('route') == QueryRoute.SQL
                  and analysis and analysis.suggested_topic and analysis.confidence >= 0.6):
                logger.info("Overriding routing decision to LightRAG based on keyword analysis: %s (%.2f)",
                            analysis.suggested_topic, analysis.confidence)
                routing_decision = {
                    'route': QueryRoute.LIGHTRAG,
                    'reason': 'Keyword analysis override',
                    'topic': analysis.suggested_topic,
                    'confidence': analysis.confidence
                }
        except Exception:
            logger.debug("Failed to apply routing heuristic override; using original routing decision")
        logger.info(f"Decisão de roteamento: {routing_decision['route'].value} (confiança: {routing_decision['confidence']:.2f})")
        logger.info(f"Razão: {routing_decision.get('reason','-')}")
        
        # Processa baseado na rota
        if routing_decision['route'] == QueryRoute.SQL:
            return self._process_sql_route(clean_question, routing_decision)
        
        elif routing_decision['route'] == QueryRoute.LIGHTRAG:
            return self._process_lightrag_route(clean_question, routing_decision)
        
        elif routing_decision['route'] == QueryRoute.HYBRID:
            return self._process_hybrid_route(clean_question, routing_decision)
        
        else:
            logger.error(f"Rota desconhecida: {routing_decision['route']}")
            return {
                'success': False,
                'response': create_error_response('parse_error', user_question),
                'route': 'unknown',
                'error': 'Unknown routing decision'
            }
    
    # -----------------------
    # SQL route
    # -----------------------
    def _process_sql_route(self, user_question: str, routing_decision: Dict) -> Dict:
        """Processa pergunta usando rota SQL"""
        logger.info("Processando via rota SQL")
        
        # Gerar condições SQL
        sql_conditions = self._generate_sql_conditions(user_question)
        if sql_conditions is None:
            return {
                'success': False,
                'response': create_error_response('parse_error', user_question),
                'route': 'sql',
                'error': 'Failed to parse SQL conditions'
            }
        
        # Construir e executar query
        sql_query = build_sql_query(BASE_QUERY, sql_conditions)
        query_results = self._execute_query(sql_query)
        
        if query_results is None:
            return {
                'success': False,
                'response': create_error_response('db_error', user_question),
                'route': 'sql',
                'error': 'Database query failed'
            }
        
        if len(query_results) == 0:
            return {
                'success': False,
                'response': create_error_response('no_data', user_question),
                'route': 'sql',
                'sql_query': sql_query,
                'results_count': 0
            }
        
        # Gerar resposta em linguagem natural
        natural_response = self._generate_natural_response(user_question, query_results)
        
        if natural_response is None:
            return {
                'success': False,
                'response': create_error_response('db_error', user_question),
                'route': 'sql',
                'sql_query': sql_query,
                'results_count': len(query_results),
                'error': 'Failed to generate natural response'
            }
        
        log_conversation(user_question, sql_query, len(query_results), natural_response)
        
        return {
            'success': True,
            'response': natural_response,
            'route': 'sql',
            'sql_query': sql_query,
            'results_count': len(query_results),
            'conditions': sql_conditions
        }
    
    # -----------------------
    # LightRAG route
    # -----------------------
    def _process_lightrag_route(self, user_question: str, routing_decision: Dict) -> Dict:
        """Processa pergunta usando rota LightRAG"""
        logger.info("Processando via rota LightRAG")
        
        topic = routing_decision.get('topic') or user_question
        state = self._extract_state_from_question(user_question)
        
        lightrag_result = self._call_lightrag_query(topic, user_question, state)

        if lightrag_result is None:
            return {
                'success': False,
                'response': create_error_response('no_data', user_question),
                'route': 'lightrag',
                'topic': topic,
                'error': 'No LightRAG data found'
            }
        content = None
        sources = []
        metadata = {}

        if isinstance(lightrag_result, dict):
            content = lightrag_result.get('content') or lightrag_result.get('answer') or lightrag_result.get('text')
            sources = lightrag_result.get('sources') or lightrag_result.get('references') or []
            metadata = lightrag_result.get('metadata') or {}
        else:
            content = str(lightrag_result)
            sources = []
            metadata = {}

        if not content:
            logger.warning("LightRAG retornou conteúdo vazio")
            return {
                'success': False,
                'response': create_error_response('no_data', user_question),
                'route': 'lightrag',
                'topic': topic,
                'error': 'LightRAG returned empty content'
            }
        
        natural_response = self._generate_lightrag_response(
            user_question, 
            content
        )
        
        if natural_response is None:
            return {
                'success': False,
                'response': create_error_response('db_error', user_question),
                'route': 'lightrag',
                'topic': topic,
                'error': 'Failed to generate response from LightRAG data'
            }
        
        return {
            'success': True,
            'response': natural_response,
            'route': 'lightrag',
            'topic': topic,
            'sources': sources,
            'metadata': metadata
        }
    
    # -----------------------
    # Hybrid route
    # -----------------------
    def _process_hybrid_route(self, user_question: str, routing_decision: Dict) -> Dict:
        """Processa pergunta usando ambas as rotas (SQL + LightRAG)"""
        logger.info("Processando via rota HÍBRIDA (SQL + LightRAG)")
        
        # Parte 1: Executar query SQL (se aplicável)
        sql_conditions = self._generate_sql_conditions(user_question)
        sql_results = None
        sql_query = None
        
        if sql_conditions:
            sql_query = build_sql_query(BASE_QUERY, sql_conditions)
            sql_results = self._execute_query(sql_query)
        
        # Parte 2: Consultar LightRAG
        topic = routing_decision.get('topic') or user_question
        state = self._extract_state_from_question(user_question)
        lightrag_result = self._call_lightrag_query(topic, user_question, state)

        # Verificar se temos pelo menos um dos dois
        if (sql_results is None or len(sql_results) == 0) and lightrag_result is None:
            return {
                'success': False,
                'response': create_error_response('no_data', user_question),
                'route': 'hybrid',
                'error': 'No data found in either SQL or LightRAG'
            }
        
        # Normaliza conteúdo do LightRAG (mesma lógica usada antes)
        lightrag_content = ""
        if isinstance(lightrag_result, dict):
            lightrag_content = lightrag_result.get('content') or lightrag_result.get('answer') or ""
        else:
            lightrag_content = str(lightrag_result) if lightrag_result else ""

        # Gerar resposta combinada
        natural_response = self._generate_hybrid_response(
            user_question,
            sql_results or [],
            lightrag_content
        )
        
        if natural_response is None:
            return {
                'success': False,
                'response': create_error_response('db_error', user_question),
                'route': 'hybrid',
                'error': 'Failed to generate hybrid response'
            }
        
        return {
            'success': True,
            'response': natural_response,
            'route': 'hybrid',
            'sql_query': sql_query,
            'results_count': len(sql_results) if sql_results else 0,
            'topic': topic,
            'sources': (lightrag_result.get('sources') if isinstance(lightrag_result, dict) else []) or []
        }
    
    # -----------------------
    # Auxiliares (SQL / LLM)
    # -----------------------
    def _generate_sql_conditions(self, user_question: str) -> Optional[Dict]:
        """Gera as condições SQL a partir da pergunta"""
        try:
            system_prompt = get_sql_generation_prompt()
            response_text = self.llm_client.generate_sql_conditions(
                user_question, 
                system_prompt
            )
            
            if response_text is None:
                logger.error("LLM retornou None para condições SQL")
                return None
            
            conditions = extract_json_from_response(response_text)
            
            if conditions is None:
                logger.error("Falha ao extrair JSON da resposta do LLM")
                logger.debug(f"Resposta do LLM: {response_text}")
                return None
            
            if not validate_sql_conditions(conditions):
                logger.error("Condições SQL inválidas")
                return None
            
            logger.info(f"Condições extraídas: {conditions}")
            return conditions
            
        except Exception as e:
            logger.error(f"Erro ao gerar condições SQL: {e}")
            return None
    
    def _execute_query(self, sql_query: str) -> Optional[List[Tuple]]:
        """Executa a query no banco de dados"""
        try:
            results = self.db_manager.execute_query(sql_query)
            logger.info(f"Query executada: {len(results)} resultados")
            return results
        except Exception as e:
            logger.error(f"Erro ao executar query: {e}")
            return None
    
    def _generate_natural_response(self, user_question: str, query_results: List[Tuple]) -> Optional[str]:
        """Gera uma resposta em linguagem natural para resultados SQL"""
        try:
            system_prompt = get_response_generation_prompt(user_question, query_results)
            logger.debug("Response generation system prompt: %s", system_prompt)
            response = self.llm_client.generate_natural_response(
                user_question,
                system_prompt
            )
            return response
        except Exception as e:
            logger.error(f"Erro ao gerar resposta natural: {e}")
            return None
    
    def _generate_lightrag_response(self, user_question: str, lightrag_content: str) -> Optional[str]:
        """Gera resposta baseada em conteúdo do LightRAG"""
        try:
            system_prompt = get_lightrag_response_prompt(user_question, lightrag_content)
            response = self.llm_client.generate_natural_response(
                user_question,
                system_prompt
            )
            return response
        except Exception as e:
            logger.error(f"Erro ao gerar resposta LightRAG: {e}")
            return None
    
    def _generate_hybrid_response(self, user_question: str, sql_results: List[Tuple], lightrag_content: str) -> Optional[str]:
        """Gera resposta híbrida combinando SQL e LightRAG"""
        try:
            system_prompt = get_hybrid_response_prompt(user_question, sql_results, lightrag_content)
            response = self.llm_client.generate_natural_response(
                user_question,
                system_prompt
            )
            return response
        except Exception as e:
            logger.error(f"Erro ao gerar resposta híbrida: {e}")
            return None
    
    def _extract_state_from_question(self, user_question: str) -> Optional[str]:
        """Tenta extrair nome do estado da pergunta do usuário"""
        from config import VALID_STATES
        
        question_lower = user_question.lower()
        for state in VALID_STATES:
            if state.lower() in question_lower:
                return state
        return None


    def test_components(self) -> Dict[str, bool]:
        """Testa todos os componentes do pipeline"""
        logger.info("Testando componentes do pipeline...")
        
        results = {
            'database': False,
            'llm': False,
            'lightrag': False,
            'router': True  # Router não precisa de conexão externa
        }
        
        try:
            results['database'] = self.db_manager.test_connection()
        except Exception as e:
            logger.error(f"Erro ao testar banco de dados: {e}")
        
        try:
            results['llm'] = self.llm_client.test_connection()
        except Exception as e:
            logger.error(f"Erro ao testar LLM: {e}")
        
        try:
            fn = getattr(self.lightrag_client, "test_connection", None)
            if fn is None:
                results['lightrag'] = False
            elif inspect.iscoroutinefunction(fn):
                results['lightrag'] = asyncio.run(fn())
            else:
                results['lightrag'] = fn()
        except Exception as e:
            logger.error(f"Erro ao testar LightRAG: {e}")
            results['lightrag'] = False
        
        logger.info(f"Resultados dos testes: {results}")
        return results


def create_pipeline(use_mock_lightrag: bool = False) -> MinimumWagePipeline:
    """
    Factory function para criar uma instância do pipeline
    
    Args:
        use_mock_lightrag: Se True, usa implementação mock do LightRAG
        
    Returns:
        Instância configurada do MinimumWagePipeline
    """
    return MinimumWagePipeline(use_mock_lightrag=use_mock_lightrag)