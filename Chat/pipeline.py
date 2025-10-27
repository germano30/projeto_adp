# pipeline.py
"""Pipeline principal para processar perguntas sobre salários mínimos"""

import logging
from typing import Optional, Dict, Tuple, List

from config import BASE_QUERY
from prompts import (
    get_sql_generation_prompt, 
    get_response_generation_prompt,
    get_lightrag_response_prompt,
    get_hybrid_response_prompt
)
from utils import (
    extract_json_from_response,
    validate_sql_conditions,
    build_sql_query,
    format_query_results,
    sanitize_user_input,
    log_conversation,
    create_error_response
)
from database import get_db_manager
from llm_client import get_llm_client
from router import get_query_router, QueryRoute
from lightrag_client import get_lightrag_client

logger = logging.getLogger(__name__)


class MinimumWagePipeline:
    """Pipeline completo para responder perguntas sobre salários mínimos"""
    
    def __init__(self, use_mock_lightrag: bool = True):
        """
        Inicializa o pipeline com os componentes necessários
        
        Args:
            use_mock_lightrag: Se True, usa mock do LightRAG para desenvolvimento
        """
        self.db_manager = get_db_manager()
        self.llm_client = get_llm_client()
        self.router = get_query_router()
        self.lightrag_client = get_lightrag_client(use_mock=use_mock_lightrag)
        logger.info("Pipeline inicializado")
    
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
        
        # Sanitiza o input
        clean_question = sanitize_user_input(user_question)
        
        # Passo 1: Roteamento - decidir qual sistema usar
        routing_decision = self.router.route_question(clean_question)
        logger.info(f"Decisão de roteamento: {routing_decision['route'].value} (confiança: {routing_decision['confidence']:.2f})")
        logger.info(f"Razão: {routing_decision['reason']}")
        
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
    
    def _process_lightrag_route(self, user_question: str, routing_decision: Dict) -> Dict:
        """Processa pergunta usando rota LightRAG"""
        logger.info("Processando via rota LightRAG")
        
        topic = routing_decision.get('topic')
        if not topic:
            logger.warning("Tópico não especificado para rota LightRAG")
            # Tenta inferir estado da pergunta para contexto
            state = self._extract_state_from_question(user_question)
        else:
            state = self._extract_state_from_question(user_question)
        
        # Consulta LightRAG
        lightrag_result = self.lightrag_client.query_topic(topic, user_question, state)
        
        if lightrag_result is None:
            return {
                'success': False,
                'response': create_error_response('no_data', user_question),
                'route': 'lightrag',
                'topic': topic,
                'error': 'No LightRAG data found'
            }
        
        # Gerar resposta usando conteúdo do LightRAG
        natural_response = self._generate_lightrag_response(
            user_question, 
            lightrag_result['content']
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
            'sources': lightrag_result.get('sources', []),
            'metadata': lightrag_result.get('metadata', {})
        }
    
    def _process_hybrid_route(self, user_question: str, routing_decision: Dict) -> Dict:
        """Processa pergunta usando ambas as rotas (SQL + LightRAG)"""
        logger.info("Processando via rota HÍBRIDA (SQL + LightRAG)")
        
        # Parte 1: Executar query SQL
        sql_conditions = self._generate_sql_conditions(user_question)
        sql_results = None
        sql_query = None
        
        if sql_conditions:
            sql_query = build_sql_query(BASE_QUERY, sql_conditions)
            sql_results = self._execute_query(sql_query)
        
        # Parte 2: Consultar LightRAG
        topic = routing_decision.get('topic')
        state = self._extract_state_from_question(user_question)
        lightrag_result = self.lightrag_client.query_topic(topic, user_question, state)
        
        # Verificar se temos pelo menos um dos dois
        if (sql_results is None or len(sql_results) == 0) and lightrag_result is None:
            return {
                'success': False,
                'response': create_error_response('no_data', user_question),
                'route': 'hybrid',
                'error': 'No data found in either SQL or LightRAG'
            }
        
        # Gerar resposta combinada
        natural_response = self._generate_hybrid_response(
            user_question,
            sql_results or [],
            lightrag_result['content'] if lightrag_result else ""
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
            'sources': lightrag_result.get('sources', []) if lightrag_result else []
        }
    
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
    
    def _generate_hybrid_response(self, user_question: str, sql_results: List[Tuple], 
                                   lightrag_content: str) -> Optional[str]:
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
        
        # Testa banco de dados
        try:
            results['database'] = self.db_manager.test_connection()
        except Exception as e:
            logger.error(f"Erro ao testar banco de dados: {e}")
        
        # Testa LLM
        try:
            results['llm'] = self.llm_client.test_connection()
        except Exception as e:
            logger.error(f"Erro ao testar LLM: {e}")
        
        # Testa LightRAG
        try:
            results['lightrag'] = self.lightrag_client.test_connection()
        except Exception as e:
            logger.error(f"Erro ao testar LightRAG: {e}")
        
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