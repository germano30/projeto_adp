# lightrag_client.py
"""Cliente para interação com LightRAG armazenado em PostgreSQL"""

import logging
from typing import Optional, List, Dict
import psycopg2
from psycopg2 import Error

from config import DATABASE_CONFIG

logger = logging.getLogger(__name__)


class LightRAGClient:
    """Cliente para consultar dados do LightRAG no PostgreSQL"""
    
    def __init__(self, config: dict = None):
        """
        Inicializa o cliente LightRAG
        
        Args:
            config: Configurações de conexão com o banco
        """
        self.config = config or DATABASE_CONFIG
        logger.info("LightRAGClient inicializado")
    
    def query_topic(self, topic: str, user_question: str, state: Optional[str] = None) -> Optional[Dict]:
        """
        Consulta o LightRAG sobre um tópico específico
        
        Args:
            topic: Tópico a consultar (ex: 'Agricultural Employment')
            user_question: Pergunta original do usuário
            state: Estado específico (opcional)
            
        Returns:
            Dict com:
                - content: Texto com a informação do LightRAG
                - sources: Lista de fontes/referências
                - metadata: Metadados adicionais
        """
        try:
            logger.info(f"Consultando LightRAG - Tópico: {topic}, Estado: {state}")
            
            # Aqui você implementaria a consulta real ao LightRAG
            # Por enquanto, vou criar uma estrutura de exemplo
            
            # Exemplo de query que você adaptaria para seu schema do LightRAG
            query = """
            SELECT 
                document_content,
                source_url,
                metadata,
                state_name
            FROM lightrag_documents
            WHERE topic = %s
            """
            
            params = [topic]
            
            if state:
                query += " AND (state_name = %s OR state_name IS NULL)"
                params.append(state)
            
            query += " ORDER BY relevance_score DESC LIMIT 5"
            
            with psycopg2.connect(**self.config) as conn:
                with conn.cursor() as cur:
                    cur.execute(query, params)
                    results = cur.fetchall()
                    
                    if not results:
                        logger.warning(f"Nenhum resultado encontrado para tópico: {topic}")
                        return None
                    
                    # Agrega os resultados
                    content_parts = []
                    sources = []
                    
                    for row in results:
                        content, source, metadata, state_name = row
                        if content:
                            content_parts.append(content)
                        if source:
                            sources.append(source)
                    
                    return {
                        'content': '\n\n'.join(content_parts),
                        'sources': list(set(sources)),  # Remove duplicatas
                        'metadata': {
                            'topic': topic,
                            'state': state,
                            'num_sources': len(results)
                        }
                    }
        
        except Error as e:
            logger.error(f"Erro ao consultar LightRAG: {e}")
            return None
        except Exception as e:
            logger.error(f"Erro inesperado no LightRAG: {e}")
            return None
    
    def search_similar(self, query_text: str, top_k: int = 5) -> Optional[List[Dict]]:
        """
        Busca documentos similares usando embeddings/vetores
        
        Args:
            query_text: Texto da consulta
            top_k: Número de resultados a retornar
            
        Returns:
            Lista de documentos relevantes ou None se falhar
        """
        try:
            logger.info(f"Buscando documentos similares para: '{query_text[:50]}...'")
            
            # Exemplo de query com similaridade vetorial (ajuste conforme seu schema)
            query = """
            SELECT 
                document_content,
                topic,
                state_name,
                source_url,
                similarity_score
            FROM lightrag_documents
            WHERE embedding <-> %s::vector < 0.5
            ORDER BY embedding <-> %s::vector
            LIMIT %s
            """
            
            # Aqui você precisaria gerar o embedding do query_text
            # query_embedding = generate_embedding(query_text)
            
            # Por enquanto, retorno mock
            logger.warning("search_similar não implementado completamente - usando mock")
            return None
            
        except Exception as e:
            logger.error(f"Erro ao buscar documentos similares: {e}")
            return None
    
    def get_available_topics(self) -> List[str]:
        """
        Retorna lista de tópicos disponíveis no LightRAG
        
        Returns:
            Lista de tópicos
        """
        try:
            query = "SELECT DISTINCT topic FROM lightrag_documents ORDER BY topic"
            
            with psycopg2.connect(**self.config) as conn:
                with conn.cursor() as cur:
                    cur.execute(query)
                    results = cur.fetchall()
                    return [row[0] for row in results]
        
        except Error as e:
            logger.error(f"Erro ao buscar tópicos disponíveis: {e}")
            return []
    
    def test_connection(self) -> bool:
        """
        Testa se a conexão com o LightRAG está funcionando
        
        Returns:
            True se conectou com sucesso, False caso contrário
        """
        try:
            query = "SELECT COUNT(*) FROM lightrag_documents"
            
            with psycopg2.connect(**self.config) as conn:
                with conn.cursor() as cur:
                    cur.execute(query)
                    count = cur.fetchone()[0]
                    logger.info(f"LightRAG connection OK - {count} documentos disponíveis")
                    return True
        
        except Error as e:
            logger.error(f"Erro ao testar conexão LightRAG: {e}")
            return False


# Mock implementation para desenvolvimento (remova quando tiver o LightRAG real)
class MockLightRAGClient:
    """Cliente mock do LightRAG para desenvolvimento"""
    
    def __init__(self, config: dict = None):
        self.mock_data = self._create_mock_data()
        logger.info("MockLightRAGClient inicializado (DEVELOPMENT ONLY)")
    
    def _create_mock_data(self) -> Dict:
        """Cria dados mock para teste"""
        return {
            'Agricultural Employment': {
                'content': """
                Agricultural workers may be subject to different minimum wage rules in some states.
                
                In California, agricultural workers must be paid at least the state minimum wage.
                Some states have exemptions for small farms or seasonal agricultural employment.
                
                Federal law provides some exemptions for agricultural workers under the Fair Labor Standards Act (FLSA).
                """,
                'sources': ['https://www.dol.gov/agencies/whd/agriculture'],
                'metadata': {'topic': 'Agricultural Employment'}
            },
            'Minimum Paid Rest Periods': {
                'content': """
                Rest period requirements vary by state. 
                
                California requires a 10-minute paid rest break for every 4 hours worked.
                New York requires similar rest periods for certain industries.
                
                These breaks are separate from meal periods and must be paid time.
                """,
                'sources': ['https://www.dir.ca.gov/dlse/faq_restperiods.htm'],
                'metadata': {'topic': 'Minimum Paid Rest Periods'}
            },
            'Minimum Meal Periods': {
                'content': """
                Meal period requirements ensure workers get adequate time to eat during shifts.
                
                California requires a 30-minute meal break for shifts over 5 hours.
                Many states have similar requirements, though specifics vary.
                
                Meal breaks are typically unpaid, unlike rest breaks.
                """,
                'sources': ['https://www.dir.ca.gov/dlse/faq_mealperiods.htm'],
                'metadata': {'topic': 'Minimum Meal Periods'}
            }
        }
    
    def query_topic(self, topic: str, user_question: str, state: Optional[str] = None) -> Optional[Dict]:
        """Mock query do tópico"""
        logger.info(f"[MOCK] Consultando tópico: {topic}, estado: {state}")
        
        if topic in self.mock_data:
            result = self.mock_data[topic].copy()
            if state:
                result['metadata']['state'] = state
            return result
        
        logger.warning(f"[MOCK] Tópico não encontrado: {topic}")
        return None
    
    def get_available_topics(self) -> List[str]:
        """Mock de tópicos disponíveis"""
        return list(self.mock_data.keys())
    
    def test_connection(self) -> bool:
        """Mock de teste de conexão"""
        logger.info("[MOCK] LightRAG connection OK")
        return True


# Factory function
_lightrag_client = None

def get_lightrag_client(use_mock: bool = True) -> LightRAGClient:
    """
    Retorna uma instância singleton do LightRAGClient
    
    Args:
        use_mock: Se True, usa implementação mock para desenvolvimento
        
    Returns:
        Instância do LightRAGClient ou MockLightRAGClient
    """
    global _lightrag_client
    if _lightrag_client is None:
        if use_mock:
            _lightrag_client = MockLightRAGClient()
        else:
            _lightrag_client = LightRAGClient()
    return _lightrag_client