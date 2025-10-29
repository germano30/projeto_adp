# lightrag_client.py
"""Cliente para interação com LightRAG armazenado em PostgreSQL"""

import logging
from typing import Optional, List, Dict
from lightrag import LightRAG, QueryParam
from lightrag.llm.openai import gpt_4o_mini_complete, openai_embed
import asyncio


from config import DATABASE_CONFIG

logger = logging.getLogger(__name__)


class LightRAGClient:
    
    def __init__(self, working_dir: str = "./lightrag_storage"):
        self.working_dir = working_dir
        self.rag = LightRAG(
            working_dir=working_dir,
            llm_model_func=gpt_4o_mini_complete,
            embedding_func=openai_embed
        )
        # Inicializa storages
        asyncio.run(self.rag.initialize_storages())

    
    async def query_topic(self, topic: str, state: str = None):
        """
        Faz uma consulta assíncrona ao LightRAG usando aquery(),
        gerando resposta e referências automaticamente.
        """
        query = f"Explique as leis relacionadas a {topic}"
        if state:
            query += f" no estado de {state}"

        print(f"🔎 Rodando consulta: {query}")

        # Chamada assíncrona ao LLM + RAG interno
        result = await self.rag.aquery(query)

        # O `result` geralmente é um dict contendo: {"answer": "...", "references": [...]}
        if isinstance(result, dict):
            return {
                "answer": result.get("answer", "⚠️ Nenhuma resposta retornada."),
                "references": result.get("references", [])
            }

        # Se aquery retornar apenas string (versões antigas)
        return {"answer": str(result), "references": []}

    
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

def get_lightrag_client(use_mock: bool = False) -> LightRAGClient:
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