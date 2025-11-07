import asyncio
import dotenv
import functools
import logging
import os
from typing import Dict, List, Optional
import numpy as np
import psycopg2
from google import genai
from google.genai import types
from lightrag import LightRAG, QueryParam
from lightrag.utils import EmbeddingFunc
from psycopg2 import Error
from sentence_transformers import SentenceTransformer

dotenv.load_dotenv()
gemini_api_key = os.getenv("GOOGLE_API_KEY")

logger = logging.getLogger(__name__)
logging.getLogger("lightrag").setLevel(logging.WARNING)

class LightRAGClient:
    def __init__(self, working_dir="./lightrag_storage"):
        self.working_dir = working_dir
        self.embedding_model = SentenceTransformer(
            "BAAI/bge-large-en-v1.5", device='cpu'
        )
        self.rag = None

    async def async_init(self):
        """Inicialização assíncrona para evitar conflito de loops."""
        self.rag = LightRAG(
            kv_storage="PGKVStorage",
            vector_storage="PGVectorStorage",
            graph_storage="PGGraphStorage",
            doc_status_storage="PGDocStatusStorage",
            llm_model_func=self.llm_model_func,
            embedding_func=EmbeddingFunc(
                embedding_dim=384,
                max_token_size=8192,
                func=self.embedding_func,
            ),
            vector_db_storage_cls_kwargs={"embed_dim": 384},
        )
        await self.rag.initialize_storages()
        return self

    async def embedding_func(self, texts: list[str]) -> np.ndarray:
        """Executa embeddings de forma segura dentro do loop ativo."""
        loop = asyncio.get_running_loop()
        encode = functools.partial(self.embedding_model.encode, convert_to_numpy=True)
        embeddings = await loop.run_in_executor(None, encode, texts)
        return embeddings

    async def llm_model_func(
        self, prompt, system_prompt=None, keyword_extraction=False, **kwargs
    ) -> str:
        client = genai.Client(api_key=gemini_api_key)
        combined_prompt = ""
        if system_prompt:
            combined_prompt += f"{system_prompt}\n"
        combined_prompt += f"user: {prompt}"
        response = client.models.generate_content(
            model="gemini-2.0-flash",
            contents=[combined_prompt],
            config=types.GenerateContentConfig(max_output_tokens=500, temperature=0.1),
        )
        return response.text

    async def query_topic(self, topic: str, state: str = None):
        """Consulta assíncrona ao LightRAG."""
        query = f"Explain the laws related to {topic}"
        if state:
            query += f" at the state of {state}"

        logger.info(f"Executing query: {query}")
        result = await self.rag.aquery(
            query,
            param=QueryParam(mode='mix', only_need_context=True, include_references=True)
        )

        if isinstance(result, dict):
            return {
                "answer": result.get("answer", "Error: No response generated."),
                "references": result.get("references", []),
            }
        return {"answer": str(result), "references": []}

    def test_connection(self) -> bool:
        """Teste de conexão básica."""
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


_lightrag_client = None

async def get_lightrag_client(use_mock: bool = False):
    """Retorna instância singleton assíncrona do LightRAGClient."""
    global _lightrag_client
    if _lightrag_client is None:
        client = LightRAGClient()
        _lightrag_client = await client.async_init()
    return _lightrag_client
