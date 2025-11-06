import dotenv
dotenv.load_dotenv()

from typing import Dict, List, Tuple
from lightrag import LightRAG
from lightrag.kg.shared_storage import initialize_pipeline_status
from lightrag.utils import EmbeddingFunc
from sentence_transformers import SentenceTransformer
import os
import numpy as np
import asyncio
from google import genai
from google.genai import types
import functools

gemini_api_key = os.getenv("GEMINI_API_KEY")

class ExtraInfoProcessor:
    """Processor for inserting extra labor info into LightRAG."""
    
    def __init__(self):
        self.rag = None
        
        # Fixo 1: Forçar o uso de "cpu" para evitar erros de CUDA
        embedding_device = "cpu"
        print(f"Inicializando o modelo de embedding no dispositivo: {embedding_device}")
        
        self.embedding_model = SentenceTransformer(
            "BAAI/bge-large-en-v1.5", 
            device=embedding_device
        )
        
        self.insertion_stats = {
            'total_documents': 0,
            'total_footnotes': 0,
            'by_type': {}
        }

    @classmethod
    async def create(cls, ):
        """Fábrica assíncrona para criar a instância com LightRAG inicializado."""
        self = cls()
        await self._initialize()
        return self
    
    async def embedding_func(self, texts: list[str]) -> np.ndarray:
        loop = asyncio.get_event_loop()
        
        encode_func_with_kwargs = functools.partial(
            self.embedding_model.encode, 
            convert_to_numpy=True
        )
        
        embeddings = await loop.run_in_executor(
            None, encode_func_with_kwargs, texts
        )
        return embeddings
    
    async def llm_model_func(
        self, prompt, system_prompt=None, history_messages=[], keyword_extraction=False, **kwargs
    ) -> str:
        client = genai.Client(api_key=gemini_api_key)
        if history_messages is None:
            history_messages = []
        combined_prompt = ""
        if system_prompt:
            combined_prompt += f"{system_prompt}\n"
        for msg in history_messages:
            combined_prompt += f"{msg['role']}: {msg['content']}\n"
        combined_prompt += f"user: {prompt}"
        response = client.models.generate_content(
            model="gemini-2.0-flash",
            contents=[combined_prompt],
            config=types.GenerateContentConfig(max_output_tokens=500, temperature=0.1),
        )
        return response.text

    async def _initialize(self):
        """Inicializa o LightRAG de forma assíncrona."""
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
            vector_db_storage_cls_kwargs={"embed_dim": 384}
        )
        
        print("Inicializando storages (criando tabelas)...")
        await self.rag.initialize_storages()
        print("Tabelas criadas.")
        
        await initialize_pipeline_status()

    async def process(self, scraped_data: Dict[str, Tuple[List[Dict], List[Dict]]]) -> Dict:
        """Processa todos os dados e insere no LightRAG."""
        print("\n" + "=" * 80)
        print("PROCESSING EXTRA LABOR INFORMATION")
        print("=" * 80)
        
        for data_type, (state_docs, footnote_docs) in scraped_data.items():
            print(f"\n📝 Processing {data_type}...")
            await self._insert_documents(state_docs, data_type, 'state')
            if footnote_docs:
                await self._insert_documents(footnote_docs, data_type, 'footnote')
            
            self.insertion_stats['by_type'][data_type] = {
                'documents': len(state_docs),
                'footnotes': len(footnote_docs)
            }
            self.insertion_stats['total_documents'] += len(state_docs)
            self.insertion_stats['total_footnotes'] += len(footnote_docs)
        

        print("\n✅ Processing completed!")
        self._print_stats()
        return self.insertion_stats

    async def _insert_documents(self, docs: List[Dict], data_type: str, doc_category: str):
        """Insere documentos no LightRAG com metadados embutidos, de forma segura."""
        tasks = []
        for doc in docs:
            text = doc.get('text', '')
            if not text.strip():
                continue
            metadata_lines = [
                f"{k}: {v}" for k, v in {
                    'data_type': data_type, 'category': doc_category,
                    'topic': doc.get('topic', ''), 'type': doc.get('type', ''),
                    'site_url': doc.get('site_url', ''), 'state': doc.get('state', ''),
                    'regulation_category': doc.get('category', '')
                }.items() if v
            ]
            text_with_meta = "[METADATA]\n" + "\n".join(metadata_lines) + "\n[/METADATA]\n\n" + text
            
            # Fixo 4: Usar a função assíncrona 'ainsert'
            tasks.append(self.rag.ainsert(text_with_meta))
        
        if tasks:
            await asyncio.gather(*tasks)

    def _print_stats(self):
        """Exibe estatísticas de inserção."""
        print("\n" + "=" * 80)
        print("INSERTION STATISTICS")
        print("=" * 80)
        print(f"\n📊 Total Documents Inserted: {self.insertion_stats['total_documents']}")
        print(f"📊 Total Footnotes Inserted: {self.insertion_stats['total_footnotes']}")
        print(f"\n{'Type':<30} {'Documents':<15} {'Footnotes':<15}")
        print("-" * 60)
        for data_type, stats in self.insertion_stats['by_type'].items():
            print(f"{data_type:<30} {stats['documents']:<15} {stats['footnotes']:<15}")