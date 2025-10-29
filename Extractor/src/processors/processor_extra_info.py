from typing import Dict, List, Tuple
from lightrag import LightRAG
from lightrag.llm.openai import gpt_4o_mini_complete, openai_embed
from lightrag.kg.shared_storage import initialize_pipeline_status
import os
import asyncio

class ExtraInfoProcessor:
    """Processor for inserting extra labor info into LightRAG."""
    
    def __init__(self, working_dir: str):
        self.working_dir = working_dir
        os.makedirs(working_dir, exist_ok=True)
        self.rag = None
        self.insertion_stats = {
            'total_documents': 0,
            'total_footnotes': 0,
            'by_type': {}
        }

    @classmethod
    async def create(cls, working_dir: str = "./lightrag_storage"):
        """Fábrica assíncrona para criar a instância com LightRAG inicializado."""
        self = cls(working_dir)
        await self._initialize()
        return self

    async def _initialize(self):
        """Inicializa o LightRAG de forma assíncrona."""
        self.rag = LightRAG(
            working_dir=self.working_dir,
            llm_model_func=gpt_4o_mini_complete,
            embedding_func=openai_embed,
        )
        await self.rag.initialize_storages()
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
        
        # 🔹 Força o salvamento

        print("\n✅ Processing completed!")
        self._print_stats()
        return self.insertion_stats

    async def _insert_documents(self, docs: List[Dict], data_type: str, doc_category: str):
        """Insere documentos no LightRAG com metadados embutidos, de forma segura."""
        for doc in docs:
            text = doc.get('text', '')
            if not text.strip():
                continue

            metadata_lines = [
                f"{k}: {v}" for k, v in {
                    'data_type': data_type,
                    'category': doc_category,
                    'topic': doc.get('topic', ''),
                    'type': doc.get('type', ''),
                    'site_url': doc.get('site_url', ''),
                    'state': doc.get('state', ''),
                    'regulation_category': doc.get('category', '')
                }.items() if v
            ]
            text_with_meta = "[METADATA]\n" + "\n".join(metadata_lines) + "\n[/METADATA]\n\n" + text

            # Insere de forma segura usando thread pool
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, self.rag.insert, text_with_meta)




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
