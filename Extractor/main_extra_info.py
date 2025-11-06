"""
Supplementary Labor Law Information Extraction Pipeline.

This module implements a specialized pipeline for extracting, processing, and storing
additional labor law information beyond basic wage data. It focuses on:
- Employment regulations
- Worker protections
- Special cases and exemptions
- Jurisdiction-specific rules
"""

import asyncio
import logging
import os
import warnings
from datetime import datetime
from typing import Dict, Optional

from src.processors.processor_extra_info import ExtraInfoProcessor
from src.scrapers.scrapper_extra_info import ExtraInfoScraper
from src.transformers.transformer_extra_info import ExtraInfoTransformer

# Configure logging
logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

# Suppress non-critical warnings
warnings.filterwarnings('ignore')


class ExtraInfoPipeline:
    """
    Pipeline for extracting and processing supplementary labor law information.
    
    This class orchestrates the extraction and processing of additional labor law
    information, utilizing RAG (Retrieval Augmented Generation) for enhanced
    context understanding and storage.
    
    The pipeline handles:
    1. Data extraction from authoritative sources
    2. Natural language processing and context analysis
    3. Storage in both traditional and vector databases
    4. Integration with existing wage data
    """
    
    def __init__(
        self, 
        output_dir: str = "./output",
        rag_dir: str = "lightrag_storage",
        db_config: Optional[Dict] = None
    ):
        """
        Initialize the supplementary information pipeline.
        
        Parameters
        ----------
        output_dir : str, optional
            Directory for storing intermediate and processed data files,
            defaults to "./output"
        rag_dir : str, optional
            Directory for RAG storage components, defaults to "lightrag_storage"
        db_config : Dict, optional
            Database configuration parameters. If not provided, uses environment
            variables with default fallbacks
        """
        self.output_dir = output_dir
        self.rag_dir = rag_dir
        self.db_config = db_config or {
            'host': os.getenv('DB_HOST', 'localhost'),
            'port': os.getenv('DB_PORT', '5432'),
            'database': os.getenv('DB_NAME', 'chat'),
            'user': os.getenv('DB_USER', 'agermano'),
            'password': os.getenv('DB_PASSWORD', 'devpass')
        }
        self.timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        os.makedirs(output_dir, exist_ok=True)
        os.makedirs(rag_dir, exist_ok=True)
    
    def run_extraction(self) -> dict:
        """
        Fase 1: Extração dos dados.
        
        Returns:
            Dictionary with scraped data
        """
        logger.info("Initiating Phase 1: Data Extraction")
        
        scraper = ExtraInfoScraper()
        scraped_data = scraper.scrape_all()
        
        if not scraped_data:
            raise Exception("❌ Falha ao extrair dados")
        
        return scraped_data
    
    async def run_processing(self, scraped_data: dict) -> dict:
        """Fase 2: Processamento e inserção no LightRAG (async)."""
        logger.info("Initiating Phase 2: LightRAG Processing")
        
        processor = await ExtraInfoProcessor.create()
        stats = await processor.process(scraped_data)
        
        return stats

    async def run_async(self):
        """Executa o pipeline completo de forma assíncrona."""
        print("\n" + "=" * 80)
        print("INICIANDO PIPELINE DE INFORMAÇÕES EXTRAS DE LEIS TRABALHISTAS")
        print("=" * 80)
        
        start_time = datetime.now()
        
        try:
            # Fase 1: Extração
            scraped_data = self.run_extraction()
            
            # Fase 2: Processamento (LightRAG)
            processing_stats = await self.run_processing(scraped_data)
            
            # Fase 3: Transformação (PostgreSQL)
            transformation_stats = self.run_transformation(scraped_data)
            
            # Resumo final
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            print("\n" + "=" * 80)
            print("PIPELINE CONCLUÍDO COM SUCESSO!")
            print("=" * 80)
            print(f"\n⏱️  Tempo total: {duration:.2f} segundos")
            print(f"📊 Total de documentos processados: {processing_stats['total_documents']}")
            print(f"📊 Total de footnotes processados: {processing_stats['total_footnotes']}")
            
            return {
                'scraped_data': scraped_data,
                'processing_stats': processing_stats,
                'transformation_stats': transformation_stats,
                'duration': duration
            }
            
        except Exception as e:
            print(f"\n❌ ERRO NO PIPELINE: {str(e)}")
            import traceback
            traceback.print_exc()
            raise

    
    def run_transformation(self, scraped_data: dict) -> dict:
        """
        Fase 3: Transformação e inserção no PostgreSQL.
        
        Args:
            scraped_data: Dictionary with scraped data
            
        Returns:
            Dictionary with transformation statistics
        """
        print("\n" + "=" * 80)
        print("FASE 3: TRANSFORMAÇÃO E POSTGRESQL")
        print("=" * 80)
        
        transformer = ExtraInfoTransformer(db_config=self.db_config)
        stats = transformer.transform_and_insert(scraped_data)
        
        return stats
    
    def run(self):
        """Executa o pipeline completo."""
        print("\n" + "=" * 80)
        print("INICIANDO PIPELINE DE INFORMAÇÕES EXTRAS DE LEIS TRABALHISTAS")
        print("=" * 80)
        
        start_time = datetime.now()
        
        try:
            # Fase 1: Extração
            scraped_data = self.run_extraction()
            
            # Fase 2: Processamento (LightRAG)
            processing_stats = self.run_processing(scraped_data)
            
            # Fase 3: Transformação (PostgreSQL)
            transformation_stats = self.run_transformation(scraped_data)
            
            # Resumo final
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            print("\n" + "=" * 80)
            print("PIPELINE CONCLUÍDO COM SUCESSO!")
            print("=" * 80)
            print(f"\n⏱️  Tempo total: {duration:.2f} segundos")
            print(f"📊 Total de documentos processados: {processing_stats['total_documents']}")
            print(f"📊 Total de footnotes processados: {processing_stats['total_footnotes']}")
            
            return {
                'scraped_data': scraped_data,
                'processing_stats': processing_stats,
                'transformation_stats': transformation_stats,
                'duration': duration
            }
            
        except Exception as e:
            print(f"\n❌ ERRO NO PIPELINE: {str(e)}")
            import traceback
            traceback.print_exc()
            raise


def main():
    """Função principal."""
    pipeline = ExtraInfoPipeline()
    results = asyncio.run(pipeline.run_async())
    return results


if __name__ == "__main__":
    main()