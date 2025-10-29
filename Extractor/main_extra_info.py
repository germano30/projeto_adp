import os
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')
import asyncio

from src.scrapers.scrapper_extra_info import ExtraInfoScraper
from src.processors.processor_extra_info import ExtraInfoProcessor
from src.transformers.transformer_extra_info import ExtraInfoTransformer


class ExtraInfoPipeline:
    """Pipeline principal para informações extras de leis trabalhistas."""
    
    def __init__(
        self, 
        output_dir: str = "./output",
        rag_dir: str = "lightrag_storage",
        db_config: dict = None
    ):
        """
        Initialize the pipeline.
        
        Args:
            output_dir: Directory for output files
            rag_dir: Directory for LightRAG storage
            db_config: Database configuration dictionary
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
        print("\n" + "=" * 80)
        print("FASE 1: EXTRAÇÃO DE DADOS")
        print("=" * 80)
        
        scraper = ExtraInfoScraper()
        scraped_data = scraper.scrape_all()
        
        if not scraped_data:
            raise Exception("❌ Falha ao extrair dados")
        
        return scraped_data
    
    async def run_processing(self, scraped_data: dict) -> dict:
        """Fase 2: Processamento e inserção no LightRAG (async)."""
        print("\n" + "=" * 80)
        print("FASE 2: PROCESSAMENTO COM LIGHTRAG")
        print("=" * 80)
        
        processor = await ExtraInfoProcessor.create(working_dir=self.rag_dir)
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