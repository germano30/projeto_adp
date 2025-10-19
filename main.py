
"""
Pipeline principal para extração, processamento e transformação
dos dados de salário mínimo
"""
import pandas as pd
import os
from datetime import datetime
import sys

# Imports dos módulos do projeto
from src.scrapers.scrapper_minimum_wage import MinimumWageScraper
from src.scrapers.scrapper_tipped_wage import TippedWageScraper
from src.processors.processor_standard_wage import StandardWageProcessor
from src.processors.processor_tipped_wage import TippedWageProcessor
from src.transformers.transformer_unified import DataTransformer
from config import OUTPUT_DIR, TIPPED_WAGE_START_YEAR, TIPPED_WAGE_END_YEAR


class MinimumWagePipeline:
    """Classe principal do pipeline"""
    
    def __init__(self, output_dir: str = OUTPUT_DIR):
        self.output_dir = output_dir
        self.timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Criar diretório de saída se não existir
        os.makedirs(output_dir, exist_ok=True)
    
    def run_extraction(self):
        """Fase 1: Extração dos dados"""
        print("\n" + "=" * 80)
        print("FASE 1: EXTRAÇÃO DE DADOS")
        print("=" * 80)
        
        # 1. Scraping salário mínimo padrão
        print("\n📥 Extraindo salário mínimo padrão...")
        scraper_standard = MinimumWageScraper()
        df_standard_raw = scraper_standard.scrape()
        
        if df_standard_raw.empty:
            raise Exception("❌ Falha ao extrair dados de salário mínimo padrão")
        
        # 2. Scraping tipped wages
        print("\n📥 Extraindo tipped wages...")
        scraper_tipped = TippedWageScraper()
        df_tipped_raw = scraper_tipped.scrape(
            start_year=TIPPED_WAGE_START_YEAR,
            end_year=TIPPED_WAGE_END_YEAR
        )
        
        if df_tipped_raw.empty:
            raise Exception("❌ Falha ao extrair dados de tipped wages")
        
        return df_standard_raw, df_tipped_raw
    
    def run_processing(self, df_standard_raw, df_tipped_raw):
        """Fase 2: Processamento dos dados"""
        print("\n" + "=" * 80)
        print("FASE 2: PROCESSAMENTO DOS DADOS")
        print("=" * 80)
        
        # 1. Processar salário padrão
        processor_standard = StandardWageProcessor(df_standard_raw)
        df_standard_processed = processor_standard.process()
        
        # 2. Processar tipped wages
        processor_tipped = TippedWageProcessor(df_tipped_raw)
        df_tipped_processed = processor_tipped.process()
        
        return df_standard_processed, df_tipped_processed
    
    def run_transformation(self, df_standard_processed, df_tipped_processed):
        """Fase 3: Transformação e criação do modelo dimensional"""
        print("\n" + "=" * 80)
        print("FASE 3: TRANSFORMAÇÃO E MODELAGEM DIMENSIONAL")
        print("=" * 80)
        
        transformer = DataTransformer(df_standard_processed, df_tipped_processed)
        tables = transformer.transform()
        
        return tables
    
    def save_outputs(self, tables: dict):
        """Fase 4: Salvar outputs"""
        print("\n" + "=" * 80)
        print("FASE 4: SALVANDO OUTPUTS")
        print("=" * 80)
        
        output_files = {}
        
        for table_name, df in tables.items():
            filename = f"{table_name}_{self.timestamp}.xlsx"
            filepath = os.path.join(self.output_dir, filename)
            
            df.to_csv(filepath, index=False, encoding='utf-8')
            output_files[table_name] = filepath
            
            print(f"✅ {table_name}: {filepath}")
            print(f"   Registros: {len(df)}")
        
        # Salvar também em Excel (todas as tabelas em uma workbook)
        excel_filename = f"minimum_wage_data_{self.timestamp}.csv"
        excel_path = os.path.join(self.output_dir, excel_filename)
        
        with pd.ExcelWriter(excel_path, engine='openpyxl') as writer:
            for table_name, df in tables.items():
                df.to_excel(writer, sheet_name=table_name, index=False)
        
        print(f"\n📊 Excel consolidado: {excel_path}")
        
        return output_files
    
    def run(self):
        """Executa o pipeline completo"""
        print("\n" + "🚀" * 40)
        print("INICIANDO PIPELINE DE DADOS DE SALÁRIO MÍNIMO")
        print("🚀" * 40)
        
        start_time = datetime.now()
        
        try:
            # Fase 1: Extração
            df_standard_raw, df_tipped_raw = self.run_extraction()
            
            # Fase 2: Processamento
            df_standard_processed, df_tipped_processed = self.run_processing(
                df_standard_raw, df_tipped_raw
            )
            
            # Fase 3: Transformação
            tables = self.run_transformation(
                df_standard_processed, df_tipped_processed
            )
            
            # Fase 4: Salvar
            output_files = self.save_outputs(tables)
            
            # Resumo final
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            print("\n" + "=" * 80)
            print("✅ PIPELINE CONCLUÍDO COM SUCESSO!")
            print("=" * 80)
            print(f"⏱️  Tempo total: {duration:.2f} segundos")
            print(f"📊 Tabelas geradas:")
            for name, df in tables.items():
                print(f"   • {name}: {len(df)} registros")
            print("=" * 80)
            
            return tables, output_files
            
        except Exception as e:
            print(f"\n❌ ERRO NO PIPELINE: {str(e)}")
            import traceback
            traceback.print_exc()
            raise


def main():
    """Função principal"""
    pipeline = MinimumWagePipeline()
    tables, output_files = pipeline.run()
    
    # Opcional: Mostrar preview das tabelas
    print("\n📋 PREVIEW DAS TABELAS:")
    print("=" * 80)
    
    for name, df in tables.items():
        print(f"\n{name.upper()} (primeiras 3 linhas):")
        print(df.head(3).to_string())
    
    return tables, output_files


if __name__ == "__main__":
    main()