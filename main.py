
"""
Pipeline principal para extração, processamento e transformação
dos dados de salário mínimo
"""
import pandas as pd
import os
from datetime import datetime
import sys
import psycopg2
# Imports dos módulos do projeto
from src.scrapers.scrapper_minimum_wage import MinimumWageScraper
from src.scrapers.scrapper_tipped_wage import TippedWageScraper
from src.processors.processor_standard_wage import StandardWageProcessor
from src.processors.processor_tipped_wage import TippedWageProcessor
from src.transformers.transformer_unified import DataTransformer
from config import OUTPUT_DIR, TIPPED_WAGE_START_YEAR, TIPPED_WAGE_END_YEAR, DATABASE_CONFIG


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
        try:
            conn = psycopg2.connect(**DATABASE_CONFIG)
            cur = conn.cursor()
            
            for table_name, df in tables.items():
                print(f"\n🔹 Inserindo {table_name} ({len(df)} registros)...")
                
                if table_name.lower() == "dim_category":
                    for _, row in df.iterrows():
                        cur.execute("""
                            INSERT INTO DimCategory (ID, CategoryName, CategoryType)
                            VALUES (%s, %s, %s)
                            ON CONFLICT (ID) DO UPDATE
                            SET CategoryName = EXCLUDED.CategoryName,
                                CategoryType = EXCLUDED.CategoryType;
                        """, (row['category_id'], row['category_name'], row.get('category_type', None)))
                
                elif table_name.lower() == "dim_state":
                    for _, row in df.iterrows():
                        cur.execute("""
                            INSERT INTO DimState (ID, StateName, IsTerritory)
                            VALUES (%s, %s, %s)
                            ON CONFLICT (ID) DO UPDATE
                            SET StateName = EXCLUDED.StateName,
                                IsTerritory = EXCLUDED.IsTerritory;
                        """, (row['state_id'], row['state_name'], row.get('is_territory', False)))
                
                elif table_name.lower() == "dim_footnotes":
                    for _, row in df.iterrows():
                        cur.execute("""
                            INSERT INTO DimFootnote (ID, FootnoteText, FootnoteHash)
                            VALUES (%s, %s, %s)
                            ON CONFLICT (ID) DO UPDATE
                            SET FootnoteText = EXCLUDED.FootnoteText,
                                FootnoteHash = EXCLUDED.FootnoteHash;
                        """, (row['footnote_id'], row['footnote_text'], row['footnote_hash']))
                
                elif table_name.lower() == "fact":
                    for _, row in df.iterrows():
                        cur.execute("""
                            INSERT INTO FactMinimumWage (
                                ID, StateID, CategoryID, Year, EffectiveDate,
                                BaseWagePerHour, MinimumCashWage, MaximumTipCredit,
                                FrequencyID, SourceURL, Notes
                            )
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                            ON CONFLICT (ID) DO UPDATE
                            SET StateID = EXCLUDED.StateID,
                                CategoryID = EXCLUDED.CategoryID,
                                Year = EXCLUDED.Year,
                                EffectiveDate = EXCLUDED.EffectiveDate,
                                BaseWagePerHour = EXCLUDED.BaseWagePerHour,
                                MinimumCashWage = EXCLUDED.MinimumCashWage,
                                MaximumTipCredit = EXCLUDED.MaximumTipCredit,
                                FrequencyID = EXCLUDED.FrequencyID,
                                SourceURL = EXCLUDED.SourceURL,
                                Notes = EXCLUDED.Notes;
                        """, (
                            row['wage_id'], row['state_id'], row['category_id'], row['year'], row.get('effective_date', None),
                            row.get('base_wage_per_hour', None), row.get('minimum_cash_wage', None), row.get('maximum_tip_credit', None),
                            1, row.get('source_url', None), row.get('notes', None)
                        ))

                elif table_name.lower() == "bridge":
                    for _, row in df.iterrows():
                        cur.execute("""
                            INSERT INTO BridgeFactMinimumWageFootnote (WageID, FootnoteID, Context)
                            VALUES (%s, %s, %s)
                            ON CONFLICT (WageID, FootnoteID) DO UPDATE
                            SET Context = EXCLUDED.Context;
                        """, (row['wage_id'], row['footnote_id'], row.get('column_reference', None)))

                conn.commit()
                print("\n✅ Todos os dados inseridos/upserted com sucesso!")

        except Exception as e:
            print("❌ Erro ao inserir dados:", e)
            if conn:
                conn.rollback()
        finally:
            if 'cur' in locals():
                cur.close()
            if 'conn' in locals():
                conn.close()


    
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