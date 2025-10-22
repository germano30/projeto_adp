"""
Transformador que unifica os datasets de salário padrão e tipped wages
Cria a estrutura dimensional (Star Schema) com Dim_Footnotes normalizado
"""
import pandas as pd
import sys
sys.path.append('..')
from utils import generate_hash
from config import WAGE_CATEGORIES
import re
from scrapers.scrapper_minimum_wage import MinimumWageScraper
from scrapers.scrapper_tipped_wage import TippedWageScraper
from processors.processor_standard_wage import StandardWageProcessor
from processors.processor_tipped_wage import TippedWageProcessor


class DataTransformer:
    """Classe para transformar e unificar os datasets"""
    
    def __init__(self, df_standard: pd.DataFrame, df_tipped: pd.DataFrame):
        self.df_standard = df_standard
        self.df_tipped = df_tipped
        
        # Tabelas dimensionais
        self.dim_states = None
        self.dim_categories = None
        self.dim_footnotes = None
        self.fact_minimum_wage = None
        self.bridge_wage_footnote = None
    
    def extract_footnote_references(self, text: str) -> list:
        """
        Extrai referências de footnotes de um texto (notes)
        
        Examples:
            "[combinedrate] Text" -> extrai do padrão [coluna]
            "foot3" -> extrai foot3
            "[a]" -> extrai [a]
        """
        if pd.isna(text) or not isinstance(text, str):
            return []
        
        # Padrões de footnote
        patterns = [
            r'foot\d+',  # foot3, foot8
            r'\[([a-z])\]',  # [a], [b]
            r'\(([a-z])\)',  # (a), (b)
        ]
        
        refs = []
        for pattern in patterns:
            matches = re.findall(pattern, text, re.IGNORECASE)
            refs.extend(matches)
        
        return list(set(refs))  # Remove duplicatas
    
    def create_dim_footnotes_unified(self, all_footnotes_dict: dict) -> pd.DataFrame:
        """
        Cria Dim_Footnotes unificada de todos os datasets
        
        Args:
            all_footnotes_dict: {footnote_key: footnote_text}
        
        Returns:
            DataFrame com footnote_id, footnote_key, footnote_text
        """
        print("📊 Criando Dim_Footnotes unificada...")
        
        footnotes_data = []
        for key, text in all_footnotes_dict.items():
            footnotes_data.append({
                'footnote_key': key,
                'footnote_text': text,
                'footnote_hash': generate_hash(text)
            })
        
        df_footnotes = pd.DataFrame(footnotes_data)
        df_footnotes = df_footnotes.drop_duplicates(subset=['footnote_hash'])
        df_footnotes['footnote_id'] = range(1, len(df_footnotes) + 1)
        
        self.dim_footnotes = df_footnotes[['footnote_id', 'footnote_key', 'footnote_text']]
        
        return self.dim_footnotes
    
    def extract_notes_from_footnote_text(self, text: str) -> tuple:
        """
        Separa notes puros de footnote references
        
        Args:
            text: Texto que pode conter [coluna] footnote ou notas puras
        
        Returns:
            (notes_clean, footnote_refs)
        
        Examples:
            "[combinedrate] Text ; [tipcredit] More" -> (None, ['combinedrate', 'tipcredit'])
            "This state uses federal minimum" -> ("This state uses federal minimum", [])
        """
        if pd.isna(text) or not isinstance(text, str):
            return None, []
        
        # Extrair todas as referências de footnotes
        footnote_refs = self.extract_footnote_references(text)
        
        # Remover padrões de footnote do texto para obter notes puros
        clean_text = text
        
        # Remover padrões como "[combinedrate] footnote_text"
        clean_text = re.sub(r'\[[^\]]+\]\s*[^;]+', '', clean_text)
        
        # Remover foot3, [a], etc
        for ref in footnote_refs:
            clean_text = clean_text.replace(ref, '')
        
        # Limpar
        clean_text = re.sub(r'\s+', ' ', clean_text).strip()
        clean_text = clean_text.strip(';').strip()
        
        # Se sobrou texto real, é note
        if clean_text and clean_text not in ['', ';']:
            return clean_text, footnote_refs
        
        return None, footnote_refs
    
    def transform_standard_to_long(self) -> pd.DataFrame:
        """Transforma dataset padrão para formato compatível"""
        print("🔄 Transformando salário padrão...")
        
        df = self.df_standard.copy()
        df = df.rename(columns={'state': 'jurisdiction', 'minimal_wage': 'base_wage_per_hour'})
        
        # Notes e footnotes já vêm separados do scraper
        # Apenas garantir que as colunas existem
        if 'footnotes' not in df.columns:
            df['footnotes'] = None
        
        df['category_name'] = WAGE_CATEGORIES['standard']
        df['category_type'] = 'standard'
        df['minimum_cash_wage'] = None
        df['maximum_tip_credit'] = None
        df['source_url'] = 'https://www.dol.gov/agencies/whd/state/minimum-wage/history'
        
        return df
    
    def transform_tipped_to_long(self) -> pd.DataFrame:
        """Transforma dataset tipped para formato long"""
        print("🔄 Transformando tipped wages...")
        
        df_long = []
        
        for _, row in self.df_tipped.iterrows():
            # Notes e footnotes já vêm separados do scraper
            notes_clean = row.get('notes')
            footnote_refs = row.get('footnotes', [])
            
            base_row = {
                'jurisdiction': row['jurisdiction'],
                'year': row['year'],
                'frequency': 1,
                'source_url': f"https://www.dol.gov/agencies/whd/state/minimum-wage/tipped/{row['year']}",
                'notes': notes_clean,
                'footnotes': footnote_refs
            }
            
            # Combined Rate
            if pd.notna(row.get('combinedrate')):
                df_long.append({
                    **base_row,
                    'category_name': WAGE_CATEGORIES['tipped_combined'],
                    'category_type': 'tipped',
                    'base_wage_per_hour': row['combinedrate'],
                    'value_type': row.get('combinedrate_type'),
                    'minimum_cash_wage': None,
                    'maximum_tip_credit': None
                })
            
            # Tip Credit
            if pd.notna(row.get('tipcredit')):
                df_long.append({
                    **base_row,
                    'category_name': WAGE_CATEGORIES['tipped_credit'],
                    'category_type': 'tipped',
                    'base_wage_per_hour': None,
                    'value_type': row.get('tipcredit_type'),
                    'minimum_cash_wage': None,
                    'maximum_tip_credit': row['tipcredit']
                })
            
            # Cash Wage
            if pd.notna(row.get('cashwage')):
                df_long.append({
                    **base_row,
                    'category_name': WAGE_CATEGORIES['tipped_cash'],
                    'category_type': 'tipped',
                    'base_wage_per_hour': None,
                    'value_type': row.get('cashwage_type'),
                    'minimum_cash_wage': row['cashwage'],
                    'maximum_tip_credit': None
                })
        
        return pd.DataFrame(df_long)
    
    def create_dim_states(self, df_unified: pd.DataFrame):
        """Cria tabela dimensional de estados"""
        print("📊 Criando Dim_States...")
        
        states = df_unified[['jurisdiction']].drop_duplicates().reset_index(drop=True)
        states = states.rename(columns={'jurisdiction': 'state_name'})
        states['state_id'] = states.index + 1
        states['is_territory'] = False
        
        self.dim_states = states[['state_id', 'state_name', 'is_territory']]
        return self.dim_states
    
    def create_dim_categories(self, df_unified: pd.DataFrame):
        """Cria tabela dimensional de categorias"""
        print("📊 Criando Dim_Categories...")
        
        categories = df_unified[['category_name', 'category_type']].drop_duplicates().reset_index(drop=True)
        categories['category_id'] = categories.index + 1
        
        self.dim_categories = categories[['category_id', 'category_name', 'category_type']]
        return self.dim_categories
    
    def create_fact_table(self, df_unified: pd.DataFrame):
        """Cria tabela fato"""
        print("📊 Criando Fact_MinimumWage...")
        
        # Merge com dimensões
        df = df_unified.merge(
            self.dim_states.rename(columns={'state_name': 'jurisdiction'}),
            on='jurisdiction',
            how='left'
        )
        
        df = df.merge(
            self.dim_categories[['category_id', 'category_name']],
            on='category_name',
            how='left'
        )
        
        # Guardar footnotes para criar bridge depois
        self.footnote_refs_by_wage = df[['footnotes']].copy()
        self.footnote_refs_by_wage.index = range(1, len(self.footnote_refs_by_wage) + 1)
        self.footnote_refs_by_wage['wage_id'] = self.footnote_refs_by_wage.index
        
        # Selecionar colunas da fato (SEM footnotes)
        fact_columns = [
            'state_id', 'category_id', 'year', 'base_wage_per_hour',
            'minimum_cash_wage', 'maximum_tip_credit', 'frequency', 'source_url', 'notes'
        ]
        
        fact = df[[c for c in fact_columns if c in df.columns]].copy()
        fact['wage_id'] = range(1, len(fact) + 1)
        fact['effective_date'] = None
        
        # Reordenar (notes fica, footnotes NÃO)
        self.fact_minimum_wage = fact[[
            'wage_id', 'state_id', 'category_id', 'year', 'effective_date',
            'base_wage_per_hour', 'minimum_cash_wage', 'maximum_tip_credit',
            'frequency', 'source_url', 'notes'
        ]]
        
        return self.fact_minimum_wage
    
    def create_bridge_table(self, all_footnotes_dict: dict):
        """Cria tabela bridge entre wage e footnotes"""
        print("📊 Criando Bridge_Wage_Footnote...")
        
        bridge_data = []
        
        for _, row in self.footnote_refs_by_wage.iterrows():
            wage_id = row['wage_id']
            footnote_refs = row.get('footnotes')
            
            if pd.isna(footnote_refs):
                continue
            
            # footnote_refs pode ser lista ou string
            if isinstance(footnote_refs, str):
                footnote_refs = [footnote_refs]
            elif not isinstance(footnote_refs, list):
                continue
            
            for ref in footnote_refs:
                # Buscar footnote_id correspondente no Dim_Footnotes
                footnote_row = self.dim_footnotes[self.dim_footnotes['footnote_key'] == ref]
                
                if not footnote_row.empty:
                    bridge_data.append({
                        'wage_id': wage_id,
                        'footnote_id': footnote_row.iloc[0]['footnote_id']
                    })
        
        if bridge_data:
            self.bridge_wage_footnote = pd.DataFrame(bridge_data).drop_duplicates()
        else:
            self.bridge_wage_footnote = pd.DataFrame(columns=['wage_id', 'footnote_id'])
        
        return self.bridge_wage_footnote
    
    def collect_all_footnotes(self, df_standard, df_tipped) -> dict:
        """Coleta todos os footnotes de todos os datasets"""
        print("📝 Coletando todos os footnotes...")
        
        all_footnotes = {}
        
        # Standard wage footnotes (do scraper)
        if hasattr(self, 'standard_footnotes'):
            all_footnotes.update(self.standard_footnotes)
        
        # Tipped wage footnotes (do scraper)
        if hasattr(self, 'tipped_footnotes'):
            all_footnotes.update(self.tipped_footnotes)
        
        # Youth employment footnotes (se houver)
        if hasattr(self, 'youth_footnotes'):
            all_footnotes.update(self.youth_footnotes)
        
        print(f"   ✓ {len(all_footnotes)} footnotes únicos coletados")
        
        return all_footnotes
    
    def transform(self, standard_footnotes: dict = None, tipped_footnotes: dict = None):
        """Executa o pipeline completo de transformação"""
        print("\n🔄 INICIANDO TRANSFORMAÇÃO")
        print("=" * 60)
        
        # Guardar footnotes para usar depois
        self.standard_footnotes = standard_footnotes or {}
        self.tipped_footnotes = tipped_footnotes or {}
        
        # 1. Transformar datasets
        df_standard_transformed = self.transform_standard_to_long()
        df_tipped_transformed = self.transform_tipped_to_long()
        
        # 2. Garantir colunas comuns
        common_columns = [
            'jurisdiction', 'year', 'category_name', 'category_type',
            'base_wage_per_hour', 'minimum_cash_wage', 'maximum_tip_credit',
            'frequency', 'notes', 'source_url', 'footnotes'  # footnotes em vez de footnote_refs
        ]
        
        for col in common_columns:
            if col not in df_standard_transformed.columns:
                df_standard_transformed[col] = None
            if col not in df_tipped_transformed.columns:
                df_tipped_transformed[col] = None
        
        # 3. Unificar
        print("🔗 Unificando datasets...")
        df_unified = pd.concat([
            df_standard_transformed[common_columns],
            df_tipped_transformed[common_columns]
        ], ignore_index=True)
        
        print(f"   ✓ Total de {len(df_unified)} registros unificados")
        
        # 4. Criar Dim_Footnotes (antes de criar fato)
        all_footnotes = self.collect_all_footnotes(df_standard_transformed, df_tipped_transformed)
        self.create_dim_footnotes_unified(all_footnotes)
        print(f"   ✓ {len(self.dim_footnotes)} footnotes na dimensão")
        
        # 5. Criar dimensões
        self.create_dim_states(df_unified)
        print(f"   ✓ {len(self.dim_states)} estados")
        
        self.create_dim_categories(df_unified)
        print(f"   ✓ {len(self.dim_categories)} categorias")
        
        # 6. Criar fato
        self.create_fact_table(df_unified)
        print(f"   ✓ {len(self.fact_minimum_wage)} registros na fato")
        
        # 7. Criar bridge
        self.create_bridge_table(all_footnotes)
        print(f"   ✓ {len(self.bridge_wage_footnote)} relacionamentos na bridge")
        
        print("=" * 60)
        print("✅ TRANSFORMAÇÃO CONCLUÍDA!")
        print("=" * 60)
        
        return {
            'fact': self.fact_minimum_wage,
            'dim_states': self.dim_states,
            'dim_categories': self.dim_categories,
            'dim_footnotes': self.dim_footnotes,
            'bridge': self.bridge_wage_footnote
        }


def main():
    """Função principal para teste"""
    # Criar dados de exemplo]
    scraper_standard = MinimumWageScraper()
    df_standard_raw = scraper_standard.scrape()
    
    scraper_tipped = TippedWageScraper()
    df_tipped_raw = scraper_tipped.scrape(
            start_year=2003,
            end_year=2025
        )
    
    print("\n📋 Preview dos dados brutos:")
    print("\nSalário Padrão:")
    print(df_standard_raw.head())
    print("\nTipped Wages:")
    print(df_tipped_raw.head())

    processor_standard = StandardWageProcessor(df_standard_raw)
    df_standard_processed = processor_standard.process()
        
        # 2. Processar tipped wages
    processor_tipped = TippedWageProcessor(df_tipped_raw)
    df_tipped_processed = processor_tipped.process()

    print("\n📋 Preview dos dados processados:")
    print("\nSalário Padrão Processado:")
    print(df_standard_processed.head())
    print("\nTipped Wages Processado:")
    print(df_tipped_processed.head())
    # Footnotes fictícios
    standard_footnotes = {'combinedrate': 'California rate info', 'foot3': 'Texas footnote'}
    tipped_footnotes = {'tipcredit': 'Tip credit explanation'}
    
    transformer = DataTransformer(df_standard_processed, df_tipped_processed)
    result = transformer.transform(
        standard_footnotes=standard_footnotes,
        tipped_footnotes=tipped_footnotes
    )
    
    print("\n📋 Preview das tabelas:")
    for name, df in result.items():
        print(f"\n{name.upper()}:")
        print(df.head())
    
    return result
if __name__ == "__main__":
    main()