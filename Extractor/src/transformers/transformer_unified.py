"""
Transformador que unifica os datasets de salário padrão e tipped wages
Cria a estrutura dimensional (Star Schema) com Dim_Footnotes normalizado
"""
import pandas as pd
import sys
sys.path.append('..')
from utils import generate_hash
from config import WAGE_CATEGORIES, TIPPED_WAGE_TYPE
import re
import warnings
warnings.filterwarnings('ignore')
class DataTransformer:
    """Classe para transformar e unificar os datasets"""
    
    def __init__(self, df_standard: pd.DataFrame, df_tipped: pd.DataFrame, youth_rules: pd.DataFrame):
        self.df_standard = df_standard
        self.df_tipped = df_tipped
        
        # Tabelas dimensionais
        self.dim_states = None
        self.dim_categories = None
        self.dim_footnotes = None
        self.fact_minimum_wage = None
        self.bridge_wage_footnote = None
        self.dim_youth_rules = youth_rules


    def normalize_text(self,text: str) -> str:
        """Normaliza o texto para evitar duplicatas falsas."""
        if not text:
            return ""
        text = text.strip()                 # remove espaços no início/fim
        text = re.sub(r'\s+', ' ', text)   # substitui múltiplos espaços por 1
        text = text.replace('\n', ' ')     # remove quebras de linha
        text = text.lower()                 # deixa tudo minúsculo
        text = re.sub(r'\s*\.\s*', '. ', text)  # padroniza pontos
        text = re.sub(r'\s*,\s*', ', ', text)   # padroniza vírgulas
        text = re.sub(r'\s*;\s*', '; ', text)   # padroniza ponto e vírgula
        return text.strip()

    
    def create_dim_footnotes_unified(self, all_footnotes_dict: dict) -> pd.DataFrame:
        """
        Cria Dim_Footnotes unificada de todos os datasets
        
        Args:
            all_footnotes_dict: {footnote_key: footnote_text}
        
        Returns:
            DataFrame com footnote_id, footnote_key, footnote_text
        """
        
        footnotes_data = []
        for key, text in all_footnotes_dict.items():
            for foot_key, foot_text in text.items():
                clean_text = self.normalize_text(foot_text)
                footnotes_data.append({
                    'footnote_key': foot_key,
                    'footnote_text': clean_text,
                    'footnote_year': int(key) if key != 1 else 1900,
                    'category_id': 1 if key == 1 else 2,
                    'footnote_hash': generate_hash(clean_text)
                })
        
        df_footnotes = pd.DataFrame(footnotes_data)
        # df_footnotes = df_footnotes.drop_duplicates(subset=['footnote_key','footnote_hash'])
        
        df_footnotes['footnote_id'] = range(1, len(df_footnotes) + 1)
        
        self.dim_footnotes = df_footnotes[['footnote_id', 'footnote_key', 'footnote_text', 'footnote_year', 'category_id']]
        
        return self.dim_footnotes
    
    def transform_standard_to_long(self) -> pd.DataFrame:
        """Transforma dataset padrão para formato compatível"""        
        df = self.df_standard.copy()
        df = df.rename(columns={'state': 'jurisdiction', 'minimal_wage': 'base_wage_per_hour'})
        df['category_name'] = WAGE_CATEGORIES['standard']
        df['category_type'] = 'standard'
        df['minimum_cash_wage'] = None
        df['maximum_tip_credit'] = None
        df['source_url'] = 'https://www.dol.gov/agencies/whd/state/minimum-wage/history'
        
        return df
    
    def transform_tipped_to_long(self) -> pd.DataFrame:
        """Transforma dataset tipped para formato long"""        
        df_long = []
        for _, row in self.df_tipped.iterrows():
            # Notes e footnotes já vêm separados do scraper
            notes_clean = row.get('notes')    
                    
            base_row = {
                'jurisdiction': row['jurisdiction'],
                'year': row['year'],
                'frequency': 1,
                'source_url': f"https://www.dol.gov/agencies/whd/state/minimum-wage/tipped/{row['year']}",
                'notes': notes_clean,
                'footnote_wage': row.get('footnotes')
            }
            
            # Combined Rate
            if pd.notna(row.get('combinedrate')):
                df_long.append({
                    **base_row,
                    'category_name': WAGE_CATEGORIES['tipped_combined'],
                    'category_type': 'tipped',
                    'base_wage_per_hour': row['combinedrate'],
                    'value_type': TIPPED_WAGE_TYPE[row.get('combinedrate_type')],
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
                    'value_type': TIPPED_WAGE_TYPE[row.get('tipcredit_type')],
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
                    'value_type': TIPPED_WAGE_TYPE[row.get('cashwage_type')],
                    'minimum_cash_wage': row['cashwage'],
                    'maximum_tip_credit': None
                })
        
        return pd.DataFrame(df_long)
    
    def create_dim_states(self, df_unified: pd.DataFrame):
        
        states = df_unified[['jurisdiction']].drop_duplicates().reset_index(drop=True)
        states = states.rename(columns={'jurisdiction': 'state_name'})
        states['state_id'] = states.index + 1
        states['is_territory'] = False
        
        self.dim_states = states[['state_id', 'state_name', 'is_territory']]
        return self.dim_states
    
    def create_dim_categories(self, df_unified: pd.DataFrame):
        
        categories = df_unified[['category_name', 'category_type']].drop_duplicates().reset_index(drop=True)
        categories['category_id'] = categories.index + 1
        
        self.dim_categories = categories[['category_id', 'category_name', 'category_type']]
        return self.dim_categories
    
    def create_fact_table(self, df_unified: pd.DataFrame):
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
        self.footnote_refs_by_wage = df[['footnote_wage','year', 'category_id']].copy()
        self.footnote_refs_by_wage.index = range(1, len(self.footnote_refs_by_wage) + 1)
        self.footnote_refs_by_wage['wage_id'] = self.footnote_refs_by_wage.index
        fact_columns = [
            'state_id', 'category_id', 'year', 'base_wage_per_hour',
            'minimum_cash_wage', 'maximum_tip_credit', 'frequency', 'source_url', 'notes'
        ]
        
        fact = df[[c for c in fact_columns if c in df.columns]].copy()
        fact['wage_id'] = range(1, len(fact) + 1)
        fact['effective_date'] = None
        
        self.fact_minimum_wage = fact[[
            'wage_id', 'state_id', 'category_id', 'year', 'effective_date',
            'base_wage_per_hour', 'minimum_cash_wage', 'maximum_tip_credit',
            'frequency', 'source_url', 'notes'
        ]]
        
        return self.fact_minimum_wage
    
    def create_bridge_table(self, all_footnotes_dict: dict):
        """Cria tabela bridge entre wage e footnotes"""
        
        bridge_data = []
        for _, row in self.footnote_refs_by_wage.iterrows():
            wage_id = row['wage_id']
            footnote_refs = row.get('footnote_wage')
            if wage_id == 3102 or wage_id == 3103:
                print(row)
            if isinstance(footnote_refs, str):
                footnote_refs = [footnote_refs]
            elif not isinstance(footnote_refs, list):
                continue
            if len(footnote_refs)==0:
                continue
            if wage_id == 3102 or wage_id == 3103:
                print(footnote_refs)
            for ref in footnote_refs:
                # Buscar footnote_id correspondente no Dim_Footnotes
                category_filter = 1 if row['category_id'] == 1 else 2
                dim_footnotes = self.dim_footnotes.loc[self.dim_footnotes['category_id'] == category_filter]
                
                dim_footnotes['footnote_key_clean'] = dim_footnotes['footnote_key'].apply(
                    lambda x: re.sub(r'[\(\)\[\]\{\}]', '', str(x))
                )
                ref_clean = re.sub(r'[\(\)\[\]\{\}]', '', str(ref))

                footnote_row = dim_footnotes[dim_footnotes['footnote_key_clean'] == ref_clean]
                footnote_row = footnote_row[footnote_row['footnote_year'] == row['year']]

                if not footnote_row.empty:
                    bridge_data.append({
                        'wage_id': wage_id,
                        'footnote_id': footnote_row.iloc[0]['footnote_id']
                    })
        
        if bridge_data:
            self.bridge_wage_footnote = pd.DataFrame(bridge_data).drop_duplicates()
        else:
            self.bridge_wage_footnote = pd.DataFrame(columns=['wage_id', 'footnote_id'])
            self.bridge_wage_footnote[['wage_id', 'footnote_id']] = self.bridge_wage_footnote[['wage_id', 'footnote_id']].values.astype(int) 

        return self.bridge_wage_footnote
    
    def default_state_name(self, df_unified):
        df_unified['jurisdiction'] = df_unified['jurisdiction'].str.replace('\t','')
        df_unified['jurisdiction'] = df_unified['jurisdiction'].str.replace('\n',' ')
        df_unified['jurisdiction'] = df_unified['jurisdiction'].apply(lambda x: ' '.join(x.split()))
        df_unified['jurisdiction'] = df_unified['jurisdiction'].apply(lambda x: re.sub(r'[^a-zA-Z\s]', '',x))
        df_unified.loc[df_unified['jurisdiction'].isin(['Federal FLSA', 'FEDERAL']), 'jurisdiction'] = 'Federal' 
        return df_unified    
        
    def collect_all_footnotes(self, df_standard, df_tipped) -> dict:
        """Coleta todos os footnotes de todos os datasets"""
        
        all_footnotes = {}
        if hasattr(self, 'standard_footnotes'):
            all_footnotes[1]=(self.standard_footnotes)
        
        # Tipped wage footnotes (do scraper)
        if hasattr(self, 'tipped_footnotes'):
            all_footnotes.update(self.tipped_footnotes)
        
        # Youth employment footnotes (se houver)
        if hasattr(self, 'youth_footnotes'):
            all_footnotes.update(self.youth_footnotes)
        
        return all_footnotes
    
    def create_dim_youth_rules(self):
        self.dim_youth_rules['year'] = self.dim_youth_rules['year'].apply(lambda x: int(x) if pd.notna(x) else None)
        self.dim_youth_rules['is_issued_by_labor'] = self.dim_youth_rules['is_issued_by_labor'].apply(lambda x: int(x) if pd.notna(x) else None)
        self.dim_youth_rules['is_issued_by_school'] = self.dim_youth_rules['is_issued_by_school'].apply(lambda x: int(x) if pd.notna(x) else None)
        self.dim_youth_rules['age_min'] = self.dim_youth_rules['age_min'].apply(lambda x: int(x) if pd.notna(x) else None)
        self.dim_youth_rules['age_max'] = self.dim_youth_rules['age_max'].apply(lambda x: int(x) if pd.notna(x) else None)
        def normalize_footnote(x):
            if isinstance(x, list):
                return ','.join(map(str, x))
            elif pd.isna(x):
                return None
            else:
                return str(x)

        self.dim_youth_rules['footnotes'] = self.dim_youth_rules['footnotes'].apply(normalize_footnote) 
        print(self.dim_youth_rules.info())
    
    def transform(self, standard_footnotes: dict = None, tipped_footnotes: dict = None):
        """Executa o pipeline completo de transformação"""
        
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
            'frequency', 'notes', 'source_url', 'footnote_wage' 
        ]
        
        for col in common_columns:
            if col not in df_standard_transformed.columns:
                df_standard_transformed[col] = None
            if col not in df_tipped_transformed.columns:
                df_tipped_transformed[col] = None
        
        df_unified = pd.concat([
            df_standard_transformed[common_columns],
            df_tipped_transformed[common_columns]
        ], ignore_index=True)
        # 4. Criar Dim_Footnotes (antes de criar fato)
        all_footnotes = self.collect_all_footnotes(df_standard_transformed, df_tipped_transformed)
        df_unified = self.default_state_name(df_unified=df_unified)
        self.create_dim_footnotes_unified(all_footnotes)
        
        # 5. Criar dimensões
        self.create_dim_states(df_unified)
        
        self.create_dim_categories(df_unified)
        
        # 6. Criar fato
        self.create_fact_table(df_unified)
        
        # 7. Criar bridge
        self.create_bridge_table(all_footnotes)
        
        self.create_dim_youth_rules()
        return {
            'dim_youth_rules': self.dim_youth_rules,
            'dim_states': self.dim_states,
            'dim_categories': self.dim_categories,
            'dim_footnotes': self.dim_footnotes,
            'fact': self.fact_minimum_wage,
            'bridge': self.bridge_wage_footnote
        }


def main():
    """Função principal para teste"""
    # Criar dados de exemplo]
    scraper_standard = MinimumWageScraper()
    df_standard_raw = scraper_standard.scrape()
    processor_standard = StandardWageProcessor(df_standard_raw, scraper_standard.footnotes_dict)
    df_standard_processed = processor_standard.process()
    
    scraper_tipped = TippedWageScraper()
    df_tipped_raw = scraper_tipped.scrape(
            start_year=2003,
            end_year=2025
        )
    


    
        # 2. Processar tipped wages
    processor_tipped = TippedWageProcessor(df_tipped_raw, scraper_tipped.footnotes_dict)
    df_tipped_processed = processor_tipped.process()


  
    transformer = DataTransformer(df_standard_processed, df_tipped_processed)
    result = transformer.transform(
        standard_footnotes=processor_standard.footnotes_dict,
        tipped_footnotes=processor_tipped.footnotes_dict
    )

    
    return result
if __name__ == "__main__":
    main()