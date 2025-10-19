"""
Transformador que unifica os datasets de salário padrão e tipped wages
Cria a estrutura dimensional (Star Schema)
"""
import pandas as pd
import sys
sys.path.append('..')

from utils import extract_footnotes_from_notes, generate_hash
from config import WAGE_CATEGORIES

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
            base_row = {
                'jurisdiction': row['jurisdiction'],
                'year': row['year'],
                'notes': row.get('notes'),
                'frequency': 1,
                'source_url': f"https://www.dol.gov/agencies/whd/state/minimum-wage/tipped/{row['year']}"
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
        
        states = df_unified[['jurisdiction']].drop_duplicates().reset_index(drop=True)
        states = states.rename(columns={'jurisdiction': 'state_name'})
        states['state_id'] = states.index + 1
        states['is_territory'] = False  # Pode ser ajustado posteriormente
        
        self.dim_states = states[['state_id', 'state_name', 'is_territory']]
        return self.dim_states
    
    def create_dim_categories(self, df_unified: pd.DataFrame):
        """Cria tabela dimensional de categorias"""
        
        categories = df_unified[['category_name', 'category_type']].drop_duplicates().reset_index(drop=True)
        categories['category_id'] = categories.index + 1
        
        self.dim_categories = categories[['category_id', 'category_name', 'category_type']]
        return self.dim_categories
    
    def create_dim_footnotes(self, df_unified: pd.DataFrame):
        """Cria tabela dimensional de footnotes"""
        
        all_footnotes = {}  # {hash: text}
        bridge_data = []
        
        for idx, row in df_unified.iterrows():
            notes = row.get('notes')
            if pd.notna(notes):
                footnotes = extract_footnotes_from_notes(notes)
                
                for column_ref, text in footnotes:
                    fn_hash = generate_hash(text)
                    
                    if fn_hash not in all_footnotes:
                        all_footnotes[fn_hash] = text
                    
                    bridge_data.append({
                        'temp_row_id': idx,
                        'footnote_hash': fn_hash,
                        'column_reference': column_ref
                    })
        
        # Criar Dim_Footnotes
        footnotes_df = pd.DataFrame([
            {'footnote_hash': h, 'footnote_text': t}
            for h, t in all_footnotes.items()
        ])
        footnotes_df['footnote_id'] = footnotes_df.index + 1
        
        self.dim_footnotes = footnotes_df[['footnote_id', 'footnote_text', 'footnote_hash']]
        
        return self.dim_footnotes, pd.DataFrame(bridge_data)
    
    def create_fact_table(self, df_unified: pd.DataFrame):
        """Cria tabela fato"""
        
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
        
        # Selecionar colunas da fato
        fact_columns = [
            'state_id', 'category_id', 'year', 'base_wage_per_hour',
            'minimum_cash_wage', 'maximum_tip_credit', 'frequency', 'source_url'
        ]
        
        fact = df[fact_columns].copy()
        fact['wage_id'] = fact.index + 1
        fact['effective_date'] = None
        
        # Manter apenas notas gerais (sem [column])
        fact['notes'] = df['notes'].apply(
            lambda x: x if pd.notna(x) and '[' not in str(x) else None
        )
        
        # Reordenar
        self.fact_minimum_wage = fact[[
            'wage_id', 'state_id', 'category_id', 'year', 'effective_date',
            'base_wage_per_hour', 'minimum_cash_wage', 'maximum_tip_credit',
            'frequency', 'source_url', 'notes'
        ]]
        
        return self.fact_minimum_wage
    
    def create_bridge_table(self, bridge_data: pd.DataFrame):
        """Cria tabela bridge"""
        
        # Mapear temp_row_id para wage_id
        bridge_data['wage_id'] = bridge_data['temp_row_id'] + 1
        
        # Merge com footnote_id
        bridge = bridge_data.merge(
            self.dim_footnotes[['footnote_hash', 'footnote_id']],
            on='footnote_hash',
            how='left'
        )
        
        # Limpar e dedupe
        self.bridge_wage_footnote = bridge[['wage_id', 'footnote_id', 'column_reference']].drop_duplicates()
        
        return self.bridge_wage_footnote
    
    def transform(self):
        """Executa o pipeline completo de transformação"""
        
        # 1. Transformar datasets
        df_standard_transformed = self.transform_standard_to_long()
        df_tipped_transformed = self.transform_tipped_to_long()
        # 2. Garantir colunas comuns
        common_columns = [
            'jurisdiction', 'year', 'category_name', 'category_type',
            'base_wage_per_hour', 'minimum_cash_wage', 'maximum_tip_credit',
            'frequency', 'notes', 'source_url'
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
        
        # 4. Criar dimensões
        self.create_dim_states(df_unified)
        
        self.create_dim_categories(df_unified)
        
        _, bridge_data = self.create_dim_footnotes(df_unified)
        
        # 5. Criar fato
        self.create_fact_table(df_unified)
        
        # 6. Criar bridge
        self.create_bridge_table(bridge_data)
        

        
        return {
            'fact': self.fact_minimum_wage,
            'dim_states': self.dim_states,
            'dim_categories': self.dim_categories,
            'dim_footnotes': self.dim_footnotes,
            'bridge': self.bridge_wage_footnote
        }
    
def main():
    """Função principal para teste"""
    # Criar dados de exemplo
    df_standard = pd.DataFrame({
        'state': ['California', 'Texas'],
        'year': [2023, 2023],
        'minimal_wage': [15.50, 7.25],
        'frequency': [1, 1],
        'notes': [None, 'Federal minimum']
    })
    
    df_tipped = pd.DataFrame({
        'jurisdiction': ['California', 'Texas'],
        'year': [2023, 2023],
        'combinedrate': [15.50, 7.25],
        'tipcredit': [0.00, 5.12],
        'cashwage': [15.50, 2.13],
        'notes': [None, None]
    })
    
    transformer = DataTransformer(df_standard, df_tipped)
    result = transformer.transform()
    
    print("\n📋 Preview das tabelas:")
    for name, df in result.items():
        print(f"\n{name.upper()}:")
        print(df.head())
    
    return result



if __name__ == "__main__":
    main()