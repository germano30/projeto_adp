"""
Scraper para dados de salário mínimo padrão
"""
import requests
import pandas as pd
import re
from bs4 import BeautifulSoup
from typing import Dict, List, Tuple
import sys
sys.path.append('..')
from Extractor.utils import add_leading_zero, extract_multiple_values, append_note
from typing import Dict, Tuple
from scrapers.scrapper_minimum_wage import MinimumWageScraper

class StandardWageProcessor:
    """Classe para processar dados de salário mínimo padrão"""
    
    def __init__(self, df: pd.DataFrame, footnotes_dict: Dict[str, str] = None):
        self.df = df.copy()
        self.footnotes_dict = footnotes_dict or {}
    
    def add_footnote_references(self, df: pd.DataFrame) -> pd.DataFrame:
        """Adiciona coluna footnotes com referências baseadas no ano"""
        
        def get_footnote_ref(row):
            year_str = str(row['year'])
            if year_str in self.footnote_year_bridge:
                return self.footnote_year_bridge[year_str]
            return None
        
        df['footnotes'] = df.apply(get_footnote_ref, axis=1)
        
        return df
    
    def melt_dataframe(self) -> pd.DataFrame:
        """Transforma de wide para long format"""
        
        df_melted = self.df.melt(
            id_vars=['state'], 
            var_name='year', 
            value_name='minimal_wage'
        ).dropna()
        
        df_melted['year'] = df_melted['year'].astype(int)
        
        return df_melted
    
    def clean_wage_values(self, df: pd.DataFrame) -> pd.DataFrame:
        """Limpa valores de salário"""
        
        # Remover $ e outros caracteres
        df['minimal_wage'] = df['minimal_wage'].str.replace('$', '', regex=False)
        # Remover referências a footnotes [1], (a), etc
        df['footnote_wage'] = df['minimal_wage'].apply(
            lambda x: re.findall(r'[\[\(](.*?)[\]\)]', x) if pd.notna(x) else []
        )
        df['minimal_wage'] = df['minimal_wage'].str.replace(r'[\[\(].*?[\]\)]', '', regex=True)
        
        # Substituir valores especiais por NA
        df['minimal_wage'] = df['minimal_wage'].mask(
            df['minimal_wage'].isin(['...', 'NA', '']), 
            None
        )
        
        return df
    
    def process_multiple_rates(self, row):
        """Processa linhas com múltiplas taxas"""
        wage = row['minimal_wage']
        
        if pd.notna(wage) and isinstance(wage, str):
            # 1. Detectar frequency markers
            frequency = None
            if '/day' in wage:
                frequency = 2
                wage = wage.replace('/day', '').strip()
            elif '/wk' in wage:
                frequency = 3
                wage = wage.replace('/wk', '').strip()
            
            # 2. Detectar múltiplos valores
            pattern = r'\$?\d+\.?\d*'
            matches = re.findall(pattern, wage)
            
            if len(matches) >= 2:
                first_value = add_leading_zero(matches[0])
                second_value = add_leading_zero(matches[1])
                
                row['minimal_wage'] = first_value
                note = f"Or can be {second_value}, this reflects rates that differ by industry, occupation or other factors"
                row['notes'] = append_note(row.get('notes'), note)
            
            elif len(matches) == 1:
                row['minimal_wage'] = add_leading_zero(matches[0])
            
            # 3. Atualizar frequency
            if frequency is not None:
                row['frequency'] = frequency
        
        return row
    
    def add_default_notes(self, row):
        """Adiciona notas padrão para valores nulos"""
        if pd.isna(row['minimal_wage']) and pd.isna(row.get('notes')):
            return "This state utilizes the federal minimum wage"
        return row.get('notes')

    def process(self) -> pd.DataFrame:
        """Executa o pipeline completo de processamento"""
                # 1. Melt
        df = self.melt_dataframe()
        # 2. Limpar valores
        df = self.clean_wage_values(df)
        # 3. Inicializar colunas
        if 'notes' not in df.columns:
            df['notes'] = None
        if 'frequency' not in df.columns:
            df['frequency'] = None
        
        # 4. Processar múltiplas taxas
        df = df.apply(self.process_multiple_rates, axis=1)
        # 5. Converter para numérico
        df['minimal_wage'] = df['minimal_wage'].astype(str).str.extract(r'([\d.]+)', expand=False)
        df['minimal_wage'] = pd.to_numeric(df['minimal_wage'], errors='coerce')
        
        # 6. Adicionar notas padrão
        df['notes'] = df.apply(self.add_default_notes, axis=1)
        df['id'] = range(1, len(df) + 1)
        
        # 8. Definir frequency padrão
        df['frequency'] = df['frequency'].fillna(1)
        
        # 9. Adicionar ID
        
        # 10. Reorganizar colunas
        df = df[['id', 'state', 'year', 'minimal_wage', 'frequency', 'notes','footnote_wage']]
        
        return df


def main():
    """Função principal para teste"""
    scraper = MinimumWageScraper()
    df = scraper.scrape()
    print("\n📋 Preview dos dados:")
    print(df.head())
    print(df)
    print()
    print(scraper.footnotes_dict)
    return df