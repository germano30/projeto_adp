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
from Extractor.config import BASE_URL_MINIMUM_WAGE, REQUEST_TIMEOUT
import warnings
warnings.filterwarnings('ignore')

class MinimumWageScraper:
    """Classe para extrair dados de salário mínimo padrão"""
    
    def __init__(self, url: str = BASE_URL_MINIMUM_WAGE):
        self.url = url
        self.soup = None
        self.footnotes_dict = {}
        
    def fetch_page(self) -> bool:
        """Busca a página HTML"""
        try:
            response = requests.get(self.url, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
            self.soup = BeautifulSoup(response.content, 'html.parser')
            return True
        except requests.RequestException as e:
            print(f"❌ Erro ao buscar página: {e}")
            return False
    
    def extract_footnotes(self) -> Dict[str, str]:
        """Extrai footnotes da página"""
        if not self.soup:
            return {}
        
        footnotes = self.soup.find('div', id='content')
        if not footnotes:
            return {}
        
        list_footnotes = []
        for p in footnotes.find_all('p'):
            if re.match(r'^[\[\(].[\]\)]', p.text):
                id_footnote = p.text.strip().split(' ')[0]
                text_footnote = ' '.join(p.text.strip().split(' ')[1:]).replace('- ', '')
                list_footnotes.append((id_footnote, text_footnote))
        
        self.footnotes_dict = {id_: text for id_, text in list_footnotes}
        return self.footnotes_dict
    
    def extract_tables(self) -> List[pd.DataFrame]:
        """Extrai todas as tabelas da página"""
        if not self.soup:
            return []
        
        tables = self.soup.find_all('table')
        df_list = []
        
        for table in tables:
            rows = table.find_all('tr')
            if not rows:
                continue
            
            # Extrair cabeçalho
            header = rows[0]

            years = [re.sub(r'[a-zA-Z()\[\]]', '',th.text) for th in header.find_all('th')[1:]]
            
            # Extrair dados
            states = []
            for state_row in rows[1:]:
                states.append([td.text for td in state_row.find_all('td')])
            
            df = pd.DataFrame(states, columns=['state'] + years)
            df_list.append(df)
        
        return df_list
    
    def process_footnote_columns(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[str, str]]:
        """Processa colunas que contêm referências a footnotes"""
        columns_to_adjust = [col for col in df.columns if not col.isnumeric() and col != 'state']
        footnote_year_bridge = {}
        
        for key in self.footnotes_dict.keys():
            for col in columns_to_adjust:
                if key in col:
                    clean_col = col.replace(key, '').strip()
                    footnote_year_bridge[clean_col] = key
                    df = df.rename(columns={col: clean_col})
        
        return df, footnote_year_bridge
    
    def scrape(self) -> pd.DataFrame:
        """Executa o scraping completo"""
        print("🔍 Iniciando scraping de salário mínimo padrão...")
        
        if not self.fetch_page():
            return pd.DataFrame()
        
        self.extract_footnotes()

        df_list = self.extract_tables()

        if not df_list:
            return pd.DataFrame()
        # Concatenar todas as tabelas
        df = pd.concat(df_list, ignore_index=True)
        print(f"✅ Scraping concluído: {len(df)} registros")

        return df


def main():
    """Função principal para teste"""
    scraper = MinimumWageScraper()
    df = scraper.scrape()
    print("\n📋 Preview dos dados:")
    print(df.head())
    return df


if __name__ == "__main__":
    main()