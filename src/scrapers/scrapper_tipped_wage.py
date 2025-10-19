"""
Scraper para dados de tipped minimum wage
"""
import requests
import pandas as pd
import re
from bs4 import BeautifulSoup
from typing import Dict, List
import sys
sys.path.append('..')
from config import BASE_URL_TIPPED_WAGE, TIPPED_WAGE_START_YEAR, TIPPED_WAGE_END_YEAR, REQUEST_TIMEOUT


class TippedWageScraper:
    """Classe para extrair dados de tipped minimum wage"""
    
    def __init__(self, base_url: str = BASE_URL_TIPPED_WAGE):
        self.base_url = base_url
        self.header_order = ['jurisdiction', 'combinedrate', 'tipcredit', 'cashwage', 'definition']
    
    def extract_footnotes(self, soup: BeautifulSoup) -> Dict[str, str]:
        """Extrai footnotes de uma página"""
        footnotes_dict = {}
        
        for a_tag in soup.find_all('a', attrs={'name': lambda x: x and x.startswith('foot')}):
            name = a_tag.get('name')
            parent_p = a_tag.find_parent('p')
            
            if parent_p:
                footnote_num = a_tag.get_text(strip=True)
                texto_completo = ' '.join(parent_p.get_text().split())
                texto_nota = texto_completo.replace(footnote_num, '', 1).strip()
                footnotes_dict[name] = texto_nota
        
        return footnotes_dict
    
    def processar_celula_valor(self, td_element, column_name: str, footnotes_dict: Dict) -> tuple:
        """Extrai valor limpo e footnotes de uma célula"""
        if not td_element:
            return None, None, []
        
        # Procurar links de footnote
        footnote_refs = []
        for link in td_element.find_all('a', href=True):
            href = link.get('href')
            if href:
                match = re.search(r'#(foot\d+)', href)
                if match:
                    footnote_refs.append(match.group(1))
        
        # Criar cópia e remover links
        td_html = str(td_element)
        soup_copy = BeautifulSoup(td_html, 'html.parser')
        
        for link in soup_copy.find_all('a'):
            link.decompose()
        
        # Extrair valor limpo
        valor = ' '.join(soup_copy.get_text().split())
        
        # Buscar textos dos footnotes
        footnote_texts = []
        for ref in footnote_refs:
            if ref in footnotes_dict:
                footnote_texts.append(f"[{column_name}] {footnotes_dict[ref]}")
        
        footnote_text = ' ; '.join(footnote_texts) if footnote_texts else None
        
        return valor if valor else None, footnote_text, footnote_refs
    
    def processar_jurisdiction(self, td_element, footnotes_dict: Dict) -> tuple:
        """Extrai o nome limpo da jurisdiction e seus footnotes"""
        if not td_element:
            return None, None, None
        
        # Procurar links de footnote
        footnote_refs = []
        for link in td_element.find_all('a', href=True):
            href = link.get('href')
            if href:
                match = re.search(r'#(foot\d+)', href)
                if match:
                    footnote_refs.append(match.group(1))
        
        # Criar cópia e remover links
        td_html = str(td_element)
        soup_copy = BeautifulSoup(td_html, 'html.parser')
        
        for link in soup_copy.find_all('a'):
            link.decompose()
        
        strong_tag = soup_copy.find('strong')
        if strong_tag:
            texto = ' '.join(strong_tag.get_text().split())
            nome_limpo = re.sub(r'[^a-zA-Z0-9\s]', '', texto)
        else:
            nome_limpo = soup_copy.get_text(strip=True)
        
        extra_text = soup_copy.get_text().replace(nome_limpo, '').strip()
        
        # Buscar textos dos footnotes
        footnote_texts = [footnotes_dict[ref] for ref in footnote_refs if ref in footnotes_dict]
        footnote_text = ' ; '.join(footnote_texts) if footnote_texts else None
        
        return nome_limpo, footnote_text, extra_text
    
    def extract_table_for_year(self, year: int) -> pd.DataFrame:
        """Extrai tabela de um ano específico"""
        url = f'{self.base_url}/{year}'
        
        try:
            response = requests.get(url, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
        except requests.RequestException:
            print(f"❌ Falha ao obter dados de {year}")
            return pd.DataFrame()
        
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Extrair footnotes
        footnotes_dict = self.extract_footnotes(soup)
        
        # Processar tabela
        tip_table = soup.find('table')
        if not tip_table:
            print(f"⚠️ Nenhuma tabela encontrada em {year}")
            return pd.DataFrame()
        
        tip_linhas = tip_table.find_all('tr')[1:]
        
        dados_tabela = []
        ultima_jurisdiction = None
        ultima_footnote = None
        
        for tr in tip_linhas:
            row_data = {}
            tds = tr.find_all('td')
            
            # Pular linhas com colspan
            if tds and tds[0].get('colspan'):
                continue
            
            td_jurisdiction = tr.find('td', headers='jurisdiction')
            todas_notas = []
            
            if td_jurisdiction and td_jurisdiction.find('strong'):
                jurisdiction_limpa, footnote_text, extra_text = self.processar_jurisdiction(
                    td_jurisdiction, footnotes_dict
                )
                ultima_jurisdiction = jurisdiction_limpa
                ultima_footnote = footnote_text
                row_data['jurisdiction'] = jurisdiction_limpa
                
                if footnote_text:
                    todas_notas.append(footnote_text)
                if extra_text:
                    todas_notas.append(extra_text)
            else:
                if ultima_jurisdiction:
                    row_data['jurisdiction'] = ultima_jurisdiction
                    if ultima_footnote:
                        todas_notas.append(ultima_footnote)
            
            # Processar valores das colunas
            for td in tds:
                header_name = td.get('headers')[0] if td.get('headers') else None
                if not header_name:
                    idx = tds.index(td)
                    header_name = self.header_order[idx] if len(tds) == 4 else self.header_order[idx - 1]
                
                valor_limpo, footnote_text, _ = self.processar_celula_valor(
                    td, header_name, footnotes_dict
                )
                
                if header_name != 'jurisdiction':
                    row_data[header_name] = valor_limpo
                
                if footnote_text:
                    todas_notas.append(footnote_text)
            
            if todas_notas:
                row_data['notes'] = ' ; '.join(todas_notas)
            
            if row_data and any(v for k, v in row_data.items() if k not in ['jurisdiction', 'notes']):
                row_data['year'] = year
                dados_tabela.append(row_data)
        
        return pd.DataFrame(dados_tabela)
    
    def scrape(self, start_year: int = TIPPED_WAGE_START_YEAR, 
               end_year: int = TIPPED_WAGE_END_YEAR) -> pd.DataFrame:
        """Executa o scraping para todos os anos"""
        print(f"🔍 Iniciando scraping de tipped wages ({start_year}-{end_year})...")
        
        dfs = []
        for year in range(start_year, end_year + 1):
            print(f"   📅 Processando {year}...", end=' ')
            df_year = self.extract_table_for_year(year)
            if not df_year.empty:
                dfs.append(df_year)
                print(f"✓ {len(df_year)} registros")
            else:
                print("✗")
        
        if not dfs:
            print("❌ Nenhum dado foi extraído")
            return pd.DataFrame()
        
        df_final = pd.concat(dfs, ignore_index=True)
        print(f"\n✅ Scraping concluído: {len(df_final)} registros totais")
        
        return df_final
