"""
Scraper para dados de tipped minimum wage
"""
import requests
import pandas as pd
import re
from bs4 import BeautifulSoup
from typing import Dict, List, Tuple
import sys
sys.path.append('..')
from config import BASE_URL_TIPPED_WAGE, TIPPED_WAGE_START_YEAR, TIPPED_WAGE_END_YEAR, REQUEST_TIMEOUT


class TippedWageScraper:
    """Classe para extrair dados de tipped minimum wage"""
    
    def __init__(self, base_url: str = BASE_URL_TIPPED_WAGE):
        self.base_url = base_url
        self.header_order = ['jurisdiction', 'combinedrate', 'tipcredit', 'cashwage', 'definition']
        self.footnotes_dict = {}
    
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
            return None, [], []
        
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
        if soup_copy.find('strong'):
            valor = None
        return valor if valor else None, footnote_refs

    def processar_jurisdiction(self, td_element, footnotes_dict: Dict) -> Tuple[str, list, str]:
        """Extrai o nome limpo da jurisdiction e seus footnotes"""
        if not td_element:
            return None, [], ""
        
        # Extrair footnotes a partir dos links
        footnote_refs = []
        for link in td_element.find_all('a', href=True):
            href = link.get('href')
            if href:
                match = re.search(r'#(foot\d+)', href)
                if match:
                    footnote_refs.append(match.group(1))

        # Copiar HTML e remover os links e strongs para não afetar o texto final
        soup_copy = BeautifulSoup(str(td_element), 'html.parser')

        # Extrair nome da jurisdição (normalmente no primeiro <strong>)
        first_strong = soup_copy.find('strong')
        if first_strong:
            nome_limpo = re.sub(r'[^a-zA-Z0-9\s]', '', first_strong.get_text(strip=True))
        else:
            nome_limpo = soup_copy.get_text(strip=True)
        

        # Remover <strong> e <a> completamente para isolar o texto explicativo
        for tag in soup_copy.find_all(['strong', 'a']):
            tag.decompose()

        # Agora pegar só o texto restante
        other_extra_text = ' '.join(soup_copy.get_text(strip=True).split())

        return nome_limpo, footnote_refs, other_extra_text



    def extract_table_for_year(self, year: int) -> pd.DataFrame:
        """Extrai tabela de um ano específico"""
        url = f'{self.base_url}/{year}'
        
        try:
            response = requests.get(url, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
        except requests.RequestException:
            return pd.DataFrame()
        
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Extrair footnotes
        footnotes = self.extract_footnotes(soup)
        self.footnotes_dict[year] = footnotes
        # Processar tabela
        tip_table = soup.find('table')
        if not tip_table:
            return pd.DataFrame()
        
        tip_linhas = tip_table.find_all('tr')[1:]
        
        dados_tabela = []
        ultima_jurisdiction = None
        ultima_footnote = None
        ultima_footnote_refs = []
        
        for tr in tip_linhas:
            row_data = {}
            tds = tr.find_all('td')
            if len(tds) > len(self.header_order):
                tds.pop(1)
            # Pular linhas com colspan
            if tds and tds[0].get('colspan'):
                continue
            
            td_jurisdiction = tr.find('td', headers='jurisdiction')
            todas_notas = []
            all_footnote_refs = []
            all_footnote_texts = []
            
            if td_jurisdiction and td_jurisdiction.find('strong'):
                jurisdiction_limpa, footnote_refs, note_text = self.processar_jurisdiction(
                    td_jurisdiction, self.footnotes_dict
                )
                ultima_jurisdiction = jurisdiction_limpa
                ultima_footnote_refs = footnote_refs
                if note_text:
                    row_data['notes'] = note_text
                row_data['jurisdiction'] = jurisdiction_limpa
                
                if footnote_refs:
                    all_footnote_refs.extend(footnote_refs)
            else:
                if ultima_jurisdiction:
                    row_data['jurisdiction'] = ultima_jurisdiction
                    if ultima_footnote_refs:
                        all_footnote_refs.extend(ultima_footnote_refs)
            
            # Processar valores das colunas
            for i, td in enumerate(tds):
                header_name = td.get('headers')[0] if td.get('headers') else None
                if not header_name:
                    header_name = self.header_order[i]

                
                valor_limpo, footnote_refs = self.processar_celula_valor(
                    td, header_name, self.footnotes_dict
                )
                if valor_limpo:
                    if header_name != 'jurisdiction':
                        row_data[header_name] = valor_limpo
                    else:
                        row_data['notes'] = valor_limpo
                    if footnote_refs:
                        all_footnote_refs.extend(footnote_refs)
                
            
            if all_footnote_refs:
                row_data['footnotes'] = list(set(all_footnote_refs))  # Remove duplicatas
            
            if row_data and any(v for k, v in row_data.items() if k not in ['jurisdiction', 'notes', 'footnotes']):
                row_data['year'] = year
                dados_tabela.append(row_data)

        return pd.DataFrame(dados_tabela)
    
    def scrape(self, start_year: int = TIPPED_WAGE_START_YEAR, 
            end_year: int = TIPPED_WAGE_END_YEAR) -> pd.DataFrame:
        """Executa o scraping para todos os anos"""
        
        dfs = []
        for year in range(start_year, end_year + 1):
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

        return df_final


def main():
    """Função principal para teste"""
    scraper = TippedWageScraper()
    df = scraper.scrape(start_year=2003, end_year=2003)  # Teste com poucos anos
    print("\n📋 Preview dos dados:")
    return df


if __name__ == "__main__":
    main()