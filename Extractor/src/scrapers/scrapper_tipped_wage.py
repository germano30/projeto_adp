"""
Tipped Employee Minimum Wage Data Scraper.

This module implements a specialized web scraper for extracting tipped employee
wage regulations from the U.S. Department of Labor website. It handles:
- Combined wage rates (total with tips)
- Maximum tip credits
- Minimum cash wages
- State-specific definitions and rules
"""

import logging
import re
import sys
import warnings
from typing import Dict, List, Optional, Tuple, Union

import pandas as pd
import requests
from bs4 import BeautifulSoup, Tag

sys.path.append('..')
from config import (BASE_URL_TIPPED_WAGE, REQUEST_TIMEOUT,
                   TIPPED_WAGE_END_YEAR, TIPPED_WAGE_START_YEAR)

# Configure logging
logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

# Suppress non-critical warnings
warnings.filterwarnings('ignore')

class TippedWageScraper:
    """
    Web scraper specialized for tipped employee wage regulations.
    
    This class handles the extraction of complex tipped wage data, including:
    - Base wage requirements
    - Tip credit calculations
    - State-specific variations
    - Historical data across multiple years
    """
    
    def __init__(self, base_url: str = BASE_URL_TIPPED_WAGE):
        """
        Initialize the tipped wage scraper.
        
        Parameters
        ----------
        base_url : str, optional
            Base URL for the Department of Labor tipped wage data pages,
            defaults to BASE_URL_TIPPED_WAGE from config
        """
        self.base_url = base_url
        self.header_order = [
            'jurisdiction',  # State or territory
            'combinedrate', # Total wage including tips
            'tipcredit',   # Maximum allowable tip credit
            'cashwage',    # Minimum required cash wage
            'definition'   # State-specific definitions
        ]
        self.footnotes_dict = {}
    
    def extract_footnotes(self, soup: BeautifulSoup) -> Dict[str, str]:
        """
        Extract footnotes from a webpage providing regulatory context.
        
        This method identifies and extracts footnotes that provide crucial
        details about exceptions, special cases, and regulatory nuances
        in tipped wage laws.
        
        Parameters
        ----------
        soup : BeautifulSoup
            Parsed HTML content containing footnote information
            
        Returns
        -------
        Dict[str, str]
            Dictionary mapping footnote IDs to their full text content
        """
        footnotes_dict: Dict[str, str] = {}

        # Find anchor tags that designate footnotes and extract surrounding paragraph
        for a_tag in soup.find_all('a', attrs={'name': lambda x: x and str(x).lower().startswith('foot')}):
            name = a_tag.get('name')
            parent_p = a_tag.find_parent('p')

            if parent_p and name:
                footnote_num = a_tag.get_text(strip=True)
                full_text = ' '.join(parent_p.get_text().split())
                note_text = full_text.replace(footnote_num, '', 1).strip()
                footnotes_dict[str(name)] = note_text

        logger.debug("Extracted %d footnotes for tipped wages", len(footnotes_dict))
        return footnotes_dict
    
    def processar_celula_valor(self, td_element, column_name: str, footnotes_dict: Dict) -> Tuple[Optional[str], List[str]]:
        """Extract a cleaned value and any footnote refs from a table cell.

        Returns a tuple (value_or_none, list_of_footnote_ids).
        """
        if not td_element:
            return None, []

        # Find inline footnote links like href="#foot1"
        footnote_refs: List[str] = []
        for link in td_element.find_all('a', href=True):
            href = link.get('href')
            if href:
                match = re.search(r'#(foot\d+)', href)
                if match:
                    footnote_refs.append(match.group(1))

        # Remove links and extract visible text
        soup_copy = BeautifulSoup(str(td_element), 'html.parser')
        for link in soup_copy.find_all('a'):
            link.decompose()

        # If the cell includes strong markup we treat it as a header/label, not a value
        if soup_copy.find('strong'):
            return None, footnote_refs

        valor = ' '.join(soup_copy.get_text().split())
        return valor if valor else None, footnote_refs

    def processar_jurisdiction(self, td_element, footnotes_dict: Dict) -> Tuple[Optional[str], List[str], str]:
        """Extract a cleaned jurisdiction name, footnote refs and extra text.

        Returns (name_or_none, list_of_footnote_refs, extra_text).
        """
        if not td_element:
            return None, [], ""

        # Extract inline footnote links
        footnote_refs: List[str] = []
        for link in td_element.find_all('a', href=True):
            href = link.get('href')
            if href:
                match = re.search(r'#(foot\d+)', href)
                if match:
                    footnote_refs.append(match.group(1))

        soup_copy = BeautifulSoup(str(td_element), 'html.parser')
        first_strong = soup_copy.find('strong')
        if first_strong:
            nome_limpo = re.sub(r'[^a-zA-Z0-9\s]', '', first_strong.get_text(strip=True))
        else:
            nome_limpo = soup_copy.get_text(strip=True)

        # Remove strong and anchor tags to isolate remaining explanatory text
        for tag in soup_copy.find_all(['strong', 'a']):
            tag.decompose()

        other_extra_text = ' '.join(soup_copy.get_text(strip=True).split())
        return nome_limpo, footnote_refs, other_extra_text



    def extract_table_for_year(self, year: int) -> pd.DataFrame:
        """Extrai tabela de um ano específico"""
        url = f'{self.base_url}/{year}'
        
        try:
            response = requests.get(url, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
        except requests.RequestException as e:
            logger.warning("Failed to fetch tipped wage page for %s: %s", year, e)
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
            all_footnote_refs: List[str] = []
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

            # Process column values
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
        """Run scraping for the range of years and return concatenated DataFrame.

        Returns an empty DataFrame if no data is found or if recoverable errors occur.
        """
        dfs: List[pd.DataFrame] = []
        for year in range(start_year, end_year + 1):
            df_year = self.extract_table_for_year(year)
            if not df_year.empty:
                dfs.append(df_year)
                logger.info("Year %d: %d records", year, len(df_year))
            else:
                logger.debug("Year %d: no data", year)

        if not dfs:
            logger.warning("No tipped wage data was extracted for %d..%d", start_year, end_year)
            return pd.DataFrame()

        try:
            df_final = pd.concat(dfs, ignore_index=True)
            logger.info("Tipped wage scraping completed: %d records", len(df_final))
            return df_final
        except Exception as e:
            logger.error("Failed concatenating tipped wage tables: %s", e)
            return pd.DataFrame()


def main():
    """Função principal para teste"""
    scraper = TippedWageScraper()
    df = scraper.scrape(start_year=2003, end_year=2003)  # Teste com poucos anos
    print("\n📋 Preview dos dados:")
    return df


if __name__ == "__main__":
    main()