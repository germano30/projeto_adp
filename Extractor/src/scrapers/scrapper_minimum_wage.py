"""
Standard Minimum Wage Data Scraper.

This module implements a web scraper for extracting standard minimum wage data
from the U.S. Department of Labor website. It handles both tabular wage data
and associated footnotes/annotations.
"""

import logging
import re
import sys
import warnings
from typing import Dict, List, Optional, Tuple

import pandas as pd
import requests
from bs4 import BeautifulSoup

sys.path.append('..')
from config import BASE_URL_MINIMUM_WAGE, REQUEST_TIMEOUT

# Configure logging
logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

# Suppress non-critical warnings
warnings.filterwarnings('ignore')

class MinimumWageScraper:
    """
    Web scraper for standard minimum wage data extraction.
    
    This class handles the extraction of minimum wage data from the Department
    of Labor website, including both tabular wage data and associated footnotes.
    It implements robust error handling and data validation.
    """
    
    def __init__(self, url: str = BASE_URL_MINIMUM_WAGE):
        """
        Initialize the minimum wage scraper.
        
        Parameters
        ----------
        url : str, optional
            URL of the Department of Labor minimum wage data page,
            defaults to BASE_URL_MINIMUM_WAGE from config
        """
        self.url = url
        self.soup = None
        self.footnotes_dict = {}
        
    def fetch_page(self) -> bool:
        """
        Retrieve and parse the HTML content from the target URL.
        
        Returns
        -------
        bool
            True if page was successfully retrieved and parsed,
            False otherwise
        """
        try:
            response = requests.get(self.url, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
            self.soup = BeautifulSoup(response.content, 'html.parser')
            logger.debug("Page fetched and parsed successfully")
            return True
        except requests.RequestException as e:
            logger.error("Failed to retrieve page: %s", e)
            return False
    
    def extract_footnotes(self) -> Dict[str, str]:
        """
        Extract and parse footnotes from the webpage.
        
        Footnotes provide crucial context and exceptions to the standard
        minimum wage rules. This method identifies and extracts them using
        pattern matching.
        
        Returns
        -------
        Dict[str, str]
            Dictionary mapping footnote IDs to their text content
        """
        if not self.soup:
            return {}

        # Try to locate a content area that contains footnotes; fallback to whole document
        container = self.soup.find('div', id='content') or self.soup

        footnotes = {}
        # Look for paragraphs that start with a bracketed id like "[1] text" or "(1) text"
        pattern = re.compile(r'^[\[\(]\s*(?P<id>[^\)\]\s]+)[\)\]]\s*(?P<text>.+)')
        for p in container.find_all('p'):
            txt = p.get_text(strip=True)
            m = pattern.match(txt)
            if m:
                fid = m.group('id')
                ftext = m.group('text').replace('- ', '').strip()
                footnotes[fid] = ftext

        self.footnotes_dict = footnotes
        logger.debug("Extracted %d footnotes", len(footnotes))
        return self.footnotes_dict
    
    def extract_tables(self) -> List[pd.DataFrame]:
        """Extract all HTML tables and convert them into DataFrames.

        Returns a list of DataFrames (one per table). Year/header extraction
        will attempt to keep only numeric year values for columns.
        """
        if not self.soup:
            return []

        tables = self.soup.find_all('table')
        df_list: List[pd.DataFrame] = []

        for table in tables:
            rows = table.find_all('tr')
            if not rows:
                continue

            # Extract header cells; skip the first header cell (state label)
            header = rows[0]
            ths = header.find_all('th')
            # Build year labels cleaning non-digit characters; keep original if not a year
            years = []
            for th in ths[1:]:
                txt = th.get_text(strip=True)
                y = re.sub(r'[^0-9]', '', txt)
                years.append(y if y else txt)

            # Extract data rows
            states = []
            for state_row in rows[1:]:
                tds = state_row.find_all('td')
                states.append([td.get_text(strip=True) for td in tds])

            # If row length doesn't match, skip this table
            if states and len(states[0]) != len(years) + 1:
                logger.debug("Skipping table due to column mismatch: expected %d, got %d", len(years) + 1, len(states[0]))
                continue

            df = pd.DataFrame(states, columns=['state'] + years)
            df_list.append(df)

        logger.debug("Extracted %d tables from page", len(df_list))
        return df_list
    
    def process_footnote_columns(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[str, str]]:
        """Process columns that contain footnote references in their header names.

        The function looks for footnote keys embedded in column names (e.g.
        "2020 [1]") and removes the footnote id from the column name while
        returning a mapping of cleaned column -> footnote id.
        """
        # columns that are not plain year columns (YYYY) and not the state column
        columns_to_adjust = [col for col in df.columns if not re.match(r'^\d{4}$', str(col)) and col != 'state']
        footnote_year_bridge: Dict[str, str] = {}

        for key in list(self.footnotes_dict.keys()):
            for col in columns_to_adjust:
                if str(key) in str(col):
                    clean_col = str(col).replace(str(key), '').strip()
                    footnote_year_bridge[clean_col] = str(key)
                    df = df.rename(columns={col: clean_col})

        logger.debug("Processed footnote columns, mappings: %s", footnote_year_bridge)
        return df, footnote_year_bridge
    
    def scrape(self) -> pd.DataFrame:
        """Run the full scraping flow and return a consolidated DataFrame.

        On any recoverable error the function returns an empty DataFrame.
        """
        logger.info("Starting standard minimum wage scraping for %s", self.url)

        if not self.fetch_page():
            return pd.DataFrame()

        # Extract footnotes early to allow column processing
        try:
            self.extract_footnotes()
        except Exception as e:
            logger.warning("Failed to extract footnotes: %s", e)

        df_list = self.extract_tables()
        if not df_list:
            logger.warning("No tables found on page %s", self.url)
            return pd.DataFrame()

        # Concatenate all tables into a single DataFrame
        try:
            df = pd.concat(df_list, ignore_index=True)
            logger.info("Scraping completed: %d records", len(df))
        except Exception as e:
            logger.error("Failed concatenating tables: %s", e)
            return pd.DataFrame()

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