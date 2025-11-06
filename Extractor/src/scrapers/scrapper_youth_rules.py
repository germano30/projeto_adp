"""
Scraper melhorado para Youth Employment Rules
Baseado na sua implementação, com melhorias adicionais
"""
import logging
import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
from typing import Dict, List, Tuple, Optional
import warnings

warnings.filterwarnings('ignore')

# Configure module logger
logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

class YouthEmploymentScraperImproved:
    """Scraper melhorado para Age Certificates"""
    
    def __init__(self, url: str = "https://www.dol.gov/agencies/whd/state/age-certificates"):
        self.url = url
        self.soup = None
        self.footnotes_dict = {}
        self.year = None
    
    def fetch_page(self) -> bool:
        """Busca a página HTML"""
        try:
            response = requests.get(self.url, timeout=30)
            response.raise_for_status()
            self.soup = BeautifulSoup(response.text, "html.parser")
            logger.debug("Fetched youth rules page")
            return True
        except requests.RequestException as e:
            logger.error("Error fetching youth rules page: %s", e)
            return False
    
    def extract_year(self) -> Optional[int]:
        """Extrai o ano dos dados"""
        if not self.soup:
            return None

        # Try a few patterns for a publication or revision year
        text = self.soup.get_text(separator=' ')
        patterns = [r'January\s+\d+,\s+(\d{4})', r'Updated:\s*(\d{4})', r'Revised:\s*(\d{4})', r'©\s*(\d{4})']
        for pat in patterns:
            match = re.search(pat, text, flags=re.IGNORECASE)
            if match:
                try:
                    self.year = int(match.group(1))
                    logger.debug("Detected year: %s", self.year)
                    return self.year
                except Exception:
                    continue

        return None
    
    def extract_footnotes(self) -> Dict[str, str]:
        """Extrai footnotes completos da página"""
        if not self.soup:
            return {}

        # Prefer extracting footnotes from a dedicated section if present
        text = self.soup.get_text(separator=' ')
        footnotes: Dict[str, str] = {}

        # Try to find a labelled Footnotes: section first
        if 'Footnotes:' in text:
            footnote_section = text.split('Footnotes:')[1]
            pattern = r'\[?(\d+)\]?\s+(.+?)(?=\s*\[?\d+\]?\s+|$)'
            matches = re.findall(pattern, footnote_section, re.DOTALL)
            for num, text_content in matches:
                clean_text = ' '.join(text_content.split()).strip()
                clean_text = re.sub(r'^Footnote\s+\d+:\s*', '', clean_text, flags=re.IGNORECASE)
                if clean_text:
                    footnotes[num] = clean_text

        # Fallback: try to extract anchors that look like footnote markers
        if not footnotes:
            for a in self.soup.find_all('a', attrs={'name': True}):
                name = a.get('name')
                if name and re.match(r'foot\d+', str(name)):
                    parent = a.find_parent('p')
                    if parent:
                        txt = ' '.join(parent.get_text(separator=' ').split())
                        # remove marker text
                        txt = re.sub(r'^\[?\w+\]?\s*', '', txt)
                        footnotes[name.replace('foot', '')] = txt

        self.footnotes_dict = footnotes
        logger.debug("Extracted %d youth footnotes", len(footnotes))
        return self.footnotes_dict
    
    @staticmethod
    def detect_requirement_level(text: str) -> Optional[int]:
        """
        Identifica o nível de requisito no texto.
        
        Returns:
            1 = Mandatory (M)
            2 = On Request (R)
            3 = Practice (P)
            None = Não especificado
        """
        mapping = {'(M)': 1, '(R)': 2, '(P)': 3}
        
        for mark, level in mapping.items():
            if mark in text:
                return level
        
        return None
    
    @staticmethod
    def extract_text(td) -> str:
        """Extrai texto limpo de uma célula <td>."""
        return '; '.join(part.strip() for part in td.stripped_strings)
    
    @staticmethod
    def remove_requirement_marks(text: str) -> str:
        """Remove marcas de requisito do texto."""
        return re.sub(r'\s*\(M\)|\s*\(R\)|\s*\(P\)', '', text).strip()
    
    def detect_footnote(self, values: List) -> Optional[List[Dict]]:
        """
        Remove links das células e retorna as referências encontradas.
        
        Returns:
            Lista de dicts com: {'href': número, 'index': posição_coluna, 'clean_td': célula_limpa}
        """
        links: List[Dict] = []

        for idx, td in enumerate(values):
            anchors = td.find_all("a", href=True)
            if anchors:
                for a in anchors:
                    href_text = a.get_text(strip=True)
                    if re.match(r'^\d+$', href_text):
                        # preserve a copy of the cell without the anchor
                        a.decompose()
                        links.append({
                            "href": href_text,
                            "index": idx,
                            "clean_td": td
                        })

        return links if links else None
    
    def extract_age_ranges(self, text: str) -> Tuple[Optional[int], Optional[int]]:
        """
        Extrai idade mínima e máxima do texto.
        
        Examples:
            "14 and 15" -> (14, 15)
            "16 and 17" -> (16, 17)
            "18 in mines" -> (18, 18)
        """
        if not text or text.strip() in ['', 'No provision']:
            return None, None
        
        # Extrair todos os números
        ages = [int(n) for n in re.findall(r'\b(\d{2})\b', text)]
        
        if not ages:
            return None, None
        
        return min(ages), max(ages)
    
    def parse_state_row(self, state_row) -> Optional[Tuple[Dict, Dict]]:
        """
        Extrai informações estruturadas de uma linha da tabela.
        
        Returns:
            Tupla com (employment_dict, age_dict) ou None se inválido
        """
        try:
            # Extrair jurisdiction
            th = state_row.find('th')
            if not th:
                return None
            
            strong = th.find('strong')
            if not strong:
                return None
            
            jurisdiction = strong.get_text(strip=True)
            
            # Extrair células
            values = state_row.find_all("td")
            
            if len(values) < 6:
                return None
            
            # Detectar e processar footnotes
            footnote_refs = self.detect_footnote(values)
            
            if footnote_refs:
                for ref in footnote_refs:
                    values[ref["index"]] = ref["clean_td"]
            
            # Extrair textos limpos
            v = [self.extract_text(td) for td in values]
            
            # Extrair faixas etárias (das últimas colunas, se houver)
            age_min, age_max = None, None
            if len(values) > 6:
                age_text = ' '.join([self.extract_text(td) for td in values[6:]])
                age_min, age_max = self.extract_age_ranges(age_text)
            
            # Footnotes por tipo
            employment_footnotes = []
            age_footnotes = []
            
            if footnote_refs:
                for ref in footnote_refs:
                    if ref['index'] <= 2:  # Employment columns (0, 1, 2)
                        employment_footnotes.append(ref['href'])
                    elif ref['index'] >= 3:  # Age columns (3, 4, 5)
                        age_footnotes.append(ref['href'])
            
            # Criar registro de Employment Certificate
            employment = {
                "state": jurisdiction,
                "year": self.year,
                "certificate_type": "Employment",
                "rule_description": self.remove_requirement_marks(v[0]) if v[0] else None,
                "is_issued_by_labor": 1 if "X" in v[1] else 0,
                "is_issued_by_school": 1 if "X" in v[2] else 0,
                "requirement_level": self.detect_requirement_level(v[0]) if v[0] else None,
                "age_min": age_min,
                "age_max": age_max,
                "notes": f"Labor: {v[1].replace('X', '').strip() or 'Não'}; School: {v[2].replace('X', '').strip() or 'Não'}",
                "footnotes": employment_footnotes if employment_footnotes else None
            }
            
            # Criar registro de Age Certificate
            age = {
                "state": jurisdiction,
                "year": self.year,
                "certificate_type": "Age",
                "rule_description": self.remove_requirement_marks(v[3]) if v[3] else None,
                "is_issued_by_labor": 1 if "X" in v[4] else 0,
                "is_issued_by_school": 1 if "X" in v[5] else 0,
                "requirement_level": self.detect_requirement_level(v[3]) if v[3] else None,
                "age_min": age_min,
                "age_max": age_max,
                "notes": f"Labor: {v[4].replace('X', '').strip() or 'Não'}; School: {v[5].replace('X', '').strip() or 'Não'}",
                "footnotes": age_footnotes if age_footnotes else None
            }
            
            return employment, age

        except Exception as e:
            logger.warning("Error parsing state row: %s", e)
            return None
    
    def attach_footnote_texts(self, data: List[Dict]) -> List[Dict]:
        """Anexa textos completos dos footnotes aos registros (mantém coluna separada)"""
        for record in data:
            footnote_refs = record.get('footnotes')

            if footnote_refs:
                footnote_texts = []
                for ref in footnote_refs:
                    if str(ref) in self.footnotes_dict:
                        footnote_texts.append(f"[{ref}] {self.footnotes_dict[str(ref)]}")

                record['footnote_text'] = ' | '.join(footnote_texts) if footnote_texts else None
            else:
                record['footnote_text'] = None

        return data
    
    def scrape(self) -> pd.DataFrame:
        """Executa o scraping completo"""
        logger.info("Starting Youth Employment Rules scraping: %s", self.url)

        if not self.fetch_page():
            return pd.DataFrame()

        # Extract year (optional)
        self.extract_year()
        logger.debug("Detected year: %s", self.year)

        # Extract footnotes
        logger.debug("Extracting footnotes...")
        try:
            self.extract_footnotes()
            logger.debug("Found %d footnotes", len(self.footnotes_dict))
        except Exception as e:
            logger.warning("Footnote extraction failed: %s", e)

        # Locate the main table
        table = self.soup.find("table")
        if not table:
            logger.warning("No table found on youth rules page")
            return pd.DataFrame()

        # Skip header rows if present (robustly find data rows)
        all_rows = table.find_all("tr")
        data_rows = all_rows[4:] if len(all_rows) > 6 else all_rows[1:]
        logger.info("Processing %d state rows", len(data_rows))

        youth_employment: List[Dict] = []
        for row in data_rows:
            parsed = self.parse_state_row(row)
            if parsed:
                youth_employment.extend(parsed)

        logger.info("Extracted %d youth employment records", len(youth_employment))

        # Attach footnote texts
        youth_employment = self.attach_footnote_texts(youth_employment)

        df = pd.DataFrame(youth_employment)
        logger.info("Scraping completed: %d records", len(df))
        return df


def main():
    """Função principal"""
    scraper = YouthEmploymentScraperImproved()
    df = scraper.scrape()
    
    if not df.empty:
        print("\n📋 Preview (primeiras 10 linhas):")
        print(df.head(10).to_string())
        
        print(f"\n📊 Estatísticas:")
        print(f"   Total: {len(df)} registros")
        print(f"   Estados únicos: {df['state'].nunique()}")
        
        print(f"\n📄 Tipos de certificado:")
        print(df['certificate_type'].value_counts().to_string())
        
        print(f"\n📍 Requisitos (requirement_level):")
        print(df['requirement_level'].value_counts(dropna=False).to_string())
        
        print(f"\n🏢 Quem emite:")
        print(f"   Labor: {df['is_issued_by_labor'].sum()} registros")
        print(f"   School: {df['is_issued_by_school'].sum()} registros")
    
    return df


if __name__ == "__main__":
    main()