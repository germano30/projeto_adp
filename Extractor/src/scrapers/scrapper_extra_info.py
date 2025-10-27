import requests
import pandas as pd
import re
from bs4 import BeautifulSoup
from typing import Dict, List, Tuple
import sys
sys.path.append('..')

class ExtraInfoScrapper:
    def __init__(self):
        pass

    def get_table(self, url : str):
        tables = pd.read_html(url)
        return tables
    
    def get_footnotes(self, url: str, div: str = "content"):
        response = requests.get(url)
        soup = BeautifulSoup(response.content, "html.parser")
        content = soup.find("div", id=div)
        if not content:
            return []

        table = content.find("table")

        if table:
            footnote_elements = []
            for sibling in table.find_next_siblings():
                # Para evitar ruído (espaços em branco, '\n', etc.)
                if getattr(sibling, "name", None):
                    footnote_elements.append(sibling)
            return footnote_elements

        elements = list(content.children)
        start_index = None
        for i, el in enumerate(elements):
            if hasattr(el, "get_text") and "FOOTNOTE" in el.get_text().upper():
                start_index = i
                break

        if start_index is not None:
            return elements[start_index + 1 :]
        else:
            return []
        

    def extract_paid_rest_period(self):
        table = self.get_table(url='https://www.dol.gov/agencies/whd/state/rest-periods')[0]
        footnote_elements = self.get_footnotes('https://www.dol.gov/agencies/whd/state/rest-periods')
        def process_footnote():
            footnotes = []
            for el in footnote_elements:
                if el.name == 'p':
                    a_tag = el.find('a', attrs={'name': True})
                    if a_tag:
                        footnotes.append({
                            'footnote_id': a_tag['name'][-1],
                            'footnote_text': el.get_text(strip=True)[1:]
                        })
            return footnotes

        footnote_dict = process_footnote()
        
        table.columns = [re.sub('[0-9/:]', '', column).strip() for column in table.columns]
        table['State'] = table['State'].apply(lambda x: re.sub('[0-9]', '', x))

        documents = []
        for _, row in table.iterrows():
            state = row["State"]
            doc_text = f"""
            In {state}, adult employees in the private sector are entitled to {row["Basic Standard"]}.
            This regulation is established by {row["Prescribed By"]} and applies to {row["Coverage"]}.
            Additional notes: {row["Comments"]}.
            """
            documents.append({
                "state": state,
                "text": doc_text.strip(),
                "type": "rest_period",
                "topic": "Minimum Paid Rest Period Requirements Under State Law for Adult Employees in Private Sector"
            })

        # Cria documentos separados para os footnotes
        footnote_docs = [
            {
                "text": f"Footnote ({fn['footnote_id']}): {fn['footnote_text']}",
                "type": "footnote",
                "topic": "Minimum Paid Rest Period Requirements Under State Law for Adult Employees in Private Sector",
                "id": fn["footnote_id"]
            }
            for fn in footnote_dict
        ]
        
        return documents, footnote_docs

    
    def extract_meal_breaks(self):
        table = self.get_table(url = 'https://www.dol.gov/agencies/whd/state/meal-breaks')[0] 
        footnote_elements = self.get_footnotes('https://www.dol.gov/agencies/whd/state/meal-breaks')
        def process_footnote():
            footnotes = []
            for el in footnote_elements:
                if el.name == 'p': 
                    a_tag = el.find('a', attrs={'name': True})
                    if a_tag:
                        footnotes.append({
                            'footnote_id': a_tag['name'][-1],
                            'footnote_text': el.get_text(strip=True)[2:]
                        })
            return footnotes
        footnote_dict = process_footnote()
        table.columns = [re.sub('[0-9/:]','',column).strip() for column in table.columns]
        table['State'] = table['State'].apply(lambda x: re.sub('[0-9]','',x))
        state_docs = []
        for _, row in table.iterrows():
            state_text = f"""
            In {row["Jurisdiction"]}, meal periods for adult employees in the private sector are regulated by state law.
            The standard is: {row["Basic Standard"]}.
            Prescribed by: {row["Prescribed By"]}.
            Coverage: {row["Coverage"]}.
            Comments: {row["Comments"]}.
            """

            state_docs.append({
                "text": state_text.strip(),
                "metadata": {
                    "type": "meal_period",
                    "jurisdiction": row["Jurisdiction"],
                    "topic": "Meal Period Requirements Under State Law"
                }
            })
            
        footnote_docs = []
        for fn in footnote_dict:
            footnote_docs.append({
                "text": f"Footnote ({fn['footnote_id']}): {fn['footnote_text']}",
                "metadata": {
                    "type": "footnote",
                    "topic": "Meal Period Requirements Under State Law",
                    "footnote_id": fn["footnote_id"]
                }
            })
            
        return state_docs, footnote_docs
    
    def extract_dollar_threshold(self):
        table = self.get_table(url = 'https://www.dol.gov/agencies/whd/state/prevailing-wages')[0] 
        footnote_elements = self.get_footnotes('https://www.dol.gov/agencies/whd/state/prevailing-wages')

        def process_footnote():
            footnotes = []
            for el in footnote_elements:
                if el.name == 'p': 
                    a_tag = el.find('a', attrs={'name': True})
                    if a_tag:
                        footnotes.append({
                            'footnote_id': a_tag['name'][-1],
                            'footnote_text': el.get_text(strip=True)[2:]
                        })
            return footnotes
        footnote_dict = process_footnote()
        table.columns = [re.sub('[0-9/:]','',column).strip().capitalize() for column in table.columns]
        table['State'] = table['State'].apply(lambda x: re.sub('[0-9]','',x))
        documents = []
        for _, row in table.iterrows():
            state = row["State"]
            threshold = row["Threshold amount"]
            doc_text = f"""
            In {state}, the dollar threshold amount for contract coverage under state prevailing wage laws is {threshold}.
            This indicates the minimum contract value at which prevailing wage requirements apply to public works or government-funded projects in {state}.
            """

            documents.append({
                "state": state,
                "text": doc_text.strip(),
                "type": "prevailing_wage",
                "topic": "Dollar Threshold Amount for Contract Coverage Under State Prevailing Wage Laws"
            })

        # Cria documentos para os footnotes
        footnote_docs = [
            {
                "text": f"Footnote ({fn['footnote_id']}): {fn['footnote_text']}",
                "type": "footnote",
                "topic": "Dollar Threshold Amount for Contract Coverage Under State Prevailing Wage Laws",
                "id": fn["footnote_id"]
            }
            for fn in footnote_dict
        ]
        return documents, footnote_docs
    
    def extract_payday_requirement(self):
        table = self.get_table(url = 'https://www.dol.gov/agencies/whd/state/payday')[0] 
        footnote_elements = self.get_footnotes('https://www.dol.gov/agencies/whd/state/payday')
        def process_footnote():
                    footnotes = []
                    for el in footnote_elements:
                        if el.name == 'p': 
                            a_tag = el.find('a', attrs={'name': True})
                            if a_tag:
                                footnotes.append({
                                    'footnote_id': a_tag['name'][-1],
                                    'footnote_text': el.get_text(strip=True)[1:]
                                })
                    return footnotes
        footnote_dict = process_footnote()
        table.columns = [re.sub('[0-9/:]','',column).strip().capitalize() for column in table.columns]
        table['State'] = table['State'].apply(lambda x: re.sub('[0-9]','',x))
        documents = []
        for _, row in table.iterrows():
            state = row["State"]

            doc_text = f"""
            In {state}, the pay frequency requirements for private sector employers are as follows:
            - Weekly: {row.get('Weekly', 'N/A')}
            - Bi-weekly: {row.get('Bi-weekly', 'N/A')}
            - Semi-monthly: {row.get('Semi-monthly', 'N/A')}
            - Monthly: {row.get('Monthly', 'N/A')}.
            """

            documents.append({
                "state": state,
                "text": doc_text.strip(),
                "type": "pay_frequency",
                "topic": "Pay Frequency Requirements by State"
            })

        footnote_docs = [
            {
                "text": f"Footnote ({fn['footnote_id']}): {fn['footnote_text']}",
                "type": "footnote",
                "topic": "Pay Frequency Requirements by State",
                "id": fn["footnote_id"]
            }
            for fn in footnote_dict
        ]
        return documents, footnote_docs
    
    def extract_child_non_farm(self):
        table = self.get_table(url = 'https://www.dol.gov/agencies/whd/state/child-labor')[0] 
        footnote_elements = self.get_footnotes('https://www.dol.gov/agencies/whd/state/child-labor')
        print(table.iloc[0].values)
        print(footnote_elements)
ExtraInfoScrapper().extract_child_non_farm()