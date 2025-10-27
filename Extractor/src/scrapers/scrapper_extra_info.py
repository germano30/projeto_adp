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
    
    def get_footnotes(self,url:str, div: str = 'content'):
        response = requests.get(url)
        soup = BeautifulSoup(response.content, 'html.parser')
        content = soup.find('div',id = div)
        elements = list(content.children)
        start_index = None
        for i, el in enumerate(elements):
            if hasattr(el, 'get_text') and 'FOOTNOTE' in el.get_text().upper():
                start_index = i
                break
        footnote_elements = elements[start_index + 1:]
        return footnote_elements
        

    def extract_paid_rest_period(self):
        table = self.get_table(url = 'https://www.dol.gov/agencies/whd/state/rest-periods')[0]
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
        table.columns = [re.sub('[0-9/:]','',column).strip() for column in table.columns]
        table['State'] = table['State'].apply(lambda x: re.sub('[0-9]','',x))
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
                "text": doc_text.strip()
            })

ExtraInfoScrapper().extract_paid_rest_period()