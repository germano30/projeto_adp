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
        
        elements = list(content.children)
        start_index = None
        for i, el in enumerate(elements):
            if hasattr(el, "get_text") and "FOOTNOTE" in el.get_text().upper():
                start_index = i
                break

        if start_index is not None:
            return elements[start_index + 1 :]
        
        tables = content.find_all("table")
        if tables:
            footnote_elements = []
            for table in tables:
                if table.find_next_siblings():
                    for sibling in table.find_next_siblings():
                        if getattr(sibling, "name", None):
                            footnote_elements.append(sibling)
            return footnote_elements
        
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
        table['Jurisdiction'] = table['Jurisdiction'].apply(lambda x: re.sub(r'[0-9]', '', str(x)) if pd.notna(x) else '')
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
                                    'footnote_id': a_tag['name'],
                                    'footnote_text': el.get_text(strip=True)[1:] if not a_tag['name'][-2].isnumeric() else el.get_text(strip=True)[2:]
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
        response = requests.get('https://www.dol.gov/agencies/whd/state/child-labor')
        soup = BeautifulSoup(response.content, "html.parser")
        
        table.columns = ['State','max_daily_weekly_under16','max_daily_weekly_16_17','nightwork_under16','nightwork_16_17']
        
        def process_footnote():
            footnotes = []
            # Procura por paragrafos com IDs que começam com "foot"
            for p in soup.find_all('p', id=lambda x: x and x.startswith('foot')):
                a_tag = p.find('a', attrs={'name': True})
                if a_tag and 'name' in a_tag.attrs:
                    footnote_id = a_tag['name'][-1]
                    full_text = p.get_text(strip=True)
                    if full_text[0].lower() in ['a', 'b', 'c']:
                        footnote_text = full_text[1:].strip()
                    else:
                        footnote_text = full_text
                    
                    footnotes.append({
                        'footnote_id': footnote_id,
                        'footnote_text': footnote_text
                    })
            return footnotes
        footnote_dict = process_footnote()

        documents = []
        for _, row in table.iterrows():
            state = row["State"]
            doc_text = f"""
            In {state}, child labor laws define the following limitations for minors in Non-farm Employment:
            - Maximum daily and weekly hours (Under 16): {row['max_daily_weekly_under16']}
            - Maximum daily and weekly hours (Ages 16 and 17): {row['max_daily_weekly_16_17']}
            - Nightwork prohibited (Under 16): {row['nightwork_under16']}
            - Nightwork prohibited (Ages 16 and 17): {row['nightwork_16_17']}.
            """

            documents.append({
                "state": state,
                "text": doc_text.strip(),
                "type": "child_labor",
                "topic": "Child Labor State Laws and Restrictions for Minors"
            })

        footnote_docs = [
            {
                "text": f"Footnote ({fn['footnote_id']}): {fn['footnote_text']}",
                "type": "footnote",
                "topic": "Child Labor State Laws and Restrictions for Minors",
                "id": fn["footnote_id"]
            }
            for fn in footnote_dict
        ]
        return documents, footnote_docs
    
    def extract_child_farm(self):
        table = self.get_table(url = 'https://www.dol.gov/agencies/whd/state/child-labor/agriculture')
        min_age_max_hour_req = table[0]
        min_age_max_hour_req.columns = ['State','min_age_during_school','min_age_outside_school','employment_required_certificate','age_required_certificate','max_daily_week_for_under_16','max_day_per_week_for_under_16']
        dangerous = table[1]
        dangerous.column = ['State','prohibited_for_under_16_unless_other_age','dangerous_occupation_prohibited_age']
        footnote_elements = self.get_footnotes('https://www.dol.gov/agencies/whd/state/child-labor/agriculture')
        documents = []
        
        response = requests.get('https://www.dol.gov/agencies/whd/state/child-labor')
        soup = BeautifulSoup(response.content, "html.parser")
        
        def process_footnote():
            footnotes = []
            for p in soup.find_all('p', id=lambda x: x and x.startswith('foot')):
                a_tag = p.find('a', attrs={'name': True})
                if a_tag and 'name' in a_tag.attrs:
                    footnote_id = a_tag['name'][-1]
                    full_text = p.get_text(strip=True)
                    if full_text[0].lower() in ['a', 'b', 'c']:
                        footnote_text = full_text[1:].strip()
                    else:
                        footnote_text = full_text
                    
                    footnotes.append({
                        'footnote_id': footnote_id,
                        'footnote_text': footnote_text
                    })
            for el in footnote_elements:
                        if el.name == 'p': 
                            a_tag = el.find('a', attrs={'name': True})
                            if a_tag:
                                footnotes.append({
                                    'footnote_id': a_tag['name'],
                                    'footnote_text': el.get_text(strip=True)[1:] if not a_tag['name'][-2].isnumeric() else el.get_text(strip=True)[2:]
                                })
            return footnotes
        
        footnote_dict = process_footnote()

        for _, row in min_age_max_hour_req.iterrows():
            state = row["State"]

            doc_text = f"""
            In {state}, child labor laws applicable to agricultural employment establish the following conditions:
            - Minimum age during school hours: {row['min_age_during_school']}
            - Minimum age outside school hours: {row['min_age_outside_school']}
            - Certificate required for employment: {row['employment_required_certificate']}
            - Certificate required for age: {row['age_required_certificate']}
            - Maximum daily/weekly hours for minors under 16: {row['max_daily_week_for_under_16']}
            - Maximum days per week for minors under 16: {row['max_day_per_week_for_under_16']}.
            """

            documents.append({
                "state": state,
                "text": doc_text.strip(),
                "type": "child_labor_agriculture",
                "topic": "State Child Labor Laws Applicable to Agricultural Employment"
            })
        
        for _, row in dangerous.iterrows():
            state = row["State"]

            doc_text = f"""
            In {state}, additional child labor restrictions apply to agricultural work involving hazardous occupations:
            - Agricultural work prohibited for minors under 16 unless specific conditions apply: {row['prohibited_for_under_16_unless_other_age']}.
            - Minimum age for engagement in hazardous agricultural occupations: {row['dangerous_occupation_prohibited_age']}.
            
            These provisions ensure that minors are protected from hazardous agricultural tasks and environments, in accordance with state labor laws.
            """

            documents.append({
                "state": state,
                "text": doc_text.strip(),
                "type": "child_labor_agriculture_hazardous",
                "topic": "Hazardous Agricultural Occupations Restrictions for Minors Under State Law"
            })
            
        footnote_docs = [
            {
                "text": f"Footnote ({fn['footnote_id']}): {fn['footnote_text']}",
                "type": "footnote",
                "topic": "State Child Labor Laws Applicable to Agricultural Employment",
                "id": fn["footnote_id"]
            }
            for fn in footnote_dict
        ]
        return documents, footnote_docs
    
    def extract_child_entertaniment(self):
        table = self.get_table(url = 'https://www.dol.gov/agencies/whd/state/child-labor/entertainment')[0]
        documents = []
        for _, row in table.iterrows():
            state = row["STATE"]
            regulates = row["REGULATES CHILD ENTERTAINMENT"]
            work_permit = row["WORK PERMIT"]
            law_comment = row["LAW/COMMENT"]
            doc_text = f"""
            In {state}, child entertainment employment is subject to state law as follows:
            - Regulates child entertainment: {regulates}.
            - Work permit required: {work_permit}.
            - Additional comments / legal reference: {law_comment}.
            
            These provisions reflect {state}’s approach to permitting minors to work as actors, models or performers in the entertainment industry, and the accompanying safeguards regarding schooling, welfare and safety.
            """
            documents.append({
                "state": state,
                "text": doc_text.strip(),
                "type": "child_entertainment",
                "topic": "State Child Entertainment Employment Laws for Minors"
            })
        return documents, []
ExtraInfoScrapper().extract_child_entertaniment()