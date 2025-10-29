import requests
import pandas as pd
import re
from bs4 import BeautifulSoup
from typing import Dict, List, Tuple


class ExtraInfoScraper:
    """Scraper for extracting additional labor law information from DOL website."""
    
    def __init__(self):
        self.base_url = "https://www.dol.gov/agencies/whd/state"

    def get_table(self, url: str) -> List[pd.DataFrame]:
        """
        Fetches and parses HTML tables from a given URL.
        
        Args:
            url: The URL containing HTML tables
            
        Returns:
            List of pandas DataFrames, one for each table found
        """
        tables = pd.read_html(url)
        return tables
    
    def get_footnotes(self, url: str, div: str = "content") -> List:
        """
        Extracts footnote elements from a webpage.
        
        Args:
            url: The URL to fetch footnotes from
            div: The div ID containing the content (default: "content")
            
        Returns:
            List of BeautifulSoup elements representing footnotes
        """
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
            return elements[start_index + 1:]
        
        tables = content.find_all("table")
        if tables:
            footnote_elements = []
            for table in tables:
                if table.find_next_siblings():
                    for sibling in table.find_next_siblings():
                        if getattr(sibling, "name", None):
                            footnote_elements.append(sibling)
            return footnote_elements
        
        return []

    def scrape_all(self) -> Dict[str, Tuple[List[Dict], List[Dict]]]:
        """
        Executes all scraping methods and returns a dictionary with results.
        
        Returns:
            Dictionary with keys as data types and values as tuples of (state_docs, footnote_docs)
        """
        print("\n Starting extraction of extra labor information...")
        
        results = {}
        
        try:
            print(" Extracting paid rest periods...")
            results['rest_periods'] = self.extract_paid_rest_period()
            
            print(" Extracting meal breaks...")
            results['meal_breaks'] = self.extract_meal_breaks()
            
            print(" Extracting dollar thresholds...")
            results['dollar_threshold'] = self.extract_dollar_threshold()
            
            print(" Extracting payday requirements...")
            results['payday'] = self.extract_payday_requirement()
            
            print(" Extracting child labor (non-farm)...")
            results['child_labor_non_farm'] = self.extract_child_non_farm()
            
            print(" Extracting child labor (farm)...")
            results['child_labor_farm'] = self.extract_child_farm()
            
            print(" Extracting child entertainment laws...")
            results['child_entertainment'] = self.extract_child_entertainment()
            
            print(" Extracting door-to-door sales regulations...")
            results['door_to_door_sales'] = self.extract_door_to_door_sales()
            
            print("✅ Extraction completed successfully!")
            
        except Exception as e:
            print(f"❌ Error during extraction: {e}")
            raise
        
        return results

    def extract_paid_rest_period(self) -> Tuple[List[Dict], List[Dict]]:
        """Extracts paid rest period requirements by state."""
        url = f'{self.base_url}/rest-periods'
        table = self.get_table(url=url)[0]
        footnote_elements = self.get_footnotes(url)
        
        def process_footnote():
            footnotes = []
            for el in footnote_elements:
                if el.name == 'p':
                    a_tag = el.find('a', attrs={'name': True})
                    if a_tag:
                        footnotes.append({
                            'footnote_id': a_tag['name'][-1],
                            'footnote_text': el.get_text(strip=True)[1:],
                            'site_url': url
                        })
            return footnotes

        footnote_dict = process_footnote()
        
        table.columns = [re.sub('[0-9/:]', '', column).strip() for column in table.columns]
        table['State'] = table['State'].apply(lambda x: re.sub('[0-9]', '', x))

        state_docs = []
        for _, row in table.iterrows():
            state = row["State"]
            doc_text = f"""
In {state}, adult employees in the private sector are entitled to {row["Basic Standard"]}.
This regulation is established by {row["Prescribed By"]} and applies to {row["Coverage"]}.
Additional notes: {row["Comments"]}.
            """
            state_docs.append({
                "state": state,
                "text": doc_text.strip(),
                "type": "rest_period",
                "topic": "Minimum Paid Rest Period Requirements Under State Law for Adult Employees in Private Sector",
                "site_url": url
            })

        footnote_docs = [
            {
                "text": f"Footnote ({fn['footnote_id']}): {fn['footnote_text']}",
                "type": "footnote",
                "topic": "Minimum Paid Rest Period Requirements Under State Law for Adult Employees in Private Sector",
                "id": fn["footnote_id"],
                "site_url": fn["site_url"]
            }
            for fn in footnote_dict
        ]
        
        return state_docs, footnote_docs

    def extract_meal_breaks(self) -> Tuple[List[Dict], List[Dict]]:
        """Extracts meal break requirements by state."""
        url = f'{self.base_url}/meal-breaks'
        table = self.get_table(url=url)[0]
        footnote_elements = self.get_footnotes(url)
        
        def process_footnote():
            footnotes = []
            for el in footnote_elements:
                if el.name == 'p':
                    a_tag = el.find('a', attrs={'name': True})
                    if a_tag:
                        footnotes.append({
                            'footnote_id': a_tag['name'][-1],
                            'footnote_text': el.get_text(strip=True)[2:],
                            'site_url': url
                        })
            return footnotes
        
        footnote_dict = process_footnote()
        
        table.columns = [re.sub('[0-9/:]', '', column).strip() for column in table.columns]
        table['Jurisdiction'] = table['Jurisdiction'].apply(
            lambda x: re.sub(r'[0-9]', '', str(x)) if pd.notna(x) else ''
        )
        
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
                "state": row["Jurisdiction"],
                "text": state_text.strip(),
                "type": "meal_period",
                "topic": "Meal Period Requirements Under State Law",
                "site_url": url
            })
            
        footnote_docs = [
            {
                "text": f"Footnote ({fn['footnote_id']}): {fn['footnote_text']}",
                "type": "footnote",
                "topic": "Meal Period Requirements Under State Law",
                "id": fn["footnote_id"],
                "site_url": fn["site_url"]
            }
            for fn in footnote_dict
        ]
        
        return state_docs, footnote_docs
    
    def extract_dollar_threshold(self) -> Tuple[List[Dict], List[Dict]]:
        """Extracts dollar threshold amounts for prevailing wage laws by state."""
        url = f'{self.base_url}/prevailing-wages'
        table = self.get_table(url=url)[0]
        footnote_elements = self.get_footnotes(url)

        def process_footnote():
            footnotes = []
            for el in footnote_elements:
                if el.name == 'p':
                    a_tag = el.find('a', attrs={'name': True})
                    if a_tag:
                        footnotes.append({
                            'footnote_id': a_tag['name'][-1],
                            'footnote_text': el.get_text(strip=True)[2:],
                            'site_url': url
                        })
            return footnotes
        
        footnote_dict = process_footnote()
        
        table.columns = [re.sub('[0-9/:]', '', column).strip().capitalize() for column in table.columns]
        table['State'] = table['State'].apply(lambda x: re.sub('[0-9]', '', x))
        
        state_docs = []
        for _, row in table.iterrows():
            state = row["State"]
            threshold = row["Threshold amount"]
            doc_text = f"""
In {state}, the dollar threshold amount for contract coverage under state prevailing wage laws is {threshold}.
This indicates the minimum contract value at which prevailing wage requirements apply to public works or government-funded projects in {state}.
            """
            state_docs.append({
                "state": state,
                "text": doc_text.strip(),
                "type": "prevailing_wage",
                "topic": "Dollar Threshold Amount for Contract Coverage Under State Prevailing Wage Laws",
                "site_url": url
            })

        footnote_docs = [
            {
                "text": f"Footnote ({fn['footnote_id']}): {fn['footnote_text']}",
                "type": "footnote",
                "topic": "Dollar Threshold Amount for Contract Coverage Under State Prevailing Wage Laws",
                "id": fn["footnote_id"],
                "site_url": fn["site_url"]
            }
            for fn in footnote_dict
        ]
        
        return state_docs, footnote_docs
    
    def extract_payday_requirement(self) -> Tuple[List[Dict], List[Dict]]:
        """Extracts payday frequency requirements by state."""
        url = f'{self.base_url}/payday'
        table = self.get_table(url=url)[0]
        footnote_elements = self.get_footnotes(url)
        
        def process_footnote():
            footnotes = []
            for el in footnote_elements:
                if el.name == 'p':
                    a_tag = el.find('a', attrs={'name': True})
                    if a_tag:
                        footnotes.append({
                            'footnote_id': a_tag['name'],
                            'footnote_text': el.get_text(strip=True)[1:] if not a_tag['name'][-2].isnumeric() else el.get_text(strip=True)[2:],
                            'site_url': url 
                                
                        })
            return footnotes
        
        footnote_dict = process_footnote()
        
        table.columns = [re.sub('[0-9/:]', '', column).strip().capitalize() for column in table.columns]
        table['State'] = table['State'].apply(lambda x: re.sub('[0-9]', '', x))
        
        state_docs = []
        for _, row in table.iterrows():
            state = row["State"]
            doc_text = f"""
In {state}, the pay frequency requirements for private sector employers are as follows:
- Weekly: {row.get('Weekly', 'N/A')}
- Bi-weekly: {row.get('Bi-weekly', 'N/A')}
- Semi-monthly: {row.get('Semi-monthly', 'N/A')}
- Monthly: {row.get('Monthly', 'N/A')}.
            """
            state_docs.append({
                "state": state,
                "text": doc_text.strip(),
                "type": "pay_frequency",
                "topic": "Pay Frequency Requirements by State",
                "site_url": url
            })

        footnote_docs = [
            {
                "text": f"Footnote ({fn['footnote_id']}): {fn['footnote_text']}",
                "type": "footnote",
                "topic": "Pay Frequency Requirements by State",
                "id": fn["footnote_id"],

            }
            for fn in footnote_dict
        ]

        return state_docs, footnote_docs
    
    def extract_child_non_farm(self) -> Tuple[List[Dict], List[Dict]]:
        """Extracts child labor laws for non-farm employment by state."""
        url = f'{self.base_url}/child-labor'
        table = self.get_table(url=url)[0]
        response = requests.get(url)
        soup = BeautifulSoup(response.content, "html.parser")
        
        table.columns = [
            'State',
            'max_daily_weekly_under16',
            'max_daily_weekly_16_17',
            'nightwork_under16',
            'nightwork_16_17'
        ]
        
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
                        'footnote_text': footnote_text,
                        'site_url': url
                    })
            return footnotes
        
        footnote_dict = process_footnote()

        state_docs = []
        for _, row in table.iterrows():
            state = row["State"]
            doc_text = f"""
In {state}, child labor laws define the following limitations for minors in Non-farm Employment:
- Maximum daily and weekly hours (Under 16): {row['max_daily_weekly_under16']}
- Maximum daily and weekly hours (Ages 16 and 17): {row['max_daily_weekly_16_17']}
- Nightwork prohibited (Under 16): {row['nightwork_under16']}
- Nightwork prohibited (Ages 16 and 17): {row['nightwork_16_17']}.
            """
            state_docs.append({
                "state": state,
                "text": doc_text.strip(),
                "type": "child_labor",
                "topic": "Child Labor State Laws and Restrictions for Minors",
                "site_url": url
            })

        footnote_docs = [
            {
                "text": f"Footnote ({fn['footnote_id']}): {fn['footnote_text']}",
                "type": "footnote",
                "topic": "Child Labor State Laws and Restrictions for Minors",
                "id": fn["footnote_id"],
                "site_url": fn["site_url"]
            }
            for fn in footnote_dict
        ]
        
        return state_docs, footnote_docs
    
    def extract_child_farm(self) -> Tuple[List[Dict], List[Dict]]:
        """Extracts child labor laws for agricultural employment by state."""
        url = f'{self.base_url}/child-labor/agriculture'
        tables = self.get_table(url=url)
        
        min_age_max_hour_req = tables[0]
        min_age_max_hour_req.columns = [
            'State',
            'min_age_during_school',
            'min_age_outside_school',
            'employment_required_certificate',
            'age_required_certificate',
            'max_daily_week_for_under_16',
            'max_day_per_week_for_under_16'
        ]
        
        dangerous = tables[1]
        dangerous.columns = [
            'State',
            'prohibited_for_under_16_unless_other_age',
            'dangerous_occupation_prohibited_age'
        ]
        
        footnote_elements = self.get_footnotes(url)
        response = requests.get(url)
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
                        'footnote_text': footnote_text,
                        'site_url': url
                    })
            
            for el in footnote_elements:
                if el.name == 'p':
                    a_tag = el.find('a', attrs={'name': True})
                    if a_tag:
                        footnotes.append({
                            'footnote_id': a_tag['name'],
                            'footnote_text': el.get_text(strip=True)[1:] if not a_tag['name'][-2].isnumeric() else el.get_text(strip=True)[2:],
                            'site_url': url 
                                
                        })
            return footnotes
        
        footnote_dict = process_footnote()

        state_docs = []
        
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
            state_docs.append({
                "state": state,
                "text": doc_text.strip(),
                "type": "child_labor_agriculture",
                "topic": "State Child Labor Laws Applicable to Agricultural Employment",
                "site_url": url
            })
        
        for _, row in dangerous.iterrows():
            state = row["State"]
            doc_text = f"""
In {state}, additional child labor restrictions apply to agricultural work involving hazardous occupations:
- Agricultural work prohibited for minors under 16 unless specific conditions apply: {row['prohibited_for_under_16_unless_other_age']}.
- Minimum age for engagement in hazardous agricultural occupations: {row['dangerous_occupation_prohibited_age']}.

These provisions ensure that minors are protected from hazardous agricultural tasks and environments, in accordance with state labor laws.
            """
            state_docs.append({
                "state": state,
                "text": doc_text.strip(),
                "type": "child_labor_agriculture_hazardous",
                "topic": "Hazardous Agricultural Occupations Restrictions for Minors Under State Law",
                "site_url": url
            })
            
        footnote_docs = [
            {
                "text": f"Footnote ({fn['footnote_id']}): {fn['footnote_text']}",
                "type": "footnote",
                "topic": "State Child Labor Laws Applicable to Agricultural Employment",
                "id": fn["footnote_id"],
                "site_url": fn["site_url"]
            }
            for fn in footnote_dict
        ]
        
        return state_docs, footnote_docs
    
    def extract_child_entertainment(self) -> Tuple[List[Dict], List[Dict]]:
        """Extracts child entertainment employment laws by state."""
        url = f'{self.base_url}/child-labor/entertainment'
        table = self.get_table(url=url)[0]
        
        state_docs = []
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

These provisions reflect {state}'s approach to permitting minors to work as actors, models or performers in the entertainment industry, and the accompanying safeguards regarding schooling, welfare and safety.
            """
            state_docs.append({
                "state": state,
                "text": doc_text.strip(),
                "type": "child_entertainment",
                "topic": "State Child Entertainment Employment Laws for Minors",
                "site_url": url
            })
        
        return state_docs, []
    
    def extract_door_to_door_sales(self) -> Tuple[List[Dict], List[Dict]]:
        """Extracts door-to-door sales regulations for minors by state."""
        url = f'{self.base_url}/child-labor/door-to-door-sales'
        response = requests.get(url)
        soup = BeautifulSoup(response.content, "html.parser")
        
        content = soup.find("div", id="content")
        if not content:
            return [], []
        
        state_docs = []
        current_category = None
        elements = content.find_all(['p', 'ul'])
        
        for element in elements:
            if element.name == 'p':
                strong_tag = element.find('strong')
                if strong_tag:
                    category_text = strong_tag.get_text(strip=True)
                    current_category = category_text.rstrip(':')
            
            elif element.name == 'ul' and current_category:
                for li in element.find_all('li', recursive=False):
                    state_tag = li.find('strong')
                    if state_tag:
                        state = state_tag.get_text(strip=True)
                        full_text = li.get_text(strip=True)
                        
                        details = full_text.replace(state, '', 1).strip()
                        if details.startswith('-'):
                            details = details[1:].strip()
                        
                        doc_text = f"""
State: {state.strip()}
Category: {current_category}
Regulation Details: {details}
                        """
                        
                        state_docs.append({
                            "state": state,
                            "text": doc_text.strip(),
                            "type": "door_to_door_sales",
                            "category": current_category,
                            "topic": "State Regulation of For-profit Door-to-door Sales by Minors"
                        })
        
        return state_docs, []
