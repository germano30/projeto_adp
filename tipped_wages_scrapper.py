import requests
from bs4 import BeautifulSoup
import re
from datetime import datetime
from decimal import Decimal

URL = 'https://www.dol.gov/agencies/whd/state/minimum-wage/tipped'

def extract_footnotes(soup):
    """Extrai todas as footnotes da página"""
    footnotes = {}

    # Procura por seções de footnotes (normalmente em div, ol, ou ul)
    footnote_sections = soup.find_all(['ol', 'ul', 'div'], class_=re.compile('footnote|note', re.I))

    for section in footnote_sections:
        items = section.find_all('li')
        for item in items:
            text = item.get_text(strip=True)
            # Extrai marcador (ex: [1], (a), etc)
            marker_match = re.match(r'^[\[\(]?([a-zA-Z0-9]+)[\]\)]?[\s:\.]', text)
            if marker_match:
                marker = marker_match.group(1)
                footnote_text = text[marker_match.end():].strip()
                footnotes[marker] = footnote_text

    # Também procura por footnotes inline
    for sup in soup.find_all('sup'):
        marker = sup.get_text(strip=True)
        # Tenta encontrar o texto da footnote próxima
        parent = sup.find_parent(['p', 'div', 'td'])
        if parent:
            next_elem = sup.find_next_sibling(string=True)
            if next_elem and len(str(next_elem).strip()) > 10:
                footnotes[marker] = str(next_elem).strip()[:200]

    return footnotes

def clean_currency(value_str):
    """Limpa e converte string de moeda para Decimal"""
    if not value_str:
        return None
    # Remove tudo exceto dígitos e ponto decimal
    cleaned = re.sub(r'[^\d.]', '', str(value_str))
    if cleaned and cleaned != '.':
        try:
            value = Decimal(cleaned)
            # Validação: valores de tip credit geralmente não excedem $20
            # Se for muito alto, provavelmente é um erro de parsing
            if value > 100:
                return None
            return value
        except:
            return None
    return None

def extract_footnote_markers(text):
    """Extrai marcadores de footnote de um texto"""
    if not text:
        return []
    # Procura por padrões como [1], (a), *, etc
    markers = re.findall(r'[\[\(]([a-zA-Z0-9]+)[\]\)]|\*+', text)
    return markers

def parse_state_name(text):
    """Extrai o nome do estado de uma célula, removendo marcadores de footnote"""
    if not text:
        return None

    # Remove marcadores de footnote (números sobrescritos, colchetes, parênteses, etc)
    clean_text = re.sub(r'[\[\(][a-zA-Z0-9]+[\]\)]', '', text)  # Remove [1], (a), etc
    clean_text = re.sub(r'\*+', '', clean_text)  # Remove asteriscos
    clean_text = re.sub(r'\s+\d+\s*$', '', clean_text)  # Remove números no final
    clean_text = re.sub(r'^\d+\s+', '', clean_text)  # Remove números no início
    clean_text = clean_text.strip()

    # Remove texto após dois pontos (geralmente indica descrição adicional)
    if ':' in clean_text:
        clean_text = clean_text.split(':')[0].strip()

    # Padrões que indicam que NÃO é um nome de estado
    non_state_patterns = [
        r'^business',
        r'^employer',
        r'^hotel',
        r'^bartender',
        r'^occupation',
        r'^employee',
        r'^worker',
        r'gross annual sales',
        r'receipts',
        r'or more',
        r'or less',
        r'minimum wage varies'
    ]

    clean_lower = clean_text.lower()
    for pattern in non_state_patterns:
        if re.search(pattern, clean_lower):
            return None

    # Se o texto tem conteúdo significativo e não corresponde aos padrões de exclusão,
    # assume que é um nome de jurisdição
    if len(clean_text) > 2 and clean_text[0].isupper():
        return clean_text

    return None

def scrape_tipped_wages():
    """Extrai dados completos de salário mínimo para trabalhadores que recebem gorjetas"""

    response = requests.get(URL)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, 'html.parser')

    # Extrai footnotes
    footnotes = extract_footnotes(soup)

    # Encontra a tabela
    table = soup.find('table')
    if not table:
        raise ValueError('❌ Tabela não encontrada na página')

    # Extrai data de última atualização
    last_updated = datetime.now().strftime('%Y-%m-%d')
    update_text = soup.find(string=re.compile(r'Last updated|Updated on|Effective', re.I))
    if update_text:
        date_match = re.search(r'(\d{1,2}[/-]\d{1,2}[/-]\d{2,4}|\w+ \d{1,2},? \d{4})', str(update_text))
        if date_match:
            try:
                last_updated = datetime.strptime(date_match.group(1), '%m/%d/%Y').strftime('%Y-%m-%d')
            except:
                pass

    data = []
    current_state = None

    # Pula o header da tabela
    rows = table.find_all('tr')[1:]

    for row in rows:
        cols = row.find_all(['td', 'th'])

        if len(cols) < 4:
            continue

        # Extrai texto de cada coluna
        col_texts = [col.get_text(separator=' ', strip=True) for col in cols]

        # Coluna 0: Jurisdiction (State)
        jurisdiction_cell = cols[0]
        jurisdiction_text = col_texts[0]

        # Verifica se há tag <strong> na célula (indica estado principal)
        has_strong = jurisdiction_cell.find('strong') is not None

        # Verifica se há tag <em> (indica subcategoria)
        has_em = jurisdiction_cell.find('em') is not None

        # Lógica de detecção de estado:
        if has_strong:
            # É uma linha de estado principal - extrai apenas o texto dentro de <strong>
            strong_tag = jurisdiction_cell.find('strong')
            state_text = strong_tag.get_text(strip=True)
            state_name = parse_state_name(state_text)
            if state_name:
                current_state = state_name
        elif has_em or (not has_strong and jurisdiction_text and len(jurisdiction_text) > 2):
            # É uma linha de subcategoria/detalhamento - mantém o estado atual
            # (linhas com <em> ou linhas sem <strong> mas com conteúdo)
            pass

        # Se não há estado atual, pula esta linha
        if not current_state:
            continue

        # Extrai marcadores de footnote da primeira coluna
        footnote_markers = extract_footnote_markers(jurisdiction_text)

        # Coluna 1: Combined Minimum Wage (basic combined cash & tip)
        combined_wage_text = col_texts[1] if len(col_texts) > 1 else ''

        # Coluna 2: Maximum Tip Credit
        tip_credit_text = col_texts[2] if len(col_texts) > 2 else ''

        # Coluna 3: Minimum Cash Wage (base wage per hour)
        cash_wage_text = col_texts[3] if len(col_texts) > 3 else ''

        # Coluna 4: Definition of Tipped Employee
        definition_text = col_texts[4] if len(col_texts) > 4 else ''

        # Extrai valores monetários - mais cuidadoso para evitar pegar valores de definição
        def extract_wage_value(text):
            """Extrai valor monetário de salário (formato $X.XX)"""
            if not text:
                return None
            # Procura especificamente padrão de moeda $X.XX (com 2 casas decimais)
            # Isso evita pegar números da coluna de definição
            match = re.search(r'\$?\s*(\d{1,2}\.\d{2})\b', text)
            if match:
                try:
                    return Decimal(match.group(1))
                except:
                    return None
            return None

        # Limpa e converte valores
        base_wage = extract_wage_value(cash_wage_text)
        tip_credit = extract_wage_value(tip_credit_text)
        combined_wage = extract_wage_value(combined_wage_text)

        # Detecta se é uma linha de subcategoria (diferente profissão, idade, etc)
        is_subcategory = False
        notes = ""

        # Verifica se a linha contém informações sobre categoria específica
        category_keywords = [
            'hotel', 'restaurant', 'bartender', 'business', 'employer',
            'gross sales', 'annual sales', 'receipts', 'employees',
            'age', 'under', 'over', 'occupation'
        ]

        if any(keyword in jurisdiction_text.lower() for keyword in category_keywords):
            is_subcategory = True
            notes = jurisdiction_text

        # Se for subcategoria mas não temos valores, tenta extrair da definição
        if is_subcategory and not base_wage:
            # Às vezes o valor está na coluna de definição
            if definition_text:
                temp_wage = clean_currency(definition_text)
                if temp_wage:
                    base_wage = temp_wage

        # Monta o texto completo das footnotes
        footnote_text = ""
        if footnote_markers:
            footnote_text_parts = []
            for marker in footnote_markers:
                if marker in footnotes:
                    footnote_text_parts.append(f"[{marker}] {footnotes[marker]}")
            footnote_text = " | ".join(footnote_text_parts)

        # Effective date - tenta extrair da definição ou notas
        effective_date = None
        date_patterns = [
            r'effective\s+(\w+\s+\d{1,2},?\s+\d{4})',
            r'as of\s+(\w+\s+\d{1,2},?\s+\d{4})',
            r'(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})'
        ]

        combined_text = f"{jurisdiction_text} {definition_text}"
        for pattern in date_patterns:
            date_match = re.search(pattern, combined_text, re.I)
            if date_match:
                effective_date = date_match.group(1)
                break

        # Cria o registro
        record = {
            'state_name': current_state,
            'category_name': 'Tipped',
            'base_wage_per_hour': base_wage,
            'maximum_tip_credit': tip_credit,
            'combined_minimum_wage': combined_wage if combined_wage else (
                base_wage + tip_credit if base_wage and tip_credit else None
            ),
            'effective_date': effective_date,
            'footnote_marker': ', '.join(footnote_markers) if footnote_markers else None,
            'footnote_text': footnote_text if footnote_text else None,
            'source_url': URL,
            'last_updated': last_updated,
            'notes': notes if is_subcategory else None
        }

        # Só adiciona se tiver pelo menos o base wage
        if base_wage is not None:
            data.append(record)
            print(f"✅ {current_state}: ${base_wage}" + (f" ({notes[:50]}...)" if notes else ""))

    return data

def save_to_txt(data, filename='tipped_minimum_wage_data.txt'):
    """Salva os dados em formato de texto delimitado por pipe (|)"""

    with open(filename, 'w', encoding='utf-8') as f:
        # Escreve header
        headers = [
            'state_name',
            'category_name',
            'base_wage_per_hour',
            'maximum_tip_credit',
            'combined_minimum_wage',
            'effective_date',
            'footnote_marker',
            'footnote_text',
            'source_url',
            'last_updated',
            'notes'
        ]
        f.write('|'.join(headers) + '\n')

        # Escreve dados
        for record in data:
            row = []
            for header in headers:
                value = record.get(header)
                if value is None:
                    row.append('')
                elif isinstance(value, Decimal):
                    row.append(f"{value:.2f}")
                else:
                    # Escapa pipes no texto
                    row.append(str(value).replace('|', '\\|'))

            f.write('|'.join(row) + '\n')

    print(f"\n💾 Arquivo salvo: '{filename}'")

if __name__ == '__main__':
    print("🔍 Iniciando scraping de dados de salário mínimo para gorjetas...\n")

    data = scrape_tipped_wages()

    print(f"\n📈 RESUMO:")
    print(f"   Total de registros: {len(data)}")

    # Conta estados únicos
    unique_states = set(record['state_name'] for record in data)
    print(f"   Estados únicos: {len(unique_states)}")

    # Conta registros com múltiplas linhas
    from collections import Counter
    state_counts = Counter(record['state_name'] for record in data)
    multiple_rows = {state: count for state, count in state_counts.items() if count > 1}
    if multiple_rows:
        print(f"\n   Estados com múltiplas linhas:")
        for state, count in sorted(multiple_rows.items()):
            print(f"      - {state}: {count} registros")

    # Salva em arquivo txt
    save_to_txt(data)

    print("\n✅ Scraping concluído com sucesso!")
