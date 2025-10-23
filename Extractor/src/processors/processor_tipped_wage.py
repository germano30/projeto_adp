"""
Processador de dados de tipped minimum wage
"""
import pandas as pd
import re
import sys
import ast
sys.path.append('..')
from utils import is_monetary_value, is_percentage, extract_multiple_values, append_note, consolidate_notes_simple 
import warnings
warnings.filterwarnings('ignore')
class TippedWageProcessor:
    """Classe para processar dados de tipped minimum wage"""
    
    def __init__(self, df: pd.DataFrame, footnotes_dict = {}):
        self.df = df.copy()
        self.footnotes_dict = footnotes_dict

    def move_text_to_notes(self, column_name: str, value, row):
        """Move texto descritivo para notes"""
        if pd.isna(value) or not isinstance(value, str):
            return value, row
        
        # Se não é valor monetário nem porcentagem, é texto descritivo
        if not is_monetary_value(value) and not is_percentage(value):
            note_text = f"[{column_name}] {value}"
            row['notes'] = append_note(row.get('notes'), note_text)
            return None, row
        
        return value, row
                        
    def process_tip_wages(self, row):
        """Processa valores de salário tipped"""
        for col in ['combinedrate', 'tipcredit', 'cashwage']:
            if col not in row:
                continue
            
            value = row[col]
            
            if pd.isna(value) or value == 'Missing value':
                continue
            
            # 1. Verificar se tem múltiplos valores
            multiple_values = extract_multiple_values(value)
            
            if multiple_values:
                first_value = multiple_values[0]
                if not first_value.startswith('$'):
                    first_value = f'${first_value}'
                
                row[col] = first_value
                
                other_values = ', '.join(multiple_values[1:])
                note_text = f"[{col}] Alternative rate(s): {other_values}"
                row['notes'] = append_note(row.get('notes'), note_text)
            
            # 2. Se não é valor monetário nem porcentagem, mover para notes
            else:
                value, row = self.move_text_to_notes(col, value, row)
                row[col] = value
        
        return row
    
    def convert_with_context(self, value, column_name: str, row):
        """Converte valores para float mantendo contexto"""
        if pd.isna(value):
            return None, None, row
        
        if not isinstance(value, str):
            return float(value) if isinstance(value, (int, float)) else None, 'exact', row
        
        original = value.strip()
        value = original.replace('$', '')
        
        if value.lower() in ['not specified', 'missing value', '']:
            return None, None, row
        
        # Porcentagem
        if '%' in value:
            match = re.search(r'(\d+\.?\d*)\s*%', value)
            if match:
                note = f"[{column_name}] Original value: {original}"
                row['notes'] = append_note(row.get('notes'), note)
                return float(match.group(1)), 'percentage', row
        
        # Range (up to, more than, at least)
        range_patterns = {
            'up to': r'up to\s+(\d+\.?\d*)',
            'more than': r'more than\s+(\d+\.?\d*)',
            'at least': r'at least\s+(\d+\.?\d*)',
            'to': r'to\s+(\d+\.?\d*)'
        }
        
        for range_type, pattern in range_patterns.items():
            match = re.search(pattern, value, re.IGNORECASE)
            if match:
                note = f"[{column_name}] {range_type.capitalize()} {match.group(1)}"
                row['notes'] = append_note(row.get('notes'), note)
                return float(match.group(1)), 'range', row
        
        # Valor exato
        try:
            return float(value), 'exact', row
        except ValueError:
            return None, None, row
    
    def process_with_types(self, row):
        """Processa valores e adiciona tipo"""
        for col in ['combinedrate', 'tipcredit', 'cashwage']:
            if col in row:
                value, value_type, row = self.convert_with_context(row[col], col, row)
                row[col] = value
                row[f'{col}_type'] = value_type
        return row
    
    def process(self) -> pd.DataFrame:
        """Executa o pipeline completo de processamento"""
        
        df = self.df.copy()
        
        df = df.apply(self.process_tip_wages, axis=1)
        for col in ['combinedrate', 'tipcredit', 'cashwage']:
            if col in df.columns:
                df[col] = df[col].apply(lambda x: x.str.replace('$', '', regex=False) if hasattr(x, 'str') else x)
        df = df.apply(self.process_with_types, axis=1)
        df['notes'] = df.apply(lambda row: consolidate_notes_simple(row['notes'], row['definition']), axis=1)
        df['id']=range(1, len(df) + 1)
        return df


def main():
    """Função principal para teste"""
    # Criar dados de exemplo
    objects = TippedWageScraper() 
    df = objects.scrape()

    processor = TippedWageProcessor(df, footnote_dict=objects.footnotes_dict)
    df_processed = processor.process()

    return df_processed


if __name__ == "__main__":
    main()