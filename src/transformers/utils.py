"""
Funções utilitárias gerais
"""
import re
import hashlib
import pandas as pd
from typing import Optional, List, Tuple
# import psycopg2
from config import DATABASE_CONFIG, SQL_SCRIPTS
import os

def consolidate_notes_simple(notes: str, definition: str) -> Optional[str]:
    """
    Versão simples: concatena notes e definition
    
    Args:
        notes: Notas existentes
        definition: Definição/descrição
    
    Returns:
        String consolidada ou None
    """
    parts = []
    
    if pd.notna(notes) and str(notes).strip():
        parts.append(str(notes).strip())
    
    if pd.notna(definition) and str(definition).strip():
        parts.append(f"[DEFINITION] {str(definition).strip()}")
    
    return ' ; '.join(parts) if parts else None


def add_leading_zero(value: str) -> str:
    """Adiciona zero à frente de valores decimais que começam com ponto"""
    value = value.strip()
    if value.startswith('.'):
        return '0' + value
    return value


def generate_hash(text: str, length: int = 16) -> str:
    """Gera hash MD5 de um texto"""
    return hashlib.md5(text.encode('utf-8')).hexdigest()[:length]


def clean_numeric_value(value: str) -> Optional[str]:
    """Remove caracteres não numéricos, mantendo ponto decimal"""
    if pd.isna(value):
        return None
    value = str(value).replace('$', '').strip()
    # Extrair apenas dígitos e ponto
    match = re.search(r'(\d+\.?\d*)', value)
    return match.group(1) if match else None


def is_monetary_value(value) -> bool:
    """Verifica se é um valor monetário válido"""
    if pd.isna(value) or not isinstance(value, str):
        return False
    clean = value.strip().replace('$', '')
    return bool(re.match(r'^\d+\.?\d*$', clean))


def is_percentage(value) -> bool:
    """Verifica se é uma porcentagem"""
    if pd.isna(value) or not isinstance(value, str):
        return False
    return '%' in value


def extract_footnotes_from_notes(notes_text: str) -> List[Tuple[str, str]]:
    """
    Extrai footnotes individuais de uma string de notas
    Retorna: lista de (column_reference, footnote_text)
    """
    if pd.isna(notes_text) or not isinstance(notes_text, str):
        return []
    
    # Padrão: [column_name] texto ; [column_name] texto
    pattern = r'\[([^\]]+)\]\s*([^;]+)'
    matches = re.findall(pattern, notes_text)
    
    footnotes = []
    for column_ref, text in matches:
        text_clean = text.strip()
        if text_clean:
            footnotes.append((column_ref.strip(), text_clean))
    
    # Se não tem padrão [column], considerar nota geral
    if not matches and notes_text.strip():
        footnotes.append(('general', notes_text.strip()))
    
    return footnotes


def extract_multiple_values(value: str) -> Optional[List[str]]:
    """Extrai múltiplos valores monetários de uma string"""
    if pd.isna(value) or not isinstance(value, str):
        return None
    
    pattern = r'\$?\d+\.?\d*'
    matches = re.findall(pattern, value)
    valid_matches = [m for m in matches if re.match(r'^\$?\d+\.\d+$', m)]
    
    return valid_matches if len(valid_matches) > 1 else None


def append_note(existing_notes, new_note: str) -> str:
    """Adiciona uma nota preservando notas existentes"""
    if pd.notna(existing_notes) and existing_notes != 'Missing value':
        return f"{existing_notes} ; {new_note}"
    return new_note


def create_directory_structure(base_path: str = '.'):
    """Cria estrutura de diretórios do projeto"""
    import os
    
    directories = [
        f'{base_path}/data',
        f'{base_path}/output',
        f'{base_path}/logs',
        f'{base_path}/src',
        f'{base_path}/src/scrapers',
        f'{base_path}/src/processors',
        f'{base_path}/src/transformers',
    ]
    
    for directory in directories:
        os.makedirs(directory, exist_ok=True)
    
    print("✅ Estrutura de diretórios criada com sucesso!")

def config_database(sql_dir="database/sql"):
    """Executa os scripts SQL para configurar o banco."""
    try:
        conn = psycopg2.connect(**DATABASE_CONFIG)
        conn.autocommit = True
        cur = conn.cursor()

        cur.execute("SET search_path TO public;")

        for script_name in SQL_SCRIPTS:
            script_path = os.path.join(sql_dir, script_name)
            print(f"Executando {script_path} ...")
            with open(script_path, "r", encoding="utf-8") as f:
                sql_code = f.read()
                commands = sql_code.split(";")
                for command in commands:
                    command = command.strip()
                    if command:
                        cur.execute(command)

        print("Banco de dados configurado com sucesso!")

    except Exception as e:
        print("Erro ao configurar banco de dados:", e)

    finally:
        if 'cur' in locals():
            cur.close()
        if 'conn' in locals():
            conn.close()