"""
Configurações centralizadas do projeto
"""
import os

DATABASE_CONFIG = {
    'user': 'agermano',
    'password': 'devpass',
    'host': 'localhost',
    'port': 5432,
    'dbname': 'chat',
    'options': '-c search_path=public' 
}

LIGHTRAG_DB_CONFIG = {
    'user': 'agermano',
    'password': 'devpass',
    'host': 'localhost',
    'port': 5432,
    'dbname': 'chat',
    'options': '-c search_path=lightrag,public' 
}
LIGHTRAG_WORKING_DIR = "./lightrag_storage"
LIGHTRAG_MODEL = "gpt-4o-mini"  # ou seu modelo preferido

# URLs base
BASE_URL_MINIMUM_WAGE = "https://www.dol.gov/agencies/whd/state/minimum-wage/history"
BASE_URL_TIPPED_WAGE = "https://www.dol.gov/agencies/whd/state/minimum-wage/tipped"

# Configurações de scraping
TIPPED_WAGE_START_YEAR = 2003
TIPPED_WAGE_END_YEAR = 2024
REQUEST_TIMEOUT = 30

# Configurações de frequência
FREQUENCY_MAP = {
    'hourly': 1,
    'daily': 2,
    'weekly': 3
}

# Padrões de texto para limpeza
TEXT_PATTERNS = {
    'not_specified': ['not specified', 'missing value', 'na', '...'],
    'frequency_markers': {
        '/day': 2,
        '/wk': 3
    }
}

DATABASE_CONFIG = {
    'user': 'agermano',
    'password': 'devpass',
    'host': 'localhost',
    'port': 5432,
    'dbname': 'chat'
}

SQL_SCRIPTS = [
    "DimCategory.sql",
    "DimState.sql",
    "DimFootnotes.sql",
    "DimFrequency.sql",
    "FactMinimumWage.sql",
    "BridgeFactMinimumWageFootnote.sql"
]

# Caminhos de saída
OUTPUT_DIR = 'output'
DATA_DIR = 'data'
LOGS_DIR = 'logs'

# Colunas esperadas
STANDARD_WAGE_COLUMNS = ['state', 'year', 'minimal_wage', 'frequency', 'notes']
TIPPED_WAGE_COLUMNS = ['jurisdiction', 'combinedrate', 'tipcredit', 'cashwage', 'definition', 'notes', 'year']

# Categorias de salário
WAGE_CATEGORIES = {
    'standard': 'Standard Minimum Wage',
    'tipped_combined': 'Tipped Combined Rate',
    'tipped_credit': 'Tipped Credit',
    'tipped_cash': 'Tipped Cash Wage'
}

TIPPED_WAGE_TYPE = {
    'exact': 1,
    'percentage': 2
}