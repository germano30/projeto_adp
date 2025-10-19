"""
Script de setup para criar a estrutura de diretórios e arquivos
"""
import os
import sys


def create_directory(path):
    """Cria um diretório se não existir"""
    os.makedirs(path, exist_ok=True)
    print(f"✅ Diretório criado: {path}")


def create_file(path, content=""):
    """Cria um arquivo se não existir"""
    if not os.path.exists(path):
        with open(path, 'w', encoding='utf-8') as f:
            f.write(content)
        print(f"✅ Arquivo criado: {path}")
    else:
        print(f"⚠️  Arquivo já existe: {path}")


def setup_project():
    """Configura a estrutura completa do projeto"""
    print("\n" + "=" * 80)
    print("🚀 SETUP DO PROJETO MINIMUM WAGE PIPELINE")
    print("=" * 80 + "\n")
    
    # 1. Criar estrutura de diretórios
    print("📁 Criando estrutura de diretórios...\n")
    
    directories = [
        'src',
        'src/scrapers',
        'src/processors',
        'src/transformers',
        'data',
        'output',
        'logs',
        'tests',
    ]
    
    for directory in directories:
        create_directory(directory)
    
    # 2. Criar arquivos __init__.py
    print("\n📝 Criando arquivos __init__.py...\n")
    
    init_files = {
        'src/__init__.py': '''"""
Minimum Wage Data Pipeline
"""

__version__ = "1.0.0"
''',
        'src/scrapers/__init__.py': '''"""
Módulos de scraping
"""

from .scraper_minimum_wage import MinimumWageScraper
from .scraper_tipped_wage import TippedWageScraper

__all__ = ['MinimumWageScraper', 'TippedWageScraper']
''',
        'src/processors/__init__.py': '''"""
Módulos de processamento
"""

from .processor_standard_wage import StandardWageProcessor
from .processor_tipped_wage import TippedWageProcessor

__all__ = ['StandardWageProcessor', 'TippedWageProcessor']
''',
        'src/transformers/__init__.py': '''"""
Módulos de transformação
"""

from .transformer_unified import DataTransformer

__all__ = ['DataTransformer']
''',
        'tests/__init__.py': '# Test package\n'
    }
    
    for filepath, content in init_files.items():
        create_file(filepath, content)
    
    # 3. Criar .gitignore
    print("\n📝 Criando .gitignore...\n")
    
    gitignore_content = '''# Python
__pycache__/
*.py[cod]
*$py.class
*.so
.Python
env/
venv/
ENV/
build/
dist/
*.egg-info/

# IDE
.vscode/
.idea/
*.swp
*.swo
*~

# Data
data/
output/
logs/

# OS
.DS_Store
Thumbs.db

# Jupyter
.ipynb_checkpoints/
*.ipynb

# Pytest
.pytest_cache/
.coverage
htmlcov/
'''
    
    create_file('.gitignore', gitignore_content)
    
    # 4. Verificar requirements.txt
    print("\n📝 Verificando requirements.txt...\n")
    
    if not os.path.exists('requirements.txt'):
        requirements_content = '''# Core dependencies
requests>=2.31.0
beautifulsoup4>=4.12.0
pandas>=2.0.0
openpyxl>=3.1.0
lxml>=4.9.0

# Optional - Data analysis
numpy>=1.24.0
matplotlib>=3.7.0
seaborn>=0.12.0

# Optional - Testing
pytest>=7.4.0
pytest-cov>=4.1.0
'''
        create_file('requirements.txt', requirements_content)
    else:
        print("⚠️  requirements.txt já existe")
    
    # 5. Resumo final
    print("\n" + "=" * 80)
    print("✅ SETUP CONCLUÍDO COM SUCESSO!")
    print("=" * 80)
    print("\n📋 Próximos passos:")
    print("   1. Instalar dependências: pip install -r requirements.txt")
    print("   2. Adicionar os arquivos Python nos diretórios src/")
    print("   3. Executar o pipeline: python main.py")
    print("\n" + "=" * 80 + "\n")


if __name__ == "__main__":
    try:
        setup_project()
    except Exception as e:
        print(f"\n❌ ERRO no setup: {str(e)}")
        sys.exit(1)