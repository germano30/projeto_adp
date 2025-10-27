"""Aplicação principal para consulta de salários mínimos"""

import logging
import sys
from typing import Optional

from pipeline import create_pipeline
from utils import format_query_results

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('minimum_wage_assistant.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


def print_banner():
    """Imprime o banner da aplicação"""
    banner = """
    ╔══════════════════════════════════════════════════════════════╗
    ║        ASSISTENTE DE CONSULTA DE SALÁRIOS MÍNIMOS           ║
    ║                  Powered by LLM + PostgreSQL                 ║
    ╚══════════════════════════════════════════════════════════════╝
    """
    print(banner)


def print_result(result: dict, show_details: bool = False):
    """
    Imprime o resultado de forma formatada
    
    Args:
        result: Dict com o resultado do pipeline
        show_details: Se True, mostra detalhes técnicos
    """
    print("\n" + "="*80)
    
    if result['success']:
        # Mostra badge da rota usada
        route_badge = {
            'sql': '🔍 SQL',
            'lightrag': '📚 LightRAG',
            'hybrid': '🔗 Híbrido (SQL + LightRAG)'
        }.get(result.get('route', 'unknown'), '❓ Desconhecido')
        
        print(f"✓ RESPOSTA [{route_badge}]:")
        print("="*80)
        print(result['response'])
        
        if show_details:
            print("\n" + "-"*80)
            print("DETALHES TÉCNICOS:")
            print("-"*80)
            print(f"Rota utilizada: {result.get('route', 'N/A')}")
            
            if 'sql_query' in result:
                print(f"Query SQL: {result['sql_query']}")
            if 'results_count' in result:
                print(f"Resultados encontrados: {result['results_count']}")
            if 'conditions' in result:
                print(f"Condições extraídas: {result['conditions']}")
            if 'topic' in result and result['topic']:
                print(f"Tópico LightRAG: {result['topic']}")
            if 'sources' in result and result['sources']:
                print(f"Fontes: {', '.join(result['sources'][:3])}")
    else:
        print("✗ ERRO:")
        print("="*80)
        print(result['response'])
        if 'error' in result:
            print(f"\nDetalhes técnicos: {result['error']}")
    
    print("="*80 + "\n")


def interactive_mode(pipeline):
    """
    Modo interativo para fazer múltiplas perguntas
    
    Args:
        pipeline: Instância do MinimumWagePipeline
    """
    print("\nModo Interativo - Digite 'sair' para encerrar")
    print("Digite 'detalhes' para ativar/desativar detalhes técnicos")
    print("-"*80 + "\n")
    
    show_details = False
    
    while True:
        try:
            user_input = input("Sua pergunta: ").strip()
            
            if not user_input:
                continue
            
            if user_input.lower() in ['sair', 'exit', 'quit']:
                print("\nEncerrando... Até logo!")
                break
            
            if user_input.lower() == 'detalhes':
                show_details = not show_details
                status = "ativados" if show_details else "desativados"
                print(f"\nDetalhes técnicos {status}\n")
                continue
            
            # Processa a pergunta
            result = pipeline.process_question(user_input)
            print_result(result, show_details)
            
        except KeyboardInterrupt:
            print("\n\nEncerrando... Até logo!")
            break
        except Exception as e:
            logger.error(f"Erro inesperado: {e}")
            print(f"\n✗ Erro inesperado: {e}\n")


def single_query_mode(pipeline, question: str, show_details: bool = False):
    """
    Modo de consulta única
    
    Args:
        pipeline: Instância do MinimumWagePipeline
        question: Pergunta a processar
        show_details: Se True, mostra detalhes técnicos
    """
    print(f"\nProcessando: '{question}'\n")
    result = pipeline.process_question(question)
    print_result(result, show_details)


def test_mode(pipeline):
    """
    Testa todos os componentes do sistema
    
    Args:
        pipeline: Instância do MinimumWagePipeline
    """
    print("\nTestando componentes do sistema...")
    print("-"*80)
    
    test_results = pipeline.test_components()
    
    for component, status in test_results.items():
        status_str = "✓ OK" if status else "✗ FALHOU"
        print(f"{component.upper():.<40} {status_str}")
    
    print("-"*80)
    
    all_ok = all(test_results.values())
    if all_ok:
        print("\n✓ Todos os componentes estão funcionando!\n")
    else:
        print("\n✗ Alguns componentes falharam. Verifique as configurações.\n")
    
    return all_ok


def main():
    """Função principal da aplicação"""
    print_banner()
    
    # Cria o pipeline
    try:
        pipeline = create_pipeline()
        logger.info("Pipeline criado com sucesso")
    except Exception as e:
        logger.error(f"Erro ao criar pipeline: {e}")
        print(f"\n✗ Erro ao inicializar o sistema: {e}\n")
        sys.exit(1)
    
    # Parseia argumentos da linha de comando
    if len(sys.argv) > 1:
        command = sys.argv[1].lower()
        
        if command in ['test', '-t', '--test']:
            # Modo de teste
            test_mode(pipeline)
        
        elif command in ['help', '-h', '--help']:
            # Ajuda
            print("""
Uso: python main.py [comando] [opções]

Comandos:
    (nenhum)        Inicia o modo interativo
    test            Testa todos os componentes do sistema
    -q "pergunta"   Faz uma única consulta
    help            Mostra esta mensagem

Exemplos:
    python main.py
    python main.py test
    python main.py -q "What is the minimum wage in California?"
    python main.py -q "Show me tipped wages for Texas in 2023" --details
            """)
        
        elif command in ['-q', '--query']:
            # Modo de consulta única
            if len(sys.argv) < 3:
                print("\n✗ Erro: Pergunta não fornecida\n")
                print("Uso: python main.py -q \"sua pergunta aqui\"")
                sys.exit(1)
            
            question = sys.argv[2]
            show_details = '--details' in sys.argv or '-d' in sys.argv
            single_query_mode(pipeline, question, show_details)
        
        else:
            print(f"\n✗ Comando desconhecido: {command}")
            print("Use 'python main.py help' para ver os comandos disponíveis\n")
    
    else:
        # Modo interativo (padrão)
        interactive_mode(pipeline)


if __name__ == "__main__":
    main()