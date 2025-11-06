"""Funções auxiliares para o sistema de consulta de salários mínimos"""

import json
import re
from typing import Optional, Dict, List, Tuple, Any
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def extract_json_from_response(text: str) -> Optional[Dict]:
    """
    Extrai JSON da resposta do modelo, mesmo se vier com texto extra ou markdown
    
    Args:
        text: Texto da resposta do modelo
        
    Returns:
        Dict com o JSON parseado ou None se falhar
    """
    text = re.sub(r'```json\s*', '', text)
    text = re.sub(r'```\s*', '', text)
    
    json_match = re.search(r'\{.*\}', text, re.DOTALL)
    if json_match:
        try:
            return json.loads(json_match.group())
        except json.JSONDecodeError as e:
            logger.error(f"Erro ao parsear JSON: {e}")
            logger.debug(f"Texto que falhou: {json_match.group()}")
            return None
    
    logger.warning("Nenhum JSON encontrado na resposta")
    return None


def validate_sql_conditions(conditions: Dict) -> bool:
    """
    Valida se as condições extraídas estão no formato correto
    
    Args:
        conditions: Dicionário com as condições extraídas
        
    Returns:
        True se válido, False caso contrário
    """
    required_keys = ['states', 'years', 'category_type', 'sql_where']
    
    if not all(key in conditions for key in required_keys):
        logger.error(f"Campos faltando. Esperado: {required_keys}, Recebido: {list(conditions.keys())}")
        return False
    
    if not isinstance(conditions['states'], list):
        logger.error("Campo 'states' deve ser uma lista")
        return False
    
    if not isinstance(conditions['years'], list):
        logger.error("Campo 'years' deve ser uma lista")
        return False
    
    if conditions['category_type'] not in ['tipped', 'standard']:
        logger.error(f"Campo 'category_type' inválido: {conditions['category_type']}")
        return False
    
    return True


def build_sql_query(base_query: str, conditions: Dict) -> str:
    """
    Constrói a query SQL final combinando a query base com as condições
    
    Args:
        base_query: Query SQL base
        conditions: Condições extraídas do prompt do usuário
        
    Returns:
        Query SQL completa
    """
    sql_where = conditions.get('sql_where', '')
    final_query = base_query.strip()
    
    if sql_where:
        final_query += " " + sql_where.strip()
    
    final_query += " ORDER BY dimstate.statename, factminimumwage.year DESC, dimcategory.categorytype"
    
    logger.info(f"Query SQL gerada: {final_query}")
    return final_query


def format_query_results(results: List[Tuple]) -> str:
    """
    Formata os resultados da query para exibição amigável
    
    Args:
        results: Lista de tuplas com os resultados da query
        
    Returns:
        String formatada com os resultados
    """
    if not results:
        return "Nenhum resultado encontrado para os critérios especificados."
    
    output = []
    output.append(f"\n{'='*80}")
    output.append(f"RESULTADOS DA CONSULTA ({len(results)} registros)")
    output.append(f"{'='*80}\n")
    
    for idx, row in enumerate(results, 1):
        state, year, category_name, category_type, base_wage, tip_credit, min_cash, notes, footnote, source = row
        
        output.append(f"[{idx}] {state} - {year} - {category_name}")
        output.append(f"    Base Wage: ${base_wage:.2f}" if base_wage else "    Base Wage: N/A")
        if tip_credit:
            output.append(f"    Tip Credit: ${tip_credit:.2f}")
        if min_cash:
            output.append(f"    Min Cash Wage: ${min_cash:.2f}")
        if notes:
            output.append(f"    Notes: {notes[:100]}..." if len(notes) > 100 else f"    Notes: {notes}")
        output.append("")
    
    return "\n".join(output)


def sanitize_user_input(user_input: str) -> str:
    """
    Sanitiza o input do usuário para prevenir SQL injection
    
    Args:
        user_input: Input do usuário
        
    Returns:
        Input sanitizado
    """
    dangerous_chars = [';', '--', '/*', '*/', 'xp_', 'sp_']
    cleaned = user_input
    
    for char in dangerous_chars:
        cleaned = cleaned.replace(char, '')
    
    return cleaned.strip()


def log_conversation(user_question: str, sql_query: str, results_count: int, response: str):
    """
    Loga a conversa completa para debug e análise
    
    Args:
        user_question: Pergunta do usuário
        sql_query: Query SQL gerada
        results_count: Número de resultados retornados
        response: Resposta final gerada
    """
    logger.info("="*80)
    logger.info("CONVERSAÇÃO COMPLETA")
    logger.info("="*80)
    logger.info(f"Pergunta do Usuário: {user_question}")
    logger.info(f"Query SQL: {sql_query}")
    logger.info(f"Resultados Retornados: {results_count}")
    logger.info(f"Resposta: {response[:200]}..." if len(response) > 200 else f"Resposta: {response}")
    logger.info("="*80)


def create_error_response(error_type: str, user_question: str) -> str:
    """
    Cria uma resposta amigável para diferentes tipos de erro
    
    Args:
        error_type: Tipo do erro ('no_data', 'parse_error', 'db_error', etc)
        user_question: Pergunta original do usuário
        
    Returns:
        Mensagem de erro amigável
    """
    error_messages = {
        'no_data': f"Desculpe, não encontrei dados para sua consulta: '{user_question}'. Tente especificar um estado, ano ou categoria diferente.",
        'parse_error': "Tive dificuldade em entender sua pergunta. Poderia reformular de forma mais específica? Por exemplo: 'Qual o salário mínimo na Califórnia em 2024?'",
        'db_error': "Ocorreu um erro ao consultar o banco de dados. Por favor, tente novamente em alguns instantes.",
        'validation_error': "Os dados extraídos da sua pergunta não estão no formato esperado. Tente ser mais específico sobre estado, ano e tipo de salário.",
    }
    
    return error_messages.get(error_type, "Ocorreu um erro inesperado. Por favor, tente novamente.")