# database.py
"""Módulo para gerenciar conexões e queries ao banco de dados"""

import psycopg2
from psycopg2 import Error
from typing import List, Tuple, Optional
import logging
from contextlib import contextmanager

from config import DATABASE_CONFIG

logger = logging.getLogger(__name__)


class DatabaseManager:
    """Gerencia conexões e operações com o banco de dados PostgreSQL"""
    
    def __init__(self, config: dict = None):
        """
        Inicializa o gerenciador de banco de dados
        
        Args:
            config: Configurações de conexão (usa DATABASE_CONFIG se None)
        """
        self.config = config or DATABASE_CONFIG
    
    @contextmanager
    def get_connection(self):
        """
        Context manager para gerenciar conexões ao banco
        
        Yields:
            Conexão do psycopg2
        """
        conn = None
        try:
            conn = psycopg2.connect(**self.config)
            logger.info("Conexão com banco de dados estabelecida")
            yield conn
        except Error as e:
            logger.error(f"Erro ao conectar ao banco de dados: {e}")
            raise
        finally:
            if conn:
                conn.close()
                logger.info("Conexão com banco de dados fechada")
    
    def execute_query(self, query: str) -> List[Tuple]:
        """
        Executa uma query SELECT e retorna os resultados
        
        Args:
            query: Query SQL para executar
            
        Returns:
            Lista de tuplas com os resultados
            
        Raises:
            Error: Se houver erro na execução da query
        """
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    logger.info(f"Executando query: {query[:100]}...")
                    cur.execute(query)
                    results = cur.fetchall()
                    logger.info(f"Query retornou {len(results)} resultados")
                    return results
        except Error as e:
            logger.error(f"Erro ao executar query: {e}")
            logger.error(f"Query que falhou: {query}")
            raise
    
    def test_connection(self) -> bool:
        """
        Testa se a conexão com o banco está funcionando
        
        Returns:
            True se conectou com sucesso, False caso contrário
        """
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1")
                    result = cur.fetchone()
                    if result and result[0] == 1:
                        logger.info("Teste de conexão bem-sucedido")
                        return True
            return False
        except Error as e:
            logger.error(f"Teste de conexão falhou: {e}")
            return False
    
    def get_table_info(self, table_name: str) -> List[Tuple]:
        """
        Retorna informações sobre uma tabela
        
        Args:
            table_name: Nome da tabela
            
        Returns:
            Lista com informações das colunas
        """
        query = f"""
        SELECT column_name, data_type, is_nullable
        FROM information_schema.columns
        WHERE table_name = '{table_name}'
        ORDER BY ordinal_position;
        """
        try:
            return self.execute_query(query)
        except Error as e:
            logger.error(f"Erro ao buscar informações da tabela {table_name}: {e}")
            return []
    
    def get_states_list(self) -> List[str]:
        """
        Retorna lista de todos os estados no banco
        
        Returns:
            Lista com nomes dos estados
        """
        query = "SELECT DISTINCT statename FROM dimstate ORDER BY statename"
        try:
            results = self.execute_query(query)
            return [row[0] for row in results]
        except Error as e:
            logger.error(f"Erro ao buscar lista de estados: {e}")
            return []
    
    def get_available_years(self) -> List[int]:
        """
        Retorna lista de anos disponíveis no banco
        
        Returns:
            Lista com os anos
        """
        query = "SELECT DISTINCT year FROM factminimumwage ORDER BY year DESC"
        try:
            results = self.execute_query(query)
            return [row[0] for row in results]
        except Error as e:
            logger.error(f"Erro ao buscar anos disponíveis: {e}")
            return []


# Função auxiliar para criar instância única do DatabaseManager
_db_manager = None

def get_db_manager() -> DatabaseManager:
    """
    Retorna uma instância singleton do DatabaseManager
    
    Returns:
        Instância do DatabaseManager
    """
    global _db_manager
    if _db_manager is None:
        _db_manager = DatabaseManager()
    return _db_manager