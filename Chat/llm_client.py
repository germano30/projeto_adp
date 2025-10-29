"""Cliente para interação com modelos de linguagem via HuggingFace"""

from huggingface_hub import InferenceClient
from typing import Optional, Dict, List
import logging

from config import HUGGINGFACE_TOKEN, MODEL_NAME

logger = logging.getLogger(__name__)


class LLMClient:
    """Cliente para interagir com modelos de linguagem"""
    
    def __init__(self, token: str = None, model: str = None):
        """
        Inicializa o cliente LLM
        
        Args:
            token: Token de API do HuggingFace
            model: Nome do modelo a usar
        """
        self.token = token or HUGGINGFACE_TOKEN
        self.model = model or MODEL_NAME
        self.client = InferenceClient(token=self.token)
        logger.info(f"LLM Client inicializado com modelo: {self.model}")
    
    def generate_sql_conditions(self, user_question: str, system_prompt: str) -> Optional[str]:
        """
        Gera as condições SQL baseado na pergunta do usuário
        
        Args:
            user_question: Pergunta do usuário
            system_prompt: Prompt do sistema com instruções
            
        Returns:
            Resposta do modelo (JSON em string) ou None se falhar
        """
        try:
            logger.info(f"Gerando condições SQL para: '{user_question}'")
            
            response = self.client.chat_completion(
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_question}
                ],
                model=self.model,
                max_tokens=300,
                temperature=0.1  # Baixa temperatura para maior consistência
            )
            
            result = response.choices[0].message.content
            print(result)
            logger.info("Condições SQL geradas com sucesso")
            logger.debug(f"Resposta do modelo: {result}")
            
            return result
            
        except Exception as e:
            logger.error(f"Erro ao gerar condições SQL: {e}")
            return None
    
    def generate_natural_response(self, user_question: str, system_prompt: str) -> Optional[str]:
        """
        Gera uma resposta em linguagem natural baseada nos dados
        
        Args:
            user_question: Pergunta original do usuário
            system_prompt: Prompt com instruções e dados da query
            
        Returns:
            Resposta em linguagem natural ou None se falhar
        """
        try:
            logger.info("Gerando resposta em linguagem natural")
            
            response = self.client.chat_completion(
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": "Generate the response now."}
                ],
                model=self.model,
                max_tokens=500,
                temperature=0.3  # Um pouco mais de criatividade para respostas naturais
            )
            
            result = response.choices[0].message.content
            logger.info("Resposta natural gerada com sucesso")
            logger.debug(f"Resposta: {result[:100]}...")
            
            return result
            
        except Exception as e:
            logger.error(f"Erro ao gerar resposta natural: {e}")
            return None
    
    def test_connection(self) -> bool:
        """
        Testa se a conexão com a API está funcionando
        
        Returns:
            True se conectou com sucesso, False caso contrário
        """
        try:
            response = self.client.chat_completion(
                messages=[
                    {"role": "user", "content": "Say 'OK' if you can read this."}
                ],
                model=self.model,
                max_tokens=10
            )
            result = response.choices[0].message.content
            logger.info(f"Teste de conexão LLM bem-sucedido: {result}")
            return True
        except Exception as e:
            logger.error(f"Teste de conexão LLM falhou: {e}")
            return False


# Função auxiliar para criar instância única do LLMClient
_llm_client = None

def get_llm_client() -> LLMClient:
    """
    Retorna uma instância singleton do LLMClient
    
    Returns:
        Instância do LLMClient
    """
    global _llm_client
    if _llm_client is None:
        _llm_client = LLMClient()
    return _llm_client