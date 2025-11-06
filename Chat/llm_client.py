import logging
import os
import google.generativeai as genai
from typing import Optional

logger = logging.getLogger(__name__)

GEMINI_MODEL_NAME = "gemini-2.5-flash-lite"

class LLMClient:
    """Cliente para interagir com a API Google Gemini (AI Studio)."""
    
    def __init__(self, api_key: str = None):
        """
        Inicializa o cliente Gemini.
        
        Args:
            api_key: Sua chave de API do Google AI Studio.
        """
        try:
            self.api_key =os.environ.get("GOOGLE_API_KEY")
            
            if not self.api_key:
                raise ValueError("Nenhuma chave de API do Google foi encontrada.")
                
            genai.configure(api_key=self.api_key)
            
            json_extraction_config = {
                "response_mime_type": "application/json",
            }
            
            self.extraction_model = genai.GenerativeModel(
                GEMINI_MODEL_NAME,
                generation_config=json_extraction_config
            )
            
            self.text_model = genai.GenerativeModel(GEMINI_MODEL_NAME)
            
            self.chat_session = self.text_model.start_chat(history=[])
            
            logger.info(f"Cliente Gemini inicializado com sucesso com o modelo: {GEMINI_MODEL_NAME}")

        except Exception as e:
            logger.exception(f"Erro ao inicializar o cliente Gemini: {e}")
            self.extraction_model = None
            self.text_model = None

    def generate_sql_conditions(self, user_question: str, system_prompt: str) -> Optional[str]:
        """
        Gera condições SQL (JSON) usando Gemini com JSON Mode.
        """
        if not self.extraction_model:
            logger.error("Cliente Gemini (extração) não foi inicializado. Abortando.")
            return None
            
        try:
            logger.info(f"Gerando condições SQL (Gemini API) para: '{user_question}'")
            
            full_prompt = f"{system_prompt}\n\nPERGUNTA DO USUÁRIO:\n{user_question}"
            
            response = self.extraction_model.generate_content(
                full_prompt,
                request_options={"timeout": 60} # Timeout de 60s
            )
            
            result_json = response.text
            
            logger.info("Condições SQL (Gemini API) geradas com sucesso.")
            logger.debug(f"Resposta JSON do modelo: {result_json}")

            return result_json

        except Exception as e:
            logger.exception(f"Erro ao gerar condições SQL com Gemini: {e}")
            return None

    def generate_natural_response(self, user_question: str, system_prompt: str) -> Optional[str]:
        """
        Gera uma resposta em linguagem natural usando o *mesmo* modelo.
        (Aqui usamos a sessão de chat normal, sem JSON mode)
        """
        if not self.text_model:
            logger.error("Cliente Gemini (texto) não foi inicializado. Abortando.")
            return None
            
        try:
            logger.info("Gerando resposta natural (Gemini API)")
            
            full_prompt = f"{system_prompt}\n\nCom base nisso, responda à pergunta original do usuário:\n{user_question}"
            
            response = self.text_model.generate_content(
                full_prompt,
                request_options={"timeout": 60}
            )
            
            result_text = response.text
            
            logger.info("Resposta natural (Gemini API) gerada com sucesso")
            return result_text.strip()
            
        except Exception as e:
            logger.exception(f"Erro ao gerar resposta natural (Gemini API): {e}")
            return None


_llm_client = None

def get_llm_client() -> LLMClient:
    global _llm_client
    if _llm_client is None:
        _llm_client = LLMClient() 
    return _llm_client