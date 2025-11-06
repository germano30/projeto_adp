"""Demonstration module for minimum wage query system."""

import logging

from pipeline import create_pipeline

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)


def run_examples():
    """Execute demonstration queries across different query types."""
    
    pipeline = create_pipeline(use_mock_lightrag=True)
    
    sql_examples = [
        "What is the minimum wage in California?",
        "Show me tipped wages for Texas in 2023",
        "Compare minimum wages between California, New York, and Texas",
        "What's the cash wage for tipped workers in Massachusetts?",
        "What were the minimum wages in Florida from 2020 to 2024?",
    ]
    
    lightrag_examples = [
        "Do agricultural workers have different minimum wage rules?",
        "What are the rest break requirements in California?",
        "Tell me about meal period requirements for workers",
        "What are prevailing wage requirements?",
        "When must employers pay their workers?",
    ]
    
    hybrid_examples = [
        "What's the minimum wage for agricultural workers in California?",
        "Do entertainment workers in New York have special wage rules?",
        "What are the wage and break requirements for restaurant workers in Texas?",
    ]
    
    logger.info("Executing SQL Query Examples")
    
    for i, question in enumerate(sql_examples, 1):
        logger.info("Example %d: %s", i, question)
        result = pipeline.process_question(question)
        if result['success']:
            logger.info("Route: %s", result['route'])
            logger.info("Response: %s...", result['response'][:200])
        else:
            logger.error("Error: %s", result['response'])

    logger.info("%s", "\n" + "="*80)
    logger.info("EXEMPLOS DE CONSULTAS LIGHTRAG (Leis Trabalhistas)")
    logger.info("%s", "="*80 + "\n")

    for i, question in enumerate(lightrag_examples, 1):
        logger.info("[Exemplo %d] %s", i, question)
        logger.info("%s", "-"*80)
        result = pipeline.process_question(question)
        if result['success']:
            logger.info("Rota: %s", result['route'])
            logger.info("Tópico: %s", result.get('topic', 'N/A'))
            logger.info("Resposta: %s...", result['response'][:200])
        else:
            logger.error("Erro: %s", result['response'])

    logger.info("%s", "\n" + "="*80)
    logger.info("EXEMPLOS DE CONSULTAS HÍBRIDAS (SQL + LightRAG)")
    logger.info("%s", "="*80 + "\n")

    for i, question in enumerate(hybrid_examples, 1):
        logger.info("[Exemplo %d] %s", i, question)
        logger.info("%s", "-"*80)
        result = pipeline.process_question(question)
        if result['success']:
            logger.info("Rota: %s", result['route'])
            logger.info("Tópico: %s", result.get('topic', 'N/A'))
            logger.info("Resposta: %s...", result['response'][:200])
        else:
            logger.error("Erro: %s", result['response'])


def test_routing():
    """Testa o sistema de roteamento isoladamente"""
    from router import get_query_router
    
    router = get_query_router()
    
    logger.info("%s", "\n" + "="*80)
    logger.info("TESTE DO SISTEMA DE ROTEAMENTO")
    logger.info("%s", "="*80 + "\n")
    
    test_questions = [
        "What is the minimum wage in California?",
        "Do agricultural workers get paid differently?",
        "What are rest break requirements?",
        "Compare wages in Texas and Florida",
        "What's the minimum wage for farm workers in New York?",
        "Tell me about payday requirements",
        "Show me tipped wages in Massachusetts",
        "Are there special rules for entertainers?",
    ]
    
    for question in test_questions:
        logger.info("Pergunta: %s", question)
        decision = router.route_question(question)
        logger.info("  → Rota: %s", decision['route'].value)
        logger.info("  → Razão: %s", decision['reason'])
        logger.info("  → Tópico: %s", decision.get('topic', 'N/A'))
        logger.info("  → Confiança: %.2f%%", decision['confidence'] * 100)


def demonstrate_mock_vs_real():
    """Demonstra a diferença entre mock e implementação real"""
    
    logger.info("%s", "\n" + "="*80)
    logger.info("MOCK vs IMPLEMENTAÇÃO REAL")
    logger.info("%s", "="*80 + "\n")

    logger.info("""
O sistema está configurado com dois modos:

1. MODO MOCK (Desenvolvimento):
   - Usa MockLightRAGClient
   - Dados pré-definidos para teste
   - Não requer LightRAG real configurado
   - Ideal para desenvolvimento e testes

2. MODO REAL (Produção):
   - Usa LightRAGClient
   - Conecta ao PostgreSQL com dados do LightRAG
   - Requer schema e dados reais
   - Para uso em produção

Para alternar entre os modos:

```python
# Modo Mock (desenvolvimento)
pipeline = create_pipeline(use_mock_lightrag=True)

# Modo Real (produção)
pipeline = create_pipeline(use_mock_lightrag=False)
```

SCHEMA NECESSÁRIO PARA LIGHTRAG REAL:
```sql
CREATE TABLE lightrag_documents (
    id SERIAL PRIMARY KEY,
    document_content TEXT NOT NULL,
    topic VARCHAR(255) NOT NULL,
    state_name VARCHAR(100),
    source_url TEXT,
    metadata JSONB,
    relevance_score FLOAT,
    embedding VECTOR(768),  -- Para busca vetorial
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_lightrag_topic ON lightrag_documents(topic);
CREATE INDEX idx_lightrag_state ON lightrag_documents(state_name);
CREATE INDEX idx_lightrag_embedding ON lightrag_documents USING ivfflat (embedding vector_cosine_ops);
```
    """)



if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1:
        command = sys.argv[1].lower()
        
        if command == 'examples':
            run_examples()
        elif command == 'routing':
            test_routing()
        elif command == 'mock':
            demonstrate_mock_vs_real()
        else:
            logger.error("Comando desconhecido: %s", command)
            logger.info("\nComandos disponíveis:")
            logger.info("  python examples.py examples      - Executa exemplos completos")
            logger.info("  python examples.py routing       - Testa sistema de roteamento")
            logger.info("  python examples.py mock          - Explica modo mock vs real")
    else:
        logger.info("\nEscolha uma opção:")
        logger.info("1. Executar exemplos completos")
        logger.info("2. Testar roteamento")
        logger.info("3. Entender Mock vs Real")

        choice = input("\nOpção (1-3): ").strip()

        if choice == '1':
            run_examples()
        elif choice == '2':
            test_routing()
        elif choice == '3':
            demonstrate_mock_vs_real()
        else:
            logger.error("Opção inválida")