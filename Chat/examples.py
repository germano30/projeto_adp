"""Exemplos de uso do sistema de consulta de salários mínimos"""

from pipeline import create_pipeline


def run_examples():
    """Executa exemplos de diferentes tipos de consultas"""
    
    pipeline = create_pipeline(use_mock_lightrag=True)
    
    sql_examples = [
        "What is the minimum wage in California?",
        "Show me tipped wages for Texas in 2023",
        "Compare minimum wages between California, New York, and Texas",
        "What's the cash wage for tipped workers in Massachusetts?",
        "What were the minimum wages in Florida from 2020 to 2024?",
    ]
    
    # Exemplos de queries LightRAG
    lightrag_examples = [
        "Do agricultural workers have different minimum wage rules?",
        "What are the rest break requirements in California?",
        "Tell me about meal period requirements for workers",
        "What are prevailing wage requirements?",
        "When must employers pay their workers?",
    ]
    
    # Exemplos de queries híbridas
    hybrid_examples = [
        "What's the minimum wage for agricultural workers in California?",
        "Do entertainment workers in New York have special wage rules?",
        "What are the wage and break requirements for restaurant workers in Texas?",
    ]
    
    print("="*80)
    print("EXEMPLOS DE CONSULTAS SQL (Dados de Salário Direto)")
    print("="*80 + "\n")
    
    for i, question in enumerate(sql_examples, 1):
        print(f"\n[Exemplo {i}] {question}")
        print("-"*80)
        result = pipeline.process_question(question)
        if result['success']:
            print(f"Rota: {result['route']}")
            print(f"Resposta: {result['response'][:200]}...")
        else:
            print(f"Erro: {result['response']}")
        print()
    
    print("\n" + "="*80)
    print("EXEMPLOS DE CONSULTAS LIGHTRAG (Leis Trabalhistas)")
    print("="*80 + "\n")
    
    for i, question in enumerate(lightrag_examples, 1):
        print(f"\n[Exemplo {i}] {question}")
        print("-"*80)
        result = pipeline.process_question(question)
        if result['success']:
            print(f"Rota: {result['route']}")
            print(f"Tópico: {result.get('topic', 'N/A')}")
            print(f"Resposta: {result['response'][:200]}...")
        else:
            print(f"Erro: {result['response']}")
        print()
    
    print("\n" + "="*80)
    print("EXEMPLOS DE CONSULTAS HÍBRIDAS (SQL + LightRAG)")
    print("="*80 + "\n")
    
    for i, question in enumerate(hybrid_examples, 1):
        print(f"\n[Exemplo {i}] {question}")
        print("-"*80)
        result = pipeline.process_question(question)
        if result['success']:
            print(f"Rota: {result['route']}")
            print(f"Tópico: {result.get('topic', 'N/A')}")
            print(f"Resposta: {result['response'][:200]}...")
        else:
            print(f"Erro: {result['response']}")
        print()


def test_routing():
    """Testa o sistema de roteamento isoladamente"""
    from router import get_query_router
    
    router = get_query_router()
    
    print("\n" + "="*80)
    print("TESTE DO SISTEMA DE ROTEAMENTO")
    print("="*80 + "\n")
    
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
        print(f"\nPergunta: {question}")
        decision = router.route_question(question)
        print(f"  → Rota: {decision['route'].value}")
        print(f"  → Razão: {decision['reason']}")
        print(f"  → Tópico: {decision.get('topic', 'N/A')}")
        print(f"  → Confiança: {decision['confidence']:.2%}")


def demonstrate_mock_vs_real():
    """Demonstra a diferença entre mock e implementação real"""
    
    print("\n" + "="*80)
    print("MOCK vs IMPLEMENTAÇÃO REAL")
    print("="*80 + "\n")
    
    print("""
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
            print(f"Comando desconhecido: {command}")
            print("\nComandos disponíveis:")
            print("  python examples.py examples      - Executa exemplos completos")
            print("  python examples.py routing       - Testa sistema de roteamento")
            print("  python examples.py mock          - Explica modo mock vs real")
    else:
        print("\nEscolha uma opção:")
        print("1. Executar exemplos completos")
        print("2. Testar roteamento")
        print("3. Entender Mock vs Real")

        choice = input("\nOpção (1-3): ").strip()

        if choice == '1':
            run_examples()
        elif choice == '2':
            test_routing()
        elif choice == '3':
            demonstrate_mock_vs_real()
        else:
            print("Opção inválida")