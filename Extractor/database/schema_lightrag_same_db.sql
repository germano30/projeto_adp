-- ====================================================================
-- SETUP DO SCHEMA LIGHTRAG NO MESMO DATABASE
-- ====================================================================

-- 1. Criar schema separado
CREATE SCHEMA IF NOT EXISTS lightrag;

-- 2. Garantir que extensões estão no schema public (compartilhadas)
CREATE EXTENSION IF NOT EXISTS vector SCHEMA public;
CREATE EXTENSION IF NOT EXISTS pg_trgm SCHEMA public;

-- 3. Criar tabelas no schema lightrag
SET search_path TO lightrag, public;

-- Tabela de documentos
CREATE TABLE IF NOT EXISTS lightrag.documents (
    id SERIAL PRIMARY KEY,
    doc_id VARCHAR(255) UNIQUE NOT NULL,
    content TEXT NOT NULL,
    topic VARCHAR(200),
    data_type VARCHAR(100),
    state_name VARCHAR(100),
    source_url TEXT,
    embedding public.vector(1536),  -- Usa vector do schema public
    metadata JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Tabela de entidades (Knowledge Graph Nodes)
CREATE TABLE IF NOT EXISTS lightrag.entities (
    id SERIAL PRIMARY KEY,
    entity_name VARCHAR(500) NOT NULL,
    entity_type VARCHAR(100) NOT NULL,
    description TEXT,
    embedding public.vector(1536),
    metadata JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(entity_name, entity_type)
);

-- Tabela de relacionamentos (Knowledge Graph Edges)
CREATE TABLE IF NOT EXISTS lightrag.relationships (
    id SERIAL PRIMARY KEY,
    source_entity_id INTEGER REFERENCES lightrag.entities(id) ON DELETE CASCADE,
    target_entity_id INTEGER REFERENCES lightrag.entities(id) ON DELETE CASCADE,
    relationship_type VARCHAR(100) NOT NULL,
    strength FLOAT DEFAULT 1.0,
    metadata JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(source_entity_id, target_entity_id, relationship_type)
);

-- Tabela de conexão documentos-entidades
CREATE TABLE IF NOT EXISTS lightrag.doc_entities (
    id SERIAL PRIMARY KEY,
    doc_id VARCHAR(255) REFERENCES lightrag.documents(doc_id) ON DELETE CASCADE,
    entity_id INTEGER REFERENCES lightrag.entities(id) ON DELETE CASCADE,
    context TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(doc_id, entity_id)
);

-- ====================================================================
-- ÍNDICES PARA PERFORMANCE
-- ====================================================================

CREATE INDEX IF NOT EXISTS idx_documents_topic ON lightrag.documents(topic);
CREATE INDEX IF NOT EXISTS idx_documents_state ON lightrag.documents(state_name);
CREATE INDEX IF NOT EXISTS idx_documents_data_type ON lightrag.documents(data_type);
CREATE INDEX IF NOT EXISTS idx_documents_embedding ON lightrag.documents 
    USING ivfflat (embedding public.vector_cosine_ops) WITH (lists = 100);

CREATE INDEX IF NOT EXISTS idx_entities_name ON lightrag.entities(entity_name);
CREATE INDEX IF NOT EXISTS idx_entities_type ON lightrag.entities(entity_type);
CREATE INDEX IF NOT EXISTS idx_entities_embedding ON lightrag.entities 
    USING ivfflat (embedding public.vector_cosine_ops) WITH (lists = 50);

CREATE INDEX IF NOT EXISTS idx_relationships_source ON lightrag.relationships(source_entity_id);
CREATE INDEX IF NOT EXISTS idx_relationships_target ON lightrag.relationships(target_entity_id);
CREATE INDEX IF NOT EXISTS idx_relationships_type ON lightrag.relationships(relationship_type);

CREATE INDEX IF NOT EXISTS idx_documents_content_fts ON lightrag.documents 
    USING gin(to_tsvector('english', content));

-- ====================================================================
-- VIEWS PARA FACILITAR QUERIES CROSS-SCHEMA
-- ====================================================================

-- View do Knowledge Graph
CREATE OR REPLACE VIEW lightrag.knowledge_graph AS
SELECT 
    r.id as relationship_id,
    se.entity_name as source_entity,
    se.entity_type as source_type,
    r.relationship_type,
    te.entity_name as target_entity,
    te.entity_type as target_type,
    r.strength,
    r.metadata
FROM lightrag.relationships r
JOIN lightrag.entities se ON r.source_entity_id = se.id
JOIN lightrag.entities te ON r.target_entity_id = te.id;

-- ====================================================================
-- FUNÇÃO PARA CALCULAR RELEVÂNCIA
-- ====================================================================

CREATE OR REPLACE FUNCTION lightrag.calculate_relevance_score(
    query_embedding public.vector(1536),
    doc_embedding public.vector(1536),
    text_similarity float
) RETURNS float AS $$
BEGIN
    RETURN (1 - (query_embedding <=> doc_embedding)) * 0.7 + text_similarity * 0.3;
END;
$$ LANGUAGE plpgsql IMMUTABLE;

-- ====================================================================
-- PERMISSÕES (AJUSTE CONFORME NECESSÁRIO)
-- ====================================================================

GRANT USAGE ON SCHEMA lightrag TO agermano;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA lightrag TO agermano;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA lightrag TO agermano;

-- Reset search_path para default
RESET search_path;
