import psycopg2
from psycopg2.extras import execute_values
from typing import Dict, List, Tuple
import hashlib
import json


class ExtraInfoTransformer:
    """Transformer for inserting extra labor info into PostgreSQL with pgvector."""
    
    def __init__(self, db_config: Dict):
        """
        Initialize the transformer with database configuration.
        
        Args:
            db_config: Database configuration dictionary
        """
        self.db_config = db_config
        self.conn = None
        self.cur = None
    
    def connect(self):
        """Establish database connection."""
        try:
            self.conn = psycopg2.connect(**self.db_config)
            self.cur = self.conn.cursor()
            print("✅ Database connection established")
        except Exception as e:
            print(f"❌ Error connecting to database: {e}")
            raise
    
    def disconnect(self):
        """Close database connection."""
        if self.cur:
            self.cur.close()
        if self.conn:
            self.conn.close()
        print("✅ Database connection closed")
    
    def create_tables(self):
        """Create necessary tables if they don't exist."""
        print("\n📋 Creating tables...")
        
        try:
            self.cur.execute("""
                CREATE EXTENSION IF NOT EXISTS vector;
            """)
            
            # ====================== DOCUMENTS ======================
            self.cur.execute("""
                CREATE TABLE IF NOT EXISTS labor_law_documents (
                    id SERIAL PRIMARY KEY,
                    doc_id VARCHAR(255) UNIQUE NOT NULL,
                    state TEXT,
                    data_type TEXT NOT NULL,
                    doc_category VARCHAR(50) NOT NULL,
                    topic TEXT,
                    regulation_type TEXT,
                    regulation_category TEXT,
                    content TEXT NOT NULL,
                    embedding vector(1536),
                    metadata JSONB,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)

            # ====================== FOOTNOTES ======================
            self.cur.execute("""
                DROP TABLE IF EXISTS labor_law_footnotes CASCADE;
            """)
            
            self.cur.execute("""
                CREATE TABLE IF NOT EXISTS labor_law_footnotes (
                    id SERIAL PRIMARY KEY,
                    site_id VARCHAR(255) NOT NULL DEFAULT 'unknown',
                    footnote_id VARCHAR(100) NOT NULL,
                    data_type VARCHAR(100) NOT NULL,
                    topic TEXT,
                    content TEXT NOT NULL,
                    embedding vector(1536),
                    metadata JSONB,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE (site_id, footnote_id)
                );
            """)

            # ====================== INDEXES ======================
            self.cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_documents_state 
                ON labor_law_documents(state);
            """)
            
            self.cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_documents_data_type 
                ON labor_law_documents(data_type);
            """)
            
            self.cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_documents_doc_category 
                ON labor_law_documents(doc_category);
            """)
            
            self.cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_documents_embedding 
                ON labor_law_documents USING ivfflat (embedding vector_cosine_ops);
            """)
            
            self.cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_footnotes_embedding 
                ON labor_law_footnotes USING ivfflat (embedding vector_cosine_ops);
            """)
            
            self.cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_documents_metadata 
                ON labor_law_documents USING gin(metadata);
            """)

            self.conn.commit()
            print("✅ Tables created successfully")

        except Exception as e:
            print(f"❌ Error creating tables: {e}")
            self.conn.rollback()
            raise

    
    def _generate_doc_id(self, doc: Dict) -> str:
        """Generate a unique document ID based on content."""
        content = f"{doc.get('state', '')}{doc.get('type', '')}{doc.get('topic', '')}"
        return hashlib.md5(content.encode()).hexdigest()
    
    def transform_and_insert(
        self, 
        scraped_data: Dict[str, Tuple[List[Dict], List[Dict]]],
        embeddings_func=None
    ) -> Dict:
        """
        Transform and insert scraped data into PostgreSQL.
        
        Args:
            scraped_data: Dictionary with data types as keys and (state_docs, footnote_docs) as values
            embeddings_func: Optional function to generate embeddings
            
        Returns:
            Dictionary with insertion statistics
        """
        print("\n" + "=" * 80)
        print("TRANSFORMING AND INSERTING INTO POSTGRESQL")
        print("=" * 80)
        
        stats = {
            'total_documents': 0,
            'total_footnotes': 0,
            'by_type': {}
        }
        
        try:
            self.connect()
            self.create_tables()
            
            for data_type, (state_docs, footnote_docs) in scraped_data.items():
                print(f"\n📝 Inserting {data_type}...")
                
                doc_count = self._insert_documents(state_docs, data_type, embeddings_func)
                footnote_count = self._insert_footnotes(footnote_docs, data_type, embeddings_func)
                
                stats['by_type'][data_type] = {
                    'documents': doc_count,
                    'footnotes': footnote_count
                }
                stats['total_documents'] += doc_count
                stats['total_footnotes'] += footnote_count
            
            self.conn.commit()
            print("\n✅ All data inserted successfully!")
            self._print_stats(stats)
            
        except Exception as e:
            print(f"❌ Error during transformation: {e}")
            if self.conn:
                self.conn.rollback()
            raise
        finally:
            self.disconnect()
        
        return stats
    
    def _insert_documents(self, docs: List[Dict], data_type: str, embeddings_func) -> int:
        """Insert state documents into database, including site_url in metadata."""
        if not docs:
            return 0

        values = []
        seen_ids = set()

        for doc in docs:
            site_url = doc.get("site_url", "")  # novo campo
            content = doc['text']
            content_for_id = f"{doc.get('state', '')}{doc.get('type', '')}{doc.get('topic', '')}{site_url}{content}"
            doc_id = hashlib.md5(content_for_id.encode()).hexdigest()

            if doc_id in seen_ids:
                continue
            seen_ids.add(doc_id)

            state = doc.get('state', None)
            topic = doc.get('topic', '')
            regulation_type = doc.get('type', '')
            regulation_category = doc.get('category', None)

            embedding = None
            if embeddings_func:
                embedding = embeddings_func(content)

            metadata = {'original_doc': doc, 'site_url': site_url}  # site_url no metadata

            values.append((
                doc_id,
                state,
                data_type,
                'state_document',
                topic,
                regulation_type,
                regulation_category,
                content,
                embedding,
                json.dumps(metadata)
            ))

        if not values:
            return 0

        execute_values(
            self.cur,
            """
            INSERT INTO labor_law_documents 
            (doc_id, state, data_type, doc_category, topic, regulation_type, 
            regulation_category, content, embedding, metadata)
            VALUES %s
            ON CONFLICT (doc_id) DO UPDATE SET
                content = EXCLUDED.content,
                embedding = EXCLUDED.embedding,
                metadata = EXCLUDED.metadata,
                updated_at = CURRENT_TIMESTAMP
            """,
            values
        )

        print(f"  ✓ Inserted {len(values)} documents")
        return len(values)


    def _insert_footnotes(self, footnotes, data_type, embeddings_func):
        """
        Insert footnotes into the database with deduplication and conflict handling.
        """

        if not footnotes:
            return 0

        values = []
        for fn in footnotes:
            site_id = fn.get('site_id') or fn.get('source_url') or 'unknown'
            footnote_id = fn.get('footnote_id') or fn.get('id')
            topic = fn.get('topic', '')
            content = fn.get('content', '')
            metadata = fn.get('metadata', {})

            embedding = embeddings_func(content) if embeddings_func else None

            values.append((
                site_id,
                footnote_id,
                data_type,
                topic,
                content,
                embedding,
                json.dumps(metadata)
            ))

        # 🔹 Deduplicar pela chave (site_id, footnote_id)
        unique_values = {}
        for v in values:
            key = (v[0], v[1])
            if key not in unique_values:
                unique_values[key] = v
        values = list(unique_values.values())

        insert_query = """
            INSERT INTO labor_law_footnotes
            (site_id, footnote_id, data_type, topic, content, embedding, metadata)
            VALUES %s
            ON CONFLICT (site_id, footnote_id) DO UPDATE
            SET
                topic = EXCLUDED.topic,
                content = EXCLUDED.content,
                embedding = EXCLUDED.embedding,
                metadata = EXCLUDED.metadata;
        """

        try:
            execute_values(self.cur, insert_query, values, page_size=100)
            self.conn.commit()
            print(f"  ✓ Inserted {len(values)} footnotes (deduplicated).")
        except Exception as e:
            self.conn.rollback()
            print(f"❌ Error inserting footnotes: {e}")
            raise

        return len(values)



    
    def _print_stats(self, stats: Dict):
        """Print insertion statistics."""
        print("\n" + "=" * 80)
        print("INSERTION STATISTICS")
        print("=" * 80)
        print(f"\n📊 Total Documents Inserted: {stats['total_documents']}")
        print(f"📊 Total Footnotes Inserted: {stats['total_footnotes']}")
        print(f"\n{'Type':<30} {'Documents':<15} {'Footnotes':<15}")
        print("-" * 60)
        
        for data_type, type_stats in stats['by_type'].items():
            print(f"{data_type:<30} {type_stats['documents']:<15} {type_stats['footnotes']:<15}")