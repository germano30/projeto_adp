# Minimum Wage GenAI Tool

An intelligent natural language interface for accessing and analyzing US state minimum wage data, powered by advanced language models and a comprehensive database of labor regulations.

## Overview

This project implements a sophisticated solution that enables users to query US state minimum wage information using natural language. It combines data from the US Department of Labor with advanced AI capabilities to provide accurate, contextual responses about minimum wage regulations, tipped wages, and youth employment rules across all US states.

## Key Features

- **Natural Language Queries**: Ask questions about minimum wage in plain English
- **Comprehensive Data Coverage**:
  - Standard minimum wage rates
  - Tipped worker regulations
  - Youth employment rules
  - Historical wage data from 2003-2024
- **Multi-Component Architecture**:
  - Data extraction and processing pipeline
  - Intelligent query routing
  - Hybrid response generation (SQL + RAG)
  - PostgreSQL database with vector storage
  - LightRAG integration for enhanced context

## Project Structure

```
.
├── Chat/                 # Core chat interface and query processing
├── Extractor/           # Data extraction and processing pipeline
│   ├── database/        # Database schemas and SQL scripts
│   ├── src/            
│   │   ├── processors/  # Data processors for different wage types
│   │   ├── scrapers/    # Web scrapers for DOL data
│   │   └── transformers/# Data transformation logic
├── jupyter/             # Jupyter notebooks for analysis
└── lightrag_storage/    # Vector storage for enhanced responses
```

## Technical Stack

- **Backend**: Python
- **Database**: PostgreSQL with vector extension
- **AI/ML**: 
  - Large Language Models for query understanding
  - Vector embeddings for semantic search
  - LightRAG for enhanced context retrieval
- **Data Sources**: US Department of Labor (DOL) official data

## Getting Started

### Prerequisites

- Python 3.8+
- PostgreSQL 13+ with vector extension
- Access to Gemini API (for Gemini models)

### Database Setup
For complete PostgreSQL 16 installation and configuration instructions, including pgvector and Apache AGE extensions, see [README_postgres_setup.md](./README_postgres_setup.md).

**Quick setup:**
1. Follow sections 2-4 of README_postgres_setup.md to install PostgreSQL 16
2. Create the chat database following section 6.1:
   - Replace `<sql_user>` with your username
   - Replace `<sql_database>` with `chat`
3. Enable the vector extension (section 5.2)
4. Configure your environment variables as shown below

### Installation

1. Clone the repository:

```bash
git clone https://github.com/germano30/projeto_adp.git
cd projeto_adp
```

2. Create .venv
```bash
python -m venv <venv_name>
```
3. Activate venv
```bash
source <venv_name>/bin/activate
```

4. Install dependencies:

```bash
pip install -r requirements.txt
```

5. Run the data extraction pipeline:

```bash
python Extractor/main.py
```

6. Initialize the chat interface:

```bash
python Chat/main.py
```

## Usage Examples

```python
from Chat.pipeline import create_pipeline

# Create pipeline instance
pipeline = create_pipeline()

# Ask questions in natural language
questions = [
    "What is the minimum wage in California for 2024?",
    "How do tipped wages work in New York?",
    "What are the youth employment rules in Oregon?",
    "Compare minimum wages between Texas, Florida, and Nevada from 2020-2024"
]

for question in questions:
    response = pipeline.process_question(question)
    print(f"Q: {question}\nA: {response}\n")
```

## Data Model

The system uses a sophisticated data model to represent minimum wage information:

- **Standard Wages**: Regular minimum wage for non-tipped workers
- **Tipped Wages**: 
  - Combined rates (total with tips)
  - Tip credits
  - Cash wages
- **Youth Employment**: 
  - Age restrictions
  - Certificate requirements
  - Labor department rules

## Contributing

We welcome contributions! Please feel free to submit pull requests with improvements to:

- Data extraction accuracy
- Query understanding
- Response generation
- Documentation
- Test coverage

## Acknowledgments

- US Department of Labor for providing the source data
- PostgreSQL Vector extension contributors
- LightRAG project contributors