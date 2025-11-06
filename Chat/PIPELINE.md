## Chat pipeline — detailed design and operational guide

This document explains the Chat pipeline implementation in this repository. The Chat subsystem was substantially refactored — this doc describes the components, routing logic, keyword analysis, configuration/tuning knobs, examples, and recommendations for testing and diagnostics.

## Purpose

The pipeline accepts a natural-language question about US state minimum wage rules and returns a contextual answer. It can answer queries from structured (SQL) data, unstructured sources via LightRAG (RAG), or a hybrid of both.

## High-level components

- `Chat/main.py`: CLI and minimal interface layer. Accepts flags such as `-q` and `--details` to run queries and produce diagnostics.
- `Chat/pipeline.py` (MinimumWagePipeline): Orchestrator. Steps: sanitize question → keyword analysis → route decision → execute SQL / LightRAG / both → generate natural response via LLM → return structured result.
- `Chat/router.py`: Initial router that decides SQL, LightRAG, or HYBRID. It may use an LLM hint for ambiguous inputs but defers keyword analysis to the shared analysis module.
- `Chat/analysis.py`: Shared keyword/topic analysis. Uses a combined similarity approach (Jaccard + cosine) against configured LightRAG keywords and topics to suggest a topic and a confidence score.
- `Chat/lightrag_client.py`, `Chat/llm_client.py`, `database.py`: External integrations used by the pipeline to fetch RAG content, call the LLM, or query the DB.

## Processing flow (step-by-step)

1. Sanitize: user input is normalized by `sanitize_user_input` (removes problematic characters, trims whitespace).
2. Keyword/topic analysis: `MinimumWagePipeline.analyze_keywords()` calls `Chat/analysis.py` to produce a `KeywordAnalysis`-like result: { suggested_topic, matched_keywords, confidence }.
   - The analysis uses two similarity measures:
     - Jaccard similarity between token sets (fast, interpretable)
     - Cosine similarity on simple TF vectors (captures some distributional similarity)
   - The two scores are combined into a single confidence using tunable weights.
3. Initial routing: `router.route_question()` returns a routing decision (SQL, LIGHTRAG, HYBRID) — this is the router's preferred route.
4. Pipeline heuristic overrides: the pipeline applies conservative rules to possibly change the route:
   - If the question contains wage-related tokens (e.g., "wage", "minimum", "tipped") AND (analysis suggests a LightRAG topic OR router/LLM hint suggests non-SQL), the pipeline will prefer HYBRID (SQL + LightRAG). This prevents losing structured query results when a regulatory aspect is present.
   - If the router selected SQL but analysis strongly suggests a distinct LightRAG topic (analysis.confidence >= configured threshold), the pipeline can switch to LightRAG.
   - These behaviours are intentionally conservative to avoid unnecessary LLM costs and to preserve accurate, structured answers.
5. Execute the chosen route(s):
   - SQL: `_generate_sql_conditions()` uses the LLM to extract JSON conditions, they are validated and a DB query is executed.
   - LightRAG: `_call_lightrag_query()` invokes the lightrag client (supports async/sync clients).
   - Hybrid: both of the above are executed and results are merged into a single response prompt.
6. Response generation: the LLM generates a natural language answer using templates: SQL-only, LightRAG-only, or Hybrid prompt.

## Configuration and tuning

- `HYBRID_CONFIDENCE` (env var): float, default 0.55. Controls pipeline-level decision to prefer hybrid when analysis/LLM hints are present. Example:

```bash
export HYBRID_CONFIDENCE=0.6
```

- The analysis weights and match thresholds are currently defined inside `Chat/analysis.py` but can be parameterized if you want to tune them from environment or config. Recommended starting points:
  - jaccard_weight: 0.6
  - cosine_weight: 0.4
  - match_threshold: ~0.35

## Diagnostics and --details mode

- Run `python Chat/main.py -q "<your question>" --details` to get an extended response that includes:
  - the router decision and reason
  - keyword analysis (matched keywords, suggested topic, confidence)
  - SQL conditions and generated SQL (if a SQL route was used)
  - LightRAG topic and sources (if any)

This diagnostic output helps tune thresholds and troubleshoot routing decisions.

## Examples

CLI examples:

```bash
python Chat/main.py -q "What is the minimum wage in California for 2024?"
python Chat/main.py -q "How do tipped wages work in New York?" --details
python Chat/main.py -q "What is the tipped minimum wage in Texas and are there special rules for servers?" --details
```

Programmatic example:

```python
from Chat.pipeline import create_pipeline
pipeline = create_pipeline()
resp = pipeline.process_question("What is the minimum wage in California?")
print(resp)
```

## Testing and next steps

- Add unit tests for `Chat/analysis.py` to lock down expected suggestions and confidence values for sample queries.
- Add an integration test that exercises SQL, LightRAG, and Hybrid flows (can be done via the CLI or pytest harness that mocks external clients).
- Consider exposing analysis weights and thresholds via config or environment variables for easier production tuning.
- If LLM call duplication is a cost concern (router and pipeline both calling LLM hints), consider returning the LLM hint alongside the router decision to avoid duplicated model calls.

## Troubleshooting

- If SQL responses are missing expected rows, check the LLM-generated SQL conditions in `--details` mode and validate them against the DB schema.
- For unexpected LightRAG behaviour, inspect the `Chat/PIPELINE.md` diagnostics: matched keywords, suggested topic, and LightRAG sources.

---

