from huggingface_hub import InferenceClient

client = InferenceClient(token="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")

database_schema = """
DATABASE SCHEMA:

Table: factminimumwage
- id (int)
- stateid (int, FK to dimstate.id)
- year (int, example: 2024)
- categoryid (int, FK to dimcategory.id)
- basewageperhour (decimal)
- maximumtipcredit (decimal)
- minimumcashwage (decimal)
- notes (text)
- sourceurl (text)

Table: dimstate
- id (int)
- statename (varchar, example: 'California', 'Texas', 'District of Columbia')

Table: dimcategory
- id (int)
- categoryname (varchar)
- categorytype (varchar, values: 'tipped' or 'standard')

Table: dimfootnote
- id (int)
- footnotetext (text)

Table: bridgefactminimumwagefootnote
- wageid (int, FK to factminimumwage.id)
- footnoteid (int, FK to dimfootnote.id)
"""

base_query = """
SELECT
    dimstate.statename,
    factminimumwage.year,
    dimcategory.categoryname,
    factminimumwage.basewageperhour,
    factminimumwage.maximumtipcredit,
    factminimumwage.minimumcashwage,
    factminimumwage.notes,
    dimfootnote.footnotetext,
    factminimumwage.sourceurl
FROM factminimumwage
INNER JOIN dimstate ON factminimumwage.stateid = dimstate.id
LEFT JOIN bridgefactminimumwagefootnote ON factminimumwage.id = bridgefactminimumwagefootnote.wageid
LEFT JOIN dimfootnote ON bridgefactminimumwagefootnote.footnoteid = dimfootnote.id
INNER JOIN dimcategory ON factminimumwage.categoryid = dimcategory.id
WHERE 1=1
"""

# Use few-shot examples para treinar o modelo
examples = """
EXAMPLES:

User: "What is the minimum wage in California?"
Output:
{
  "states": ["California"],
  "years": [2024],
  "category_type": "standard",
  "sql_where": "AND dimstate.statename IN ('California') AND factminimumwage.year = 2024 AND dimcategory.categorytype = 'standard'"
}

User: "Show me tipped wages for Texas and Florida in 2023"
Output:
{
  "states": ["Texas", "Florida"],
  "years": [2023],
  "category_type": "tipped",
  "sql_where": "AND dimstate.statename IN ('Texas', 'Florida') AND factminimumwage.year = 2023 AND dimcategory.categorytype = 'tipped'"
}

User: "What were the wages in New York from 2020 to 2022?"
Output:
{
  "states": ["New York"],
  "years": [2020, 2021, 2022],
  "category_type": "standard",
  "sql_where": "AND dimstate.statename = 'New York' AND factminimumwage.year IN (2020, 2021, 2022) AND dimcategory.categorytype = 'standard'"
}
"""

system_prompt = f"""You are a SQL query assistant specialized in minimum wage data.

{database_schema}

BASE QUERY:
{base_query}

RULES:
1. Default year is 2024 if not specified
2. Default category is 'standard' if not specified
3. State names must be capitalized (e.g., 'California', 'New York')
4. Category types are either 'tipped' or 'standard'
5. Output ONLY valid JSON with: states (array), years (array), category_type (string), sql_where (string)

{examples}

IMPORTANT: Return ONLY the JSON object, no other text or explanation."""

user_prompt = "What is the minimal wage for california?"

response = client.chat_completion(
    messages=[
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_prompt}
    ],
    model="mistralai/Mistral-7B-Instruct-v0.2",
    max_tokens=300,
    temperature=0.1  # Baixa temperatura para respostas mais determinísticas
)

print(response.choices[0].message.content)