import os
import dotenv
dotenv.load_dotenv()

DATABASE_CONFIG = {
    'user': os.getenv("DB_USER"),
    'password': os.getenv("DB_PASSWORD"),
    'host': os.getenv("DB_HOST"),
    'port': os.getenv("DB_PORT"),
    'dbname': os.getenv("DB_DATABASE")
}



DATABASE_SCHEMA = """
DATABASE SCHEMA:

Table: FactMinimumWage (MAIN TABLE - Contains all wage data)
- ID (int, primary key)
- StateID (int, FK to DimState.ID)
- CategoryID (int, FK to DimCategory.ID)
- Year (int, example: 2025, 2024, 2023, 2022...)
- EffectiveDate (timestamp with time zone, when the wage became effective)
- BaseWagePerHour (float, the base wage amount in dollars per hour)
- MinimumCashWage (float, for tipped workers - minimum cash they must receive)
- MaximumTipCredit (float, for tipped workers - maximum tip credit allowed)
- FrequencyID (int, FK to DimFrequency.ID, default=1 for hourly)
- SourceURL (text, data source URL)
- Notes (text, additional contextual information)

Table: DimState
- ID (int, primary key)
- StateName (varchar(100), example: 'California', 'Texas', 'New York', 'District of Columbia')

Table: DimCategory (Types of minimum wage)
- ID (int, primary key)
- CategoryName (varchar(50), display name)
- CategoryType (varchar(50), values: 'standard' or 'tipped')

IMPORTANT WAGE CATEGORIES:
1. 'Standard Minimum Wage' (type: standard) - Regular minimum wage for all workers
2. 'Tipped Combined Rate' (type: tipped) - Total wage including tips for tipped workers
3. 'Tipped Credit' (type: tipped) - Maximum tip credit employers can claim
4. 'Tipped Cash Wage' (type: tipped) - Minimum cash wage for tipped workers

Table: DimFrequency
- ID (int, primary key)
- Description (varchar(20), values: 'hourly', 'daily', 'weekly')
Note: Most wages are 'hourly' (ID=1)

Table: DimFootnote
- ID (int, primary key)
- FootnoteKey (varchar(15), identifier)
- FootnoteText (text, footnote content with important clarifications)
- Year (int, year this footnote applies to)
- CategoryID (int, category this footnote applies to)

Table: BridgeFactMinimumWageFootnote (Links wages to footnotes)
- WageID (int, FK to FactMinimumWage.ID)
- FootnoteID (int, FK to DimFootnote.ID)
- Context (text, specific context for this footnote)

Table: DimYouthRules (Youth minimum wage rules)
- ID (int, primary key)
- StateName (varchar(100))
- Year (int)
- CertificateType (varchar(50) - Employment / Age)
- RuleDescription (text, description of the rule)
- IsLabor (int, 1 = Issued by Labor Department, 0 = Not issued by Labor)
- IsSchool (int, 1 = Issued by School, 0 = Not issued by School)
- RequirementLevel (float, 1 = Mandated by law, 2 = Available on request, 3 = Issued as practice)
- AgeMin (int, minimum age for the rule)
- AgeMax (int, maximum age for the rule)
- Notes (text, additional notes)
- Footnote (varchar(10), footnote reference)
- FootnoteText (text, footnote content)

IMPORTANT: Youth rules are independent of wage categories and apply across all wage types.
"""

BASE_QUERY = """
SELECT
    dimstate.statename,
    factminimumwage.year,
    dimcategory.categoryname,
    dimcategory.categorytype,
    factminimumwage.basewageperhour,
    factminimumwage.maximumtipcredit,
    factminimumwage.minimumcashwage,
    factminimumwage.effectivedate,
    dimfrequency.description as frequency,
    factminimumwage.notes,
    dimfootnote.footnotetext,
    dimyouthrules.ruledescription as youth_rule,
    dimyouthrules.certificatetype as youth_certificate_type,
    dimyouthrules.notes as youth_notes,
    dimyouthrules.requirementlevel as youth_requirement_level,
    dimyouthrules.islabor as youth_issued_by_labor,
    dimyouthrules.isschool as youth_issued_by_school,
    factminimumwage.sourceurl
FROM factminimumwage
INNER JOIN dimstate ON factminimumwage.stateid = dimstate.id
INNER JOIN dimcategory ON factminimumwage.categoryid = dimcategory.id
INNER JOIN dimfrequency ON factminimumwage.frequencyid = dimfrequency.id
LEFT JOIN bridgefactminimumwagefootnote ON factminimumwage.id = bridgefactminimumwagefootnote.wageid
LEFT JOIN dimfootnote ON bridgefactminimumwagefootnote.footnoteid = dimfootnote.id
LEFT JOIN dimyouthrules ON dimstate.statename = dimyouthrules.statename AND factminimumwage.year = dimyouthrules.year
WHERE 1=1
"""

VALID_STATES = [
    "Alabama", "Alaska", "Arizona", "Arkansas", "California", "Colorado", 
    "Connecticut", "Delaware", "District of Columbia", "Florida", "Georgia", 
    "Hawaii", "Idaho", "Illinois", "Indiana", "Iowa", "Kansas", "Kentucky", 
    "Louisiana", "Maine", "Maryland", "Massachusetts", "Michigan", "Minnesota", 
    "Mississippi", "Missouri", "Montana", "Nebraska", "Nevada", "New Hampshire", 
    "New Jersey", "New Mexico", "New York", "North Carolina", "North Dakota", 
    "Ohio", "Oklahoma", "Oregon", "Pennsylvania", "Rhode Island", 
    "South Carolina", "South Dakota", "Tennessee", "Texas", "Utah", "Vermont", 
    "Virginia", "Washington", "West Virginia", "Wisconsin", "Wyoming"
]

# Categorias de salários
WAGE_CATEGORIES = {
    'standard': 'Standard Minimum Wage',
    'tipped_combined': 'Tipped Combined Rate',
    'tipped_credit': 'Tipped Credit',
    'tipped_cash': 'Tipped Cash Wage'
}

SQL_GENERATION_EXAMPLES = """
CRITICAL INSTRUCTIONS FOR YOUTH/MINOR QUERIES:
- Youth rules are INDEPENDENT of wage categories - they apply to all wage types
- When user asks about "youth", "minors", "young workers", "child labor" generally: DO NOT filter by certificatetype
- Only filter certificatetype when user SPECIFICALLY mentions "employment certificate" OR "age certificate"
- The LEFT JOIN with dimyouthrules automatically includes all relevant youth data
- CertificateType values are mutually exclusive - NEVER combine them with AND

EXAMPLES:

User: "What is the minimum wage in California?"
Output:
{
  "states": ["California"],
  "years": [2024],
  "category_type": "standard",
  "sql_where": "AND dimstate.statename = 'California' AND factminimumwage.year = 2024 AND dimcategory.categorytype = 'standard'"
}

User: "Show me tipped wages for Texas and Florida in 2023"
Output:
{
  "states": ["Texas", "Florida"],
  "years": [2023],
  "category_type": "tipped",
  "sql_where": "AND dimstate.statename IN ('Texas', 'Florida') AND factminimumwage.year = 2023 AND dimcategory.categorytype = 'tipped'"
}

User: "What's the cash wage for tipped workers in Massachusetts?"
Output:
{
  "states": ["Massachusetts"],
  "years": [2024],
  "category_type": "tipped",
  "category_name": "Tipped Cash Wage",
  "sql_where": "AND dimstate.statename = 'Massachusetts' AND factminimumwage.year = 2024 AND dimcategory.categoryname = 'Tipped Cash Wage'"
}

User: "Compare standard minimum wage across all states in 2024"
Output:
{
  "states": [],
  "years": [2024],
  "category_type": "standard",
  "sql_where": "AND factminimumwage.year = 2024 AND dimcategory.categorytype = 'standard'"
}

User: "What were New York wages from 2020 to 2023?"
Output:
{
  "states": ["New York"],
  "years": [2020, 2021, 2022, 2023],
  "category_type": "standard",
  "sql_where": "AND dimstate.statename = 'New York' AND factminimumwage.year IN (2020, 2021, 2022, 2023) AND dimcategory.categorytype = 'standard'"
}

User: "What are the youth wage rules in Oregon?"
Output:
{
  "states": ["Oregon"],
  "years": [2024],
  "category_type": "standard",
  "sql_where": "AND dimstate.statename = 'Oregon' AND factminimumwage.year = 2024 AND dimcategory.categorytype = 'standard'"
}

User: "How do minor work rules differ across states?"
Output:
{
  "states": [],
  "years": [2024],
  "category_type": "standard",
  "sql_where": "AND factminimumwage.year = 2024 AND dimcategory.categorytype = 'standard'"
}

User: "What are employment certificate requirements in Nevada?"
Output:
{
  "states": ["Nevada"],
  "years": [2024],
  "category_type": "standard",
  "sql_where": "AND dimstate.statename = 'Nevada' AND factminimumwage.year = 2024 AND dimcategory.categorytype = 'standard' AND dimyouthrules.certificatetype LIKE '%Employment%'"
}

User: "Show me age certificate states"
Output:
{
  "states": [],
  "years": [2024],
  "category_type": "standard",
  "sql_where": "AND factminimumwage.year = 2024 AND dimcategory.categorytype = 'standard' AND dimyouthrules.certificatetype LIKE '%Age%'"
}

User: "Do I need a work permit for a 16 year old in California?"
Output:
{
  "states": ["California"],
  "years": [2024],
  "category_type": "standard",
  "sql_where": "AND dimstate.statename = 'California' AND factminimumwage.year = 2024 AND dimcategory.categorytype = 'standard'"
}

User: "What are child labor laws in Texas?"
Output:
{
  "states": ["Texas"],
  "years": [2024],
  "category_type": "standard",
  "sql_where": "AND dimstate.statename = 'Texas' AND factminimumwage.year = 2024 AND dimcategory.categorytype = 'standard'"
}

User: "Compare youth employment rules between California and New York"
Output:
{
  "states": ["California", "New York"],
  "years": [2024],
  "category_type": "standard",
  "sql_where": "AND dimstate.statename IN ('California', 'New York') AND factminimumwage.year = 2024 AND dimcategory.categorytype = 'standard'"
}
"""

# Tópicos disponíveis no LightRAG
LIGHTRAG_TOPICS = {
    "employment_types": {
        "Non-farm Employment": {
            "keywords": [
                "non-farm", "non farm", "industry", "factory", "office",
                "urban job", "commercial employment", "private sector"
            ],
            "threshold": 0.35,
        },
        "Agricultural Employment": {
            "keywords": [
                "agricultural", "farm", "farmer", "farming", "agriculture",
                "ranch", "harvest", "seasonal worker", "field labor",
                "crop", "livestock"
            ],
            "threshold": 0.35,
        },
        "Entertainment": {
            "keywords": [
                "entertainment", "performer", "actor", "musician",
                "artist", "dancer", "film", "tv",  "theater",
                "performance", "production crew"
            ],
            "threshold": 0.35,
        },
        "Door-to-Door Sales": {
            "keywords": [
                "door-to-door", "sales", "salesperson", "commission",
                "solicitor", "direct sales", "traveling salesperson",
                "doorstep", "subscription sales", 'door'
            ],
            "threshold": 0.35,
        },
    },

    "labor_laws": {
        "Minimum Paid Rest Periods": {
            "keywords": [
                "rest period", "break", "rest break", "paid rest",
                "short break", "coffee break", "work interval",
                "pause", "mandatory rest"
            ],
            "threshold": 0.35,
        },
        "Minimum Meal Periods": {
            "keywords": [
                "meal period", "lunch", "dinner break", "meal break",
                "food break", "meal time", "eating period"
            ],
            "threshold": 0.35,
        },
        "Prevailing Wages": {
            "keywords": [
                "prevailing wage", "davis bacon", "public contract",
                "government project", "union rate", "construction wage",
                "federal project"
            ],
            "threshold": 0.35,
        },
        "Payday Requirements": {
            "keywords": [
                "payday", "pay frequency", "payment schedule",
                "pay period", "pay day", "wage payment", "salary frequency",
                "pay timing", "payment timing", "when employees are paid"
            ],
            "threshold": 0.35,
        },
    },

    "general_indicators": {
        "keywords": [
            "law", "regulation", "requirement", "rule", "legal",
            "compliance", "overtime", "hours worked", "workweek",
            "work hours", "minimum wage", "labor code", "employment rule"
        ],
        "threshold": 0.3,
    },
}


# Keywords que indicam necessidade de consultar LightRAG
LIGHTRAG_KEYWORDS = [
    # Employment types
    'agricultural', 'farm', 'farmer', 'agriculture', 'non-farm',
    'entertainment', 'performer', 'actor', 'musician',
    'door-to-door', 'sales', 'salesperson', 'commission',
    
    # Labor laws
    'rest period', 'break', 'meal period', 'lunch', 'dinner break',
    'prevailing wage', 'davis bacon', 'public contract', 'rest', 'rest break',
    'payday', 'pay frequency', 'payment schedule', 'pay period',
    
    # General indicators
    'law', 'regulation', 'requirement', 'rule', 'legal', 'compliance',
    'overtime', 'hours worked', 'workweek'
]

ROUTING_EXAMPLES = """
ROUTING EXAMPLES:

User: "What is the minimum wage in California?"
Output: {"route": "sql", "reason": "Direct minimum wage query"}

User: "Do agricultural workers have different minimum wages in Texas?"
Output: {"route": "lightrag", "reason": "Query about agricultural employment type", "topic": "Agricultural Employment"}

User: "What are the rest break requirements in New York?"
Output: {"route": "lightrag", "reason": "Query about labor law - rest periods", "topic": "Minimum Paid Rest Periods"}

User: "Compare tipped wages in California and Nevada"
Output: {"route": "sql", "reason": "Direct comparison of wage data"}

User: "Are there special rules for entertainers in California?"
Output: {"route": "lightrag", "reason": "Query about specific employment type", "topic": "Entertainment"}

User: "When must employers pay their workers in Massachusetts?"
Output: {"route": "lightrag", "reason": "Query about payday requirements", "topic": "Payday Requirements"}

User: "What's the prevailing wage for construction workers?"
Output: {"route": "lightrag", "reason": "Query about prevailing wages", "topic": "Prevailing Wages"}

User: "Show me minimum wages for California, Texas, and Florida from 2020-2024"
Output: {"route": "sql", "reason": "Historical wage data comparison"}

User: "What are youth work rules in Oregon?"
Output: {"route": "sql", "reason": "Youth rules are in the database"}

User: "Do minors need work permits in Nevada?"
Output: {"route": "sql", "reason": "Youth certificate requirements are in the database"}
"""