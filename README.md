# Minimum Wage GenAI Tool

## Overview

This projects goal is to implement a solution that allows you to ask natural language questions
related to the Minimum Wage of any US state, responding based on data
available on the US government website (Labor US).

CREATE DATABASE lightrag_db OWNER agermano;
\c lightrag_db
CREATE EXTENSION IF NOT EXISTS vector;
CREATE EXTENSION IF NOT EXISTS age;
GRANT ALL ON DATABASE lightrag_db TO agermano;
GRANT ALL ON SCHEMA public TO agermano;
GRANT ALL ON SCHEMA ag_catalog TO agermano;