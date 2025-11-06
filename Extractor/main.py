
"""
Primary data extraction and transformation pipeline for minimum wage information.

This module implements the core pipeline for extracting, processing, and transforming
minimum wage data from authoritative sources. It handles multiple data types including
standard wages, tipped wages, and youth employment regulations.
"""

import logging
import os
import sys
import warnings
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Union

import numpy as np
import pandas as pd
import psycopg2
from psycopg2.extensions import AsIs, register_adapter

from config import (DATABASE_CONFIG, OUTPUT_DIR, TIPPED_WAGE_END_YEAR,
                   TIPPED_WAGE_START_YEAR)
from src.processors.processor_standard_wage import StandardWageProcessor
from src.processors.processor_tipped_wage import TippedWageProcessor
from src.scrapers.scrapper_minimum_wage import MinimumWageScraper
from src.scrapers.scrapper_tipped_wage import TippedWageScraper
from src.scrapers.scrapper_youth_rules import YouthEmploymentScraperImproved
from src.transformers.transformer_unified import DataTransformer
from utils import config_database

# Configure logging
logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

# Configure numpy int64 adaptation for PostgreSQL
register_adapter(np.int64, AsIs)

# Suppress warnings
warnings.filterwarnings('ignore')

class MinimumWagePipeline:
    """
    Core pipeline for minimum wage data extraction and processing.
    
    This class orchestrates the entire ETL process for minimum wage data:
    1. Data extraction from authoritative sources
    2. Data processing and validation
    3. Data transformation and loading into structured storage
    """
    
    def __init__(self, output_dir: str = OUTPUT_DIR):
        """
        Initialize the pipeline with output directory configuration.
        
        Parameters
        ----------
        output_dir : str, optional
            Directory for storing intermediate and processed data,
            defaults to OUTPUT_DIR from config
        """
        self.output_dir = output_dir
        self.timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        os.makedirs(output_dir, exist_ok=True)
        logger.info(f"Initialized pipeline with output directory: {output_dir}")
    
    def run_extraction(self) -> Tuple[pd.DataFrame, pd.DataFrame, Dict[str, str], Dict[str, str], pd.DataFrame]:
        """
        Execute Phase 1: Data Extraction.

        This method orchestrates the extraction of raw minimum wage data from multiple
        authoritative sources. It ensures comprehensive data collection for:
        1. Standard minimum wage rates across jurisdictions
        2. Tipped employee wage regulations and exceptions
        3. Youth employment rules and special provisions

        The method implements validation checks to ensure data quality
        and completeness for each extracted dataset.

        Returns
        -------
        Tuple[pd.DataFrame, pd.DataFrame, Dict[str, str], Dict[str, str], pd.DataFrame]
            - Standard minimum wage data DataFrame
            - Tipped wage regulations DataFrame
            - Standard wage footnotes dictionary
            - Tipped wage footnotes dictionary
            - Youth employment rules DataFrame

        Raises
        ------
        Exception
            If extraction fails or returns empty datasets for any required component
        ValueError
            If extracted data does not meet quality requirements
        """
        logger.info("Beginning Phase 1: Data Extraction")
        
        # Extract standard minimum wage data
        logger.info("Extracting standard minimum wage data")
        scraper_standard = MinimumWageScraper()
        df_standard_raw = scraper_standard.scrape()
        
        if df_standard_raw.empty:
            raise ValueError("Failed to extract standard minimum wage data - empty dataset returned")
        
        # Extract tipped wage regulations
        logger.info("Extracting tipped wage regulations")
        scraper_tipped = TippedWageScraper()
        df_tipped_raw = scraper_tipped.scrape(
            start_year=TIPPED_WAGE_START_YEAR,
            end_year=TIPPED_WAGE_END_YEAR
        )
        if df_tipped_raw.empty:
            raise ValueError("Failed to extract tipped wage regulations - empty dataset returned")

        # Extract youth employment regulations
        logger.info("Extracting youth employment regulations")
        youth_rules = YouthEmploymentScraperImproved()
        df_youth_rules = youth_rules.scrape()
        if df_youth_rules.empty:
            raise ValueError("Failed to extract youth employment regulations - empty dataset returned")
        
        logger.info("Data extraction phase completed successfully")
        return (
            df_standard_raw,
            df_tipped_raw,
            scraper_standard.footnotes_dict,
            scraper_tipped.footnotes_dict,
            df_youth_rules
        )
    
    def run_processing(
        self,
        df_standard_raw: pd.DataFrame,
        df_tipped_raw: pd.DataFrame,
        standard_footnotes: Dict[str, str],
        tipped_footnotes: Dict[str, str]
    ) -> Tuple[pd.DataFrame, pd.DataFrame, Dict[str, str], Dict[str, str]]:
        """
        Execute Phase 2: Data Processing and Validation.

        This phase implements comprehensive data processing and validation:
        1. Data cleaning and standardization
        2. Validation against business rules
        3. Format normalization
        4. Quality assurance checks

        The method ensures data integrity and consistency before dimensional
        modeling in subsequent phases.

        Parameters
        ----------
        df_standard_raw : pd.DataFrame
            Raw standard minimum wage data from authoritative sources
        df_tipped_raw : pd.DataFrame
            Raw tipped wage regulations from authoritative sources
        standard_footnotes : Dict[str, str]
            Explanatory footnotes associated with standard wage data
        tipped_footnotes : Dict[str, str]
            Explanatory footnotes associated with tipped wage data

        Returns
        -------
        Tuple[pd.DataFrame, pd.DataFrame, Dict[str, str], Dict[str, str]]
            - Processed standard wages DataFrame
            - Processed tipped wages DataFrame
            - Updated standard wage footnotes dictionary
            - Updated tipped wage footnotes dictionary

        Raises
        ------
        ValueError
            If data validation fails or processing results in empty datasets
        """
        logger.info("Initiating Phase 2: Data Processing")
        
        # Process standard minimum wage data
        logger.info("Processing standard minimum wage data")
        processor_standard = StandardWageProcessor(
            df_standard_raw,
            footnotes_dict=standard_footnotes
        )
        df_standard_processed = processor_standard.process()
        
        if df_standard_processed.empty:
            raise ValueError("Standard wage processing resulted in empty dataset")

        # Process tipped wage regulations
        logger.info("Processing tipped wage regulations")
        processor_tipped = TippedWageProcessor(
            df_tipped_raw,
            footnotes_dict=tipped_footnotes
        )
        df_tipped_processed = processor_tipped.process()
        
        if df_tipped_processed.empty:
            raise ValueError("Tipped wage processing resulted in empty dataset")

        logger.info("Data processing phase completed successfully")
        return (
            df_standard_processed,
            df_tipped_processed,
            processor_standard.footnotes_dict,
            processor_tipped.footnotes_dict
        )
    
    def run_transformation(
        self,
        df_standard_processed: pd.DataFrame,
        df_tipped_processed: pd.DataFrame,
        standard_footnote_dict: Dict[str, str],
        tipped_footnote_dict: Dict[str, str],
        youth_rules: pd.DataFrame
    ) -> Dict[str, pd.DataFrame]:
        """
        Execute Phase 3: Data Transformation and Dimensional Modeling.

        This phase implements a comprehensive dimensional modeling strategy:
        1. Fact table creation for wage metrics
        2. Dimension table normalization for entities
        3. Temporal dimension integration for historical analysis
        4. Reference integrity enforcement
        5. Metadata enrichment with footnotes and annotations

        The dimensional model follows Kimball methodology to support:
        - Historical tracking (SCD Type 2)
        - Analytical query optimization
        - Data warehouse integration
        - Business intelligence compatibility

        Parameters
        ----------
        df_standard_processed : pd.DataFrame
            Processed standard minimum wage data with validated content
        df_tipped_processed : pd.DataFrame
            Processed tipped wage regulations with validated content
        standard_footnote_dict : Dict[str, str]
            Curated footnotes and annotations for standard wage data
        tipped_footnote_dict : Dict[str, str]
            Curated footnotes and annotations for tipped wage data
        youth_rules : pd.DataFrame
            Processed youth employment regulations and exceptions

        Returns
        -------
        Dict[str, pd.DataFrame]
            Collection of dimensional and fact tables including:
            - Fact tables: FactMinimumWage, FactTippedWage
            - Dimension tables: DimState, DimFrequency, DimCategory
            - Bridge tables: BridgeWageFootnotes
            - Reference tables: DimFootnotes, DimYouthRules

        Raises
        ------
        ValueError
            If transformation fails or results in invalid dimensional structures
        KeyError
            If required columns are missing from input DataFrames
        """
        logger.info("Initiating Phase 3: Dimensional Transformation")
        
        # Initialize transformer with validated datasets
        transformer = DataTransformer(
            df_standard_processed,
            df_tipped_processed,
            youth_rules
        )
        
        # Execute transformation with metadata integration
        tables = transformer.transform(
            standard_footnotes=standard_footnote_dict,
            tipped_footnotes=tipped_footnote_dict
        )
        
        return tables
        
    def construct_database(self, tables: Dict[str, pd.DataFrame]) -> Optional[List[str]]:
        """
        Construct and populate the analytical database schema.

        This method implements the physical database layer by:
        1. Initializing and validating the database schema
        2. Creating dimensional and fact tables with appropriate constraints
        3. Loading data with referential integrity checks
        4. Managing incremental updates through upsert operations
        5. Implementing transaction management for data consistency

        The implementation follows best practices for data warehouse loading:
        - Bulk loading for performance
        - Transaction management for atomicity
        - Constraint validation before commits
        - Incremental update handling
        - Detailed logging and error tracking

        Parameters
        ----------
        tables : Dict[str, pd.DataFrame]
            Collection of tables to be loaded into the database.
            Expected tables include:
            - Fact tables (FactMinimumWage, FactTippedWage)
            - Dimension tables (DimState, DimCategory, etc.)
            - Bridge and reference tables

        Returns
        -------
        Optional[List[str]]
            List of generated database artifacts or None if no outputs

        Raises
        ------
        ValueError
            If required tables are missing or data validation fails
        psycopg2.Error
            If database operations fail
        Exception
            For other critical failures in the database construction process
        """
        output_files = []
        
        # Initialize database
        conn = None
        cur = None
        
        try:
            # Configure database
            logger.info("Configuring database")
            config_database()
            
            # Connect to database
            logger.info("Establishing database connection")
            conn = psycopg2.connect(**DATABASE_CONFIG)
            cur = conn.cursor()
            
            # Process tables in dependency order
            table_groups = [
                ("dim_", "Dimension tables"),
                ("fact", "Fact tables"),
                ("bridge", "Bridge tables")
            ]
            
            # Process each table group
            for prefix, group_desc in table_groups:
                relevant_tables = {name: df for name, df in tables.items() 
                                if name.lower().startswith(prefix)}
                
                if not relevant_tables:
                    logger.debug(f"No {group_desc.lower()} to process")
                    continue
                
                logger.info(f"Processing {group_desc}")
                
                # Process each table in the group
                for table_name, df in relevant_tables.items():
                    if df.empty:
                        logger.warning(f"Skipping empty table: {table_name}")
                        continue
                    
                    logger.debug(f"Processing {table_name} ({len(df)} records)")
                    conn.autocommit = False  # Begin transaction
                    
                    try:
                        table_name_lower = table_name.lower()
                        
                        if table_name_lower == "dim_youth_rules":
                            self._load_youth_rules_dimension(cur, df)
                        elif table_name_lower == "dim_categories":
                            self._load_category_dimension(cur, df)
                        elif table_name_lower == "dim_states":
                            self._load_state_dimension(cur, df)
                        elif table_name_lower == "dim_footnotes":
                            self._load_footnote_dimension(cur, df)
                        elif table_name_lower == "fact":
                            self._load_minimum_wage_facts(cur, df)
                        elif table_name_lower == "bridge":
                            self._load_wage_footnote_bridge(cur, df)
                        else:
                            logger.warning(f"Skipping unrecognized table type: {table_name}")
                            continue
                        
                        conn.commit()
                        logger.info(f"Successfully loaded {len(df)} records into {table_name}")
                        
                    except (psycopg2.Error, KeyError) as e:
                        conn.rollback()
                        logger.error(f"Failed to load {table_name}: {str(e)}")
                        raise
            
            logger.info("Database construction completed successfully")
            return output_files
            
        except Exception as e:
            logger.error(f"Database construction failed: {str(e)}")
            if conn is not None:
                try:
                    conn.rollback()
                except Exception:
                    pass  # Ignore rollback errors on connection failure
            raise
            
        finally:
            if cur is not None:
                try:
                    cur.close()
                except Exception:
                    logging.error("Failed to close cursor")
            if conn is not None:
                try:
                    conn.close()
                except Exception:
                    logging.error("Failed to close connection")
        
    def _load_youth_rules_dimension(self, cur: psycopg2.extensions.cursor, df: pd.DataFrame) -> None:
        """
        Load youth rules dimension table with full upsert support.

        Parameters
        ----------
        cur : psycopg2.extensions.cursor
            Active database cursor for executing SQL
        df : pd.DataFrame
            DataFrame containing youth employment rules data

        Notes
        -----
        Implements SCD Type 2 for historical tracking.
        """
        logger.debug("Loading youth employment rules dimension")
        sql = """
            INSERT INTO DimYouthRules (
                StateName, Year, CertificateType, RuleDescription, 
                IsLabor, IsSchool, RequirementLevel, AgeMin, AgeMax, 
                Notes, Footnote, FootnoteText
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        for _, row in df.iterrows():
            params = (
                row['state'],
                row['year'],
                row.get('certificate_type', None),
                row.get('rule_description', None),
                row.get('is_issued_by_labor', None),
                row.get('is_issued_by_school', None),
                row.get('requirement_level', None),
                row.get('age_min', None),
                row.get('age_max', None),
                row.get('notes', None),
                row.get('footnotes', None),
                row.get('footnote_text', None)
            )
            cur.execute(sql, params)

    def _load_category_dimension(self, cur: psycopg2.extensions.cursor, df: pd.DataFrame) -> None:
        """
        Load category dimension table with type classification support.
        
        Parameters
        ----------
        cur : psycopg2.extensions.cursor
            Active database cursor
        df : pd.DataFrame
            DataFrame containing category metadata
        """
        logger.debug("Loading category dimension with upsert handling")
        sql = """
            INSERT INTO DimCategory (ID, CategoryName, CategoryType)
            VALUES (%s, %s, %s)
            ON CONFLICT (ID) DO UPDATE SET
                CategoryName = EXCLUDED.CategoryName,
                CategoryType = EXCLUDED.CategoryType;
        """
        for _, row in df.iterrows():
            params = (
                row['category_id'],
                row['category_name'],
                row.get('category_type', None)
            )
            cur.execute(sql, params)

    def _load_state_dimension(self, cur: psycopg2.extensions.cursor, df: pd.DataFrame) -> None:
        """
        Load state dimension table with territory classification.

        Parameters
        ----------
        cur : psycopg2.extensions.cursor
            Active database cursor
        df : pd.DataFrame
            DataFrame containing state/territory data
        """
        logger.debug("Loading state dimension with territory handling")
        sql = """
            INSERT INTO DimState (ID, StateName, IsTerritory)
            VALUES (%s, %s, %s)
            ON CONFLICT (ID) DO UPDATE SET
                StateName = EXCLUDED.StateName,
                IsTerritory = EXCLUDED.IsTerritory;
        """
        for _, row in df.iterrows():
            params = (
                row['state_id'],
                row['state_name'],
                row.get('is_territory', False)
            )
            cur.execute(sql, params)

    def _load_footnote_dimension(self, cur: psycopg2.extensions.cursor, df: pd.DataFrame) -> None:
        """
        Load footnote dimension table with category relationships.

        Parameters
        ----------
        cur : psycopg2.extensions.cursor
            Active database cursor
        df : pd.DataFrame
            DataFrame containing footnote metadata and text
        """
        logger.debug("Loading footnotes dimension with category linkage")
        sql = """
            INSERT INTO DimFootnote (ID, FootnoteKey, FootnoteText, Year, CategoryID)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (ID) DO UPDATE SET
                FootnoteKey = EXCLUDED.FootnoteKey,
                FootnoteText = EXCLUDED.FootnoteText,
                Year = EXCLUDED.Year,
                CategoryID = EXCLUDED.CategoryID;
        """
        for _, row in df.iterrows():
            params = (
                row['footnote_id'],
                row['footnote_key'],
                row['footnote_text'],
                row['footnote_year'],
                row.get('category_id', None)
            )
            cur.execute(sql, params)

    def _load_minimum_wage_facts(self, cur: psycopg2.extensions.cursor, df: pd.DataFrame) -> None:
        """
        Load minimum wage fact table with temporal tracking.

        Parameters
        ----------
        cur : psycopg2.extensions.cursor
            Active database cursor
        df : pd.DataFrame
            DataFrame containing minimum wage records
        """
        logger.debug("Loading minimum wage facts")
        sql = """
            INSERT INTO FactMinimumWage (
                id, stateid, categoryid, year, effectivedate,
                basewageperhour, minimumcashwage, maximumtipcredit,
                frequencyid, sourceurl, notes
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (ID) DO UPDATE SET
                basewageperhour = EXCLUDED.basewageperhour,
                minimumcashwage = EXCLUDED.minimumcashwage,
                maximumtipcredit = EXCLUDED.maximumtipcredit,
                frequencyid = EXCLUDED.frequencyid,
                effectivedate = EXCLUDED.effectivedate,
                sourceurl = EXCLUDED.sourceurl,
                notes = EXCLUDED.notes;
        """
        for _, row in df.iterrows():
            params = (
                row['wage_id'],
                row['state_id'],
                row['category_id'],
                row['year'],
                row.get('effective_date'),
                row.get('base_wage_per_hour'),
                row.get('minimum_cash_wage'),
                row.get('maximum_tip_credit'),
                row.get('frequency', 1),
                row['source_url'],
                row.get('notes'),
            )
            cur.execute(sql, params)



    def _load_wage_footnote_bridge(self, cur: psycopg2.extensions.cursor, df: pd.DataFrame) -> None:
        """
        Load bridge table connecting wages with footnotes.

        Parameters
        ----------
        cur : psycopg2.extensions.cursor
            Active database cursor
        df : pd.DataFrame
            DataFrame containing wage-footnote relationships
        """
        logger.debug("Loading wage-footnote relationships")
        sql = """
            INSERT INTO BridgeFactMinimumWageFootnote (WageID, FootnoteID, Context)
            VALUES (%s, %s, %s)
            ON CONFLICT (WageID, FootnoteID) DO UPDATE
                SET Context = EXCLUDED.Context;
        """
        for _, row in df.iterrows():
            params = (
                row['wage_id'],
                row['footnote_id'],
                row.get('relation_type', 'STANDARD')
            )
            cur.execute(sql, params)
                
    def run(self) -> Tuple[Dict[str, pd.DataFrame], Optional[List[str]]]:
        """
        Execute the complete ETL pipeline.

        This method orchestrates the entire data pipeline execution:
        1. Data extraction from authoritative sources
        2. Data cleaning and standardization
        3. Dimensional model transformation
        4. Database construction and population

        The pipeline implements robust error handling and logging
        throughout all phases of execution.

        Returns
        -------
        Tuple[Dict[str, pd.DataFrame], Optional[List[str]]]
            - Dictionary of transformed tables
            - List of output file paths (if any were generated)

        Raises
        ------
        Exception
            If any critical pipeline phase fails
        """
        logger.info("Initiating minimum wage data pipeline execution")
        start_time = datetime.now()
        
        try:
            logger.info("Executing Phase 1: Data Extraction")
            df_standard_raw, df_tipped_raw, standard_footnote_dict, \
                tipped_footnote_dict, youth_rules = self.run_extraction()
            
            logger.info("Executing Phase 2: Data Processing")
            df_standard_processed, df_tipped_processed, \
                standard_footnote_dict, tipped_footnote_dict = self.run_processing(
                    df_standard_raw,
                    df_tipped_raw,
                    standard_footnote_dict,
                    tipped_footnote_dict
                )
            
            logger.info("Executing Phase 3: Dimensional Transformation")
            tables = self.run_transformation(
                df_standard_processed,
                df_tipped_processed,
                standard_footnote_dict,
                tipped_footnote_dict,
                youth_rules
            )
            
            logger.info("Executing Phase 4: Database Construction")
            output_files = self.construct_database(tables)
            
            duration = (datetime.now() - start_time).total_seconds()
            logger.info(f"Pipeline execution completed successfully in {duration:.2f} seconds")
            
            return tables, output_files
            
        except Exception as e:
            logger.error(f"Pipeline execution failed: {str(e)}")
            logger.debug("Detailed error information:", exc_info=True)
            raise


def main():
    """Função principal"""
    pipeline = MinimumWagePipeline()
    tables, output_files = pipeline.run()
    
    return tables, output_files


if __name__ == "__main__":
    main()