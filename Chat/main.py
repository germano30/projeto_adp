"""
Main application module for the Minimum Wage Information System.

This module provides the primary interface for querying minimum wage information
using natural language processing and a hybrid SQL/RAG architecture.

The system implements a conversational interface that combines:
1. SQL-based structured data queries
2. RAG-based unstructured knowledge retrieval
3. Hybrid query processing for complex requests
"""

import logging
import sys
import time
from datetime import datetime
from typing import Optional, Dict, List, Tuple, Any

from pipeline import create_pipeline
from utils import format_query_results


class MinimumWageError(Exception):
    """Base exception for minimum wage system errors."""
    pass

class PipelineInitError(MinimumWageError):
    """Raised when pipeline initialization fails."""
    pass

class QueryProcessingError(MinimumWageError):
    """Raised when query processing fails."""
    pass

class ComponentTestError(MinimumWageError):
    """Raised when component testing fails."""
    pass

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('minimum_wage_assistant.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


def display_application_header() -> None:
    """Display the application header with system information."""
    header = """
    ═══════════════════════════════════════════════════════════════
             MINIMUM WAGE INFORMATION SYSTEM
                Powered by LLM + PostgreSQL
    ═══════════════════════════════════════════════════════════════
    """
    logger.info(header)


class QueryMetrics:
    """Tracks query execution metrics and performance data."""
    
    def __init__(self):
        """Initialize metrics tracking."""
        self.start_time = datetime.now()
        self.query_count = 0
        self.error_count = 0
        self.route_stats = {'sql': 0, 'lightrag': 0, 'hybrid': 0}
        self.avg_response_time = 0.0
    
    def update(self, result: Dict[str, Any], execution_time: float) -> None:
        """
        Update metrics with new query result.
        
        Parameters
        ----------
        result : Dict[str, Any]
            Query execution result
        execution_time : float
            Query execution time in seconds
        """
        self.query_count += 1
        if not result.get('success'):
            self.error_count += 1
        
        route = result.get('route', 'unknown')
        if route in self.route_stats:
            self.route_stats[route] += 1
            
        # Update rolling average response time
        self.avg_response_time = (
            (self.avg_response_time * (self.query_count - 1) + execution_time)
            / self.query_count
        )
    
    def get_summary(self) -> Dict[str, Any]:
        """
        Get current metrics summary.
        
        Returns
        -------
        Dict[str, Any]
            Current metrics state
        """
        return {
            'uptime': str(datetime.now() - self.start_time),
            'queries_processed': self.query_count,
            'errors': self.error_count,
            'route_distribution': self.route_stats,
            'avg_response_time': f"{self.avg_response_time:.2f}s"
        }


def display_query_result(result: Dict[str, Any], show_details: bool = False, metrics: Optional[QueryMetrics] = None) -> None:
    """
    Display the formatted query result with optional technical details.
    
    Parameters
    ----------
    result : Dict[str, Any]
        The pipeline result containing query response and metadata
    show_details : bool, optional
        If True, displays technical execution details, defaults to False
    metrics : Optional[QueryMetrics]
        Optional metrics tracker for system performance monitoring
    """
    logger.info("=" * 80)
    
    if result['success']:
        route_badge = {
            'sql': '[SQL Query]',
            'lightrag': '[Knowledge Base]',
            'hybrid': '[Hybrid Query]'
        }.get(result.get('route', 'unknown'), '[Unknown]')
        
        logger.info("✓ RESPOSTA [%s]:", route_badge)
        logger.info("%s", "="*80)
        logger.info("%s", result['response'])
        
        if show_details:
            logger.info("%s", "\n" + "-"*80)
            logger.info("DETALHES TÉCNICOS:")
            logger.info("%s", "-"*80)
            logger.info("Rota utilizada: %s", result.get('route', 'N/A'))
            
            if 'sql_query' in result:
                logger.info("Query SQL: %s", result['sql_query'])
            if 'results_count' in result:
                logger.info("Resultados encontrados: %s", result['results_count'])
            if 'conditions' in result:
                logger.info("Condições extraídas: %s", result['conditions'])
            if 'topic' in result and result['topic']:
                logger.info("Tópico LightRAG: %s", result['topic'])
            if 'sources' in result and result['sources']:
                logger.info("Fontes: %s", ', '.join(result['sources'][:3]))
    else:
        logger.error("ERRO:")
        logger.error("%s", "="*80)
        logger.error("%s", result['response'])
        if 'error' in result:
            logger.error("Detalhes técnicos: %s", result['error'])
    
    logger.info("%s", "="*80 + "\n")


def validate_query(query: str) -> Tuple[bool, Optional[str]]:
    """
    Validate user query for processing requirements.
    
    Parameters
    ----------
    query : str
        User input query to validate
    
    Returns
    -------
    Tuple[bool, Optional[str]]
        (is_valid, error_message)
    """
    if not query.strip():
        return False, "Query cannot be empty"
    if len(query) < 5:
        return False, "Query is too short - please be more specific"
    if len(query) > 500:
        return False, "Query exceeds maximum length (500 characters)"
    return True, None


def interactive_mode(pipeline, metrics: Optional[QueryMetrics] = None) -> None:
    """
    Interactive mode for processing multiple queries.
    
    Parameters
    ----------
    pipeline : Any
        Instance of MinimumWagePipeline
    metrics : Optional[QueryMetrics]
        Optional metrics tracker for monitoring
    """
    logger.info("Starting interactive mode")
    print("\nInteractive Mode - Type 'exit' to quit")
    print("Type 'details' to toggle technical details")
    print("Type 'stats' to view system metrics")
    print("-"*80 + "\n")
    
    show_details = False
    
    while True:
        try:
            user_input = input("Your question: ").strip()
            
            # Handle special commands
            if user_input.lower() in ['sair', 'exit', 'quit']:
                logger.info("User requested exit")
                print("\nShutting down... Goodbye!")
                break
            
            if user_input.lower() == 'details':
                show_details = not show_details
                status = "enabled" if show_details else "disabled"
                print(f"\nTechnical details {status}\n")
                continue
                
            if user_input.lower() == 'stats' and metrics:
                print("\nSystem Metrics:")
                print("-"*40)
                for key, value in metrics.get_summary().items():
                    print(f"{key:.<25} {value}")
                print("-"*40 + "\n")
                continue
            
            # Validate and process query
            is_valid, error_msg = validate_query(user_input)
            if not is_valid:
                logger.warning("Invalid query: %s", error_msg)
                print(f"\n⚠ {error_msg}\n")
                continue
            
            # Process query and measure performance
            query_start = time.perf_counter()
            try:
                result = pipeline.process_question(user_input)
                execution_time = time.perf_counter() - query_start
                
                if metrics:
                    metrics.update(result, execution_time)
                    
                display_query_result(result, show_details, metrics)
                
            except Exception as e:
                logger.error("Query processing failed", exc_info=True)
                if metrics:
                    metrics.update({'success': False}, time.perf_counter() - query_start)
                raise QueryProcessingError(f"Failed to process query: {str(e)}") from e
            
        except KeyboardInterrupt:
            logger.info("Terminating via KeyboardInterrupt")
            break
        except QueryProcessingError as e:
            logger.error("✗ %s", str(e))
        except Exception as e:
            logger.error("Unexpected error: %s", str(e))
            logger.debug("Detailed error information:", exc_info=True)


def single_query_mode(
    pipeline,
    question: str,
    show_details: bool = False,
    metrics: Optional[QueryMetrics] = None
) -> Dict[str, Any]:
    """
    Process a single query and return results.
    
    Parameters
    ----------
    pipeline : Any
        Instance of MinimumWagePipeline
    question : str
        Query to process
    show_details : bool, optional
        If True, displays technical details, by default False
    metrics : Optional[QueryMetrics], optional
        Metrics tracker for monitoring, by default None
    
    Returns
    -------
    Dict[str, Any]
        Query processing results
    
    Raises
    ------
    QueryProcessingError
        If query processing fails
    ValueError
        If query validation fails
    """
    logger.info("Processing query: '%s'", question)
    
    # Validate query
    is_valid, error_msg = validate_query(question)
    if not is_valid:
        raise ValueError(error_msg)
        
    # Process query with performance tracking
    query_start = time.perf_counter()
    try:
        result = pipeline.process_question(question)
        execution_time = time.perf_counter() - query_start
        
        if metrics:
            metrics.update(result, execution_time)
            
        display_query_result(result, show_details, metrics)
        return result
        
    except Exception as e:
        if metrics:
            metrics.update({'success': False}, time.perf_counter() - query_start)
        raise QueryProcessingError(f"Failed to process query: {str(e)}") from e


def test_mode(pipeline) -> bool:
    """
    Test all system components and verify functionality.
    
    Parameters
    ----------
    pipeline : Any
        Instance of MinimumWagePipeline
    
    Returns
    -------
    bool
        True if all components pass testing, False otherwise
    
    Raises
    ------
    ComponentTestError
        If critical component tests fail
    """
    logger.info("Testando componentes do sistema...")
    logger.info("%s", "-"*80)
    
    test_results = pipeline.test_components()
    
    for component, status in test_results.items():
        status_str = "✓ OK" if status else "✗ FALHOU"
        logger.info("%s %s", f"{component.upper():.<40}", status_str)
    
    logger.info("%s", "-"*80)
    
    all_ok = all(test_results.values())
    if all_ok:
        logger.info("\n✓ Todos os componentes estão funcionando!\n")
    else:
        logger.warning("\n✗ Alguns componentes falharam. Verifique as configurações.\n")
    
    return all_ok


def main() -> Optional[int]:
    """
    Main application entry point.
    
    This function orchestrates the complete execution flow:
    1. System initialization and component setup
    2. Command-line argument processing
    3. Execution mode selection and handling
    4. Error management and logging
    
    Returns
    -------
    Optional[int]
        Exit code, None if successful, 1 on error
    
    Raises
    ------
    SystemExit
        On critical failures or user termination request
    """
    start_time = time.perf_counter()
    metrics = QueryMetrics()
    display_application_header()
    
    try:
        logger.info("Initializing pipeline components...")
        pipeline = create_pipeline()
        logger.info("Pipeline created successfully in %.2f seconds", 
                   time.perf_counter() - start_time)
    except Exception as e:
        logger.error("Failed to create pipeline: %s", str(e))
        logger.debug("Detailed error information:", exc_info=True)
        raise PipelineInitError("System initialization failed") from e
    
    if len(sys.argv) > 1:
        command = sys.argv[1].lower()
        
        try:
            if command in ['test', '-t', '--test']:
                success = test_mode(pipeline)
                return 0 if success else 1
            
            elif command in ['help', '-h', '--help']:
                logger.info("""
Usage: python main.py [command] [options]

Commands:
    (none)          Start interactive mode
    test            Test all system components
    -q "question"   Process a single query
    help            Show this message

Options:
    --details, -d   Show technical details
    --metrics, -m   Show performance metrics

Examples:
    python main.py
    python main.py test
    python main.py -q "What is the minimum wage in California?"
    python main.py -q "Show me tipped wages for Texas in 2023" --details
                """)
                return 0
            
            elif command in ['-q', '--query']:
                if len(sys.argv) < 3:
                    raise ValueError("Query not provided. Usage: python main.py -q \"your question here\"")
                
                question = sys.argv[2]
                show_details = '--details' in sys.argv or '-d' in sys.argv
                show_metrics = '--metrics' in sys.argv or '-m' in sys.argv
                
                try:
                    single_query_mode(
                        pipeline,
                        question,
                        show_details,
                        metrics if show_metrics else None
                    )
                    return 0
                except (ValueError, QueryProcessingError) as e:
                    logger.error("Query processing failed: %s", str(e))
                    return 1
            
            else:
                logger.error("Unknown command: %s", command)
                logger.info("Use 'python main.py help' to see available commands")
                return 1
                
        except Exception as e:
            logger.error("Command execution failed: %s", str(e))
            logger.debug("Detailed error information:", exc_info=True)
            return 1
    
    else:
        try:
            interactive_mode(pipeline, metrics)
            return 0
        except Exception as e:
            logger.error("Interactive mode failed: %s", str(e))
            logger.debug("Detailed error information:", exc_info=True)
            return 1


if __name__ == "__main__":
    try:
        exit_code = main()
        sys.exit(exit_code if exit_code is not None else 0)
    except KeyboardInterrupt:
        logger.info("Application terminated by user")
        sys.exit(0)
    except Exception as e:
        logger.critical("Application failed with unhandled error: %s", str(e))
        logger.debug("Detailed error information:", exc_info=True)
        sys.exit(1)