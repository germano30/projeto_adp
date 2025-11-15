"""
Web application module for the Minimum Wage Information System.

This module provides a Flask-based web interface for the chatbot.
"""

import logging
import sys
from flask import Flask, render_template, request, jsonify
from flask_cors import CORS
import asyncio
from pipeline import create_pipeline

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('web_app.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

app = Flask(__name__,
            static_folder='static',
            template_folder='templates')
CORS(app)

# Global pipeline instance
pipeline = None


async def init_pipeline():
    """Initialize the pipeline asynchronously."""
    global pipeline
    try:
        logger.info("Initializing pipeline...")
        pipeline = await create_pipeline()
        logger.info("Pipeline initialized successfully")
    except Exception as e:
        logger.error(f"Failed to initialize pipeline: {e}")
        raise


@app.route('/')
def index():
    """Render the main chat interface."""
    return render_template('chat.html')


@app.route('/api/chat', methods=['POST'])
def chat():
    """
    Process chat messages and return bot responses.

    Expected JSON payload:
    {
        "message": "user question here"
    }

    Returns JSON:
    {
        "success": true/false,
        "response": "bot response",
        "route": "sql/lightrag/hybrid",
        "error": "error message if failed"
    }
    """
    try:
        data = request.get_json()
        user_message = data.get('message', '').strip()

        if not user_message:
            return jsonify({
                'success': False,
                'error': 'Message cannot be empty'
            }), 400

        if not pipeline:
            return jsonify({
                'success': False,
                'error': 'System is not initialized yet'
            }), 503

        logger.info(f"Processing message: {user_message}")

        # Process the question through the pipeline
        result = pipeline.process_question(user_message)

        return jsonify({
            'success': result['success'],
            'response': result['response'],
            'route': result.get('route', 'unknown')
        })

    except Exception as e:
        logger.error(f"Error processing chat message: {e}", exc_info=True)
        return jsonify({
            'success': False,
            'error': 'An error occurred while processing your message'
        }), 500


@app.route('/health')
def health():
    """Health check endpoint."""
    return jsonify({
        'status': 'healthy',
        'pipeline_ready': pipeline is not None
    })


def run_app(host='0.0.0.0', port=5000, debug=False):
    """
    Run the Flask application.

    Parameters
    ----------
    host : str
        Host to bind to
    port : int
        Port to listen on
    debug : bool
        Enable debug mode
    """
    # Initialize pipeline before starting the app
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(init_pipeline())

    logger.info(f"Starting web server on {host}:{port}")
    app.run(host=host, port=port, debug=debug)


if __name__ == '__main__':
    run_app(port=5001, debug=True)
