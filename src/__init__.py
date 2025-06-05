"""
LLM Evaluator Package
Autonomous evaluation pipeline for AI-generated summaries
"""
import logging
import sys
# Package version

__version__ = "1.0.0"

# Configure package-level logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('logs/evaluation.log', mode='a')
        ]
    )
# Suppress verbose logs from third-party libraries
logging.getLogger('urllib3').setLevel(logging.WARNING)
logging.getLogger('google').setLevel(logging.WARNING)
logging.getLogger('asyncio').setLevel(logging.WARNING)
