#!/usr/bin/env python3
"""
Main executable for running the customer evaluation batch process
Usage:
    python run_batch.py          # Start scheduled daily runs at 12:01 AM
    python run_batch.py --now    # Run immediately once
"""

import asyncio
import sys
import os
import schedule
import time
import logging
from datetime import datetime

# Add project directory to Python path
project_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(project_dir)

from batch_orchestrator import BatchOrchestrator

# Configure logging for the main script
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration
EVALUATION_STEPS = [
    "Check if the summary captures the main points from the transcript",
    "Verify that key customer issues are mentioned",
    "Ensure resolution details are included if applicable",
    "Validate that the summary is concise and clear"
]

CRITERIA = "The summary should accurately reflect the conversation content and include all critical information"

RECEIVER_EMAILS = ["plum.checkout@plum.com"]  # Add more emails as needed

ACCURACY_THRESHOLD = 0.85  # 85% threshold for alerts

HOURS_BACK = 24  # Process last 24 hours of data


def run_batch_process():
    """Synchronous wrapper to run the async batch process"""
    logger.info("Initializing batch process...")
    
    # Create orchestrator instance
    orchestrator = BatchOrchestrator(
        evaluation_steps=EVALUATION_STEPS,
        criteria=CRITERIA,
        receiver_emails=RECEIVER_EMAILS,
        accuracy_threshold=ACCURACY_THRESHOLD,
        hours_back=HOURS_BACK,
        max_retries=3
    )
    
    # Run the async batch process
    try:
        result = asyncio.run(orchestrator.run_batch_with_retry())
        logger.info(f"Batch completed successfully: {result['status']}")
        return result
    except Exception as e:
        logger.error(f"Batch process failed: {e}")
        raise


def run_scheduled():
    """Run the batch process on a schedule"""
    logger.info("Starting scheduled batch processor...")
    logger.info(f"Scheduled to run daily at 12:01 AM")
    logger.info(f"Current time: {datetime.now()}")
    
    # Schedule the job
    schedule.every().day.at("00:01").do(run_batch_process)
    
    logger.info("Scheduler started. Waiting for scheduled time...")
    logger.info("Press Ctrl+C to stop")
    
    # Keep running and check for scheduled jobs
    try:
        while True:
            schedule.run_pending()
            time.sleep(60)  # Check every minute
    except KeyboardInterrupt:
        logger.info("Scheduler stopped by user")
        sys.exit(0)


def main():
    """Main entry point"""
    if len(sys.argv) > 1 and sys.argv[1] == "--now":
        # Run immediately
        logger.info("Running batch process immediately (--now flag detected)")
        try:
            result = run_batch_process()
            logger.info("Batch process completed")
            sys.exit(0)
        except Exception as e:
            logger.error(f"Batch process failed: {e}")
            sys.exit(1)
    else:
        # Start scheduled runs
        run_scheduled()


if __name__ == "__main__":
    
    main()