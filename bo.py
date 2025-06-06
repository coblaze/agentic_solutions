import asyncio
import os
import sys
import logging
import traceback
from datetime import datetime, timedelta
from typing import Dict, Any

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data_processor import processor
from g_automation_processor import GAutomationProcessor
from emailing_service import EmailingService

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'batch_process_{datetime.now().strftime("%Y%m%d")}.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class BatchOrchestrator:
    def __init__(
        self,
        evaluation_steps: list,
        criteria: str,
        receiver_emails: list,
        accuracy_threshold: float = 0.85,
        hours_back: int = 24,
        max_retries: int = 3
    ):
        self.evaluation_steps = evaluation_steps
        self.criteria = criteria
        self.receiver_emails = receiver_emails
        self.accuracy_threshold = accuracy_threshold
        self.hours_back = hours_back
        self.max_retries = max_retries
        self.email_service = EmailingService(accuracy_threshold=accuracy_threshold)
    
    async def run_batch_with_retry(self) -> Dict[str, Any]:
        """Run the complete batch process with retry mechanism"""
        retry_count = 0
        last_error = None
        
        while retry_count < self.max_retries:
            try:
                logger.info(f"Starting batch process (attempt {retry_count + 1}/{self.max_retries})")
                result = await self.run_batch_process()
                logger.info("Batch process completed successfully")
                return result
                
            except Exception as e:
                retry_count += 1
                last_error = e
                logger.error(f"Batch process failed (attempt {retry_count}/{self.max_retries}): {str(e)}")
                logger.error(traceback.format_exc())
                
                if retry_count < self.max_retries:
                    wait_time = min(300, 60 * retry_count)  # Max 5 minutes wait
                    logger.info(f"Waiting {wait_time} seconds before retry...")
                    await asyncio.sleep(wait_time)
        
        # All retries failed - send error notification
        error_msg = f"Batch process failed after {self.max_retries} attempts. Last error: {str(last_error)}"
        logger.error(error_msg)
        
        self.email_service.send_error_notification(
            receiver_emails=self.receiver_emails,
            error_message=error_msg + f"\n\nFull traceback:\n{traceback.format_exc()}"
        )
        
        raise Exception(error_msg)
    
    async def run_batch_process(self) -> Dict[str, Any]:
        """Main batch process logic"""
        start_time = datetime.now()
        logger.info(f"Batch process started at {start_time}")
        
        # Step 1: Fetch data from MongoDB (last 24 hours)
        logger.info(f"Fetching data from last {self.hours_back} hours...")
        await processor.fetch_data_to_dataframe(
            hours_back=self.hours_back,
            max_retries=3
        )
        
        df = processor.get_dataframe()
        
        if df.empty:
            logger.info("No new data to process in the last 24 hours")
            # Send notification about no data
            self.email_service.send_email_with_attachment(
                receiver_emails=self.receiver_emails,
                subject=f"Evaluation Batch - No Data Found ({start_time.strftime('%Y-%m-%d')})",
                body=f"""
No new customer interactions found in the last {self.hours_back} hours.

Time Window: {(start_time - timedelta(hours=self.hours_back)).strftime('%Y-%m-%d %H:%M')} to {start_time.strftime('%Y-%m-%d %H:%M')}

Next scheduled run: {(start_time + timedelta(days=1)).strftime('%Y-%m-%d')} at 12:01 AM
""",
                attachment_path=None
            )
            return {
                'status': 'no_data',
                'timestamp': start_time,
                'records_processed': 0
            }
        
        logger.info(f"Found {len(df)} records to process")
        
        # Step 2: Process with G-Automation
        logger.info("Starting G-Automation evaluation...")
        g_processor = GAutomationProcessor(
            evaluation_steps=self.evaluation_steps,
            criteria=self.criteria
        )
        
        evaluated_df = await g_processor.process_dataframe(df)
        
        # Step 3: Save results with metrics
        logger.info("Saving evaluation results...")
        output_path = g_processor.save_results_with_metrics(evaluated_df)
        
        # Step 4: Send email based on accuracy
        logger.info(f"Sending email notification (accuracy: {g_processor.accuracy:.2%})...")
        self.email_service.send_evaluation_results(
            receiver_emails=self.receiver_emails,
            excel_file_path=output_path,
            accuracy=g_processor.accuracy,
            metrics_data=g_processor.metrics_data
        )
        
        end_time = datetime.now()
        processing_time = (end_time - start_time).total_seconds()
        
        logger.info(f"Batch process completed in {processing_time:.2f} seconds")
        logger.info(f"Results: {g_processor.metrics_data}")
        
        return {
            'status': 'success',
            'timestamp': start_time,
            'records_processed': len(df),
            'accuracy': g_processor.accuracy,
            'processing_time': processing_time,
            'output_file': output_path,
            'metrics': g_processor.metrics_data
        }