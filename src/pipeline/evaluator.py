"""
Autonomous Evaluation Pipeline
Orchestrates the entire evaluation process without human intervention
"""
import asyncio
from datetime import datetime, timedelta
import logging
import traceback
from typing import Optional, Dict, Any
import uuid
from src.services.db_service import MongoDBService
from src.services.evaluation_service import EvaluationService
from src.services.report_service import ReportService
from src.services.email_service import EmailService
from src.services.state_service import StateService
from src.models.batch_state import BatchState, BatchStatus
from src.models.evaluation import BatchEvaluation
from src.config.settings import settings
from google.cloud import aiplatform

logger = logging.getLogger(__name__)


class AutonomousEvaluationPipeline:
    """
    Main evaluation pipeline that runs autonomously
    Processes transcript-summary pairs and generates reports
    """
    
    def __init__(self):
        self.db_service = MongoDBService()
        self.report_service = ReportService()
        self.email_service = EmailService()
        self.state_service = None
        self.evaluation_service = None
        self.initialized = False
        self._initialization_lock = asyncio.Lock()
        
    async def initialize(self):
        """Initialize all services and connections"""
        async with self._initialization_lock:
            if self.initialized:
                return
                
            try:
                logger.info("Initializing evaluation pipeline...")
                
                # Connect to MongoDB
                await self.db_service.connect()
                
                # Initialize state service for tracking
                self.state_service = StateService(self.db_service)
                await self.state_service.initialize()
                
                # Initialize Google Vertex AI for LLM evaluation
                logger.info(f"Initializing Google Vertex AI (project: {settings.GOOGLE_CLOUD_PROJECT})")
                aiplatform.init(
                    project=settings.GOOGLE_CLOUD_PROJECT,
                    location=settings.GOOGLE_CLOUD_LOCATION
                )
                
                # Initialize evaluation service with LLM
                from google.cloud.aiplatform import GenerativeModel
                llm_model = GenerativeModel(settings.VERTEX_AI_MODEL)
                self.evaluation_service = EvaluationService(llm_model)
                
                self.initialized = True
                logger.info("Pipeline initialization complete")
                
            except Exception as e:
                logger.error(f"Failed to initialize pipeline: {e}")
                logger.error(traceback.format_exc())
                raise
                
    async def cleanup(self):
        """Cleanup resources"""
        if self.db_service:
            await self.db_service.disconnect()
        self.initialized = False
        logger.info("Pipeline cleanup complete")
        
    async def run_daily_evaluation(self, evaluation_date: datetime) -> bool:
        """
        Run evaluation for a specific date
        This is the main entry point for daily evaluations
        
        Args:
            evaluation_date: The date to evaluate (normalized to start of day)
            
        Returns:
            bool: True if successful, False otherwise
        """
        # Ensure we're initialized
        if not self.initialized:
            await self.initialize()
            
        # Normalize date
        evaluation_date = evaluation_date.replace(hour=0, minute=0, second=0, microsecond=0)
        logger.info(f"="*60)
        logger.info(f"Starting evaluation for {evaluation_date.strftime('%Y-%m-%d')}")
        logger.info(f"="*60)
        
        # Get or create batch state
        state = await self.state_service.get_or_create_batch_state(evaluation_date)
        
        # Check if we should process this batch
        should_process, reason = await self.state_service.should_process_batch(evaluation_date)
        
        if not should_process:
            logger.info(f"Skipping batch for {evaluation_date.strftime('%Y-%m-%d')}: {reason}")
            return True  # Not an error - already processed or shouldn't process
            
        logger.info(f"Processing evaluation batch for {evaluation_date.strftime('%Y-%m-%d')}: {reason}")
        
        # Update state to running
        state.status = BatchStatus.RUNNING
        state.started_at = datetime.utcnow()
        if state.status == BatchStatus.FAILED:
            state.increment_retry()
        await self.state_service.update_batch_state(state)
        
        try:
            # Execute the evaluation pipeline
            batch_result = await self._execute_evaluation_pipeline(evaluation_date, state)
            
            # Mark as completed
            state.status = BatchStatus.COMPLETED
            state.completed_at = datetime.utcnow()
            
            logger.info(f"Batch {evaluation_date.strftime('%Y-%m-%d')} completed successfully")
            logger.info(f"   Accuracy: {batch_result.accuracy:.2%}")
            logger.info(f"   Total: {batch_result.total_evaluations} evaluations")
            logger.info(f"   Duration: {state.get_duration():.1f} seconds")
            
            return True
            
        except Exception as e:
            # Handle failure
            logger.error(f"Batch {evaluation_date.strftime('%Y-%m-%d')} failed: {e}")
            logger.error(traceback.format_exc())
            
            # Update state
            state.set_error(e)
            
            # Determine if we should retry
            if state.can_retry():
                # Set retry delay with exponential backoff
                delay_minutes = min(5 * (2 ** state.retry_count), 60)  # Max 1 hour
                state.retry_after = datetime.utcnow() + timedelta(minutes=delay_minutes)
                logger.info(f"Will retry after {delay_minutes} minutes")
            
            # Send failure alert
            await self._send_batch_failure_alert(evaluation_date, e, state)
            
            return False
            
        finally:
            # Always update final state
            await self.state_service.update_batch_state(state)
            logger.info(f"{'='*60}")
            
    async def _execute_evaluation_pipeline(self, 
                                         evaluation_date: datetime, 
                                         state: BatchState) -> BatchEvaluation:
        """
        Execute the core evaluation pipeline steps
        
        Steps:
        1. Retrieve transcript-summary pairs
        2. Evaluate with LLM
        3. Save results
        4. Generate report
        5. Send email
        """
        pipeline_start = datetime.utcnow()
        
        # Step 1: Retrieve data for the evaluation date
        logger.info(f"Step 1/5: Retrieving data for {evaluation_date.strftime('%Y-%m-%d')}")
        
        date_start = evaluation_date
        date_end = date_start + timedelta(days=1)
        
        pairs = await self.db_service.get_unevaluated_pairs((date_start, date_end))
        
        if not pairs:
            logger.warning(f"No unevaluated pairs found for {evaluation_date.strftime('%Y-%m-%d')}")
            
            # Create empty batch result
            empty_batch = BatchEvaluation(
                batch_id=f"EMPTY-{evaluation_date.strftime('%Y%m%d')}",
                evaluation_date=evaluation_date,
                total_evaluations=0,
                passed=0,
                failed=0,
                errors=0,
                pass_percentage=0,
                fail_percentage=0,
                error_percentage=0,
                accuracy=0,
                evaluation_ids=[]
            )
            
            # Update state
            state.total_pairs = 0
            state.processed_pairs = 0
            state.batch_id = empty_batch.batch_id
            state.status = BatchStatus.SKIPPED
            state.completed_at = datetime.utcnow()
            await self.state_service.update_batch_state(state)
            
            # Still send a notification
            await self._send_empty_batch_notification(evaluation_date)
            
            return empty_batch
            
        logger.info(f"Found {len(pairs)} pairs to evaluate")
        state.total_pairs = len(pairs)
        await self.state_service.update_batch_state(state)
        
        # Step 2: Evaluate pairs with LLM
        logger.info(f"Step 2/5: Evaluating {len(pairs)} pairs with LLM")
        
        evaluation_start = datetime.utcnow()
        results, batch = await self.evaluation_service.evaluate_batch(pairs)
        evaluation_duration = (datetime.utcnow() - evaluation_start).total_seconds()
        
        logger.info(f"Evaluation complete in {evaluation_duration:.1f} seconds")
        
        # Update state with results
        state.processed_pairs = len(results)
        state.successful_evaluations = batch.passed + batch.failed
        state.error_evaluations = batch.errors
        state.passed = batch.passed
        state.failed = batch.failed
        state.accuracy = batch.accuracy
        state.batch_id = batch.batch_id
        await self.state_service.update_batch_state(state)
        
        # Step 3: Save results to MongoDB
        logger.info("Step 3/5: Saving evaluation results to database")
        
        save_start = datetime.utcnow()
        await self.db_service.save_evaluation_results(results, batch)
        save_duration = (datetime.utcnow() - save_start).total_seconds()
        
        logger.info(f"Results saved in {save_duration:.1f} seconds")
        
        # Step 4: Generate Excel report
        logger.info("Step 4/5: Generating Excel report")
        
        report_start = datetime.utcnow()
        report_path = self.report_service.generate_excel_report(batch, results)
        report_duration = (datetime.utcnow() - report_start).total_seconds()
        
        state.report_path = report_path
        state.report_generated = True
        await self.state_service.update_batch_state(state)
        
        logger.info(f"Report generated in {report_duration:.1f} seconds: {report_path}")
        
        # Step 5: Send email report
        # Step 5: Send email report (ALWAYS)
        logger.info("Step 5/5: Sending email notifications")
        
        email_start = datetime.utcnow()
        is_recovery = state.is_recovery or state.metadata.get("is_recovery", False)
        
        # Check if we need to send alert FIRST
        if batch.accuracy < settings.ACCURACY_THRESHOLD:
            logger.warning(f"Accuracy ({batch.accuracy:.2%}) below threshold ({settings.ACCURACY_THRESHOLD:.0%})")
            
            # Send alert FIRST
            await self._send_accuracy_alert(batch, evaluation_date)
            logger.info("Accuracy alert sent")
            
            # Small delay to ensure alert is received first
            await asyncio.sleep(2)
        
            # ALWAYS send the evaluation report (regardless of accuracy)
            logger.info("Sending evaluation report...")
            
            # Determine if this is an alert scenario for subject line
            is_alert_scenario = batch.accuracy < settings.ACCURACY_THRESHOLD
            
            await self.email_service.send_evaluation_report(
                batch,
                report_path,
                send_alert=is_alert_scenario,  # This affects subject line and body content
                is_recovery=is_recovery
            )
            
            email_duration = (datetime.utcnow() - email_start).total_seconds()
            
            state.email_sent = True
            state.email_recipients = [settings.ALERT_EMAIL]
            await self.state_service.update_batch_state(state)
            
            logger.info(f"Email report sent in {email_duration:.1f} seconds")
        
        # Log overall pipeline metrics
        total_duration = (datetime.utcnow() - pipeline_start).total_seconds()
        logger.info(f"Pipeline completed in {total_duration:.1f} seconds total")
        
        # Add timing metadata
        state.metadata.update({
            "pipeline_duration_seconds": total_duration,
            "evaluation_duration_seconds": evaluation_duration,
            "save_duration_seconds": save_duration,
            "report_duration_seconds": report_duration,
            "email_duration_seconds": email_duration
        })
        await self.state_service.update_batch_state(state)
        
        return batch
        
    async def _send_batch_failure_alert(self,
                                      evaluation_date: datetime,
                                      error: Exception,
                                      state: BatchState):
        """Send alert when a batch fails"""
        subject = f"CRITICAL: Evaluation Batch Failed - {evaluation_date.strftime('%Y-%m-%d')}"
        
        will_retry = state.can_retry()
        retry_note = (f"Will retry automatically (attempt {state.retry_count + 1}/{state.max_retries})" 
                     if will_retry else 
                     "Max retries exceeded - manual intervention required")
        
        error_details = traceback.format_exc()
        
        body = f"""
        <html>
        <head>
            <style>
                body {{ font-family: Arial, sans-serif; }}
                .error-box {{ background-color: #f8d7da; border: 1px solid #f5c6cb; padding: 15px; border-radius: 5px; }}
                .details {{ background-color: #f8f9fa; padding: 15px; border-radius: 5px; margin: 15px 0; }}
                pre {{ background-color: #f8f9fa; padding: 10px; border-radius: 3px; overflow-x: auto; }}
            </style>
        </head>
        <body>
            <div class="error-box">
                <h2>Batch Failure Alert</h2>
                <p>The evaluation batch has encountered a critical error.</p>
            </div>
            
            <div class="details">
                <h3>Batch Details:</h3>
                <ul>
                    <li><strong>Date:</strong> {evaluation_date.strftime('%Y-%m-%d')}</li>
                    <li><strong>Batch ID:</strong> {state.batch_id or 'Not assigned'}</li>
                    <li><strong>Status:</strong> {state.status.value}</li>
                    <li><strong>Retry Count:</strong> {state.retry_count}/{state.max_retries}</li>
                    <li><strong>Total Pairs:</strong> {state.total_pairs}</li>
                    <li><strong>Processed:</strong> {state.processed_pairs}</li>
                    <li><strong>Progress:</strong> {state.get_progress_percentage()}%</li>
                    <li><strong>Error Type:</strong> {type(error).__name__}</li>
                    <li><strong>Error Message:</strong> {str(error)}</li>
                </ul>
                
                <h3>Next Steps:</h3>
                <p><strong>{retry_note}</strong></p>
                
                <h3>Error Details:</h3>
                <pre>{error_details}</pre>
                
                <h3>State Summary:</h3>
                <pre>{state.to_summary_dict()}</pre>
            </div>
            
            <p><em>Generated at: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}</em></p>
        </body>
        </html>
        """
        
        await self.email_service.send_alert(subject, body, priority="high")
        
    async def _send_empty_batch_notification(self, evaluation_date: datetime):
        """Send notification when no data found for evaluation"""
        subject = f"No Data Found - {evaluation_date.strftime('%Y-%m-%d')}"
        
        body = f"""
        <html>
        <body>
            <h2>No Evaluation Data Found</h2>
            <p>No unevaluated transcript-summary pairs were found for {evaluation_date.strftime('%Y-%m-%d')}.</p>
            
            <p>Possible reasons:</p>
            <ul>
                <li>All pairs for this date have already been evaluated</li>
                <li>No calls were processed on this date</li>
                <li>Data ingestion may be delayed</li>
            </ul>
            
            <p>No action required unless this is unexpected.</p>
            
            <p><em>Generated at: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}</em></p>
        </body>
        </html>
        """
        
        await self.email_service.send_alert(subject, body, priority="low")
    async def _send_accuracy_alert(self, batch: BatchEvaluation, evaluation_date: datetime):
        """Send immediate alert when accuracy is below threshold"""
        subject = f"URGENT ALERT: Low Accuracy ({batch.accuracy:.2%}) - {evaluation_date.strftime('%Y-%m-%d')}"
        
        body = f"""
        <html>
        <body>
            <h2 style="color: red;">URGENT: Accuracy Below Threshold</h2>
            
            <div style="background-color: #f8d7da; border: 1px solid #f5c6cb; padding: 15px; border-radius: 5px;">
                <p><strong>IMMEDIATE ATTENTION REQUIRED</strong></p>
                <p>The evaluation accuracy has fallen below the acceptable threshold.</p>
            </div>
            
            <h3>Quick Summary:</h3>
            <ul>
                <li><strong>Date:</strong> {evaluation_date.strftime('%Y-%m-%d')}</li>
                <li><strong>Accuracy:</strong> <span style="color: red; font-size: 1.2em;">{batch.accuracy:.2%}</span></li>
                <li><strong>Threshold:</strong> {settings.ACCURACY_THRESHOLD:.0%}</li>
                <li><strong>Total Evaluations:</strong> {batch.total_evaluations}</li>
                <li><strong>Failed:</strong> {batch.failed} ({batch.fail_percentage:.2f}%)</li>
            </ul>
            
            <p><strong>A detailed report will follow shortly with complete evaluation data.</strong></p>
            
            <p style="color: red;"><strong>Action Required:</strong> Please investigate the cause of the accuracy drop immediately.</p>
        </body>
        </html>
        """
        
        # Send with high priority
        await self.email_service.send_alert(subject, body, priority="high")

    