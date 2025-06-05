"""
Automatic Recovery Manager
Handles missed batches and failures without human intervention
"""
import asyncio
from datetime import datetime, timedelta
from typing import List, Optional, Dict, Any
import logging
from src.models.batch_state import BatchState, BatchStatus
from src.pipeline.evaluator import AutonomousEvaluationPipeline
from src.config.settings import settings

logger = logging.getLogger(__name__)


class RecoveryManager:
    """
    Manages automatic recovery of missed or failed batches
    Implements intelligent recovery strategies without human intervention
    """
    
    def __init__(self):
        self.pipeline: Optional[AutonomousEvaluationPipeline] = None
        self.max_recovery_attempts = 3
        self.recovery_lookback_days = settings.RECOVERY_LOOKBACK_DAYS
        self.recovery_stats: Dict[str, Any] = {
            "total_recoveries": 0,
            "successful_recoveries": 0,
            "failed_recoveries": 0,
            "last_recovery_run": None
        }
        
    async def initialize(self, pipeline: AutonomousEvaluationPipeline):
        """Initialize with pipeline reference"""
        self.pipeline = pipeline
        logger.info(f"Recovery manager initialized (lookback: {self.recovery_lookback_days} days)")
        
    async def recover_all_missed_batches(self) -> int:
        """
        Recover all missed batches within lookback period
        Called on startup and after daily runs
        
        Returns:
            Number of batches successfully recovered
        """
        start_time = datetime.utcnow()
        self.recovery_stats["last_recovery_run"] = start_time
        
        logger.info(f"Starting comprehensive recovery check (lookback: {self.recovery_lookback_days} days)")
        
        try:
            # Find all dates that need evaluation
            missing_dates = await self._find_missing_evaluations()
            
            if not missing_dates:
                logger.info("No missed evaluations found during recovery check")
                return 0
                
            logger.warning(f"Found {len(missing_dates)} missed evaluations to recover")
            
            # Log missing dates
            for date in missing_dates:
                logger.info(f"  - {date.strftime('%Y-%m-%d')}")
                
            # Process each missing date chronologically
            recovered_count = 0
            failed_count = 0
            
            for i, date in enumerate(sorted(missing_dates), 1):
                logger.info(f"Recovery {i}/{len(missing_dates)}: Processing {date.strftime('%Y-%m-%d')}")
                
                success = await self._recover_single_batch(date)
                
                if success:
                    recovered_count += 1
                    self.recovery_stats["successful_recoveries"] += 1
                else:
                    failed_count += 1
                    self.recovery_stats["failed_recoveries"] += 1
                    
                # Small delay between recoveries to avoid overload
                if i < len(missing_dates):
                    await asyncio.sleep(5)
                    
            # Log recovery summary
            duration = (datetime.utcnow() - start_time).total_seconds()
            logger.info(f"Recovery check completed in {duration:.1f} seconds")
            logger.info(f"  - Successfully recovered: {recovered_count}")
            logger.info(f"  - Failed to recover: {failed_count}")
            
            # Send recovery summary if any batches were processed
            if recovered_count > 0 or failed_count > 0:
                await self._send_recovery_summary(missing_dates, recovered_count, failed_count)
                
            return recovered_count
            
        except Exception as e:
            logger.error(f"Error in recovery process: {e}", exc_info=True)
            await self._send_recovery_error_alert(e)
            return 0
            
    async def check_and_recover_missed_batches(self) -> int:
        """
        Quick check for recent missed batches
        Called after each daily run to catch recent failures
        
        Returns:
            Number of batches successfully recovered
        """
        # Only check last 3 days for recent misses
        recent_days = min(3, self.recovery_lookback_days)
        
        logger.info(f"Performing quick recovery check (last {recent_days} days)")
        
        end_date = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
        start_date = end_date - timedelta(days=recent_days)
        
        # Check each date
        current_date = start_date
        recovered_count = 0
        dates_to_recover = []
        
        while current_date < end_date:
            state = await self.pipeline.state_service.get_or_create_batch_state(current_date)
            
            # Check if needs recovery
            if state.status in [BatchStatus.FAILED, BatchStatus.PENDING, BatchStatus.PARTIAL]:
                if state.can_retry():
                    dates_to_recover.append(current_date)
                else:
                    logger.info(f"Skipping {current_date.strftime('%Y-%m-%d')} - max retries exceeded")
                    
            current_date += timedelta(days=1)
            
        # Recover found dates
        for date in dates_to_recover:
            logger.info(f"Quick recovery: Processing {date.strftime('%Y-%m-%d')}")
            success = await self._recover_single_batch(date)
            if success:
                recovered_count += 1
                
        if recovered_count > 0:
            logger.info(f"Quick recovery completed: {recovered_count} batches recovered")
        else:
            logger.info("Quick recovery completed: No batches needed recovery")
            
        return recovered_count
        
    async def _find_missing_evaluations(self) -> List[datetime]:
        """Find all dates that should have evaluations but don't"""
        missing_dates = []
        
        end_date = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
        start_date = end_date - timedelta(days=self.recovery_lookback_days)
        
        logger.info(f"Scanning for missing evaluations from {start_date.strftime('%Y-%m-%d')} "
                   f"to {end_date.strftime('%Y-%m-%d')}")
        
        current_date = start_date
        checked_count = 0
        
        while current_date < end_date:
            checked_count += 1
            
            # Get batch state
            state = await self.pipeline.state_service.get_or_create_batch_state(current_date)
            
            # Determine if this date needs evaluation
            needs_evaluation = False
            reason = ""
            
            if state.status == BatchStatus.COMPLETED:
                # Already completed, skip
                pass
            elif state.status == BatchStatus.SKIPPED:
                # Was skipped (no data), skip
                pass
            elif state.status in [BatchStatus.FAILED, BatchStatus.PARTIAL]:
                # Failed batch - check if can retry
                if state.can_retry():
                    needs_evaluation = True
                    reason = f"Failed batch (attempt {state.retry_count}/{state.max_retries})"
                else:
                    reason = "Failed batch - max retries exceeded"
            elif state.status == BatchStatus.PENDING:
                # Never processed - check if has data
                if await self.pipeline.db_service.has_data_for_date(current_date):
                    needs_evaluation = True
                    reason = "Never processed"
                else:
                    reason = "No data available"
            elif state.status == BatchStatus.RUNNING:
                # Check if stuck
                if state.started_at:
                    runtime = datetime.utcnow() - state.started_at
                    if runtime > timedelta(hours=2):
                        needs_evaluation = True
                        reason = f"Stuck in RUNNING state for {runtime}"
                        
            if needs_evaluation:
                missing_dates.append(current_date)
                logger.warning(f"Missing evaluation for {current_date.strftime('%Y-%m-%d')}: {reason}")
            elif reason:
                logger.debug(f"Skipping {current_date.strftime('%Y-%m-%d')}: {reason}")
                
            current_date += timedelta(days=1)
            
        logger.info(f"Scan complete: Checked {checked_count} dates, found {len(missing_dates)} missing")
        return missing_dates
        
    async def _recover_single_batch(self, batch_date: datetime) -> bool:
        """
        Recover a single batch with proper tracking
        
        Args:
            batch_date: Date to recover
            
        Returns:
            bool: True if recovery successful
        """
        recovery_start = datetime.utcnow()
        
        try:
            # Get current state
            state = await self.pipeline.state_service.get_or_create_batch_state(batch_date)
            
            # Double-check if we should attempt recovery
            if state.status == BatchStatus.COMPLETED:
                logger.info(f"Batch {batch_date.strftime('%Y-%m-%d')} already completed")
                return True
                
            if not state.can_retry():
                logger.error(f"Cannot retry batch {batch_date.strftime('%Y-%m-%d')} - "
                           f"max retries ({state.max_retries}) exceeded")
                await self._send_max_retry_alert(batch_date, state)
                return False
                
            # Mark as recovery attempt
            state.is_recovery = True
            state.recovery_attempted = True
            state.recovery_count += 1
            state.metadata["recovery_triggered_at"] = datetime.utcnow().isoformat()
            state.metadata["recovery_trigger"] = "RecoveryManager"
            state.metadata["is_recovery"] = True
            await self.pipeline.state_service.update_batch_state(state)
            
            # Log recovery attempt
            logger.info(f"Attempting recovery for {batch_date.strftime('%Y-%m-%d')} "
                       f"(attempt {state.retry_count + 1}/{state.max_retries})")
                       
            # Run evaluation
            success = await self.pipeline.run_daily_evaluation(batch_date)
            
            # Update recovery stats
            self.recovery_stats["total_recoveries"] += 1
            
            if success:
                recovery_duration = (datetime.utcnow() - recovery_start).total_seconds()
                logger.info(f"Successfully recovered batch for {batch_date.strftime('%Y-%m-%d')} "
                          f"in {recovery_duration:.1f} seconds")
                await self._send_recovery_success_notification(batch_date, state)
                return True
            else:
                logger.error(f"Recovery failed for {batch_date.strftime('%Y-%m-%d')}")
                return False
                
        except Exception as e:
            logger.error(f"Error recovering batch {batch_date.strftime('%Y-%m-%d')}: {e}", exc_info=True)
            return False
            
    async def _send_recovery_summary(self, 
                                   missing_dates: List[datetime], 
                                   recovered: int, 
                                   failed: int):
        """Send summary of recovery operation"""
        subject = f"Recovery Summary - {recovered} Recovered, {failed} Failed"
        
        # Group dates by status
        dates_html = "<ul>"
        for date in sorted(missing_dates):
            state = await self.pipeline.state_service.get_or_create_batch_state(date)
            if state.status == BatchStatus.COMPLETED:
                dates_html += f'<li>{date.strftime("%Y-%m-%d")} - Recovered successfully</li>'
            else:
                dates_html += f'<li>{date.strftime("%Y-%m-%d")} - Failed (Status: {state.status.value})</li>'
        dates_html += "</ul>"
        
        body = f"""
        <html>
        <body>
            <h2>Batch Recovery Summary</h2>
            <p>Automatic recovery process completed.</p>
            
            <h3>Summary:</h3>
            <ul>
                <li><strong>Total Batches Found:</strong> {len(missing_dates)}</li>
                <li><strong>Successfully Recovered:</strong> {recovered}</li>
                <li><strong>Failed to Recover:</strong> {failed}</li>
                <li><strong>Recovery Run Time:</strong> {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}</li>
            </ul>
            
            <h3>Batch Details:</h3>
            {dates_html}
            
            <h3>Overall Recovery Statistics:</h3>
            <ul>
                <li><strong>Total Recovery Attempts:</strong> {self.recovery_stats['total_recoveries']}</li>
                <li><strong>Successful Recoveries:</strong> {self.recovery_stats['successful_recoveries']}</li>
                <li><strong>Failed Recoveries:</strong> {self.recovery_stats['failed_recoveries']}</li>
            </ul>
        </body>
        </html>
        """
        
        await self.pipeline.email_service.send_alert(subject, body)
        
    async def _send_max_retry_alert(self, batch_date: datetime, state: BatchState):
        """Alert when max retries exceeded"""
        subject = f"CRITICAL: Max Retries Exceeded - {batch_date.strftime('%Y-%m-%d')}"
        
        body = f"""
        <html>
        <body>
            <h2 style="color: red;">Manual Intervention Required</h2>
            <p>The evaluation batch has exceeded maximum retry attempts.</p>
            
            <h3>Batch Details:</h3>
            <ul>
                <li><strong>Date:</strong> {batch_date.strftime('%Y-%m-%d')}</li>
                <li><strong>Batch ID:</strong> {state.batch_id or 'Not assigned'}</li>
                <li><strong>Status:</strong> {state.status.value}</li>
                <li><strong>Retry Count:</strong> {state.retry_count}/{state.max_retries}</li>
                <li><strong>Last Error:</strong> {state.error_message}</li>
                <li><strong>First Attempted:</strong> {state.created_at.strftime('%Y-%m-%d %H:%M:%S')}</li>
                <li><strong>Last Attempted:</strong> {state.updated_at.strftime('%Y-%m-%d %H:%M:%S')}</li>
            </ul>
            
            <h3>Action Required:</h3>
            <p>This batch will not be retried automatically. Manual investigation and intervention required.</p>
            
            <h3>Recommended Actions:</h3>
            <ol>
                <li>Check MongoDB connectivity and data availability</li>
                <li>Verify Google Cloud/Vertex AI service status</li>
                <li>Review error logs for root cause</li>
                <li>Run manual evaluation if needed</li>
            </ol>
        </body>
        </html>
        """
        
        await self.pipeline.email_service.send_alert(subject, body, priority="critical")
        
    async def _send_recovery_success_notification(self, batch_date: datetime, state: BatchState):
        """Send notification when recovery succeeds"""
        subject = f"Recovery Success - {batch_date.strftime('%Y-%m-%d')}"
        
        body = f"""
        <html>
        <body>
            <h2 style="color: green;">Batch Recovered Successfully</h2>
            <p>A previously failed batch has been successfully recovered through automatic recovery.</p>
            
            <h3>Recovery Details:</h3>
            <ul>
                <li><strong>Date:</strong> {batch_date.strftime('%Y-%m-%d')}</li>
                <li><strong>Batch ID:</strong> {state.batch_id}</li>
                <li><strong>Total Attempts:</strong> {state.retry_count}</li>
                <li><strong>Recovery Attempts:</strong> {state.recovery_count}</li>
                <li><strong>Total Evaluations:</strong> {state.total_pairs}</li>
                <li><strong>Passed:</strong> {state.passed}</li>
                <li><strong>Failed:</strong> {state.failed}</li>
                <li><strong>Accuracy:</strong> {state.accuracy:.2%}</li>
            </ul>
            
            <p>No further action required.</p>
        </body>
        </html>
        """
        
        await self.pipeline.email_service.send_alert(subject, body, priority="low")
        
    async def _send_recovery_error_alert(self, error: Exception):
        """Send alert when recovery process itself fails"""
        subject = "Recovery Process Error"
        
        body = f"""
        <html>
        <body>
            <h2>Recovery Process Error</h2>
            <p>The automatic recovery process encountered an error.</p>
            
            <h3>Error Details:</h3>
            <ul>
                <li><strong>Error Type:</strong> {type(error).__name__}</li>
                <li><strong>Error Message:</strong> {str(error)}</li>
                <li><strong>Time:</strong> {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}</li>
            </ul>
            
            <p>The recovery process may be incomplete. Please monitor for missed batches.</p>
        </body>
        </html>
        """
        
        await self.pipeline.email_service.send_alert(subject, body, priority="high")