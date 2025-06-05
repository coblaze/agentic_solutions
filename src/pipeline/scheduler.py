"""
Autonomous Scheduler
Runs evaluation pipeline daily at 12:01 AM without human intervention
Handles all recovery scenarios automatically
"""
import asyncio
import signal
from datetime import datetime, timedelta, time as dt_time
import pytz
import logging
from typing import Optional
from src.pipeline.evaluator import AutonomousEvaluationPipeline
from src.pipeline.recovery_manager import RecoveryManager
from src.utils.monitoring import HealthChecker
from src.config.settings import settings

logger = logging.getLogger(__name__)


class AutonomousScheduler:
    """
    Fully autonomous scheduler that runs daily evaluations
    and handles all recovery scenarios without human intervention
    """
    
    def __init__(self):
        self.pipeline = AutonomousEvaluationPipeline()
        self.recovery_manager = RecoveryManager()
        self.health_checker = HealthChecker()
        self.timezone = pytz.timezone(settings.TIMEZONE)
        
        # Parse run time from settings (format: "HH:MM")
        hour, minute = map(int, settings.RUN_TIME.split(':'))
        self.run_time = dt_time(hour, minute)  # Default 00:01 (12:01 AM)
        
        self.is_running = False
        self.shutdown_event = asyncio.Event()
        self._setup_signal_handlers()
        
        logger.info(f"Scheduler configured to run at {self.run_time} {settings.TIMEZONE}")
        
    def _setup_signal_handlers(self):
        """Setup graceful shutdown handlers"""
        for sig in [signal.SIGINT, signal.SIGTERM]:
            signal.signal(sig, self._signal_handler)
            
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully"""
        logger.info(f"Received signal {signum}, initiating graceful shutdown...")
        self.shutdown_event.set()
        
    def start(self):
        """Start the autonomous scheduler"""
        logger.info(f"Autonomous scheduler starting - will run daily at {self.run_time} {settings.TIMEZONE}")
        
        try:
            asyncio.run(self._run_autonomous_loop())
        except KeyboardInterrupt:
            logger.info("Scheduler stopped by keyboard interrupt")
        except Exception as e:
            logger.critical(f"Scheduler crashed: {e}", exc_info=True)
            raise
            
    async def _run_autonomous_loop(self):
        """
        Main autonomous loop that runs forever
        Executes evaluation at configured time daily
        """
        self.is_running = True
        startup_time = datetime.now(self.timezone)
        
        try:
            logger.info("="*60)
            logger.info("Autonomous LLM Evaluation Pipeline Started")
            logger.info(f"Startup Time: {startup_time.strftime('%Y-%m-%d %H:%M:%S %Z')}")
            logger.info(f"Daily Run Time: {self.run_time} {settings.TIMEZONE}")
            logger.info("="*60)
            
            # Initialize pipeline and recovery manager
            logger.info("Initializing pipeline services...")
            await self.pipeline.initialize()
            await self.recovery_manager.initialize(self.pipeline)
            logger.info("Pipeline services initialized successfully")
            
            # Perform initial health check
            logger.info("Performing initial system health check...")
            if await self.health_checker.is_healthy():
                logger.info("✅ System health check passed")
            else:
                logger.warning("⚠️ System health check failed - continuing anyway")
                await self.health_checker.send_health_alert()
            
            # Check for any missed batches on startup
            logger.info("Checking for missed batches on startup...")
            recovered = await self.recovery_manager.recover_all_missed_batches()
            
            if recovered > 0:
                logger.info(f"✅ Recovered {recovered} missed batches on startup")
            else:
                logger.info("No missed batches found on startup")
                
            logger.info("Entering main scheduler loop...")
            
            # Main scheduling loop
            loop_count = 0
            while not self.shutdown_event.is_set():
                loop_count += 1
                
                try:
                    # Calculate next run time
                    now = datetime.now(self.timezone)
                    next_run = self._calculate_next_run(now)
                    
                    # Log next execution time
                    wait_seconds = (next_run - now).total_seconds()
                    wait_hours = wait_seconds / 3600
                    
                    logger.info(f"Scheduler loop #{loop_count}")
                    logger.info(f"Current time: {now.strftime('%Y-%m-%d %H:%M:%S %Z')}")
                    logger.info(f"Next evaluation: {next_run.strftime('%Y-%m-%d %H:%M:%S %Z')}")
                    logger.info(f"Wait time: {wait_hours:.1f} hours ({wait_seconds:.0f} seconds)")
                    
                    # Wait until next run time or shutdown
                    try:
                        await asyncio.wait_for(
                            self.shutdown_event.wait(),
                            timeout=wait_seconds
                        )
                        # Shutdown was requested
                        logger.info("Shutdown requested, exiting scheduler loop")
                        break
                    except asyncio.TimeoutError:
                        # Time to run evaluation
                        logger.info("Scheduled evaluation time reached")
                        
                    # Execute daily evaluation
                    await self._execute_daily_evaluation()
                    
                    # Small delay to prevent any timing issues
                    await asyncio.sleep(60)
                    
                except Exception as e:
                    logger.error(f"Error in scheduler loop iteration {loop_count}: {e}", exc_info=True)
                    
                    # Send alert about scheduler error
                    await self._send_scheduler_error_alert(e, loop_count)
                    
                    # Continue running even if there's an error
                    logger.info("Continuing scheduler after error...")
                    await asyncio.sleep(300)  # Wait 5 minutes before continuing
                    
        except Exception as e:
            logger.critical(f"Critical scheduler error: {e}", exc_info=True)
            await self._send_critical_scheduler_alert(e)
            raise
        finally:
            self.is_running = False
            
            # Cleanup
            logger.info("Performing scheduler cleanup...")
            await self.pipeline.cleanup()
            
            # Log shutdown
            shutdown_time = datetime.now(self.timezone)
            runtime = shutdown_time - startup_time
            logger.info("="*60)
            logger.info("Autonomous Scheduler Stopped")
            logger.info(f"Shutdown Time: {shutdown_time.strftime('%Y-%m-%d %H:%M:%S %Z')}")
            logger.info(f"Total Runtime: {runtime}")
            logger.info("="*60)
            
    def _calculate_next_run(self, current_time: datetime) -> datetime:
        """Calculate the next run time in the configured timezone"""
        # Get next occurrence of run_time
        next_run = current_time.replace(
            hour=self.run_time.hour,
            minute=self.run_time.minute,
            second=0,
            microsecond=0
        )
        
        # If we've already passed today's run time, schedule for tomorrow
        if current_time >= next_run:
            next_run += timedelta(days=1)
            
        return next_run
        
    async def _execute_daily_evaluation(self):
        """
        Execute the daily evaluation for yesterday's data
        This runs at configured time to process the previous day's calls
        """
        execution_start = datetime.now(self.timezone)
        
        try:
            # Calculate yesterday's date (the day we're evaluating)
            evaluation_date = execution_start - timedelta(days=1)
            evaluation_date = evaluation_date.replace(hour=0, minute=0, second=0, microsecond=0)
            
            logger.info("="*60)
            logger.info(f"Starting scheduled daily evaluation")
            logger.info(f"Execution time: {execution_start.strftime('%Y-%m-%d %H:%M:%S %Z')}")
            logger.info(f"Evaluating data for: {evaluation_date.strftime('%Y-%m-%d')}")
            logger.info("="*60)
            
            # Perform system health check
            logger.info("Performing pre-evaluation health check...")
            health_status = await self.health_checker.is_healthy()
            
            if not health_status:
                logger.warning("⚠️ System health check failed")
                await self.health_checker.send_health_alert()
                # Continue anyway - let the pipeline handle specific failures
            else:
                logger.info("✅ System health check passed")
            
            # Run the evaluation pipeline
            pipeline_start = datetime.utcnow()
            success = await self.pipeline.run_daily_evaluation(evaluation_date)
            pipeline_duration = (datetime.utcnow() - pipeline_start).total_seconds()
            
            # Log execution result
            if success:
                logger.info(f"✅ Daily evaluation completed successfully in {pipeline_duration:.1f} seconds")
            else:
                logger.error(f"❌ Daily evaluation failed after {pipeline_duration:.1f} seconds")
                
            # After daily evaluation, check for any other missed batches
            # This ensures we catch up on any gaps
            logger.info("Checking for other missed batches...")
            recovered = await self.recovery_manager.check_and_recover_missed_batches()
            
            if recovered > 0:
                logger.info(f"✅ Recovered {recovered} additional missed batches")
                
            # Clean up old batch states periodically (once a week on Sunday)
            if execution_start.weekday() == 6:  # Sunday
                logger.info("Performing weekly cleanup of old batch states...")
                deleted = await self.pipeline.state_service.cleanup_old_states()
                logger.info(f"Cleaned up {deleted} old batch states")
                
        except Exception as e:
            logger.error(f"Error in daily evaluation execution: {e}", exc_info=True)
            await self._send_evaluation_error_alert(e, evaluation_date)
            # Recovery manager will handle this in the next run
            
        finally:
            execution_end = datetime.now(self.timezone)
            total_duration = (execution_end - execution_start).total_seconds()
            logger.info(f"Daily execution completed in {total_duration:.1f} seconds")
            logger.info("="*60)
            
    async def _send_scheduler_error_alert(self, error: Exception, loop_count: int):
        """Send alert for non-critical scheduler errors"""
        subject = "⚠️ Scheduler Error - Continuing Operation"
        
        body = f"""
        <html>
        <body>
            <h2>⚠️ Scheduler Error Detected</h2>
            <p>The scheduler encountered an error but is continuing operation.</p>
            
            <h3>Error Details:</h3>
            <ul>
                <li><strong>Loop Iteration:</strong> {loop_count}</li>
                <li><strong>Error Type:</strong> {type(error).__name__}</li>
                <li><strong>Error Message:</strong> {str(error)}</li>
                <li><strong>Time:</strong> {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}</li>
            </ul>
            
            <p>The scheduler will continue running. Monitor for repeated errors.</p>
        </body>
        </html>
        """
        
        await self.pipeline.email_service.send_alert(subject, body)
        
    async def _send_critical_scheduler_alert(self, error: Exception):
        """Send alert for critical scheduler failures"""
        subject = "🚨 CRITICAL: Scheduler System Failure"
        
        body = f"""
        <html>
        <body>
            <h2 style="color: red;">🚨 Critical Scheduler Failure</h2>
            <p>The evaluation scheduler has crashed and stopped running.</p>
            
            <h3>Error Details:</h3>
            <ul>
                <li><strong>Error Type:</strong> {type(error).__name__}</li>
                <li><strong>Error Message:</strong> {str(error)}</li>
                <li><strong>Time:</strong> {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}</li>
            </ul>
            
            <p><strong>IMMEDIATE ACTION REQUIRED:</strong> The scheduler must be restarted manually.</p>
        </body>
        </html>
        """
        
        try:
            await self.pipeline.email_service.send_alert(subject, body, priority="critical")
        except:
            # If email fails, at least log it
            logger.critical(f"Failed to send critical alert: {subject}")
            
    async def _send_evaluation_error_alert(self, error: Exception, evaluation_date: datetime):
        """Send alert when daily evaluation fails"""
        subject = f"❌ Daily Evaluation Failed - {evaluation_date.strftime('%Y-%m-%d')}"
        
        body = f""""""
