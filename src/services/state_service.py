"""
Batch State Management Service
Tracks execution state for recovery and monitoring
"""
import asyncio
from typing import List, Optional, Tuple, Dict, Any
from datetime import datetime, timedelta
import logging
from pymongo.errors import DuplicateKeyError
from src.models.batch_state import BatchState, BatchStatus, BatchRecoveryStrategy
from src.services.db_service import MongoDBService
from src.config.settings import settings

logger = logging.getLogger(__name__)


class StateService:
    """
    Manages batch execution state for tracking and recovery
    Provides atomic state transitions and recovery logic
    """
    
    def __init__(self, db_service: MongoDBService):
        self.db_service = db_service
        self.collection_name = "batch_states"
        self.recovery_strategy = BatchRecoveryStrategy(settings.BATCH_RECOVERY_STRATEGY)
        
    async def initialize(self):
        """Create indexes for state collection"""
        try:
            collection = self.db_service.db[self.collection_name]
            
            # Create indexes
            await collection.create_index([("batch_date", -1)], unique=True, background=True)
            await collection.create_index([("status", 1)], background=True)
            await collection.create_index([("created_at", -1)], background=True)
            
            # Compound indexes for queries
            await collection.create_index(
                [("status", 1), ("batch_date", -1)],
                background=True
            )
            await collection.create_index(
                [("status", 1), ("retry_count", 1)],
                background=True
            )
            
            logger.info("State service initialized with indexes")
            
        except Exception as e:
            logger.error(f"Error initializing state service: {e}")
            # Don't fail on index creation errors
            
    async def get_or_create_batch_state(self, batch_date: datetime) -> BatchState:
        """
        Get existing batch state or create new one
        Ensures batch_date is normalized to start of day
        """
        collection = self.db_service.db[self.collection_name]
        
        # Normalize to start of day
        batch_date = batch_date.replace(hour=0, minute=0, second=0, microsecond=0)
        
        # Try to find existing state
        doc = await collection.find_one({"batch_date": batch_date})
        
        if doc:
            # Remove MongoDB _id field
            doc.pop('_id', None)
            return BatchState(**doc)
            
        # Create new state
        state = BatchState(batch_date=batch_date)
        
        try:
            await collection.insert_one(state.dict())
            logger.info(f"Created new batch state for {batch_date.strftime('%Y-%m-%d')}")
        except DuplicateKeyError:
            # Race condition - another process created it
            doc = await collection.find_one({"batch_date": batch_date})
            doc.pop('_id', None)
            return BatchState(**doc)
            
        return state
        
    async def update_batch_state(self, state: BatchState) -> None:
        """
        Update batch state in database
        Maintains audit trail with previous_status
        """
        collection = self.db_service.db[self.collection_name]
        
        # Get current state to track changes
        current_doc = await collection.find_one({"batch_date": state.batch_date})
        if current_doc and current_doc.get('status') != state.status.value:
            state.previous_status = BatchStatus(current_doc['status'])
            
        # Update timestamp
        state.updated_at = datetime.utcnow()
        
        # Update in database
        result = await collection.update_one(
            {"batch_date": state.batch_date},
            {"$set": state.dict()},
            upsert=True
        )
        
        if result.modified_count > 0 or result.upserted_id:
            logger.debug(f"Updated batch state for {state.batch_date.strftime('%Y-%m-%d')} "
                        f"to {state.status.value}")
        else:
            logger.warning(f"No changes made to batch state for {state.batch_date.strftime('%Y-%m-%d')}")
            
    async def get_pending_batches(self, 
                                 start_date: Optional[datetime] = None,
                                 end_date: Optional[datetime] = None,
                                 include_failed: bool = True) -> List[BatchState]:
        """
        Get all batches that need processing
        
        Args:
            start_date: Start of date range
            end_date: End of date range
            include_failed: Whether to include failed batches
            
        Returns:
            List of BatchState objects that need processing
        """
        collection = self.db_service.db[self.collection_name]
        
        # Build query
        status_list = [BatchStatus.PENDING]
        if include_failed:
            status_list.extend([BatchStatus.FAILED, BatchStatus.PARTIAL])
            
        query = {"status": {"$in": [s.value for s in status_list]}}
        
        # Add date range filters
        if start_date or end_date:
            date_filter = {}
            if start_date:
                date_filter["$gte"] = start_date
            if end_date:
                date_filter["$lte"] = end_date
            query["batch_date"] = date_filter
            
        # Execute query
        cursor = collection.find(query).sort("batch_date", 1)
        
        states = []
        async for doc in cursor:
            doc.pop('_id', None)
            state = BatchState(**doc)
            
            # Check if can retry
            if state.can_retry():
                states.append(state)
            else:
                logger.info(f"Skipping batch {state.batch_date.strftime('%Y-%m-%d')} - "
                           f"max retries exceeded")
                
        return states
        
    async def check_missing_batches(self, lookback_days: int = 7) -> List[datetime]:
        """
        Check for dates that should have been processed but weren't
        
        Args:
            lookback_days: Number of days to look back
            
        Returns:
            List of dates that are missing evaluations
        """
        collection = self.db_service.db[self.collection_name]
        
        end_date = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
        start_date = end_date - timedelta(days=lookback_days)
        
        missing_dates = []
        current_date = start_date
        
        while current_date < end_date:
            # Check if batch exists
            doc = await collection.find_one({
                "batch_date": current_date,
                "status": BatchStatus.COMPLETED.value
            })
            
            if not doc:
                # Check if there's actually data for this date
                if await self.db_service.has_data_for_date(current_date):
                    # Check if there's a failed/pending state
                    failed_doc = await collection.find_one({"batch_date": current_date})
                    
                    if failed_doc:
                        state = BatchState(**failed_doc)
                        if state.can_retry():
                            missing_dates.append(current_date)
                            logger.warning(f"Found retryable batch for {current_date.strftime('%Y-%m-%d')} "
                                         f"(status: {state.status}, retries: {state.retry_count})")
                    else:
                        # No state at all - completely missed
                        missing_dates.append(current_date)
                        logger.warning(f"Found missing batch for {current_date.strftime('%Y-%m-%d')}")
                        
            current_date += timedelta(days=1)
            
        return missing_dates
        
    async def should_process_batch(self, batch_date: datetime) -> Tuple[bool, str]:
        """
        Determine if a batch should be processed
        
        Returns:
            Tuple of (should_process, reason)
        """
        state = await self.get_or_create_batch_state(batch_date)
        
        # Check completed
        if state.status == BatchStatus.COMPLETED:
            return False, "Batch already completed successfully"
            
        # Check if currently running
        if state.status == BatchStatus.RUNNING:
            # Check for stuck batch (running > 2 hours)
            if state.started_at:
                runtime = datetime.utcnow() - state.started_at
                if runtime > timedelta(hours=2):
                    logger.warning(f"Batch {batch_date.strftime('%Y-%m-%d')} stuck in RUNNING for {runtime}")
                    # Update status to failed so it can be retried
                    state.status = BatchStatus.FAILED
                    state.error_message = f"Batch stuck in RUNNING state for {runtime}"
                    await self.update_batch_state(state)
                    return True, "Batch was stuck, marked as failed for retry"
                else:
                    return False, f"Batch currently running ({runtime.total_seconds():.0f} seconds)"
                    
        # Check retry limit
        if not state.can_retry():
            return False, f"Max retries ({state.max_retries}) exceeded"
            
        # Check retry delay
        if state.retry_after and datetime.utcnow() < state.retry_after:
            wait_time = (state.retry_after - datetime.utcnow()).total_seconds()
            return False, f"Retry delayed for {wait_time:.0f} more seconds"
            
        # Check if data exists
        if not await self.db_service.has_data_for_date(batch_date):
            return False, "No data available for this date"
            
        # All checks passed
        if state.status == BatchStatus.FAILED:
            return True, f"Retrying failed batch (attempt {state.retry_count + 1}/{state.max_retries})"
        else:
            return True, "Batch ready for processing"
            
    async def get_batch_history(self, days: int = 30) -> List[Dict[str, Any]]:
        """Get batch processing history for monitoring"""
        collection = self.db_service.db[self.collection_name]
        
        start_date = datetime.utcnow() - timedelta(days=days)
        
        cursor = collection.find(
            {"batch_date": {"$gte": start_date}},
            {"_id": 0}
        ).sort("batch_date", -1)
        
        history = []
        async for doc in cursor:
            state = BatchState(**doc)
            history.append(state.to_summary_dict())
            
        return history
        
    async def cleanup_old_states(self, retention_days: int = 90) -> int:
        """
        Clean up old batch states beyond retention period
        
        Returns:
            Number of states deleted
        """
        collection = self.db_service.db[self.collection_name]
        
        cutoff_date = datetime.utcnow() - timedelta(days=retention_days)
        
        # Only delete completed batches
        result = await collection.delete_many({
            "batch_date": {"$lt": cutoff_date},
            "status": BatchStatus.COMPLETED.value
        })
        
        if result.deleted_count > 0:
            logger.info(f"Cleaned up {result.deleted_count} old batch states")
            
        return result.deleted_count