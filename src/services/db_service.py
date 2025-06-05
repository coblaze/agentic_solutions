"""
MongoDB Database Service
Handles all database operations including aggregations and state management
"""
import asyncio
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime, timedelta
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase
from pymongo import ASCENDING, DESCENDING, errors
from pymongo.errors import DuplicateKeyError, ConnectionFailure
import logging
from src.config.settings import settings
from src.models.evaluation import TranscriptSummaryPair, EvaluationResult, BatchEvaluation

logger = logging.getLogger(__name__)


class MongoDBService:
    """
    Asynchronous MongoDB service for all database operations
    Uses Motor for async MongoDB operations
    """
    
    def __init__(self):
        self.client: Optional[AsyncIOMotorClient] = None
        self.db: Optional[AsyncIOMotorDatabase] = None
        self._connected: bool = False
        self._connection_retries: int = 3
        self._connection_timeout: int = 30
        
    @property
    def is_connected(self) -> bool:
        """Check if database is connected"""
        return self._connected and self.client is not None
        
    async def connect(self) -> None:
        """
        Initialize MongoDB connection with retry logic
        """
        for attempt in range(self._connection_retries):
            try:
                logger.info(f"Attempting MongoDB connection (attempt {attempt + 1}/{self._connection_retries})")
                
                # Create client with connection pooling
                self.client = AsyncIOMotorClient(
                    settings.MONGODB_URI,
                    serverSelectionTimeoutMS=self._connection_timeout * 1000,
                    connectTimeoutMS=self._connection_timeout * 1000,
                    maxPoolSize=50,
                    minPoolSize=10
                )
                
                # Select database
                self.db = self.client[settings.MONGODB_DATABASE]
                
                # Test connection
                await self.client.admin.command('ping')
                
                # Create indexes
                await self._create_indexes()
                
                self._connected = True
                logger.info(f"Successfully connected to MongoDB at {settings.MONGODB_URI}")
                return
                
            except ConnectionFailure as e:
                logger.error(f"MongoDB connection attempt {attempt + 1} failed: {e}")
                if attempt < self._connection_retries - 1:
                    await asyncio.sleep(5 * (attempt + 1))  # Exponential backoff
                else:
                    raise Exception(f"Failed to connect to MongoDB after {self._connection_retries} attempts")
                    
            except Exception as e:
                logger.error(f"Unexpected error connecting to MongoDB: {e}")
                raise
                
    async def disconnect(self) -> None:
        """Close MongoDB connection"""
        if self.client:
            self.client.close()
            self._connected = False
            logger.info("Disconnected from MongoDB")
            
    async def _create_indexes(self) -> None:
        """Create necessary indexes for optimal performance"""
        try:
            logger.info("Creating MongoDB indexes...")
            
            # Transcripts collection indexes
            transcripts_col = self.db[settings.MONGODB_COLLECTION_TRANSCRIPTS]
            await transcripts_col.create_index([("id", ASCENDING)], unique=True, background=True)
            await transcripts_col.create_index([("interactionId", ASCENDING)], background=True)
            await transcripts_col.create_index([("createdAt", DESCENDING)], background=True)
            
            # Summaries collection indexes
            summaries_col = self.db[settings.MONGODB_COLLECTION_SUMMARIES]
            await summaries_col.create_index(
                [("customerInteraction.agentInteractions.callDetails.startTimestamp", DESCENDING)],
                background=True
            )
            await summaries_col.create_index([("createdAt", DESCENDING)], background=True)
            
            # Evaluations collection indexes
            evaluations_col = self.db[settings.MONGODB_COLLECTION_EVALUATIONS]
            await evaluations_col.create_index([("interaction_id", ASCENDING)], unique=True, background=True)
            await evaluations_col.create_index([("batch_id", ASCENDING)], background=True)
            await evaluations_col.create_index([("evaluated_at", DESCENDING)], background=True)
            await evaluations_col.create_index([("status", ASCENDING)], background=True)
            
            # Compound index for efficient queries
            await evaluations_col.create_index(
                [("batch_id", ASCENDING), ("status", ASCENDING)],
                background=True
            )
            
            # Batches collection indexes
            batches_col = self.db[settings.MONGODB_COLLECTION_BATCHES]
            await batches_col.create_index([("batch_id", ASCENDING)], unique=True, background=True)
            await batches_col.create_index([("evaluation_date", DESCENDING)], background=True)
            await batches_col.create_index([("created_at", DESCENDING)], background=True)
            
            # Batch states collection indexes
            states_col = self.db["batch_states"]
            await states_col.create_index([("batch_date", DESCENDING)], unique=True, background=True)
            await states_col.create_index([("status", ASCENDING)], background=True)
            await states_col.create_index([("created_at", DESCENDING)], background=True)
            
            logger.info("MongoDB indexes created successfully")
            
        except Exception as e:
            logger.error(f"Error creating indexes: {e}")
            # Don't fail on index creation errors
            
    async def get_unevaluated_pairs(self, date_range: Tuple[datetime, datetime]) -> List[TranscriptSummaryPair]:
        """
        Retrieve transcript-summary pairs that haven't been evaluated
        Uses aggregation pipeline for efficient joining
        
        Args:
            date_range: Tuple of (start_date, end_date) for filtering
            
        Returns:
            List of TranscriptSummaryPair objects ready for evaluation
        """
        try:
            start_date, end_date = date_range
            logger.info(f"Retrieving unevaluated pairs for {start_date.strftime('%Y-%m-%d')}")
            
            # Build aggregation pipeline
            pipeline = [
                # Stage 1: Match summaries in date range
                {
                    "$match": {
                        "customerInteraction.agentInteractions.callDetails.startTimestamp": {
                            "$gte": start_date,
                            "$lt": end_date
                        }
                    }
                },
                
                # Stage 2: Lookup transcripts
                {
                    "$lookup": {
                        "from": settings.MONGODB_COLLECTION_TRANSCRIPTS,
                        "localField": "customerInteraction.agentInteractions.accountInteractions.transcriptRefId",
                        "foreignField": "id",
                        "as": "transcript_data"
                    }
                },
                
                # Stage 3: Unwind transcript array
                {"$unwind": "$transcript_data"},
                
                # Stage 4: Check if already evaluated
                {
                    "$lookup": {
                        "from": settings.MONGODB_COLLECTION_EVALUATIONS,
                        "localField": "transcript_data.interactionId",
                        "foreignField": "interaction_id",
                        "as": "existing_evaluation"
                    }
                },
                
                # Stage 5: Filter out already evaluated
                {
                    "$match": {
                        "existing_evaluation": {"$size": 0}
                    }
                },
                
                # Stage 6: Process messages
                {"$unwind": "$transcript_data.messages"},
                
                # Stage 7: Group messages by interaction
                {
                    "$group": {
                        "_id": {
                            "interactionId": "$transcript_data.interactionId",
                            "IVRCallId": "$customerInteraction.IVRCallId",
                            "customerId": "$transcript_data.CCId",
                            "transcriptRefId": "$transcript_data.id",
                            "agentId": "$transcript_data.agentId",
                            "LOB": "$transcript_data.LOB",
                            "accountNumber": "$transcript_data.accountNumber",
                            "startTimestamp": "$customerInteraction.agentInteractions.callDetails.startTimestamp",
                            "postCallSummary": "$customerInteraction.agentInteractions.accountInteractions.postCallSummaryDetails.summary"
                        },
                        "messages": {
                            "$push": {
                                "utterance": "$transcript_data.messages.utterance",
                                "utterance_type": "$transcript_data.messages.utterance_type"
                            }
                        }
                    }
                },
                
                # Stage 8: Format the transcript
                {
                    "$addFields": {
                        "formatted_transcript": {
                            "$reduce": {
                                "input": "$messages",
                                "initialValue": "",
                                "in": {
                                    "$concat": [
                                        "$$value",
                                        {"$cond": [{"$eq": ["$$value", ""]}, "", "\n"]},
                                        {"$ifNull": ["$$this.utterance_type", "Unknown"]},
                                        ": ",
                                        {"$ifNull": ["$$this.utterance", ""]}
                                    ]
                                }
                            }
                        }
                    }
                },
                
                # Stage 9: Final projection
                {
                    "$project": {
                        "_id": 0,
                        "interaction_id": "$_id.interactionId",
                        "ivr_call_id": "$_id.IVRCallId",
                        "customer_id": "$_id.customerId",
                        "transcript_ref_id": "$_id.transcriptRefId",
                        "agent_id": "$_id.agentId",
                        "lob": "$_id.LOB",
                        "account_number": "$_id.accountNumber",
                        "start_timestamp": "$_id.startTimestamp",
                        "transcript": "$formatted_transcript",
                        "post_call_summary": "$_id.postCallSummary"
                    }
                },
                
                # Stage 10: Sort by timestamp
                {"$sort": {"start_timestamp": 1}}
            ]
            
            # Execute aggregation
            summaries_col = self.db[settings.MONGODB_COLLECTION_SUMMARIES]
            cursor = summaries_col.aggregate(pipeline, allowDiskUse=True)
            
            # Convert to TranscriptSummaryPair objects
            pairs = []
            async for doc in cursor:
                try:
                    # Validate required fields
                    if doc.get("transcript") and doc.get("post_call_summary"):
                        pair = TranscriptSummaryPair(**doc)
                        pairs.append(pair)
                    else:
                        logger.warning(f"Skipping pair with missing data: {doc.get('interaction_id')}")
                except Exception as e:
                    logger.error(f"Error creating TranscriptSummaryPair: {e}, doc: {doc}")
                    
            logger.info(f"Retrieved {len(pairs)} unevaluated transcript-summary pairs")
            return pairs
            
        except Exception as e:
            logger.error(f"Error retrieving unevaluated pairs: {e}")
            raise
            
    async def save_evaluation_results(self, 
                                    results: List[EvaluationResult], 
                                    batch: BatchEvaluation) -> None:
        """
        Save evaluation results and batch information
        Uses bulk operations for efficiency
        """
        try:
            logger.info(f"Saving {len(results)} evaluation results for batch {batch.batch_id}")
            
            # Save individual evaluations
            if results:
                evaluations_col = self.db[settings.MONGODB_COLLECTION_EVALUATIONS]
                
                # Convert to documents
                evaluation_docs = [result.dict() for result in results]
                
                # Use bulk insert with ordered=False for better performance
                try:
                    result = await evaluations_col.insert_many(
                        evaluation_docs,
                        ordered=False
                    )
                    logger.info(f"Inserted {len(result.inserted_ids)} evaluation results")
                except errors.BulkWriteError as e:
                    # Handle duplicate key errors gracefully
                    inserted = e.details.get('nInserted', 0)
                    logger.warning(f"Bulk insert completed with errors. Inserted: {inserted}")
                    
            # Save batch information
            batches_col = self.db[settings.MONGODB_COLLECTION_BATCHES]
            batch_doc = batch.dict()
            
            # Use replace_one with upsert to handle retries
            await batches_col.replace_one(
                {"batch_id": batch.batch_id},
                batch_doc,
                upsert=True
            )
            
            logger.info(f"Saved batch information for {batch.batch_id}")
            
        except Exception as e:
            logger.error(f"Error saving evaluation results: {e}")
            raise
            
    async def get_batch_evaluation(self, batch_id: str) -> Optional[BatchEvaluation]:
        """Retrieve batch evaluation by ID"""
        try:
            batches_col = self.db[settings.MONGODB_COLLECTION_BATCHES]
            doc = await batches_col.find_one({"batch_id": batch_id})
            
            if doc:
                # Remove MongoDB's _id field
                doc.pop('_id', None)
                return BatchEvaluation(**doc)
            return None
            
        except Exception as e:
            logger.error(f"Error retrieving batch evaluation: {e}")
            raise
            
    async def get_evaluation_results_by_batch(self, batch_id: str) -> List[EvaluationResult]:
        """Retrieve all evaluation results for a specific batch"""
        try:
            evaluations_col = self.db[settings.MONGODB_COLLECTION_EVALUATIONS]
            
            # Use cursor for memory efficiency with large batches
            cursor = evaluations_col.find(
                {"batch_id": batch_id},
                {"_id": 0}  # Exclude MongoDB _id
            ).sort("evaluated_at", ASCENDING)
            
            results = []
            async for doc in cursor:
                try:
                    results.append(EvaluationResult(**doc))
                except Exception as e:
                    logger.error(f"Error parsing evaluation result: {e}")
                    
            return results
            
        except Exception as e:
            logger.error(f"Error retrieving evaluation results: {e}")
            raise
            
    async def has_data_for_date(self, date: datetime) -> bool:
        """
        Check if there's data to process for a given date
        Used by recovery manager to determine if evaluation is needed
        """
        try:
            summaries_col = self.db[settings.MONGODB_COLLECTION_SUMMARIES]
            
            start_time = date.replace(hour=0, minute=0, second=0, microsecond=0)
            end_time = start_time + timedelta(days=1)
            
            # Use count_documents with limit for efficiency
            count = await summaries_col.count_documents(
                {
                    "customerInteraction.agentInteractions.callDetails.startTimestamp": {
                        "$gte": start_time,
                        "$lt": end_time
                    }
                },
                limit=1  # We only need to know if at least one exists
            )
            
            return count > 0
            
        except Exception as e:
            logger.error(f"Error checking data existence: {e}")
            return False
            
    async def get_daily_call_count(self, date: datetime) -> int:
        """Get total number of calls for a specific date"""
        try:
            summaries_col = self.db[settings.MONGODB_COLLECTION_SUMMARIES]
            
            start_time = date.replace(hour=0, minute=0, second=0, microsecond=0)
            end_time = start_time + timedelta(days=1)
            
            count = await summaries_col.count_documents({
                "customerInteraction.agentInteractions.callDetails.startTimestamp": {
                    "$gte": start_time,
                    "$lt": end_time
                }
            })
            
            return count
            
        except Exception as e:
            logger.error(f"Error getting daily call count: {e}")
            return 0
            
    async def health_check(self) -> bool:
        """Perform database health check"""
        try:
            if not self.is_connected:
                return False
                
            # Ping the database
            await self.client.admin.command('ping')
            return True
            
        except Exception as e:
            logger.error(f"Database health check failed: {e}")
            return False