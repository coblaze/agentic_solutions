import pandas as pd
import asyncio
from typing import List, Dict, Optional
from datetime import datetime, timedelta
from motor.motor_asyncio import AsyncIOMotorDatabase
from app.db.mongodb_client import get_db

class CustomerInteractionProcessor:
    def __init__(self):
        self.df = None
        
    async def get_aggregation_pipeline(self, hours_back: int = 24) -> List[Dict]:
        """
        Returns the aggregation pipeline with time-based filtering
        
        Args:
            hours_back: Number of hours to look back (default: 24)
        """
        # Calculate time window
        end_time = datetime.now()
        start_time = end_time - timedelta(hours=hours_back)
        
        # Convert to ISO format strings for MongoDB
        start_time_str = start_time.isoformat()
        end_time_str = end_time.isoformat()
        
        print(f"Fetching data from {start_time_str} to {end_time_str}")
        
        return [
            {
                "$lookup": {
                    "from": "transcripts",
                    "localField": "customerInteraction.agentInteractions.accountInteractions.transcriptRefId",
                    "foreignField": "id",
                    "as": "transcript"
                }
            },
            {
                "$match": {
                    "customerInteraction.agentInteractions.callDetails.startTimestamp": {
                        "$gte": start_time_str,
                        "$lt": end_time_str
                    }
                }
            },
            {"$unwind": "$transcript"},
            {"$unwind": "$transcript.messages"},
            {
                "$project": {
                    "IVRCallId": "$customerInteraction.IVRCallId",
                    "interactionId": "$transcript.interactionId",
                    "customerId": "$transcript.CCId",
                    "transcriptRefId": "$transcript.id",
                    "agentId": "$transcript.agentId",
                    "timestamp": "$transcript.messages.timestamp",
                    "startTimestamp": "$customerInteraction.agentInteractions.callDetails.startTimestamp",
                    "LOB": "$transcript.LOB",
                    "postCallSummary": "$customerInteraction.agentInteractions.accountInteractions.postCallSummaryDetails.summary",
                    "utterance": "$transcript.messages.utterance",
                    "utterance_type": "$transcript.messages.utterance_type",
                    "accountNumber": "$transcript.accountNumber"
                }
            },
            {"$sort": {"timestamp": 1}},
            {
                "$group": {
                    "_id": {
                        "interactionId": "$interactionId",
                        "agentId": "$agentId",
                        "customerId": "$customerId",
                        "LOB": "$LOB"
                    },
                    "messages": {
                        "$push": {
                            "utterance": "$utterance",
                            "utterance_type": "$utterance_type",
                            "timestamp": "$startTimestamp"
                        }
                    },
                    "_object": {"$first": "$$ROOT"}
                }
            },
            {
                "$addFields": {
                    "message": {
                        "$map": {
                            "input": "$messages",
                            "as": "msg",
                            "in": {
                                "$concat": [
                                    "$$msg.utterance_type",
                                    ": ",
                                    {"$ifNull": ["$$msg.utterance", ""]}
                                ]
                            }
                        }
                    }
                }
            },
            {
                "$project": {
                    "IVRCallId": "$_object.IVRCallId",
                    "interactionId": "$_object.interactionId",
                    "customerId": "$_object.customerId",
                    "LOB": "$_object.LOB",
                    "agentId": "$_object.agentId",
                    "startTimestamp": {"$arrayElemAt": ["$_object.startTimestamp", 0]},
                    "transcript": {
                        "$reduce": {
                            "input": "$message",
                            "initialValue": "",
                            "in": {
                                "$cond": [
                                    {"$eq": ["$$value", ""]},
                                    "$$this",
                                    {"$concat": ["$$value", "\n", "$$this"]}
                                ]
                            }
                        }
                    },
                    "transcriptRefId": "$_object.transcriptRefId",
                    "accountNumber": "$_object.accountNumber",
                    "postCallSummary": "$_object.postCallSummary"
                }
            }
        ]
    
    async def fetch_data_to_dataframe(
        self, 
        collection_name: str = "customer_interactions",
        hours_back: int = 24,
        max_retries: int = 3
    ) -> pd.DataFrame:
        """
        Fetches data from MongoDB with retry mechanism
        
        Args:
            collection_name: MongoDB collection name
            hours_back: Number of hours to look back
            max_retries: Maximum number of retry attempts
        """
        retry_count = 0
        last_error = None
        
        while retry_count < max_retries:
            try:
                # Get database connection
                db = await get_db()
                collection = db[collection_name]
                
                # Get the aggregation pipeline
                pipeline = await self.get_aggregation_pipeline(hours_back)
                
                # Execute aggregation
                cursor = collection.aggregate(pipeline)
                results = await cursor.to_list(length=None)
                
                if not results:
                    print(f"No data found in the last {hours_back} hours")
                    self.df = pd.DataFrame()  # Empty dataframe
                else:
                    # Convert to DataFrame
                    self.df = pd.DataFrame(results)
                    
                    # Remove MongoDB _id if present
                    if '_id' in self.df.columns:
                        self.df = self.df.drop('_id', axis=1)
                    
                    print(f"Successfully fetched {len(self.df)} records")
                
                # Add the required columns
                self.df['results'] = ''
                self.df['reason'] = ''
                
                return self.df
                
            except Exception as e:
                retry_count += 1
                last_error = e
                print(f"Error fetching data (attempt {retry_count}/{max_retries}): {e}")
                
                if retry_count < max_retries:
                    wait_time = 2 ** retry_count  # Exponential backoff
                    print(f"Waiting {wait_time} seconds before retry...")
                    await asyncio.sleep(wait_time)
        
        # If all retries failed
        raise Exception(f"Failed to fetch data after {max_retries} attempts. Last error: {last_error}")
    
    def get_dataframe(self) -> pd.DataFrame:
        """Returns the stored dataframe"""
        if self.df is None:
            raise ValueError("DataFrame not initialized. Call fetch_data_to_dataframe() first.")
        return self.df

# Create a singleton instance
processor = CustomerInteractionProcessor()