import pandas as pd
import asyncio
from typing import List, Dict
from motor.motor_asyncio import AsyncIOMotorDatabase
from app.db.mongodb_client import get_db  # Your existing connection module

class CustomerInteractionProcessor:
    def __init__(self):
        self.df = None
        
    async def get_aggregation_pipeline(self) -> List[Dict]:
        """Returns the modified aggregation pipeline"""
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
                        "$gte": "2025-05-28",
                        "$lt": "2025-05-29"
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
                    # Excluding preCallSummary as requested
                    "utterance": "$transcript.messages.utterance",
                    "utterance_type": "$transcript.messages.utterance_type",
                    # Excluding intent and sub_intent as requested
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
                    # Removed preCallSummary as requested
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
    
    async def fetch_data_to_dataframe(self, collection_name: str = "customer_interactions") -> pd.DataFrame:
        """
        Fetches data from MongoDB using the aggregation pipeline and returns a pandas DataFrame
        """
        # Get database connection using your existing singleton
        db = await get_db()
        
        # Get the collection
        collection = db[collection_name]
        
        # Get the aggregation pipeline
        pipeline = await self.get_aggregation_pipeline()
        
        # Execute aggregation
        cursor = collection.aggregate(pipeline)
        results = await cursor.to_list(length=None)
        
        # Convert to DataFrame
        self.df = pd.DataFrame(results)
        
        # Remove MongoDB _id if present
        if '_id' in self.df.columns:
            self.df = self.df.drop('_id', axis=1)
        
        # Add the required columns at the end
        self.df['results'] = ''
        self.df['reason'] = ''
        
        return self.df
    
    def get_dataframe(self) -> pd.DataFrame:
        """Returns the stored dataframe"""
        if self.df is None:
            raise ValueError("DataFrame not initialized. Call fetch_data_to_dataframe() first.")
        return self.df

# Create a singleton instance
processor = CustomerInteractionProcessor()
