import pandas as pd
import asyncio
import motor.motor_asyncio
from datetime import datetime
from faker import Faker
import hashlib
import os
from typing import Dict, Any

# Initialize Faker for generating untraceable account numbers
fake = Faker()

class MongoDBException(Exception):
    """Custom exception for MongoDB operations"""
    pass

async def get_db():
    """
    Get MongoDB database connection
    Replace with your actual connection string and database name
    """
    client = motor.motor_asyncio.AsyncIOMotorClient("mongodb://localhost:27017")
    return client.your_database_name

def make_account_untraceable(account_number: str) -> str:
    """
    Create an untraceable account number using faker and hashing
    """
    # Create a hash of the original account number for consistency
    hash_object = hashlib.md5(str(account_number).encode())
    hash_hex = hash_object.hexdigest()
    
    # Use the hash as seed for faker to ensure same account gets same fake number
    fake.seed_instance(int(hash_hex[:8], 16))
    
    # Generate a fake account number
    fake_account = fake.bothify(text='ACC-####-####-####')
    return fake_account

async def process_excel_to_mongo(excel_file_path: str):
    """
    Process Excel file and insert data into MongoDB collections
    """
    try:
        # Read Excel file
        df = pd.read_excel(excel_file_path)
        
        # Get database connection
        db = await get_db()
        
        # Get current timestamp
        current_timestamp = datetime.now()
        
        # Process each row
        for index, row in df.iterrows():
            # Create untraceable account number
            original_account = row.get('accountNumber', '')
            fake_account = make_account_untraceable(original_account)
            
            # Prepare document for collection1
            doc_collection1 = {
                "IVRCallId": row.get('IVRCallId'),
                "interactionId": row.get('interactionId'),
                "customerId": row.get('customerId'),
                "transcriptRefId": row.get('transcriptRefId'),
                "agentId": row.get('agentId'),
                "timestamp": current_timestamp,
                "timestamp_cst": str(current_timestamp),
                "startTimestamp": row.get('startTimestamp'),
                "LOB": row.get('LOB'),
                "postCallSummary": row.get('postCallSummary'),
                "utterance": row.get('utterance'),
                "utterance_type": row.get('utterance_type'),
                "accountNumber": fake_account,
                "created_at": current_timestamp,
                "processed_from_excel": True
            }
            
            # Prepare document for collection2 (you can modify fields as needed)
            doc_collection2 = {
                "IVRCallId": row.get('IVRCallId'),
                "interactionId": row.get('interactionId'),
                "customerId": row.get('customerId'),
                "transcriptRefId": row.get('transcriptRefId'),
                "agentId": row.get('agentId'),
                "timestamp": current_timestamp,
                "timestamp_cst": str(current_timestamp),
                "LOB": row.get('LOB'),
                "accountNumber": fake_account,
                "summary_data": {
                    "postCallSummary": row.get('postCallSummary'),
                    "utterance": row.get('utterance'),
                    "utterance_type": row.get('utterance_type')
                },
                "created_at": current_timestamp,
                "processed_from_excel": True
            }
            
            # Insert into collection1
            try:
                await db.collection1.insert_one(doc_collection1)
                print(f"Inserted row {index + 1} into collection1")
            except Exception as e:
                print(f"Error inserting row {index + 1} into collection1: {str(e)}")
                raise MongoDBException(f"Collection1 insert failed: {str(e)}")
            
            # Insert into collection2
            try:
                await db.collection2.insert_one(doc_collection2)
                print(f"Inserted row {index + 1} into collection2")
            except Exception as e:
                print(f"Error inserting row {index + 1} into collection2: {str(e)}")
                raise MongoDBException(f"Collection2 insert failed: {str(e)}")
        
        print(f"Successfully processed {len(df)} rows from Excel file")
        
    except FileNotFoundError:
        print(f"Excel file not found: {excel_file_path}")
        raise
    except Exception as e:
        print(f"Error processing Excel file: {str(e)}")
        raise MongoDBException(str(e))

async def bulk_insert_excel_to_mongo(excel_file_path: str):
    """
    Bulk insert version for better performance with large files
    """
    try:
        # Read Excel file
        df = pd.read_excel(excel_file_path)
        
        # Get database connection
        db = await get_db()
        
        # Get current timestamp
        current_timestamp = datetime.now()
        
        # Prepare bulk documents
        docs_collection1 = []
        docs_collection2 = []
        
        for index, row in df.iterrows():
            # Create untraceable account number
            original_account = row.get('accountNumber', '')
            fake_account = make_account_untraceable(original_account)
            
            # Document for collection1
            doc1 = {
                "IVRCallId": row.get('IVRCallId'),
                "interactionId": row.get('interactionId'),
                "customerId": row.get('customerId'),
                "transcriptRefId": row.get('transcriptRefId'),
                "agentId": row.get('agentId'),
                "timestamp": current_timestamp,
                "timestamp_cst": str(current_timestamp),
                "startTimestamp": row.get('startTimestamp'),
                "LOB": row.get('LOB'),
                "postCallSummary": row.get('postCallSummary'),
                "utterance": row.get('utterance'),
                "utterance_type": row.get('utterance_type'),
                "accountNumber": fake_account,
                "created_at": current_timestamp,
                "processed_from_excel": True
            }
            
            # Document for collection2
            doc2 = {
                "IVRCallId": row.get('IVRCallId'),
                "interactionId": row.get('interactionId'),
                "customerId": row.get('customerId'),
                "transcriptRefId": row.get('transcriptRefId'),
                "agentId": row.get('agentId'),
                "timestamp": current_timestamp,
                "timestamp_cst": str(current_timestamp),
                "LOB": row.get('LOB'),
                "accountNumber": fake_account,
                "summary_data": {
                    "postCallSummary": row.get('postCallSummary'),
                    "utterance": row.get('utterance'),
                    "utterance_type": row.get('utterance_type')
                },
                "created_at": current_timestamp,
                "processed_from_excel": True
            }
            
            docs_collection1.append(doc1)
            docs_collection2.append(doc2)
        
        # Bulk insert into both collections
        try:
            result1 = await db.collection1.insert_many(docs_collection1)
            print(f"Bulk inserted {len(result1.inserted_ids)} documents into collection1")
            
            result2 = await db.collection2.insert_many(docs_collection2)
            print(f"Bulk inserted {len(result2.inserted_ids)} documents into collection2")
            
        except Exception as e:
            print(f"Error during bulk insert: {str(e)}")
            raise MongoDBException(f"Bulk insert failed: {str(e)}")
            
    except Exception as e:
        print(f"Error in bulk insert process: {str(e)}")
        raise MongoDBException(str(e))

# Main execution function
async def main():
    """
    Main function to run the Excel to MongoDB ingestion
    """
    excel_file_path = "your_excel_file.xlsx"  # Replace with your Excel file path
    
    try:
        # Choose one of the methods below:
        
        # Method 1: Individual inserts (better for error tracking)
        await process_excel_to_mongo(excel_file_path)
        
        # Method 2: Bulk insert (better for performance)
        # await bulk_insert_excel_to_mongo(excel_file_path)
        
        print("Excel data ingestion completed successfully!")
        
    except Exception as e:
        print(f"Excel ingestion failed: {str(e)}")

# Run the script
if __name__ == "__main__":
    # Install required packages:
    # pip install pandas openpyxl motor faker
    
    asyncio.run(main())



# utility_functions.py

def get_timestamp():
    """
    Get current timestamp in CST (similar to reference code)
    """
    from datetime import datetime
    import pytz
    
    cst = pytz.timezone('US/Central')
    return datetime.now(cst).strftime('%Y-%m-%d %H:%M:%S %Z')

async def validate_excel_columns(excel_file_path: str, required_columns: list):
    """
    Validate that Excel file has all required columns
    """
    df = pd.read_excel(excel_file_path, nrows=0)  # Read only headers
    missing_columns = set(required_columns) - set(df.columns)
    
    if missing_columns:
        raise ValueError(f"Missing required columns: {missing_columns}")
    
    return True

# Example usage with validation
async def safe_excel_to_mongo(excel_file_path: str):
    """
    Safe version with validation
    """
    required_columns = [
        "IVRCallId", "interactionId", "customerId", "transcriptRefId",
        "agentId", "startTimestamp", "LOB", "postCallSummary",
        "utterance", "utterance_type", "accountNumber"
    ]
    
    # Validate columns first
    await validate_excel_columns(excel_file_path, required_columns)
    
    # Process the file
    await bulk_insert_excel_to_mongo(excel_file_path)


"""
Key Features:

Untraceable Account Numbers: Uses Faker with MD5 hashing to create consistent fake account numbers
Current Timestamp: Uses today's date for all timestamp fields
Dual Collection Insert: Inserts into both collection1 and collection2
Bulk Insert Option: For better performance with large files
Error Handling: Comprehensive error handling following the reference pattern
Async Operations: Maintains the async pattern from your reference code

To use this script:

Install required packages: pip install pandas openpyxl motor faker
Update the MongoDB connection string in get_db()
Replace "your_excel_file.xlsx" with your actual file path
Update collection names if needed
Run the script

The script provides both individual insert and bulk insert options. Use bulk insert for better performance with large Excel files.
"""