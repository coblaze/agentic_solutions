import asyncio
from datetime import datetime, timedelta
from app.db.mongodb_client import get_db

async def check_available_dates():
    """Check what dates have data in MongoDB"""
    print("🔍 Checking MongoDB for available data...\n")
    
    # Connect to database
    db = await get_db()
    collection = db["customer_interactions"]
    
    # Find recent records
    pipeline = [
        {
            "$project": {
                "timestamp": "$customerInteraction.agentInteractions.callDetails.startTimestamp"
            }
        },
        {"$sort": {"timestamp": -1}},
        {"$limit": 100}
    ]
    
    cursor = collection.aggregate(pipeline)
    records = await cursor.to_list(length=100)
    
    if not records:
        print("No data found in database!")
        return
    
    # Extract dates
    dates = set()
    for record in records:
        if record.get('timestamp'):
            try:
                # Parse the timestamp
                date_str = record['timestamp'][:10]  # Get just the date part
                dates.add(date_str)
            except:
                pass
    
    print("Dates with data in your database:")
    for date in sorted(dates, reverse=True)[:10]:  # Show last 10 dates
        print(f"   - {date}")
    
    print(f"\n Tip: To test a specific date, you'll need data from that date!")
    
    # Calculate what date would be processed if you run the batch now
    today = datetime.now().date()
    yesterday = today - timedelta(days=1)
    print(f"\n If you run the batch now, it will look for data from: {yesterday}")

if __name__ == "__main__":
    asyncio.run(check_available_dates())