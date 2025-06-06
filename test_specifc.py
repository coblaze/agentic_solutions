import asyncio
import sys
from datetime import datetime
from batch_orchestrator import BatchOrchestrator
from data_processor import processor

async def test_with_specific_date():
    """Test the batch process with a specific date that has data"""
    
    # CHANGE THIS DATE to one that has data in your MongoDB
    test_date = datetime(2025, 5, 28)  # Example: May 28, 2025
    
    print(f"TESTING BATCH PROCESS")
    print(f"Processing data for: {test_date.strftime('%Y-%m-%d')}")
    print("=" * 50)
    
    try:
        # Step 1: Fetch data for the specific date
        print("\nStep 1: Fetching data from MongoDB...")
        await processor.fetch_data_to_dataframe(target_date=test_date)
        df = processor.get_dataframe()
        
        if df.empty:
            print("No data found for this date!")
            print("Try a different date that has data in your MongoDB")
            return
        
        print(f"Found {len(df)} records!")
        print(f"   Sample interaction IDs: {df['interactionId'].head(3).tolist()}")
        
        # Show what will be evaluated
        print("\nSample of data to be evaluated:")
        for i in range(min(2, len(df))):
            print(f"\n   Record {i+1}:")
            print(f"   - Customer ID: {df.iloc[i]['customerId']}")
            print(f"   - Transcript preview: {df.iloc[i]['transcript'][:100]}...")
            print(f"   - Summary preview: {df.iloc[i]['postCallSummary'][:100] if df.iloc[i]['postCallSummary'] else 'No summary'}...")
        
        # Step 2: Create and run the orchestrator
        print("\nStep 2: Starting evaluation process...")
        
        evaluation_steps = [
            "Check if the summary captures the main points from the transcript",
            "Verify that key customer issues are mentioned",
            "Ensure resolution details are included if applicable"
        ]
        
        orchestrator = BatchOrchestrator(
            evaluation_steps=evaluation_steps,
            criteria="The summary should accurately reflect the conversation content",
            receiver_emails=["test@example.com"],  # Use a test email
            accuracy_threshold=0.85
        )
        
        # Run just the batch process (not the retry wrapper)
        result = await orchestrator.run_batch_process()
        
        # Step 3: Show results
        print("\nStep 3: Results")
        print("=" * 50)
        print(f"Status: {result['status']}")
        print(f"Date processed: {result['date_processed']}")
        print(f"Records processed: {result['records_processed']}")
        
        if result['status'] == 'success':
            print(f"Accuracy: {result['accuracy']:.2%}")
            print(f"Excel file saved: {result['output_file']}")
            print(f"Processing time: {result['processing_time']:.2f} seconds")
            
            metrics = result['metrics']
            print(f"\n📊 Detailed Metrics:")
            print(f"   - Total: {metrics['total_count']}")
            print(f"   - Passed: {metrics['pass_count']} ({metrics['pass_percentage']:.1f}%)")
            print(f"   - Failed: {metrics['fail_count']} ({metrics['fail_percentage']:.1f}%)")
        
    except Exception as e:
        print(f"\nError during testing: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    print("Starting batch process test...\n")
    asyncio.run(test_with_specific_date())