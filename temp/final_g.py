import asyncio
import os
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
import pandas as pd
from typing import Optional, Tuple, Dict
from deepeval.test_case import LLMTestCase, LLMTestCaseParams
from deepeval.metrics import GEval
from tqdm.asyncio import tqdm_asyncio
from api.sandbox.GT_Automation.google_vertex_ai import GoogleVertexAI

class GAutomationProcessor:
    def __init__(
        self,
        evaluation_steps=None,
        criteria=None,
        max_retries=10,
        batch_size=50,
    ):
        self.max_retries = max_retries
        self.batch_size = batch_size
        self.dataframe = None
        self.accuracy = None  # Store accuracy as instance variable
        self.metrics_data = None  # Store all metrics
        self.vertexai_gemini = GoogleVertexAI(model_name="gemini-2.0-flash-001")
        self.vertexai_gemini.load_model()
        self.evaluation_steps = evaluation_steps
        self.criteria = criteria
        self.evaluation_params = [
            LLMTestCaseParams.INPUT,
            LLMTestCaseParams.ACTUAL_OUTPUT,
        ]
        self.correctness_metric = GEval(
            model=self.vertexai_gemini,
            async_mode=False,
            name="Correctness",
            evaluation_steps=self.evaluation_steps,
            evaluation_params=self.evaluation_params,
            criteria=self.criteria,
            strict_mode=True,
            threshold=0.85,
        )

    def parse_g_eval_reason(self, reason_text: str) -> Tuple[str, str]:
        """Parse the G-Eval reason to extract result and reason"""
        if not reason_text:
            return "error", "No reason provided"
        
        reason_lower = reason_text.lower()
        
        if reason_lower.startswith("pass:"):
            result = "pass"
            reason = reason_text[5:].strip()
        elif reason_lower.startswith("error:"):
            result = "error"
            reason = reason_text[6:].strip()
        else:
            # Default to error if format is unexpected
            result = "error"
            reason = reason_text
        
        return result, reason

    async def process_batch(self, batch):
        """Process a batch of rows using ThreadPoolExecutor."""
        args = [(index, row) for index, row in batch.iterrows()]
        loop = asyncio.get_event_loop()
        
        with ThreadPoolExecutor(max_workers=3) as executor:
            results = await asyncio.gather(
                *[
                    loop.run_in_executor(executor, self.process_row_sync, arg)
                    for arg in args
                ]
            )
        
        return results

    def process_row_sync(self, args):
        """Synchronous version of process_row for ThreadPoolExecutor."""
        index, row = args
        retries = 0
        
        while retries < self.max_retries:
            try:
                test_case = LLMTestCase(
                    input=row["transcript"],
                    actual_output=row["postCallSummary"]
                )
                
                try:
                    self.correctness_metric.measure(test_case)
                    g_eval_reason = self.correctness_metric.reason
                except ValueError as e:
                    print(f"Skipping row {index} due to invalid JSON: {e}")
                    g_eval_reason = "error: Invalid JSON"
                
                # Parse the reason
                result, parsed_reason = self.parse_g_eval_reason(g_eval_reason)
                
                return {
                    "index": index,
                    "results": result,
                    "reason": parsed_reason
                }
                
            except Exception as e:
                retries += 1
                print(f"Exception encountered: {e}")
                if any(code in str(e) for code in ["429", "404", "503"]):
                    wait_time = min(2**retries, 60)
                    print(f"Rate limit exceeded. Waiting for {wait_time} seconds...")
                    asyncio.run(asyncio.sleep(wait_time))
                    # Reinitialize model
                    self.vertexai_gemini = GoogleVertexAI(model_name="gemini-2.0-flash-001")
                    self.vertexai_gemini.load_model()
                    self.correctness_metric = GEval(
                        model=self.vertexai_gemini,
                        async_mode=False,
                        name="Correctness",
                        evaluation_steps=self.evaluation_steps,
                        evaluation_params=self.evaluation_params,
                        criteria=self.criteria,
                        strict_mode=True,
                        threshold=0.85,
                    )
                if retries >= self.max_retries:
                    return {
                        "index": index,
                        "results": "error",
                        "reason": f"Max retries exceeded - {str(e)}"
                    }

    def calculate_metrics(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calculate evaluation metrics from the results"""
        total_count = len(df)
        pass_count = len(df[df['results'] == 'pass'])
        fail_count = len(df[df['results'] == 'error'])
        
        # Calculate percentages
        pass_percentage = (pass_count / total_count * 100) if total_count > 0 else 0
        fail_percentage = (fail_count / total_count * 100) if total_count > 0 else 0
        
        # Store accuracy as instance variable
        self.accuracy = (pass_count / total_count) if total_count > 0 else 0
        
        # Store all metrics for easy access
        self.metrics_data = {
            'total_count': total_count,
            'pass_count': pass_count,
            'fail_count': fail_count,
            'pass_percentage': pass_percentage,
            'fail_percentage': fail_percentage,
            'accuracy': self.accuracy
        }
        
        metrics = {
            'Metric': [
                'Total Count',
                'Pass Count',
                'Fail Count',
                'Pass Percentage',
                'Fail Percentage',
                'Total Accuracy'
            ],
            'Value': [
                total_count,
                pass_count,
                fail_count,
                f"{pass_percentage:.2f}%",
                f"{fail_percentage:.2f}%",
                f"{self.accuracy:.2%}"
            ]
        }
        
        return pd.DataFrame(metrics)

    async def process_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Main method to process the DataFrame in batches."""
        # Work with the dataframe directly - results and reason columns already exist
        self.dataframe = df.copy()
        
        batches = [
            self.dataframe.iloc[i : i + self.batch_size]
            for i in range(0, len(self.dataframe), self.batch_size)
        ]
        
        all_results = []
        for batch in tqdm_asyncio(batches, desc="Processing batches"):
            batch_results = await self.process_batch(batch)
            all_results.extend(batch_results)
        
        # Update only the results and reason columns
        for result in all_results:
            idx = result["index"]
            self.dataframe.at[idx, "results"] = result["results"]
            self.dataframe.at[idx, "reason"] = result["reason"]
        
        return self.dataframe

    def save_results_with_metrics(self, df: pd.DataFrame, output_filename: Optional[str] = None) -> str:
        """
        Save the evaluation results to Excel with metrics in a separate sheet
        
        The Excel file will contain:
        - Sheet 'Results': All original columns plus 'results' and 'reason'
        - Sheet 'Metrics': Summary statistics
        """
        # Generate filename with timestamp
        date_str = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Create output directory
        output_dir = os.path.join(
            os.getcwd(), "api", "sandbox", "GT_Automation", "output"
        )
        os.makedirs(output_dir, exist_ok=True)
        
        # Set filename
        if output_filename:
            output_file_path = os.path.join(output_dir, output_filename)
        else:
            output_file_path = os.path.join(output_dir, f"evaluation_results_{date_str}.xlsx")
        
        # Calculate metrics (this also sets self.accuracy and self.metrics_data)
        metrics_df = self.calculate_metrics(df)
        
        # Save to Excel with multiple sheets
        with pd.ExcelWriter(output_file_path, engine='openpyxl') as writer:
            # Save main results
            df.to_excel(writer, sheet_name='Results', index=False)
            
            # Save metrics
            metrics_df.to_excel(writer, sheet_name='Metrics', index=False)
            
            # Auto-adjust column widths for better readability
            for sheet_name in writer.sheets:
                worksheet = writer.sheets[sheet_name]
                for column in worksheet.columns:
                    max_length = 0
                    column_letter = column[0].column_letter
                    for cell in column:
                        try:
                            if len(str(cell.value)) > max_length:
                                max_length = len(str(cell.value))
                        except:
                            pass
                    adjusted_width = min(max_length + 2, 50)
                    worksheet.column_dimensions[column_letter].width = adjusted_width
        
        print(f"Evaluation results saved to: {output_file_path}")
        print(f"  - Results sheet: {len(df)} rows")
        print(f"  - Metrics sheet: Summary statistics")
        print(f"  - Overall Accuracy: {self.accuracy:.2%}")
        
        return output_file_path