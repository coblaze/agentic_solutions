"""
LLM Evaluation Service
Handles the core evaluation logic using Google Vertex AI
"""
import asyncio
import uuid
from typing import List, Tuple, Dict, Any, Optional
from datetime import datetime
import logging
from concurrent.futures import ThreadPoolExecutor
import time
from deepeval.test_case import LLMTestCase, LLMTestCaseParams
from deepeval.metrics import GEval
import google.cloud.aiplatform as aiplatform
from src.models.evaluation import (
    TranscriptSummaryPair, 
    EvaluationResult, 
    BatchEvaluation, 
    EvaluationStatus
)
from src.config.settings import settings
from src.utils.helpers import parse_evaluation_response

logger = logging.getLogger(__name__)


class EvaluationService:
    """
    Service for evaluating transcript-summary pairs using LLM
    Implements batching, retry logic, and error handling
    """
    
    def __init__(self, llm_model):
        self.llm_model = llm_model
        self.batch_size = settings.BATCH_SIZE
        self.max_retries = settings.MAX_RETRIES
        self.executor = ThreadPoolExecutor(max_workers=3)
        self._init_evaluator()
        
    def _init_evaluator(self):
        """Initialize the GEval metric for evaluation"""
        # Detailed evaluation steps
        self.evaluation_steps = [
            "1. Carefully read the entire transcript to understand the full conversation context.",
            "2. Identify all key points, facts, and important information discussed in the transcript.",
            "3. Read the AI-generated summary thoroughly.",
            "4. For each statement in the summary, verify if it is accurately supported by the transcript.",
            "5. Check if the summary captures all critical information from the transcript.",
            "6. Identify any factual errors, misrepresentations, or hallucinations in the summary.",
            "7. Evaluate if the summary maintains the correct context and meaning from the transcript.",
            "8. Determine if any important information from the transcript is missing in the summary."
        ]
        
        # Clear evaluation criteria
        self.criteria = """
        The summary must meet the following criteria for a PASS:
        1. FACTUAL ACCURACY: Every statement in the summary must be directly supported by the transcript.
        2. NO HALLUCINATIONS: The summary must not contain any information not present in the transcript.
        3. COMPLETENESS: All critical information from the transcript must be included in the summary.
        4. CONTEXT PRESERVATION: The summary must maintain the correct context and meaning.
        5. NO CONTRADICTIONS: The summary must not contradict any information in the transcript.
        
        The summary FAILS if:
        - It contains any factual errors or unsupported claims
        - It includes hallucinated information not in the transcript
        - It misses critical information from the transcript
        - It misrepresents the context or meaning
        - It contradicts information in the transcript
        """
        
        self.evaluation_params = [
            LLMTestCaseParams.INPUT,
            LLMTestCaseParams.ACTUAL_OUTPUT,
        ]
        
        # Initialize GEval metric
        self.correctness_metric = GEval(
            model=self.llm_model,
            async_mode=False,
            name="Transcript-Summary Factual Accuracy",
            evaluation_steps=self.evaluation_steps,
            evaluation_params=self.evaluation_params,
            criteria=self.criteria,
            strict_mode=True,
            threshold=settings.ACCURACY_THRESHOLD,
            verbose_mode=False
        )
        
    async def evaluate_batch(self, 
                           pairs: List[TranscriptSummaryPair]) -> Tuple[List[EvaluationResult], BatchEvaluation]:
        """
        Evaluate a batch of transcript-summary pairs
        
        Args:
            pairs: List of TranscriptSummaryPair objects to evaluate
            
        Returns:
            Tuple of (evaluation_results, batch_summary)
        """
        batch_id = f"BATCH-{uuid.uuid4().hex[:8]}"
        evaluation_results = []
        start_time = datetime.utcnow()
        
        logger.info(f"Starting evaluation for batch {batch_id} with {len(pairs)} pairs")
        
        # Process in smaller sub-batches for better performance
        for i in range(0, len(pairs), self.batch_size):
            sub_batch = pairs[i:i + self.batch_size]
            logger.info(f"Processing sub-batch {i//self.batch_size + 1} "
                       f"({len(sub_batch)} pairs)")
            
            # Process sub-batch
            sub_batch_results = await self._process_sub_batch(sub_batch, batch_id)
            evaluation_results.extend(sub_batch_results)
            
            # Small delay between sub-batches to avoid rate limits
            if i + self.batch_size < len(pairs):
                await asyncio.sleep(1)
                
        # Calculate batch statistics
        end_time = datetime.utcnow()
        processing_time = (end_time - start_time).total_seconds()
        
        batch_stats = self._calculate_batch_statistics(
            evaluation_results, 
            batch_id,
            processing_time
        )
        
        logger.info(f"Completed evaluation for batch {batch_id}. "
                   f"Results: {batch_stats.get_summary_text()}")
        
        return evaluation_results, batch_stats
        
    async def _process_sub_batch(self, 
                               pairs: List[TranscriptSummaryPair], 
                               batch_id: str) -> List[EvaluationResult]:
        """Process a sub-batch of pairs using ThreadPoolExecutor"""
        loop = asyncio.get_event_loop()
        
        # Create tasks for concurrent processing
        tasks = []
        for pair in pairs:
            task = loop.run_in_executor(
                self.executor,
                self._evaluate_single_pair,
                pair,
                batch_id
            )
            tasks.append(task)
            
        # Wait for all tasks to complete
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Process results and handle exceptions
        valid_results = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                logger.error(f"Error evaluating pair {pairs[i].interaction_id}: {result}")
                # Create error result
                error_result = self._create_error_result(pairs[i], batch_id, result)
                valid_results.append(error_result)
            else:
                valid_results.append(result)
                
        return valid_results
        
    def _evaluate_single_pair(self, 
                            pair: TranscriptSummaryPair, 
                            batch_id: str) -> EvaluationResult:
        """
        Evaluate a single transcript-summary pair
        Implements retry logic for transient failures
        """
        evaluation_id = str(uuid.uuid4())
        start_time = time.time()
        retries = 0
        
        while retries < self.max_retries:
            try:
                # Create test case for DeepEval
                test_case = LLMTestCase(
                    input=pair.transcript,
                    actual_output=pair.post_call_summary
                )
                
                # Measure correctness
                logger.debug(f"Evaluating pair {pair.interaction_id} (attempt {retries + 1})")
                self.correctness_metric.measure(test_case)
                
                # Parse the evaluation response
                status, reason = parse_evaluation_response(
                    self.correctness_metric.score,
                    self.correctness_metric.reason,
                    settings.ACCURACY_THRESHOLD
                )
                
                # Calculate evaluation duration
                duration = time.time() - start_time
                
                # Create evaluation result
                result = EvaluationResult(
                    evaluation_id=evaluation_id,
                    batch_id=batch_id,
                    interaction_id=pair.interaction_id,
                    status=status,
                    reason=reason,
                    confidence_score=self.correctness_metric.score,
                    transcript_ref_id=pair.transcript_ref_id,
                    ivr_call_id=pair.ivr_call_id,
                    customer_id=pair.customer_id,
                    lob=pair.lob,
                    agent_id=pair.agent_id,
                    start_timestamp=pair.start_timestamp,
                    transcript=pair.transcript,
                    post_call_summary=pair.post_call_summary,
                    account_number=pair.account_number,
                    evaluation_duration_seconds=duration,
                    llm_model_used=settings.VERTEX_AI_MODEL
                )
                
                logger.debug(f"Evaluation complete for {pair.interaction_id}: {status.value}")
                return result
                
            except Exception as e:
                retries += 1
                logger.warning(f"Evaluation attempt {retries} failed for {pair.interaction_id}: {e}")
                
                # Check if it's a rate limit error
                if any(code in str(e) for code in ["429", "503", "504"]):
                    # Exponential backoff for rate limits
                    wait_time = min(2 ** retries * 5, 60)
                    logger.info(f"Rate limit detected, waiting {wait_time} seconds...")
                    time.sleep(wait_time)
                    
                    # Reinitialize evaluator if needed
                    if retries == 2:
                        self._init_evaluator()
                elif "timeout" in str(e).lower():
                    # Shorter wait for timeouts
                    time.sleep(5)
                else:
                    # For other errors, shorter wait
                    time.sleep(2)
                    
                if retries >= self.max_retries:
                    # Max retries exceeded, create error result
                    duration = time.time() - start_time
                    return self._create_error_result(pair, batch_id, e, duration)
                    
        # Should not reach here, but just in case
        return self._create_error_result(
            pair, 
            batch_id, 
            Exception("Max retries exceeded without success")
        )
        
    def _create_error_result(self, 
                           pair: TranscriptSummaryPair, 
                           batch_id: str,
                           error: Exception,
                           duration: Optional[float] = None) -> EvaluationResult:
        """Create an error result when evaluation fails"""
        return EvaluationResult(
            evaluation_id=str(uuid.uuid4()),
            batch_id=batch_id,
            interaction_id=pair.interaction_id,
            status=EvaluationStatus.ERROR,
            reason=f"Evaluation failed: {str(error)}",
            confidence_score=0.0,
            transcript_ref_id=pair.transcript_ref_id,
            ivr_call_id=pair.ivr_call_id,
            customer_id=pair.customer_id,
            lob=pair.lob,
            agent_id=pair.agent_id,
            start_timestamp=pair.start_timestamp,
            transcript=pair.transcript,
            post_call_summary=pair.post_call_summary,
            account_number=pair.account_number,
            evaluation_duration_seconds=duration,
            llm_model_used=settings.VERTEX_AI_MODEL
        )
        
    def _calculate_batch_statistics(self, 
                                  results: List[EvaluationResult], 
                                  batch_id: str,
                                  processing_time: float) -> BatchEvaluation:
        """Calculate statistics for the batch evaluation"""
        total = len(results)
        
        if total == 0:
            # Empty batch
            return BatchEvaluation(
                batch_id=batch_id,
                evaluation_date=datetime.utcnow(),
                total_evaluations=0,
                passed=0,
                failed=0,
                errors=0,
                pass_percentage=0,
                fail_percentage=0,
                error_percentage=0,
                accuracy=0,
                evaluation_ids=[],
                processing_time_seconds=processing_time
            )
            
        # Count by status
        passed = sum(1 for r in results if r.status == EvaluationStatus.PASS)
        failed = sum(1 for r in results if r.status == EvaluationStatus.FAIL)
        errors = sum(1 for r in results if r.status == EvaluationStatus.ERROR)
        
        # Calculate percentages
        pass_percentage = (passed / total * 100) if total > 0 else 0
        fail_percentage = (failed / total * 100) if total > 0 else 0
        error_percentage = (errors / total * 100) if total > 0 else 0
        
        # Calculate accuracy (passed / (passed + failed), excluding errors)
        evaluable = passed + failed
        accuracy = (passed / evaluable) if evaluable > 0 else 0
        
        # Calculate average confidence
        confidence_scores = [r.confidence_score for r in results if r.confidence_score is not None]
        avg_confidence = sum(confidence_scores) / len(confidence_scores) if confidence_scores else None
        
        # Get evaluation IDs
        evaluation_ids = [r.evaluation_id for r in results]
        
        return BatchEvaluation(
            batch_id=batch_id,
            evaluation_date=datetime.utcnow(),
            total_evaluations=total,
            passed=passed,
            failed=failed,
            errors=errors,
            pass_percentage=pass_percentage,
            fail_percentage=fail_percentage,
            error_percentage=error_percentage,
            accuracy=accuracy,
            average_confidence=avg_confidence,
            evaluation_ids=evaluation_ids,
            processing_time_seconds=processing_time
        )
        
    def __del__(self):
        """Cleanup executor on deletion"""
        if hasattr(self, 'executor'):
            self.executor.shutdown(wait=False)