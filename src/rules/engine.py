import asyncio
import json
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Dict, List

import numpy as np


class Rule:
    """Single rule definition with vectorized evaluation support."""

    def __init__(self, name: str, rule_type: str, rule_logic: str, severity: str):
        self.name = name
        self.rule_type = rule_type
        self.logic = json.loads(rule_logic)
        self.severity = severity
        self._compiled = self._compile(self.logic)

    def _compile(self, logic: Dict[str, Any]):
        field = logic.get("field")
        op = logic.get("operator", "equals")
        value = logic.get("value")
        values = logic.get("values")

        if not field:
            return lambda claim: True
        if op == "equals":
            return lambda claim: claim.get(field) == value
        if op == "not_equals":
            return lambda claim: claim.get(field) != value
        if op == "exists":
            return lambda claim: field in claim
        if op == "gt":
            return (
                lambda claim: claim.get(field) is not None
                and value is not None
                and claim.get(field) > value
            )
        if op == "lt":
            return (
                lambda claim: claim.get(field) is not None
                and value is not None
                and claim.get(field) < value
            )
        if op == "between":
            if not values or len(values) != 2:
                return lambda claim: False
            start, end = values
            return (
                lambda claim: claim.get(field) is not None
                and start <= claim.get(field) <= end
            )
        if op == "in":
            return lambda claim: claim.get(field) in (values or [])
        return lambda claim: False

    def apply(self, claim: Dict[str, Any]) -> bool:
        """Return True if the claim satisfies the rule."""
        return bool(self._compiled(claim))

    def apply_vectorized(self, claims: List[Dict[str, Any]]) -> List[bool]:
        """Vectorized rule application for batch processing."""
        field = self.logic.get("field")
        op = self.logic.get("operator", "equals")
        value = self.logic.get("value")
        values = self.logic.get("values")

        if not field:
            return [True] * len(claims)

        # Extract field values for vectorized operations
        field_values = [claim.get(field) for claim in claims]

        try:
            # Use numpy for vectorized operations where possible
            if op == "equals":
                return [fv == value for fv in field_values]
            elif op == "not_equals":
                return [fv != value for fv in field_values]
            elif op == "exists":
                return [field in claim for claim in claims]
            elif op == "gt" and value is not None:
                return [fv is not None and fv > value for fv in field_values]
            elif op == "lt" and value is not None:
                return [fv is not None and fv < value for fv in field_values]
            elif op == "between" and values and len(values) == 2:
                start, end = values
                return [fv is not None and start <= fv <= end for fv in field_values]
            elif op == "in" and values:
                values_set = set(values)  # Optimize lookup
                return [fv in values_set for fv in field_values]
            else:
                return [False] * len(claims)
        except Exception:
            # Fallback to individual evaluation
            return [self.apply(claim) for claim in claims]


class RulesEngine:
    def __init__(self, rules: List[Rule]):
        self.rules = rules

    def evaluate(self, claim: Dict[str, Any]) -> List[str]:
        failures = []
        for rule in self.rules:
            if not rule.apply(claim):
                failures.append(rule.name)
        return failures

    def evaluate_batch(self, claims: List[Dict[str, Any]]) -> Dict[str, List[str]]:
        """Vectorized batch evaluation of claims."""
        if not claims or not self.rules:
            return {claim.get("claim_id", ""): [] for claim in claims}

        results: Dict[str, List[str]] = {}

        # Initialize results dict
        for claim in claims:
            claim_id = claim.get("claim_id", "")
            results[claim_id] = []

        # Process each rule across all claims vectorized
        for rule in self.rules:
            try:
                # Get vectorized results for this rule across all claims
                rule_results = rule.apply_vectorized(claims)

                # Map failures back to specific claims
                for i, (claim, rule_passed) in enumerate(zip(claims, rule_results)):
                    if not rule_passed:
                        claim_id = claim.get("claim_id", "")
                        results[claim_id].append(rule.name)

            except Exception:
                # Fallback to individual evaluation for this rule
                for claim in claims:
                    claim_id = claim.get("claim_id", "")
                    if not rule.apply(claim):
                        results[claim_id].append(rule.name)

        return results

    def evaluate_batch_parallel(
        self, claims: List[Dict[str, Any]], max_workers: int = 4
    ) -> Dict[str, List[str]]:
        """Parallel batch evaluation using thread pool for CPU-intensive rules."""
        if not claims or not self.rules:
            return {claim.get("claim_id", ""): [] for claim in claims}

        results: Dict[str, List[str]] = {}

        # Initialize results dict
        for claim in claims:
            claim_id = claim.get("claim_id", "")
            results[claim_id] = []

        # Split claims into chunks for parallel processing
        chunk_size = max(len(claims) // max_workers, 100)
        claim_chunks = [
            claims[i : i + chunk_size] for i in range(0, len(claims), chunk_size)
        ]

        def process_chunk(chunk: List[Dict[str, Any]]) -> Dict[str, List[str]]:
            chunk_results: Dict[str, List[str]] = {}

            for claim in chunk:
                claim_id = claim.get("claim_id", "")
                chunk_results[claim_id] = []

                for rule in self.rules:
                    if not rule.apply(claim):
                        chunk_results[claim_id].append(rule.name)

            return chunk_results

        # Process chunks in parallel
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            chunk_results = list(executor.map(process_chunk, claim_chunks))

        # Merge results
        for chunk_result in chunk_results:
            results.update(chunk_result)

        return results

    async def evaluate_batch_async(
        self, claims: List[Dict[str, Any]]
    ) -> Dict[str, List[str]]:
        """Async batch evaluation for non-blocking processing."""
        return await asyncio.get_event_loop().run_in_executor(
            None, self.evaluate_batch_parallel, claims
        )
