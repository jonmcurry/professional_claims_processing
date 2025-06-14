import uuid
import asyncio
from typing import List, Dict, Any
from concurrent.futures import ThreadPoolExecutor

from durable.lang import ruleset, when_all, m, post

from .engine import Rule


class DurableRulesEngine:
    """Rules engine implementation using durable_rules with vectorized batch processing."""

    def __init__(self, rules: List[Rule]):
        self.rules = rules
        self._failures: List[str] = []
        self.ruleset_name = f"claims_{uuid.uuid4().hex}"
        engine = self
        
        # Create durable ruleset
        with ruleset(self.ruleset_name):
            @when_all(+m)
            def evaluate(c):  # type: ignore
                engine._failures = []
                for r in engine.rules:
                    if not r.apply(dict(c.m)):
                        engine._failures.append(r.name)

    def evaluate(self, claim: Dict[str, Any]) -> List[str]:
        """Evaluate a claim and return failing rule names."""
        self._failures = []
        post(self.ruleset_name, claim)
        return list(self._failures)

    def evaluate_batch(self, claims: List[Dict[str, Any]]) -> Dict[str, List[str]]:
        """Vectorized batch evaluation of claims."""
        if not claims:
            return {}
        
        # For large batches, use parallel processing
        if len(claims) > 1000:
            return self.evaluate_batch_parallel(claims)
        
        # For smaller batches, use sequential processing with optimizations
        results: Dict[str, List[str]] = {}
        
        # Group claims by similar characteristics for batch optimization
        grouped_claims = self._group_claims_for_optimization(claims)
        
        for group_key, group_claims in grouped_claims.items():
            # Process each group
            for claim in group_claims:
                claim_id = claim.get("claim_id", "")
                results[claim_id] = self.evaluate(claim)
        
        return results

    def evaluate_batch_parallel(self, claims: List[Dict[str, Any]], max_workers: int = 4) -> Dict[str, List[str]]:
        """Parallel batch evaluation using thread pool."""
        if not claims:
            return {}
        
        # Split claims into chunks for parallel processing
        chunk_size = max(len(claims) // max_workers, 50)
        claim_chunks = [claims[i:i + chunk_size] for i in range(0, len(claims), chunk_size)]
        
        def process_chunk(chunk: List[Dict[str, Any]]) -> Dict[str, List[str]]:
            # Create a separate engine instance for this thread
            chunk_engine = DurableRulesEngine(self.rules)
            chunk_results = {}
            
            for claim in chunk:
                claim_id = claim.get("claim_id", "")
                chunk_results[claim_id] = chunk_engine.evaluate(claim)
            
            return chunk_results
        
        # Process chunks in parallel
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            chunk_results = list(executor.map(process_chunk, claim_chunks))
        
        # Merge results
        results: Dict[str, List[str]] = {}
        for chunk_result in chunk_results:
            results.update(chunk_result)
        
        return results

    async def evaluate_batch_async(self, claims: List[Dict[str, Any]]) -> Dict[str, List[str]]:
        """Async batch evaluation for non-blocking processing."""
        return await asyncio.get_event_loop().run_in_executor(
            None, self.evaluate_batch_parallel, claims
        )

    def _group_claims_for_optimization(self, claims: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
        """Group claims by similar characteristics to optimize rule evaluation."""
        groups: Dict[str, List[Dict[str, Any]]] = {}
        
        for claim in claims:
            # Create a grouping key based on characteristics that affect rule evaluation
            facility_id = claim.get("facility_id", "unknown")
            financial_class = claim.get("financial_class", "unknown")
            has_diagnosis = "yes" if claim.get("primary_diagnosis") else "no"
            
            group_key = f"{facility_id}_{financial_class}_{has_diagnosis}"
            
            if group_key not in groups:
                groups[group_key] = []
            groups[group_key].append(claim)
        
        return groups

    def evaluate_vectorized(self, claims: List[Dict[str, Any]]) -> Dict[str, List[str]]:
        """Optimized vectorized evaluation using rule preprocessing."""
        if not claims or not self.rules:
            return {claim.get("claim_id", ""): [] for claim in claims}
        
        results: Dict[str, List[str]] = {}
        
        # Initialize results
        for claim in claims:
            claim_id = claim.get("claim_id", "")
            results[claim_id] = []
        
        # Pre-extract common fields for vectorized operations
        claim_fields = self._extract_claim_fields(claims)
        
        # Evaluate each rule across all claims
        for rule in self.rules:
            try:
                # Use vectorized evaluation if the rule supports it
                if hasattr(rule, 'apply_vectorized'):
                    rule_results = rule.apply_vectorized(claims)
                    
                    for i, (claim, passed) in enumerate(zip(claims, rule_results)):
                        if not passed:
                            claim_id = claim.get("claim_id", "")
                            results[claim_id].append(rule.name)
                else:
                    # Fallback to individual evaluation
                    for claim in claims:
                        claim_id = claim.get("claim_id", "")
                        if not rule.apply(claim):
                            results[claim_id].append(rule.name)
                            
            except Exception:
                # Fallback to individual evaluation
                for claim in claims:
                    claim_id = claim.get("claim_id", "")
                    try:
                        if not rule.apply(claim):
                            results[claim_id].append(rule.name)
                    except Exception:
                        # Rule evaluation failed, consider it a failure
                        results[claim_id].append(rule.name)
        
        return results

    def _extract_claim_fields(self, claims: List[Dict[str, Any]]) -> Dict[str, List[Any]]:
        """Extract common fields from claims for vectorized operations."""
        fields = {}
        
        # Extract commonly used fields
        common_fields = [
            "facility_id", "financial_class", "primary_diagnosis", 
            "date_of_birth", "service_from_date", "service_to_date",
            "total_charge_amount", "patient_account_number"
        ]
        
        for field in common_fields:
            fields[field] = [claim.get(field) for claim in claims]
        
        return fields

    def add_rule(self, rule: Rule) -> None:
        """Add a new rule to the engine."""
        self.rules.append(rule)

    def remove_rule(self, rule_name: str) -> bool:
        """Remove a rule by name."""
        for i, rule in enumerate(self.rules):
            if rule.name == rule_name:
                del self.rules[i]
                return True
        return False

    def get_rule_statistics(self) -> Dict[str, Any]:
        """Get statistics about rule performance."""
        return {
            "total_rules": len(self.rules),
            "rule_types": [rule.rule_type for rule in self.rules],
            "rule_names": [rule.name for rule in self.rules]
        }