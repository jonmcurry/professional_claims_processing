import uuid
from typing import List, Dict, Any

from durable.lang import ruleset, when_all, m, post

from .engine import Rule


class DurableRulesEngine:
    """Rules engine implementation using durable_rules."""

    def __init__(self, rules: List[Rule]):
        self.rules = rules
        self._failures: List[str] = []
        self.ruleset_name = f"claims_{uuid.uuid4().hex}"
        engine = self
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
        """Evaluate a batch of claims."""
        results: Dict[str, List[str]] = {}
        for claim in claims:
            results[claim.get("claim_id", "")] = self.evaluate(claim)
        return results
