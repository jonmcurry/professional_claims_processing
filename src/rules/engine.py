from typing import List, Dict, Any
import json


class Rule:
    """Single rule definition."""

    def __init__(self, name: str, rule_type: str, rule_logic: str, severity: str):
        self.name = name
        self.rule_type = rule_type
        self.logic = json.loads(rule_logic)
        self.severity = severity

    def apply(self, claim: Dict[str, Any]) -> bool:
        """Return True if the claim satisfies the rule."""
        field = self.logic.get("field")
        if not field:
            return True
        op = self.logic.get("operator", "equals")
        value = self.logic.get("value")
        values = self.logic.get("values")
        actual = claim.get(field)

        if op == "equals":
            return actual == value
        if op == "not_equals":
            return actual != value
        if op == "exists":
            return field in claim
        if op == "gt":
            return actual is not None and value is not None and actual > value
        if op == "lt":
            return actual is not None and value is not None and actual < value
        if op == "between":
            if not values or len(values) != 2 or actual is None:
                return False
            start, end = values
            return start <= actual <= end
        if op == "in":
            return actual in (values or [])
        return False


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
        """Evaluate a batch of claims."""
        results: Dict[str, List[str]] = {}
        for claim in claims:
            results[claim.get("claim_id", "")] = self.evaluate(claim)
        return results
