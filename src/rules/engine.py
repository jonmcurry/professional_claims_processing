import json
from typing import Any, Dict, List


class Rule:
    """Single rule definition."""

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
