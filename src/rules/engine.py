from typing import List, Dict, Any
import json


class Rule:
    def __init__(self, name: str, rule_type: str, rule_logic: str, severity: str):
        self.name = name
        self.rule_type = rule_type
        self.logic = json.loads(rule_logic)
        self.severity = severity

    def apply(self, claim: Dict[str, Any]) -> bool:
        # Example simple rule: check claim field equality
        field = self.logic.get("field")
        value = self.logic.get("equals")
        if field:
            return claim.get(field) == value
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
