from src.rules.engine import Rule, RulesEngine


def test_rules_engine_operators():
    rule_logic_eq = '{"field": "a", "operator": "equals", "value": 1}'
    rule_logic_gt = '{"field": "b", "operator": "gt", "value": 5}'
    rules = [
        Rule("r1", "field", rule_logic_eq, "error"),
        Rule("r2", "field", rule_logic_gt, "error"),
    ]
    engine = RulesEngine(rules)
    claim = {"a": 1, "b": 10}
    assert engine.evaluate(claim) == []

    claim = {"a": 2, "b": 4}
    assert set(engine.evaluate(claim)) == {"r1", "r2"}
