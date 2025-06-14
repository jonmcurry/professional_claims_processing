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


def test_rules_engine_all_operators():
    rules = [
        Rule(
            "ne",
            "field",
            '{"field": "a", "operator": "not_equals", "value": 2}',
            "error",
        ),
        Rule("ex", "field", '{"field": "b", "operator": "exists"}', "error"),
        Rule("lt", "field", '{"field": "c", "operator": "lt", "value": 10}', "error"),
        Rule(
            "bt",
            "field",
            '{"field": "d", "operator": "between", "values": [1, 3]}',
            "error",
        ),
        Rule(
            "in",
            "field",
            '{"field": "e", "operator": "in", "values": ["x", "y"]}',
            "error",
        ),
    ]
    engine = RulesEngine(rules)
    good_claim = {"a": 1, "b": True, "c": 5, "d": 2, "e": "x"}
    assert engine.evaluate(good_claim) == []

    bad_claim = {"a": 2, "c": 20, "d": 5, "e": "z"}
    failures = set(engine.evaluate(bad_claim))
    assert failures == {"ne", "ex", "lt", "bt", "in"}


def test_rule_precompiled():
    rule = Rule("gt", "field", '{"field": "x", "operator": "gt", "value": 5}', "error")
    assert rule.apply({"x": 10})
    assert not rule.apply({"x": 1})
