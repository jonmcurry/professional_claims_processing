import asyncio
from src.validation.validator import ClaimValidator


def test_validator_split_logic():
    validator = ClaimValidator({"A"}, {"X"})
    claim = {
        "facility_id": "A",
        "financial_class": "X",
        "date_of_birth": "1990-01-01",
        "service_from_date": "1990-01-02",
        "service_to_date": "1990-01-03",
    }
    assert asyncio.run(validator.validate(claim)) == []

    claim["facility_id"] = "B"
    claim["service_to_date"] = "1989-12-31"
    errors = asyncio.run(validator.validate(claim))
    assert "invalid_facility" in errors
    assert "invalid_service_dates" in errors


def test_line_item_date_validation():
    validator = ClaimValidator({"F1"}, {"A"})
    claim = {
        "facility_id": "F1",
        "financial_class": "A",
        "service_from_date": "2020-01-01",
        "service_to_date": "2020-01-31",
        "line_items": [
            {
                "service_from_date": "2019-12-31",
                "service_to_date": "2020-01-02",
            }
        ],
    }
    errors = asyncio.run(validator.validate(claim))
    assert "line_item_date_out_of_range" in errors
