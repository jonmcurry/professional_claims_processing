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
    assert validator.validate(claim) == []

    claim["facility_id"] = "B"
    claim["service_to_date"] = "1989-12-31"
    errors = validator.validate(claim)
    assert "invalid_facility" in errors
    assert "invalid_service_dates" in errors
