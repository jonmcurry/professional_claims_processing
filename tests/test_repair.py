from src.processing.repair import ClaimRepairSuggester


def test_repair_suggestions():
    suggester = ClaimRepairSuggester()
    result = suggester.suggest(["invalid_facility", "invalid_service_dates"])
    assert "Verify facility_id" in result
    assert "Correct service dates" in result
