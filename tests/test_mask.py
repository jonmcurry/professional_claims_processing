from src.security.compliance import mask_claim_data


def test_mask_claim_data():
    claim = {
        "patient_name": "Jane Doe",
        "patient_account_number": "123456",
        "date_of_birth": "1990-01-01",
        "other": "value",
    }
    masked = mask_claim_data(claim)
    assert masked["patient_name"] == "***"
    assert masked["patient_account_number"].startswith("12")
    assert masked["patient_account_number"].endswith("***")
    assert masked["date_of_birth"] == "***"
    assert masked["other"] == "value"
