from src.security.compliance import encrypt_text, decrypt_text, encrypt_claim_fields


def test_encrypt_decrypt_roundtrip():
    key = "abcd" * 8  # 32 bytes for Fernet
    original = "secret"
    token = encrypt_text(original, key)
    assert token != original
    result = decrypt_text(token, key)
    assert result == original


def test_encrypt_claim_fields():
    key = "abcd" * 8
    claim = {"patient_account_number": "123", "patient_name": "Jane"}
    encrypted = encrypt_claim_fields(claim, key)
    assert encrypted["patient_account_number"] != "123"
    assert encrypted["patient_name"] != "Jane"
    # round trip to ensure valid encryption
    decrypted_acct = decrypt_text(encrypted["patient_account_number"], key)
    decrypted_name = decrypt_text(encrypted["patient_name"], key)
    assert decrypted_acct == "123"
    assert decrypted_name == "Jane"

