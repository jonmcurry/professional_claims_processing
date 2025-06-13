from typing import Dict, Any
from cryptography.fernet import Fernet


def _get_cipher(key: str) -> Fernet:
    return Fernet(key.encode())


def encrypt_text(text: str, key: str) -> str:
    cipher = _get_cipher(key)
    return cipher.encrypt(text.encode()).decode()


def decrypt_text(token: str, key: str) -> str:
    cipher = _get_cipher(key)
    return cipher.decrypt(token.encode()).decode()


def mask_claim_data(claim: Dict[str, Any]) -> Dict[str, Any]:
    """Return a copy of claim with PII/PHI fields masked."""
    masked = claim.copy()
    if "patient_name" in masked:
        masked["patient_name"] = "***"
    if "patient_account_number" in masked and masked["patient_account_number"]:
        masked["patient_account_number"] = masked["patient_account_number"][:2] + "***"
    if "date_of_birth" in masked:
        masked["date_of_birth"] = "***"
    return masked
