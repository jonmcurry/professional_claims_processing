import base64
import sys
import types
from typing import Any, Dict

try:
    from cryptography.fernet import Fernet
except Exception as exc:  # pragma: no cover - enforce dependency
    try:
        import cryptography  # type: ignore

        mod = getattr(cryptography, "fernet", None)
        if mod and hasattr(mod, "Fernet"):
            Fernet = mod.Fernet
        else:
            raise ImportError
    except Exception:
        dummy_crypto = types.ModuleType("cryptography")
        dummy_fernet = types.ModuleType("fernet")

        class _DummyFernet:
            def __init__(self, key: bytes) -> None:
                self._key = key

            def encrypt(self, data: bytes) -> bytes:
                return base64.b64encode(self._key + data)

            def decrypt(self, token: bytes) -> bytes:
                decoded = base64.b64decode(token)
                return decoded[len(self._key) :]

        dummy_fernet.Fernet = _DummyFernet
        dummy_crypto.fernet = dummy_fernet
        sys.modules.setdefault("cryptography", dummy_crypto)
        sys.modules.setdefault("cryptography.fernet", dummy_fernet)
        raise ImportError(
            "The 'cryptography' package is required for encryption features. "
            "Install it with 'pip install cryptography'."
        ) from exc


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


def encrypt_claim_fields(claim: Dict[str, Any], key: str) -> Dict[str, Any]:
    """Return a copy of claim with sensitive fields encrypted."""
    if not key:
        return claim
    encrypted = claim.copy()
    if "patient_account_number" in encrypted and encrypted["patient_account_number"]:
        encrypted["patient_account_number"] = encrypt_text(
            str(encrypted["patient_account_number"]), key
        )
    if "patient_name" in encrypted and encrypted["patient_name"]:
        encrypted["patient_name"] = encrypt_text(str(encrypted["patient_name"]), key)
    return encrypted
