import logging
import smtplib
from email.message import EmailMessage
from typing import Iterable


class EmailNotifier:
    """Simple SMTP email notifier."""

    def __init__(
        self,
        smtp_host: str = "localhost",
        smtp_port: int = 25,
        from_addr: str = "alerts@example.com",
    ) -> None:
        self.smtp_host = smtp_host
        self.smtp_port = smtp_port
        self.from_addr = from_addr

    def send(self, recipients: Iterable[str], subject: str, body: str) -> None:
        if not recipients:
            return
        msg = EmailMessage()
        msg["Subject"] = subject
        msg["From"] = self.from_addr
        msg["To"] = ", ".join(recipients)
        msg.set_content(body)
        try:
            with smtplib.SMTP(self.smtp_host, self.smtp_port) as smtp:
                smtp.send_message(msg)
        except Exception as exc:  # pragma: no cover - best effort
            logging.getLogger("claims_processor").error(
                "Failed to send alert email: %s", exc
            )
