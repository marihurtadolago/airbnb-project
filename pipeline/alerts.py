"""Send pipeline alerts to the team chat through an incoming webhook.

The webhook URL is read from ALERT_WEBHOOK_URL in the .env file. The same
message works for Discord, Slack and Teams incoming webhooks. If the variable
is empty, the alert is only written to the log, so the pipeline still runs on a
computer without any setup.

Check that the channel works with:
    python -m pipeline.alerts --test
"""

import logging
import os
import sys

import requests
from dotenv import load_dotenv

from pipeline.utils import PROJECT_ROOT, get_logger

load_dotenv(PROJECT_ROOT / ".env")
log = get_logger("alerts")

MAX_MESSAGE_LENGTH = 2000  # Discord rejects longer messages
TIMEOUT_SECONDS = 10


def send_alert(severity: str, title: str, details: str = "") -> bool:
    """Log the alert and post it to the webhook. Returns True if it was delivered.

    It never raises: if the chat service is down, the pipeline error that caused
    the alert must still be the one that stops the run.
    """
    message = f"[{severity}] Madrid Airbnb pipeline: {title}"
    if details:
        message += f"\n{details}"
    log.log(getattr(logging, severity, logging.WARNING), "alert: %s", message.replace("\n", " | "))

    url = os.getenv("ALERT_WEBHOOK_URL")
    if not url:
        log.warning("ALERT_WEBHOOK_URL is not set, alert written to the log only")
        return False

    message = message[:MAX_MESSAGE_LENGTH]
    try:
        response = requests.post(url, json={"content": message, "text": message},
                                 timeout=TIMEOUT_SECONDS)
        response.raise_for_status()
    except requests.RequestException as exc:
        # Only the error type is logged: the full message contains the URL, which is a secret.
        log.error("alert could not be delivered (%s)", type(exc).__name__)
        return False
    return True


if __name__ == "__main__":
    if "--test" not in sys.argv:
        sys.exit("usage: python -m pipeline.alerts --test")
    delivered = send_alert("INFO", "test alert", "If you can read this, alerts are working.")
    sys.exit(0 if delivered else 1)
