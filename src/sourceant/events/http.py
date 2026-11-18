from __future__ import annotations

import hashlib
import hmac
import json
import time
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from .event import Event


class DeliveryError(Exception):
    pass


class SignedHTTP:
    def __init__(
        self,
        url: str,
        secret: str,
        *,
        timestamp_header="X-Events-Timestamp",
        signature_header="X-Events-Signature",
        encode=None,
        timeout=15,
    ):
        if not url or not secret:
            raise ValueError("Event delivery requires a destination and signing secret")
        self.url = url
        self.secret = secret
        self.timestamp_header = timestamp_header
        self.signature_header = signature_header
        self.encode = encode or Event.to_dict
        self.timeout = timeout

    def __call__(self, event: Event):
        body = json.dumps(
            self.encode(event), separators=(",", ":"), allow_nan=False
        ).encode()
        timestamp = str(int(time.time()))
        signature = hmac.new(
            self.secret.encode(), timestamp.encode() + b"." + body, hashlib.sha256
        ).hexdigest()
        request = Request(
            self.url,
            data=body,
            method="POST",
            headers={
                "Content-Type": "application/json",
                self.timestamp_header: timestamp,
                self.signature_header: signature,
            },
        )
        try:
            with urlopen(request, timeout=self.timeout) as response:
                if 200 <= response.status < 300:
                    return
        except (HTTPError, URLError, TimeoutError, OSError):
            pass
        raise DeliveryError("Event delivery was not acknowledged")
