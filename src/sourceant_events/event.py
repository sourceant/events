from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Mapping
from uuid import uuid4


@dataclass(frozen=True)
class Event:
    type: str
    data: Mapping[str, Any] = field(default_factory=dict)
    metadata: Mapping[str, Any] = field(default_factory=dict)
    id: str = field(default_factory=lambda: str(uuid4()))
    occurred_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    version: int = 1

    def __post_init__(self):
        if not isinstance(self.type, str) or not self.type.strip() or self.type == "*":
            raise ValueError("An event requires a nonempty type other than '*'")
        if not isinstance(self.id, str) or not self.id:
            raise ValueError("An event requires an identifier")
        if not isinstance(self.data, Mapping) or not isinstance(self.metadata, Mapping):
            raise ValueError("Event data and metadata must be mappings")
        if (
            not isinstance(self.occurred_at, datetime)
            or self.occurred_at.tzinfo is None
        ):
            raise ValueError("An event requires a timezone-aware occurrence time")
        if type(self.version) is not int or self.version != 1:
            raise ValueError("Unsupported event version")

    def to_dict(self) -> dict:
        """Local payloads may contain live objects; delivery requires JSON values."""
        payload = {
            "version": self.version,
            "id": self.id,
            "type": self.type,
            "occurred_at": self.occurred_at.astimezone(timezone.utc).isoformat(),
            "data": self.data,
            "metadata": self.metadata,
        }
        try:
            return json.loads(json.dumps(payload, allow_nan=False))
        except (TypeError, ValueError) as error:
            raise ValueError(
                "Event delivery requires JSON-serializable data"
            ) from error

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> Event:
        try:
            return cls(
                type=payload["type"],
                data=payload["data"],
                metadata=payload.get("metadata", {}),
                id=payload["id"],
                occurred_at=datetime.fromisoformat(
                    payload["occurred_at"].replace("Z", "+00:00")
                ),
                version=payload["version"],
            )
        except (AttributeError, KeyError, TypeError, ValueError) as error:
            raise ValueError("Invalid event envelope") from error
