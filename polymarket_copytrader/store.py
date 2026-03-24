from __future__ import annotations

import json
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict

from .models import StateSnapshot


class StateStore:
    def __init__(self, path: str) -> None:
        self.path = Path(path)

    def load(self) -> StateSnapshot:
        if not self.path.exists():
            return StateSnapshot()
        with self.path.open("r", encoding="utf-8") as handle:
            payload = json.load(handle)
        return StateSnapshot(
            target_wallet=payload.get("target_wallet"),
            last_event_timestamp_ms=int(payload.get("last_event_timestamp_ms", 0)),
            seen_keys=list(payload.get("seen_keys", [])),
            asset_exposures_usdc={
                str(key): float(value)
                for key, value in dict(payload.get("asset_exposures_usdc", {})).items()
            },
            asset_positions_size={
                str(key): float(value)
                for key, value in dict(payload.get("asset_positions_size", {})).items()
            },
        )

    def save(self, snapshot: StateSnapshot) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.path.open("w", encoding="utf-8") as handle:
            json.dump(asdict(snapshot), handle, ensure_ascii=False, indent=2, sort_keys=True)


class EventSink:
    def __init__(self, path: str) -> None:
        self.path = Path(path)

    def write(self, kind: str, payload: Dict[str, object]) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        record = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "kind": kind,
            "payload": payload,
        }
        with self.path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(record, ensure_ascii=False) + "\n")
