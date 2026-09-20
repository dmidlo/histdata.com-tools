"""Ephemeral explicit resolution and bounded known-private-material refusal."""

from __future__ import annotations

import base64
from collections.abc import Mapping
import json
import hashlib
from typing import Protocol
from urllib.parse import quote, quote_plus

from .contracts import BrokerSecurityReason as Reason, refuse


class BrokerSecretProvider(Protocol):
    """Caller-owned resolver; handles and values are never scientific identity."""

    def resolve(self, handle: str) -> str: ...


class BrokerPrivateMaterialGuard:
    """Known strings/JSON, URL and common base64 forms; not covert-channel DLP."""

    __slots__ = ("_patterns", "refused")

    def __init__(self, values: tuple[str, ...]) -> None:
        if type(values) is not tuple or len(values) > 384:
            refuse(Reason.SECRET)
        patterns: set[str] = set()
        for value in values:
            if type(value) is not str or not value or len(value) > 8192:
                refuse(Reason.SECRET)
            try:
                encoded = value.encode("utf-8")
                forms = {
                    value,
                    quote(value, safe=""),
                    quote_plus(value, safe=""),
                    base64.b64encode(encoded).decode(),
                    base64.urlsafe_b64encode(encoded).decode(),
                    hashlib.sha256(encoded).hexdigest(),
                }
                forms |= {item.rstrip("=") for item in forms}
                forms |= {
                    json.dumps(item, ensure_ascii=True)[1:-1] for item in forms
                }
                # Native payload_json embeds a second serialized JSON layer.
                forms |= {
                    json.dumps(item, ensure_ascii=True)[1:-1] for item in forms
                }
                patterns.update(forms)
            except Exception:
                refuse(Reason.SECRET)
        if sum(map(len, patterns)) > 1_048_576:
            refuse(Reason.SECRET)
        self._patterns = tuple(sorted(patterns))
        self.refused = False

    def __repr__(self) -> str:
        return "BrokerPrivateMaterialGuard(<ephemeral>)"

    def check(self, text: str) -> None:
        if type(text) is not str or len(text) > 1_048_576:
            refuse(Reason.SECRET)
        pending: list[tuple[object, int]] = [(text, 0)]
        remaining = 4_194_304
        while pending:
            item, depth = pending.pop()
            if depth > 32:
                refuse(Reason.SECRET)
            if type(item) is str:
                remaining -= len(item) + 1
                if remaining < 0:
                    refuse(Reason.SECRET)
                if any(pattern in item for pattern in self._patterns):
                    self.refused = True
                    refuse(Reason.SECRET)
                if item.startswith(("{", "[", '"')):
                    try:
                        nested = json.loads(item)
                    except (ValueError, RecursionError):
                        continue
                    pending.append((nested, depth + 1))
            elif type(item) is dict:
                if len(item) > 128:
                    refuse(Reason.SECRET)
                pending.extend((value, depth + 1) for value in item.values())
                pending.extend((key, depth + 1) for key in item)
            elif type(item) is list:
                if len(item) > 128:
                    refuse(Reason.SECRET)
                pending.extend((value, depth + 1) for value in item)


def resolve_configuration(
    public: Mapping[str, object],
    handles: Mapping[str, str],
    fields: tuple[str, ...],
    provider: BrokerSecretProvider | None,
    private_identifiers: tuple[str, ...],
) -> tuple[dict[str, object], BrokerPrivateMaterialGuard]:
    try:
        if (
            type(private_identifiers) is not tuple
            or len(private_identifiers) > 128
        ):
            refuse(Reason.RESOLUTION)
        if (
            len(public) > 128
            or len(handles) > 128
            or set(handles) != set(fields)
            or set(public).intersection(handles)
        ):
            refuse(Reason.RESOLUTION)
        resolved: dict[str, object] = dict(public)
        values: list[str] = list(private_identifiers)
        for field, handle in handles.items():
            if (
                provider is None
                or type(handle) is not str
                or not 1 <= len(handle) <= 256
            ):
                refuse(Reason.RESOLUTION)
            value = provider.resolve(handle)
            if type(value) is not str or not 1 <= len(value) <= 8192:
                refuse(Reason.RESOLUTION)
            values.append(value)
            values.append(handle)
            resolved[field] = value
        guard = BrokerPrivateMaterialGuard(tuple(values))
        return resolved, guard
    except BaseException:
        refuse(Reason.RESOLUTION)
