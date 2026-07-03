"""User-facing instrument group catalog helpers."""

from __future__ import annotations

from typing import Any

from histdatacom.fx_enums import (
    MAJOR_TRIANGLE_PAIR_GROUPS,
    MAJOR_TRIANGLE_RELATIONSHIPS,
    PAIR_GROUP_BASKETS,
    PAIR_GROUPS,
    Pairs,
    normalize_pair_group,
    pair_group_basket_names,
)

GROUP_CATALOG_SCHEMA_VERSION = 1


def list_group_catalog(
    *,
    triangles: bool = False,
    include_all: bool = False,
) -> dict[str, Any]:
    """Return a JSON-compatible catalog of supported instrument groups."""
    if include_all:
        names = tuple(sorted(PAIR_GROUPS))
        mode = "all"
    elif triangles:
        names = tuple(sorted(MAJOR_TRIANGLE_PAIR_GROUPS))
        mode = "triangles"
    else:
        names = pair_group_basket_names()
        mode = "baskets"
    return {
        "schema_version": GROUP_CATALOG_SCHEMA_VERSION,
        "mode": mode,
        "group_count": len(names),
        "groups": [pair_group_payload(name) for name in names],
    }


def show_group_catalog(group: object) -> dict[str, Any]:
    """Return one supported instrument group payload."""
    return {
        "schema_version": GROUP_CATALOG_SCHEMA_VERSION,
        "group": pair_group_payload(normalize_pair_group(group)),
    }


def pair_group_payload(group: object) -> dict[str, Any]:
    """Return one supported group with expanded symbols and relationships."""
    name = normalize_pair_group(group)
    symbols = tuple(PAIR_GROUPS[name])
    payload: dict[str, Any] = {
        "name": name,
        "type": _group_type(name),
        "symbols": list(symbols),
        "symbol_count": len(symbols),
    }
    if name == "major-triangles":
        payload["relationship_count"] = len(MAJOR_TRIANGLE_RELATIONSHIPS)
        payload["relationship_type"] = "major-fx-triangles"
    if name in MAJOR_TRIANGLE_PAIR_GROUPS:
        relationship = MAJOR_TRIANGLE_PAIR_GROUPS[name]
        payload["relationship"] = triangle_relationship_payload(relationship)
    return payload


def triangle_relationship_payload(
    relationship: tuple[str, str, str],
) -> dict[str, Any]:
    """Return a JSON-compatible triangle relationship payload."""
    direct, numerator, denominator = relationship
    return {
        "direct": direct,
        "numerator": numerator,
        "denominator": denominator,
        "rule": triangle_rule(relationship),
    }


def triangle_rule(relationship: tuple[str, str, str]) -> str:
    """Return a readable triangular comparison rule."""
    direct, numerator, denominator = relationship
    return (
        f"{_symbol_label(numerator)} / {_symbol_label(denominator)} "
        f"~= {_symbol_label(direct)}"
    )


def format_group_catalog(payload: dict[str, Any]) -> str:
    """Return a compact human-readable group catalog."""
    groups = _groups_from_payload(payload)
    mode = str(payload.get("mode") or "")
    if mode == "triangles":
        return _format_triangle_groups(groups)
    if mode == "all":
        return "\n".join(
            [
                "Instrument groups",
                *_format_group_rows(groups),
            ]
        )
    return "\n".join(
        [
            "Instrument group baskets",
            *_format_group_rows(groups),
            "",
            "Use `histdatacom groups list --triangles` to list individual major triangle groups.",
        ]
    )


def format_group_detail(payload: dict[str, Any]) -> str:
    """Return a compact human-readable group detail."""
    group = dict(payload["group"])
    lines = [
        f"Group: {group['name']}",
        f"Type: {group['type']}",
        f"Symbols ({group['symbol_count']}): {', '.join(group['symbols'])}",
    ]
    if group.get("relationship_count") is not None:
        lines.append(f"Triangle relationships: {group['relationship_count']}")
        lines.append(
            "Use `histdatacom groups list --triangles` to list each relationship."
        )
    relationship = group.get("relationship")
    if isinstance(relationship, dict):
        lines.extend(
            [
                f"Direct: {relationship['direct']}",
                f"Numerator: {relationship['numerator']}",
                f"Denominator: {relationship['denominator']}",
                f"Rule: {relationship['rule']}",
            ]
        )
    return "\n".join(lines)


def _format_triangle_groups(groups: list[dict[str, Any]]) -> str:
    lines = ["Major triangle groups"]
    name_width = max((len(str(group["name"])) for group in groups), default=0)
    for group in groups:
        relationship = group.get("relationship")
        rule = ""
        if isinstance(relationship, dict):
            rule = str(relationship.get("rule") or "")
        symbols = ", ".join(str(symbol) for symbol in group["symbols"])
        lines.append(
            f"{str(group['name']).ljust(name_width)}  {rule}  [{symbols}]"
        )
    return "\n".join(lines)


def _format_group_rows(groups: list[dict[str, Any]]) -> list[str]:
    name_width = max((len(str(group["name"])) for group in groups), default=0)
    rows: list[str] = []
    for group in groups:
        suffix = f"{group['symbol_count']} symbols"
        if group.get("relationship_count") is not None:
            suffix += f", {group['relationship_count']} triangle relationships"
        rows.append(f"{str(group['name']).ljust(name_width)}  {suffix}")
    return rows


def _groups_from_payload(payload: dict[str, Any]) -> list[dict[str, Any]]:
    groups = payload.get("groups")
    if not isinstance(groups, list):
        return []
    return [dict(group) for group in groups if isinstance(group, dict)]


def _group_type(name: str) -> str:
    if name in MAJOR_TRIANGLE_PAIR_GROUPS:
        return "triangle"
    if name in PAIR_GROUP_BASKETS:
        return "basket"
    return "group"


def _symbol_label(symbol: str) -> str:
    return str(Pairs[symbol].value).replace("_", "")
