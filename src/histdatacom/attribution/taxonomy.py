"""Bounded exact wide taxonomy resolution, not a historical wide-table engine."""

from __future__ import annotations

from collections.abc import Sequence

from .contracts import (
    AttributionFeatureShardV1,
    AttributionFeatureV1,
    AttributionTaxonomyV1,
    reference,
)


def resolve_attribution_taxonomy(
    taxonomy: AttributionTaxonomyV1,
    shards: Sequence[AttributionFeatureShardV1],
    *,
    names: tuple[str, ...] | None = None,
) -> tuple[AttributionFeatureV1, ...]:
    """Verify exact full shard inventory; return named or all declared entries.

    At most64×256 features and128MiB canonical shard bytes are admitted. This
    describes namespaces; it does not assert any compiled strategy, historical
    column values, independence or efficient physical corpus projection.
    """
    if len(shards) != len(taxonomy.shards) or len(shards) > 64:
        raise ValueError("complete bounded taxonomy shard inventory required")
    if names is not None and (
        len(names) > 4096 or names != tuple(sorted(set(names)))
    ):
        raise ValueError(
            "selected names require bounded sorted unique inventory"
        )
    refs = {ref.native_id: ref for ref in taxonomy.shards}
    if len(refs) != len(shards):
        raise ValueError("duplicate taxonomy shard")
    total = 0
    features: dict[str, AttributionFeatureV1] = {}
    seen = set()
    for shard in shards:
        identity = shard.artifact_id
        total += len(shard.to_json())
        if total > 128 * 1024 * 1024:
            raise ValueError("aggregate taxonomy byte bound")
        if (
            identity in seen
            or refs.get(identity) != reference(shard)
            or shard.catalog != taxonomy.catalog
        ):
            raise ValueError("taxonomy shard bytes/catalog differ")
        seen.add(identity)
        for feature in shard.features:
            if feature.name in features:
                raise ValueError("feature duplicated across shards")
            features[feature.name] = feature
    if len(features) != taxonomy.feature_count:
        raise ValueError(
            "taxonomy declared count differs from complete inventory"
        )
    if names is not None and not set(names) <= features.keys():
        raise ValueError("requested taxonomy feature is absent")
    return tuple(
        features[name]
        for name in (sorted(features) if names is None else names)
    )


def attribution_feature_groups(
    feature: AttributionFeatureV1,
) -> tuple[str, ...]:
    """Inspect hierarchy labels without inventing empirical family membership."""
    groups = ["plane:" + feature.plane]
    if feature.timeframe is not None:
        groups.append("timeframe:" + feature.timeframe)
    if feature.strategy_id is not None:
        groups.extend(
            (
                "strategy:" + feature.strategy_id,
                "family:" + str(feature.strategy_family),
                "dependency_class:" + str(feature.dependency_class),
                "population:strategy-proxies",
            )
        )
    return tuple(groups)
