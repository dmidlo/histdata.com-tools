"""Negative installed consumer: a string is not a validated registry."""

from histdatacom.experiments import (
    PromotionBindingV1,
    assert_promotion_admissible,
)


def invalid_registry(promotion: PromotionBindingV1) -> None:
    assert_promotion_admissible("unchecked-registry", promotion)
