"""Lightweight isolation checks for the opt-in publication fixture clock."""

from __future__ import annotations

from contextlib import contextmanager
import inspect
from types import SimpleNamespace

import pytest

from histdatacom.broker_plugin_policy import scope
from tests.fixtures.broker_provider_policy import POLICY_NOW
from tests.unit import test_synthetic_bar_features as features


@pytest.fixture
def outside_clock(monkeypatch):
    """Avoid reading or changing the machine clock in these fixture tests."""
    current = [90_000]

    def sample():
        return current[0]

    monkeypatch.setattr(scope, "_now_ns", sample)
    return sample, current


def _replace_base(monkeypatch, function):
    monkeypatch.setattr(features, "published_source", pytest.fixture(function))


def test_clock_advances_across_setup_body_and_teardown(
    tmp_path, monkeypatch, outside_clock
):
    original, _ = outside_clock
    samples = []
    value = object()

    def base(path):
        assert path == tmp_path
        samples.append(scope._now_ns())
        yield value
        samples.append(scope._now_ns())

    _replace_base(monkeypatch, base)
    generator = features.clocked_published_source.__wrapped__(tmp_path)
    assert next(generator) is value
    samples.extend((scope._now_ns(), scope._now_ns()))
    with pytest.raises(StopIteration):
        next(generator)
    assert samples == list(range(POLICY_NOW, POLICY_NOW + 4))
    assert scope._now_ns is original


def test_unstarted_wrapper_does_not_replace_outside_clock(
    tmp_path, monkeypatch, outside_clock
):
    original, _ = outside_clock

    def base(path):
        pytest.fail("an unstarted generator invoked publication")
        yield path

    _replace_base(monkeypatch, base)
    generator = features.clocked_published_source.__wrapped__(tmp_path)
    assert scope._now_ns is original
    generator.close()
    assert scope._now_ns is original


def test_clock_restored_after_base_setup_error(
    tmp_path, monkeypatch, outside_clock
):
    original, _ = outside_clock
    error = RuntimeError("synthetic setup failure")

    def base(path):
        assert scope._now_ns() == POLICY_NOW
        raise error
        yield path

    _replace_base(monkeypatch, base)
    generator = features.clocked_published_source.__wrapped__(tmp_path)
    with pytest.raises(RuntimeError) as caught:
        next(generator)
    assert caught.value is error
    assert scope._now_ns is original


def test_clock_restored_after_base_teardown_error(
    tmp_path, monkeypatch, outside_clock
):
    original, _ = outside_clock
    error = RuntimeError("synthetic teardown failure")

    def base(path):
        yield path
        assert scope._now_ns() == POLICY_NOW
        raise error

    _replace_base(monkeypatch, base)
    generator = features.clocked_published_source.__wrapped__(tmp_path)
    assert next(generator) == tmp_path
    with pytest.raises(RuntimeError) as caught:
        next(generator)
    assert caught.value is error
    assert scope._now_ns is original


def test_generator_close_runs_base_cleanup_before_clock_restoration(
    tmp_path, monkeypatch, outside_clock
):
    original, _ = outside_clock
    cleanup = []

    def base(path):
        try:
            yield path
        finally:
            cleanup.append(scope._now_ns())

    _replace_base(monkeypatch, base)
    generator = features.clocked_published_source.__wrapped__(tmp_path)
    assert next(generator) == tmp_path
    assert scope._now_ns() == POLICY_NOW
    generator.close()
    assert cleanup == [POLICY_NOW + 1]
    assert scope._now_ns is original


def test_thrown_base_exception_propagates_and_restores_clock(
    tmp_path, monkeypatch, outside_clock
):
    original, _ = outside_clock
    cleanup = []

    class SyntheticCancellation(BaseException):
        pass

    error = SyntheticCancellation()

    def base(path):
        try:
            yield path
        finally:
            cleanup.append(scope._now_ns())

    _replace_base(monkeypatch, base)
    generator = features.clocked_published_source.__wrapped__(tmp_path)
    assert next(generator) == tmp_path
    with pytest.raises(SyntheticCancellation) as caught:
        generator.throw(error)
    assert caught.value is error
    assert cleanup == [POLICY_NOW]
    assert scope._now_ns is original


def test_original_fixture_direct_call_preserves_explicit_mutable_clock(
    tmp_path, monkeypatch, outside_clock
):
    original, current = outside_clock
    observed = []
    native_manifest = object()
    product = SimpleNamespace(
        manifest_path=tmp_path / "product.json", manifest=native_manifest
    )
    bars = SimpleNamespace(manifest_path=tmp_path / "bars.json")
    source = object()

    @contextmanager
    def inputs(path):
        assert path == tmp_path
        observed.append(scope._now_ns())
        yield (object(), (), object(), object())
        observed.append(scope._now_ns())

    @contextmanager
    def native_inputs(manifest):
        assert manifest is native_manifest
        yield

    monkeypatch.setattr(features, "_publication_inputs_scope", inputs)
    monkeypatch.setattr(
        features, "publish_reconstruction_group", lambda *a, **k: product
    )
    monkeypatch.setattr(features, "publish_derived_bars", lambda *a, **k: bars)
    monkeypatch.setattr(features, "BarFeatureSourceV1", lambda *a: source)
    monkeypatch.setattr(
        features, "provider_reconstruction_inputs", native_inputs
    )
    assert tuple(inspect.signature(features.published_source).parameters) == (
        "tmp_path",
    )
    generator = features.published_source.__wrapped__(tmp_path)
    assert next(generator) == (source, product, tmp_path)
    assert scope._now_ns is original
    current[0] = 3  # Preserve even deliberately regressed/expired test clocks.
    with pytest.raises(StopIteration):
        next(generator)
    assert observed == [90_000, 3]
    assert scope._now_ns is original


def test_explicit_body_override_is_not_blocked_by_clock_wrapper(
    tmp_path, monkeypatch, outside_clock
):
    original, _ = outside_clock

    def base(path):
        yield path

    _replace_base(monkeypatch, base)
    generator = features.clocked_published_source.__wrapped__(tmp_path)
    assert next(generator) == tmp_path
    assert scope._now_ns() == POLICY_NOW
    with pytest.MonkeyPatch.context() as body_patch:
        body_patch.setattr(scope, "_now_ns", lambda: 7)
        assert scope._now_ns() == 7
    assert scope._now_ns() == POLICY_NOW + 1
    generator.close()
    assert scope._now_ns is original
