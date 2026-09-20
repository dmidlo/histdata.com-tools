"""Real bounded publication/read paths, no historical/private inputs."""

import os
from dataclasses import replace

import pytest

from histdatacom.experiments import (
    ExperimentRegistryV1,
    artifact_filename,
    read_experiment_artifact,
    write_experiment_artifact,
)
from histdatacom.experiments import artifacts as io
from tests.fixtures.experiment_bundles import example_registry


@pytest.fixture(scope="module")
def registry():
    return example_registry()


def test_persist_real_registry_and_idempotent_no_clobber(tmp_path, registry):
    path = write_experiment_artifact(registry, tmp_path)
    original = path.read_bytes()
    assert read_experiment_artifact(path, ExperimentRegistryV1) == registry
    assert write_experiment_artifact(registry, tmp_path) == path
    assert path.read_bytes() == original
    assert list(tmp_path.glob(".experiment-*")) == []
    path.write_bytes(b"tampered")
    with pytest.raises(ValueError, match="overwrite"):
        write_experiment_artifact(registry, tmp_path)
    assert path.read_bytes() == b"tampered"


def test_reader_filename_schema_and_byte_mutations(tmp_path, registry):
    path = write_experiment_artifact(registry, tmp_path)
    wrong = tmp_path / "wrong-name.json"
    wrong.write_bytes(path.read_bytes())
    with pytest.raises(ValueError, match="filename"):
        read_experiment_artifact(wrong, ExperimentRegistryV1)
    path.write_bytes(path.read_bytes() + b"\n")
    with pytest.raises(ValueError, match="noncanonical"):
        read_experiment_artifact(path, ExperimentRegistryV1)
    path.write_bytes(b"x" * (8 * 1024 * 1024 + 1))
    with pytest.raises(ValueError, match="bounded"):
        read_experiment_artifact(path, ExperimentRegistryV1)


def test_reader_source_mutation_during_read(tmp_path, registry, monkeypatch):
    path = write_experiment_artifact(registry, tmp_path)
    native = io.os.read
    changed = False

    def mutate(fd, count):
        nonlocal changed
        data = native(fd, count)
        if not changed:
            changed = True
            with path.open("ab") as target:
                target.write(b" ")
        return data

    monkeypatch.setattr(io.os, "read", mutate)
    with pytest.raises(ValueError, match="changed"):
        read_experiment_artifact(path, ExperimentRegistryV1)


def test_symlink_refusal(tmp_path, registry):
    path = write_experiment_artifact(registry, tmp_path)
    link = tmp_path / "alias.json"
    try:
        link.symlink_to(path)
    except NotImplementedError:
        pytest.skip("symlinks unsupported")
    except OSError as exc:
        if os.name == "nt" and getattr(exc, "winerror", None) == 1314:
            pytest.skip("Windows symlink privilege unavailable")
        raise
    with pytest.raises(ValueError, match="regular"):
        read_experiment_artifact(link, ExperimentRegistryV1)


def test_interrupted_publish_has_no_complete_artifact(
    tmp_path, registry, monkeypatch
):
    def interrupted(*args, **kwargs):
        raise OSError("synthetic publication interruption")

    monkeypatch.setattr(io.os, "link", interrupted)
    with pytest.raises(OSError, match="interruption"):
        write_experiment_artifact(registry, tmp_path)
    assert not (tmp_path / artifact_filename(registry)).exists()
    assert list(tmp_path.iterdir()) == []


def test_new_snapshot_never_overwrites_old_file(tmp_path, registry):
    first = write_experiment_artifact(registry, tmp_path)
    old_bytes = first.read_bytes()
    changed = replace(registry, reports=())
    second = write_experiment_artifact(changed, tmp_path)
    assert first != second
    assert first.read_bytes() == old_bytes


@pytest.mark.skipif(
    not hasattr(os, "O_NONBLOCK"), reason="no nonblocking filesystem flag"
)
def test_raced_leaf_open_requires_nonblocking_and_refuses_fifo(
    tmp_path, registry, monkeypatch
):
    import stat

    path = write_experiment_artifact(registry, tmp_path)
    native_open, native_stat = io.os.open, io.os.fstat
    descriptors = []

    def raced_open(target, flags, *args, **kwargs):
        if target == path:
            assert flags & os.O_NONBLOCK
            fd = native_open(target, flags, *args, **kwargs)
            descriptors.append(fd)
            return fd
        return native_open(target, flags, *args, **kwargs)

    def fifo_stat(fd):
        result = native_stat(fd)
        if fd in descriptors:
            values = list(result)
            values[0] = stat.S_IFIFO | 0o600
            return os.stat_result(values)
        return result

    monkeypatch.setattr(io.os, "open", raced_open)
    monkeypatch.setattr(io.os, "fstat", fifo_stat)
    with pytest.raises(ValueError, match="changed before read"):
        read_experiment_artifact(path, ExperimentRegistryV1)
