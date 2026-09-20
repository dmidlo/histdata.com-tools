"""Canonical write-once proofs; reads always rerun admitted implementations."""

import os
import stat
from pathlib import Path

from histdatacom.schema_compatibility import CompatibilityRegistryV1

from .canonical import MAX_PROOF_BYTES, load_json, sha256
from .contracts import SemanticCompositionProofV1, SemanticMigrationProofV1
from .proofs import SemanticProofV1, verify_semantic_proof


def _read(path: Path) -> bytes:
    info = path.lstat()
    if not stat.S_ISREG(info.st_mode) or info.st_size > MAX_PROOF_BYTES:
        raise ValueError("semantic proof requires bounded regular file")
    fd = os.open(
        path,
        os.O_RDONLY
        | getattr(os, "O_NOFOLLOW", 0)
        | getattr(os, "O_NONBLOCK", 0),
    )
    with os.fdopen(fd, "rb") as handle:
        opened = os.fstat(handle.fileno())
        data = handle.read(MAX_PROOF_BYTES + 1)
        after = os.fstat(handle.fileno())

    def stamp(value: os.stat_result) -> tuple[int, ...]:
        return (
            value.st_dev,
            value.st_ino,
            value.st_mode,
            value.st_size,
            value.st_mtime_ns,
            value.st_ctime_ns,
        )

    if (
        stamp(opened) != stamp(info)
        or stamp(opened) != stamp(after)
        or len(data) != info.st_size
    ):
        raise ValueError("semantic proof changed while reading")
    return data


def read_semantic_proof(
    path: Path, registry: CompatibilityRegistryV1
) -> SemanticProofV1:
    data = _read(path)
    if path.name != sha256(data.decode("ascii")) + ".json":
        raise ValueError("semantic proof filename/content differs")
    value = data.decode("ascii")
    payload = load_json(value, maximum=MAX_PROOF_BYTES)
    if type(payload) is not dict:
        raise ValueError("semantic proof requires object envelope")
    schema = payload.get("schema_version")
    if schema == "histdatacom.schema-semantics.migration-proof.v1":
        proof: SemanticProofV1 = SemanticMigrationProofV1.from_json(value)
    elif schema == "histdatacom.schema-semantics.composition-proof.v1":
        proof = SemanticCompositionProofV1.from_json(value)
    else:
        raise ValueError("unsupported semantic proof schema")
    return verify_semantic_proof(registry, proof)


def write_semantic_proof(
    proof: SemanticProofV1, directory: Path, registry: CompatibilityRegistryV1
) -> Path:
    verify_semantic_proof(registry, proof)
    data = proof.to_json().encode("ascii")
    directory.mkdir(parents=True, exist_ok=True)
    if directory.is_symlink() or not directory.is_dir():
        raise ValueError("semantic proof directory must be real")
    target = directory / (sha256(proof.to_json()) + ".json")
    if target.exists() or target.is_symlink():
        if _read(target) != data:
            raise ValueError("existing semantic proof differs")
        return target
    # Exclusive temporary inode; hard-link is atomic no-clobber publication.
    import tempfile

    descriptor, temporary = tempfile.mkstemp(prefix=".semantic-", dir=directory)
    temporary_path = Path(temporary)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        try:
            os.link(temporary_path, target)
        except FileExistsError:
            if _read(target) != data:
                raise ValueError("concurrent semantic proof differs") from None
    finally:
        temporary_path.unlink()
    if os.name == "posix":
        fd = os.open(directory, os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
    return target
