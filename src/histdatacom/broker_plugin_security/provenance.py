"""Whitelisted installed software provenance, not raw installation metadata."""

from __future__ import annotations

import hashlib
from importlib import metadata
import json
import os
from pathlib import Path
import re
import stat

from histdatacom.broker_plugin_registry import (
    BrokerPluginCandidateV1,
    normalized_distribution_name,
    discover_broker_plugins,
)
from histdatacom.broker_plugins import BROKER_PLUGIN_SDK_VERSION

from .contracts import (
    BrokerSoftwareProvenanceV1,
    BrokerSecurityReason as Reason,
    refuse,
)


def _metadata_bytes(
    distribution: metadata.PathDistribution, name: str
) -> bytes | None:
    files = distribution.files
    if files is None or len(files) > 16_384:
        refuse(Reason.IDENTITY)
    choices = [
        item
        for item in files
        if item.name == name and item.parent.name.endswith(".dist-info")
    ]
    if not choices:
        return None
    if len(choices) != 1:
        refuse(Reason.IDENTITY)
    root_path = distribution.locate_file("")
    path = distribution.locate_file(choices[0])
    if not isinstance(root_path, Path) or not isinstance(path, Path):
        refuse(Reason.IDENTITY)
    root = root_path.resolve()
    if path.is_symlink() or not path.resolve().is_relative_to(root):
        refuse(Reason.IDENTITY)
    descriptor = os.open(
        path,
        os.O_RDONLY
        | getattr(os, "O_NOFOLLOW", 0)
        | getattr(os, "O_NONBLOCK", 0),
    )
    with os.fdopen(descriptor, "rb") as stream:
        info = os.fstat(stream.fileno())
        if not stat.S_ISREG(info.st_mode) or info.st_size > 32_768:
            refuse(Reason.IDENTITY)
        data = stream.read(32_769)
    if len(data) > 32_768:
        refuse(Reason.IDENTITY)
    return data


def installed_software_provenance(
    candidate: BrokerPluginCandidateV1,
    *,
    attestation: bytes | None = None,
) -> BrokerSoftwareProvenanceV1:
    """Fresh selected-package association; never serialize paths/raw URLs.

    Installer/origin declarations are untrusted metadata, not signatures.
    An optional attestation is merely hash-bound evidence, not verified trust.
    No URL/query/path or digest of raw direct_url.json enters this artifact.
    """
    try:
        fresh = discover_broker_plugins()
        if fresh.diagnostics or candidate not in fresh.candidates:
            refuse(Reason.IDENTITY)
        registration = candidate.registration
        matches = [
            dist
            for dist in metadata.distributions()
            if type(dist) is metadata.PathDistribution
            and normalized_distribution_name(dist.metadata.get("Name", ""))
            == registration.distribution_name
            and dist.version == registration.distribution_version
        ]
        if len(matches) != 1:
            refuse(Reason.IDENTITY)
        distribution = matches[0]
        installer_data = _metadata_bytes(distribution, "INSTALLER")
        installer = (
            "unknown"
            if installer_data is None
            else (
                installer_data.decode("utf-8").strip()
                if installer_data in (b"pip\n", b"pip", b"uv\n", b"uv")
                else "other"
            )
        )
        origin, vcs, commit, archive_hash = "index_or_unknown", "none", "", ""
        direct = _metadata_bytes(distribution, "direct_url.json")
        if direct is not None:
            value = json.loads(direct)
            if type(value) is not dict:
                refuse(Reason.IDENTITY)
            if "vcs_info" in value:
                origin = "vcs"
                details = value["vcs_info"]
                if type(details) is not dict:
                    refuse(Reason.IDENTITY)
                named_vcs = details.get("vcs")
                vcs = (
                    named_vcs
                    if isinstance(named_vcs, str)
                    and named_vcs in ("git", "hg", "svn", "bzr")
                    else "other"
                )
                revision = details.get("commit_id", "")
                if type(revision) is str and re.fullmatch(
                    r"[a-f0-9]{40}|[a-f0-9]{64}", revision
                ):
                    commit = revision
            elif "archive_info" in value:
                origin = "archive"
                details = value["archive_info"]
                if type(details) is not dict:
                    refuse(Reason.IDENTITY)
                hashes = details.get("hashes", {})
                if type(hashes) is not dict:
                    refuse(Reason.IDENTITY)
                digest = hashes.get("sha256", "")
                if type(digest) is str and re.fullmatch(
                    r"[a-f0-9]{64}", digest
                ):
                    archive_hash = digest
            elif "dir_info" in value:
                origin = "local_directory"
        if attestation is not None and (
            type(attestation) is not bytes or len(attestation) > 65_536
        ):
            refuse(Reason.IDENTITY)
        return BrokerSoftwareProvenanceV1(
            candidate.artifact_id,
            registration.distribution_name,
            registration.distribution_version,
            BROKER_PLUGIN_SDK_VERSION,
            candidate.registration_sha256,
            candidate.implementation_sha256,
            installer,
            origin,
            vcs,
            commit,
            archive_hash,
            (
                ""
                if attestation is None
                else hashlib.sha256(attestation).hexdigest()
            ),
        )
    except BaseException:
        refuse(Reason.IDENTITY)
