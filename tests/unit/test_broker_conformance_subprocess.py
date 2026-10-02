"""Pure fake-process checks: no real subprocess, plugin or native execution."""

import subprocess

import pytest

from tests.fixtures import broker_conformance_subprocess as launcher


class Pipe:
    def __init__(self, error=None):
        self.closed = False
        self.error = error

    def close(self):
        self.closed = True
        if self.error is not None:
            raise self.error


class FakeProcess:
    def __init__(self, actions, *, terminate_error=None, kill_error=None):
        self.actions = list(actions)
        self.terminate_error = terminate_error
        self.kill_error = kill_error
        self.returncode = None
        self.stdout = Pipe()
        self.stderr = Pipe()
        self.calls = []

    def communicate(self, *, timeout):
        self.calls.append(("communicate", timeout))
        action = self.actions.pop(0)
        if isinstance(action, BaseException):
            raise action
        self.returncode, stdout, stderr = action
        return stdout, stderr

    def terminate(self):
        self.calls.append(("terminate",))
        if self.terminate_error is not None:
            raise self.terminate_error

    def kill(self):
        self.calls.append(("kill",))
        if self.kill_error is not None:
            raise self.kill_error

    def wait(self, *args, **kwargs):
        pytest.fail(
            "never use wait instead of bounded pipe-draining communicate"
        )

    def __enter__(self):
        pytest.fail("never enter Popen context with unbounded __exit__ wait")

    def __exit__(self, *args):
        pytest.fail("never call unbounded Popen __exit__")


def install_fake(monkeypatch, tmp_path, process):
    calls = []

    def popen(command, **kwargs):
        calls.append((command, kwargs))
        assert kwargs == {
            "cwd": tmp_path,
            "stdin": subprocess.DEVNULL,
            "stdout": subprocess.PIPE,
            "stderr": subprocess.PIPE,
            "start_new_session": True,
        }
        return process

    monkeypatch.setattr(launcher.subprocess, "Popen", popen)
    return calls


def run(tmp_path, **kwargs):
    return launcher.run_conformance_worker(
        ["fake-python", "-I", "generated-only"],
        cwd=tmp_path,
        timeout=300,
        **kwargs,
    )


@pytest.mark.parametrize("returncode", (0, 7, -15))
def test_normal_result_preserves_exit_and_output_without_cleanup(
    monkeypatch, tmp_path, returncode
):
    process = FakeProcess([(returncode, b"stdout", b"stderr")])
    calls = install_fake(monkeypatch, tmp_path, process)
    result = run(tmp_path)
    assert result.returncode == returncode
    assert result.stdout == b"stdout" and result.stderr == b"stderr"
    assert result.args == ["fake-python", "-I", "generated-only"]
    assert len(calls) == 1
    assert process.calls == [("communicate", 300)]
    assert not hasattr(result, "conformance_cleanup")


def test_timeout_graceful_exit_keeps_original_failure_and_unknown_native(
    monkeypatch, tmp_path, capsys
):
    original = subprocess.TimeoutExpired("fake-python", 300, b"partial")
    process = FakeProcess([original, (-2, b"complete", b"diagnostic")])
    install_fake(monkeypatch, tmp_path, process)
    with pytest.raises(subprocess.TimeoutExpired) as caught:
        run(tmp_path)
    assert caught.value is original
    assert original.cmd == "fake-python" and original.timeout == 300
    assert original.output == b"partial"
    cleanup = original.conformance_cleanup
    assert cleanup.terminate_requested and cleanup.graceful_direct_exit
    assert cleanup.direct_reaped and not cleanup.hard_escalation
    assert cleanup.native_cleanup == "unknown" and cleanup.failures == ()
    assert cleanup.stdout == b"complete" and cleanup.stderr == b"diagnostic"
    assert process.calls == [
        ("communicate", 300),
        ("terminate",),
        ("communicate", 15),
    ]
    assert process.stdout.closed and process.stderr.closed
    assert "native cleanup UNKNOWN" in capsys.readouterr().err


def test_timeout_escalation_remains_unknown_even_when_direct_child_reaped(
    monkeypatch, tmp_path
):
    original = subprocess.TimeoutExpired("original", 300)
    second = subprocess.TimeoutExpired("grace", 15, b"grace partial")
    process = FakeProcess([original, second, (-9, b"after kill", b"err")])
    install_fake(monkeypatch, tmp_path, process)
    with pytest.raises(subprocess.TimeoutExpired) as caught:
        run(tmp_path)
    assert caught.value is original
    cleanup = original.conformance_cleanup
    assert cleanup.hard_escalation and cleanup.direct_reaped
    assert not cleanup.graceful_direct_exit
    assert cleanup.native_cleanup == "unknown"
    assert cleanup.failures == ("grace_communicate:TimeoutExpired",)
    assert cleanup.stdout == b"after kill"
    assert process.calls[-2:] == [("kill",), ("communicate", 5)]


@pytest.mark.parametrize("error_type", (KeyboardInterrupt, SystemExit, OSError))
def test_interrupt_or_io_error_unwinds_and_reraises_same_exception(
    monkeypatch, tmp_path, error_type
):
    original = error_type("original")
    process = FakeProcess([original, (-2, b"out", b"err")])
    install_fake(monkeypatch, tmp_path, process)
    with pytest.raises(error_type) as caught:
        run(tmp_path)
    assert caught.value is original
    assert original.args == ("original",)
    assert original.conformance_cleanup.graceful_direct_exit
    assert original.conformance_cleanup.native_cleanup == "unknown"


@pytest.mark.parametrize("phase", ("grace", "kill"))
def test_repeated_interrupt_during_cleanup_keeps_original_and_stays_bounded(
    monkeypatch, tmp_path, phase
):
    original = KeyboardInterrupt("first interrupt")
    repeated = KeyboardInterrupt("second interrupt")
    cleanup_actions = (
        [repeated, (-9, b"after kill", b"")]
        if phase == "grace"
        else [subprocess.TimeoutExpired("grace", 15), repeated]
    )
    process = FakeProcess([original, *cleanup_actions])
    install_fake(monkeypatch, tmp_path, process)
    with pytest.raises(KeyboardInterrupt) as caught:
        run(tmp_path)
    assert caught.value is original
    cleanup = original.conformance_cleanup
    assert cleanup.hard_escalation
    assert cleanup.direct_reaped is (phase == "grace")
    assert cleanup.native_cleanup == "unknown"
    assert phase + "_communicate:KeyboardInterrupt" in cleanup.failures
    assert process.calls == [
        ("communicate", 300),
        ("terminate",),
        ("communicate", 15),
        ("kill",),
        ("communicate", 5),
    ]
    assert process.stdout.closed and process.stderr.closed


@pytest.mark.parametrize("reaped", (False, True))
def test_signal_and_reap_failures_never_mask_original_or_claim_cleanup(
    monkeypatch, tmp_path, reaped
):
    original = KeyboardInterrupt("original interrupt")
    final = (0, b"late", b"") if reaped else OSError("reap failed")
    process = FakeProcess(
        [original, OSError("grace failed"), final],
        terminate_error=OSError("term failed"),
        kill_error=OSError("kill failed"),
    )
    install_fake(monkeypatch, tmp_path, process)
    with pytest.raises(KeyboardInterrupt) as caught:
        run(tmp_path)
    assert caught.value is original
    cleanup = original.conformance_cleanup
    assert not cleanup.terminate_requested and not cleanup.graceful_direct_exit
    assert cleanup.hard_escalation and cleanup.direct_reaped is reaped
    assert cleanup.native_cleanup == "unknown"
    assert cleanup.failures[:3] == (
        "terminate:OSError",
        "grace_communicate:OSError",
        "kill:OSError",
    )
    assert ("kill_communicate:OSError" in cleanup.failures) is not reaped
    assert process.stdout.closed and process.stderr.closed
    assert process.calls == [
        ("communicate", 300),
        ("terminate",),
        ("communicate", 15),
        ("kill",),
        ("communicate", 5),
    ]


def test_failed_terminate_followed_by_exit_still_stays_unknown(
    monkeypatch, tmp_path
):
    original = subprocess.TimeoutExpired("original", 300)
    process = FakeProcess(
        [original, (0, b"late", b"")],
        terminate_error=ProcessLookupError("already gone"),
    )
    install_fake(monkeypatch, tmp_path, process)
    with pytest.raises(subprocess.TimeoutExpired):
        run(tmp_path)
    cleanup = original.conformance_cleanup
    assert cleanup.direct_reaped
    assert not cleanup.graceful_direct_exit and not cleanup.hard_escalation
    assert cleanup.native_cleanup == "unknown"
    assert cleanup.failures == ("terminate:ProcessLookupError",)


def test_missing_direct_status_escalates_instead_of_inventing_reaping(
    monkeypatch, tmp_path
):
    process = FakeProcess(
        [(None, b"initial", b""), (None, b"grace", b""), (None, b"kill", b"")]
    )
    install_fake(monkeypatch, tmp_path, process)
    with pytest.raises(RuntimeError) as caught:
        run(tmp_path)
    cleanup = caught.value.conformance_cleanup
    assert cleanup.hard_escalation and not cleanup.direct_reaped
    assert cleanup.native_cleanup == "unknown"
    assert cleanup.failures == (
        "grace_communicate:RuntimeError",
        "kill_communicate:RuntimeError",
    )


def test_pipe_close_and_diagnostic_errors_do_not_mask_interrupt(
    monkeypatch, tmp_path
):
    original = KeyboardInterrupt()
    process = FakeProcess([original, (-2, b"out", b"err")])
    process.stdout.error = OSError("close failure")
    install_fake(monkeypatch, tmp_path, process)

    class BrokenDiagnostic:
        def write(self, message):
            raise OSError("diagnostic unavailable")

    monkeypatch.setattr(launcher.sys, "stderr", BrokenDiagnostic())
    with pytest.raises(KeyboardInterrupt) as caught:
        run(tmp_path)
    assert caught.value is original
    assert original.conformance_cleanup.native_cleanup == "unknown"
    assert original.conformance_cleanup.failures == ("stdout_close:OSError",)
    assert original.conformance_cleanup_diagnostic_failed


def test_spawn_error_propagates_without_inventing_process_cleanup(
    monkeypatch, tmp_path
):
    original = OSError("not started")

    def refused(*args, **kwargs):
        raise original

    monkeypatch.setattr(launcher.subprocess, "Popen", refused)
    with pytest.raises(OSError) as caught:
        run(tmp_path)
    assert caught.value is original
    assert not hasattr(original, "conformance_cleanup")


@pytest.mark.parametrize(
    "field,value",
    (
        ("timeout", float("inf")),
        ("timeout", 0),
        ("timeout", 3601),
        ("grace_timeout", 0),
        ("grace_timeout", 31),
        ("kill_timeout", float("nan")),
        ("kill_timeout", 11),
    ),
)
def test_unbounded_or_invalid_deadline_refuses_before_launch(
    monkeypatch, tmp_path, field, value
):
    process = FakeProcess([])
    calls = install_fake(monkeypatch, tmp_path, process)
    options = {"timeout": 300, field: value}
    with pytest.raises(ValueError, match="finite bounded"):
        launcher.run_conformance_worker(["fake"], cwd=tmp_path, **options)
    assert not calls
