#!/usr/bin/env python3
"""Exercise the shipped health-check command against isolated real Cairn nodes."""
import os
from pathlib import Path
import signal
import socket
import subprocess
import tempfile
import time

ROOT = Path(__file__).resolve().parents[1]
BIN = Path(os.environ.get("BIN", ROOT / "target/debug/cairn")).resolve()


def probe(env):
    result = subprocess.run(
        [str(BIN), "healthcheck"], env=env, capture_output=True, timeout=7
    )
    assert result.stdout == b"", "successful probes must stay quiet"
    return result


def exercise(ipv6=False, tls=False):
    family = socket.AF_INET6 if ipv6 else socket.AF_INET
    loopback = "::1" if ipv6 else "127.0.0.1"
    with socket.socket(family) as reservation:
        reservation.bind((loopback, 0))
        port = reservation.getsockname()[1]
    with tempfile.TemporaryDirectory(prefix="cairn-healthcheck-") as temporary:
        root = Path(temporary)
        env = {k: v for k, v in os.environ.items() if not k.startswith("CAIRN_")}
        env.update(
            CAIRN_DATA_DIR=str(root / "data"),
            CAIRN_DB_PATH=str(root / "data/cairn.db"),
            CAIRN_API_ADDR=f"[::]:{port}" if ipv6 else f"0.0.0.0:{port}",
            CAIRN_CONSOLE_ADDR="off",
            CAIRN_ALLOW_INSECURE="true",
        )
        if tls:
            env.update(
                CAIRN_TLS_CERT_PATH=str(ROOT / "crates/cairn-server/testdata/tls_a.crt"),
                CAIRN_TLS_KEY_PATH=str(ROOT / "crates/cairn-server/testdata/tls_a.key"),
            )
        with (root / "server.log").open("wb") as log:
            node = subprocess.Popen([str(BIN), "serve"], env=env, stdout=log, stderr=log)
            try:
                deadline = time.monotonic() + 30
                while probe(env).returncode != 0:
                    assert node.poll() is None, (root / "server.log").read_text()
                    assert time.monotonic() < deadline, "fixture node did not become ready"
                    time.sleep(0.05)
                # The running node owns its lock. A health probe must not contend with it or
                # open/create local state even if the probe's data path does not exist.
                detached = dict(env, CAIRN_DATA_DIR=str(root / "untouched"),
                                CAIRN_DB_PATH=str(root / "untouched/cairn.db"))
                assert probe(detached).returncode == 0
                assert not (root / "untouched").exists()
                invalid = probe(dict(env, CAIRN_API_ADDR="not-a-socket-address"))
                assert invalid.returncode == 1, "Docker probes must use exit 1, not reserved exit 2"
                assert invalid.stderr == b"health check: invalid node configuration\n"
                if tls:
                    wrong = dict(env, CAIRN_TLS_CERT_PATH=str(
                        ROOT / "crates/cairn-server/testdata/tls_b.crt"))
                    assert probe(wrong).returncode == 1, "different certificate accepted"
                # TCP remains bound while a stopped process cannot service readiness.
                node.send_signal(signal.SIGSTOP)
                started = time.monotonic()
                try:
                    assert probe(env).returncode == 1, "stalled node reported healthy"
                    assert time.monotonic() - started < 6, "probe exceeded Docker timeout"
                finally:
                    node.send_signal(signal.SIGCONT)
                assert probe(env).returncode == 0, "resumed node did not recover"
            finally:
                node.terminate()
                try:
                    node.wait(timeout=40)
                except subprocess.TimeoutExpired:
                    node.kill()
                    node.wait()
            assert probe(env).returncode == 1, "stopped node reported healthy"
        print(f"healthcheck: {'TLS' if tls else 'HTTP'} {'IPv6' if ipv6 else 'IPv4'} custom port PASS")


if __name__ == "__main__":
    for use_ipv6, use_tls in [(False, False), (True, False), (False, True)]:
        exercise(use_ipv6, use_tls)
