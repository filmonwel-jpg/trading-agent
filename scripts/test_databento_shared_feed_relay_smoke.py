#!/usr/bin/env python3
from __future__ import annotations

import json
import shutil
import socket
import subprocess
import sys
import tempfile
import time
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
RELAY_SCRIPT = REPO_ROOT / "scripts" / "databento_shared_feed_relay.py"


def wait_for_port(host: str, port: int, timeout_seconds: float) -> None:
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        try:
            with socket.create_connection((host, port), timeout=0.5):
                return
        except OSError:
            time.sleep(0.1)
    raise TimeoutError(f"relay did not start listening on {host}:{port}")


def register_client(client: socket.socket, symbol: str, client_key: str) -> str:
    client.sendall((json.dumps({"type": "register", "symbol": symbol, "clientKey": client_key}) + "\n").encode("utf-8"))
    return client.recv(4096).decode("utf-8").strip()


def read_next_event(reader) -> dict[str, object]:
    while True:
        raw_line = reader.readline()
        if raw_line == "":
            raise AssertionError("relay connection closed before expected routed event arrived")
        payload = json.loads(raw_line)
        if payload.get("event") == "status":
            continue
        return payload


def main() -> int:
    workdir = Path(tempfile.mkdtemp(prefix="databento-relay-smoke-"))
    relay_proc: subprocess.Popen[str] | None = None
    try:
        bots_dir = workdir / "bots"
        bots_dir.mkdir(parents=True, exist_ok=True)
        (bots_dir / "trading-aapl.properties").write_text(
            "trading.symbol=AAPL\ntrading.databento.option-parents=AAPL\ntrading.databento.shared-feed.port=19800\n",
            encoding="utf-8",
        )
        (bots_dir / "trading-nvda.properties").write_text(
            "trading.symbol=NVDA\ntrading.databento.option-parents=NVDA\ntrading.databento.shared-feed.port=19800\n",
            encoding="utf-8",
        )

        mock_normalizer = workdir / "mock_normalizer.py"
        normalized_option_parents_file = workdir / "normalized_option_parents.txt"
        startup_history_file = workdir / "startup_history_args.txt"
        live_gateway_file = workdir / "live_gateway_args.txt"
        mock_normalizer.write_text(
            "#!/usr/bin/env python3\n"
            "import json\n"
            "import pathlib\n"
            "import sys\n"
            "import time\n"
            f"args = sys.argv[1:]\n"
            f"parents = args[args.index('--option-parents') + 1]\n"
            f"startup_history_seconds = args[args.index('--startup-history-seconds') + 1]\n"
            f"startup_history_schema = args[args.index('--startup-history-schema') + 1]\n"
            f"live_gateway = args[args.index('--live-gateway') + 1] if '--live-gateway' in args else ''\n"
            f"pathlib.Path({str(normalized_option_parents_file)!r}).write_text(parents + '\\n', encoding='utf-8')\n"
            f"pathlib.Path({str(startup_history_file)!r}).write_text(startup_history_seconds + ',' + startup_history_schema + '\\n', encoding='utf-8')\n"
            f"pathlib.Path({str(live_gateway_file)!r}).write_text(live_gateway + '\\n', encoding='utf-8')\n"
            "print(json.dumps({\"event\":\"status\",\"message\":\"equity-subscribe dataset=EQUS.MINI schema=tbbo\"}), flush=True)\n"
            "print(json.dumps({\"event\":\"status\",\"message\":f\"options-subscribe parents={parents}\"}), flush=True)\n"
            "time.sleep(0.5)\n"
            "print(json.dumps({\"event\":\"equity_bar\",\"symbol\":\"AAPL\",\"barEpochSec\":1,\"close\":100}), flush=True)\n"
            "time.sleep(0.2)\n"
            "print(json.dumps({\"event\":\"equity_bar\",\"symbol\":\"NVDA\",\"barEpochSec\":1,\"close\":200}), flush=True)\n",
            encoding="utf-8",
        )
        mock_normalizer.chmod(0o755)

        port = 19800
        relay_proc = subprocess.Popen(
            [
                sys.executable,
                str(RELAY_SCRIPT),
                "--python-bin",
                sys.executable,
                "--normalizer-script",
                str(mock_normalizer),
                "--bots-dir",
                str(bots_dir),
                "--working-dir",
                str(workdir),
                "--listen-host",
                "127.0.0.1",
                "--listen-port",
                str(port),
                "--pid-file",
                str(workdir / "relay.pid"),
                "--live-gateway",
                "live.databento.test",
                "--startup-history-seconds",
                "360",
                "--startup-history-schema",
                "ohlcv-1s",
                "--expected-client-count",
                "2",
                "--wait-for-clients-timeout-seconds",
                "5",
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )

        wait_for_port("127.0.0.1", port, 5.0)
        clients: list[socket.socket] = []
        readers = []
        try:
            first_aapl = socket.create_connection(("127.0.0.1", port), timeout=5)
            first_aapl.settimeout(5)
            clients.append(first_aapl)
            first_ack = register_client(first_aapl, "AAPL", "AAPL:test-client")

            second_aapl = socket.create_connection(("127.0.0.1", port), timeout=5)
            second_aapl.settimeout(5)
            clients.append(second_aapl)
            second_ack = register_client(second_aapl, "AAPL", "AAPL:test-client")
            second_reader = second_aapl.makefile("r", encoding="utf-8")
            readers.append(second_reader)

            nvda = socket.create_connection(("127.0.0.1", port), timeout=5)
            nvda.settimeout(5)
            clients.append(nvda)
            nvda_ack = register_client(nvda, "NVDA", "NVDA:test-client")
            nvda_reader = nvda.makefile("r", encoding="utf-8")
            readers.append(nvda_reader)

            if "relay_client_registered" not in first_ack or "relay_client_registered" not in second_ack or "relay_client_registered" not in nvda_ack:
                raise AssertionError(f"registration ack missing: {[first_ack, second_ack, nvda_ack]}")

            duplicate_closed = False
            try:
                duplicate_closed = first_aapl.recv(4096) == b""
            except OSError:
                duplicate_closed = True
            if not duplicate_closed:
                raise AssertionError("duplicate client connection was not closed after replacement")

            aapl_payload = read_next_event(second_reader)
            nvda_payload = read_next_event(nvda_reader)
        finally:
            for reader in readers:
                try:
                    reader.close()
                except OSError:
                    pass
            for client in clients:
                try:
                    client.close()
                except OSError:
                    pass

        if aapl_payload.get("symbol") != "AAPL":
            raise AssertionError(f"AAPL client did not receive only its routed payload: {aapl_payload}")
        if nvda_payload.get("symbol") != "NVDA":
            raise AssertionError(f"NVDA client did not receive only its routed payload: {nvda_payload}")
        normalized_option_parents = normalized_option_parents_file.read_text(encoding="utf-8").strip()
        if normalized_option_parents != "AAPL.OPT,NVDA.OPT":
            raise AssertionError(f"relay did not normalize option parents: {normalized_option_parents!r}")
        startup_history_args = startup_history_file.read_text(encoding="utf-8").strip()
        if startup_history_args != "360.0,ohlcv-1s":
            raise AssertionError(f"relay did not forward startup history args: {startup_history_args!r}")
        live_gateway_arg = live_gateway_file.read_text(encoding="utf-8").strip()
        if live_gateway_arg != "live.databento.test":
            raise AssertionError(f"relay did not forward live gateway override: {live_gateway_arg!r}")

        relay_proc.terminate()
        try:
            relay_proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            relay_proc.kill()
            relay_proc.wait(timeout=5)
        relay_stderr = relay_proc.stderr.read() if relay_proc.stderr is not None else ""
        relay_stdout = relay_proc.stdout.read() if relay_proc.stdout is not None else ""
        relay_proc = None
        if "normalizer-status equity-subscribe dataset=EQUS.MINI schema=tbbo" not in relay_stderr:
            raise AssertionError(f"relay stderr missing equity-subscribe status log: {relay_stderr!r}")
        if "normalizer-status options-subscribe parents=AAPL.OPT,NVDA.OPT" not in relay_stderr:
            raise AssertionError(f"relay stderr missing options-subscribe status log: {relay_stderr!r}")
        if relay_stdout.strip():
            raise AssertionError(f"relay unexpectedly wrote to stdout: {relay_stdout!r}")

        print("relay-smoke-ok", json.dumps({"aapl": aapl_payload, "nvda": nvda_payload}))
        return 0
    finally:
        if relay_proc is not None:
            relay_proc.terminate()
            try:
                relay_proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                relay_proc.kill()
                relay_proc.wait(timeout=5)
        shutil.rmtree(workdir, ignore_errors=True)


if __name__ == "__main__":
    raise SystemExit(main())

