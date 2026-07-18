from __future__ import annotations

"""hyperparalelizer.main - bootstrap e orquestração do Hyperparalelizer.

Uso:
    python -m main_beta4 server --host 127.0.0.1 --port 9000 --max-ram-mb 2048 --max-cpu-cores 2
    python -m main_beta4 peer --host 127.0.0.1 --port 9101 --server-port 9000 --max-ram-mb 512 --max-cpu-cores 1
"""

import argparse
import asyncio
import sys

from hyperparalelizer.runtime.common import BootstrapError
from hyperparalelizer.runtime.peer_runtime import run_peer
from hyperparalelizer.runtime.server_runtime import run_server


def positive_int(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("valor deve ser maior que zero")
    return parsed


def positive_float(value: str) -> float:
    parsed = float(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("valor deve ser maior que zero")
    return parsed


def valid_port(value: str) -> int:
    parsed = int(value)
    if not (1 <= parsed <= 65535):
        raise argparse.ArgumentTypeError("porta deve estar entre 1 e 65535")
    return parsed


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Hyperparalelizer - servidor e peer distribuído"
    )
    subparsers = parser.add_subparsers(dest="mode", required=True)

    server = subparsers.add_parser("server", help="Inicia o servidor central")
    server.add_argument("--host", default="127.0.0.1")
    server.add_argument("--port", type=valid_port, default=9000)
    server.add_argument("--fragments", type=positive_int, default=10)
    server.add_argument("--task-timeout", type=positive_float, default=30.0)
    server.add_argument("--heartbeat-interval", type=positive_float, default=5.0)
    server.add_argument("--heartbeat-timeout", type=positive_float, default=12.0)
    server.add_argument("--heartbeat-failures", type=positive_int, default=3)
    server.add_argument("--stop-when-complete", action="store_true")
    server.add_argument("--min-peers", type=positive_int, default=1)
    server.add_argument("--peer-wait-timeout", type=positive_float, default=30.0)
    server.add_argument("--disable-pubsub", action="store_true")
    server.add_argument("--disable-pupil", action="store_true")
    server.add_argument(
        "--max-task-retries",
        type=positive_int,
        default=3,
        help="Limite de tentativas por tarefa",
    )
    server.add_argument(
        "--max-ram-mb",
        type=positive_float,
        default=None,
        help="Limite de RAM (memória virtual) do processo do servidor, em MiB",
    )
    server.add_argument(
        "--max-cpu-cores",
        type=positive_int,
        default=None,
        help="Quantidade de núcleos de CPU que o servidor pode usar",
    )

    peer = subparsers.add_parser("peer", help="Inicia um nó de treinamento")
    peer.add_argument("--host", default="127.0.0.1")
    peer.add_argument("--port", type=valid_port, required=True)
    peer.add_argument("--server-host", default="127.0.0.1")
    peer.add_argument("--server-port", type=valid_port, default=9000)
    peer.add_argument("--storage-dir", default=None)
    peer.add_argument("--reset-storage", action="store_true")
    peer.add_argument("--join-timeout", type=positive_float, default=60.0)
    peer.add_argument("--heartbeat-interval", type=positive_float, default=5.0)
    peer.add_argument("--heartbeat-timeout", type=positive_float, default=3.0)
    peer.add_argument("--heartbeat-failures", type=positive_int, default=3)
    peer.add_argument("--disable-maekawa", action="store_true")
    peer.add_argument("--disable-bully", action="store_true")
    peer.add_argument("--disable-pubsub", action="store_true")
    peer.add_argument(
        "--max-ram-mb",
        type=positive_float,
        default=None,
        help="Limite de RAM (memória virtual) do processo do peer, em MiB",
    )
    peer.add_argument(
        "--max-cpu-cores",
        type=positive_int,
        default=None,
        help="Quantidade de núcleos de CPU que o peer pode usar",
    )

    return parser


async def async_main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    if args.mode == "server":
        await run_server(args)
    elif args.mode == "peer":
        await run_peer(args)
    else:
        parser.error(f"Modo desconhecido: {args.mode}")


def main() -> None:
    try:
        asyncio.run(async_main())
    except KeyboardInterrupt:
        print("\n[SHUTDOWN] Interrupção recebida; encerrado pelo usuário")
    except BootstrapError as exc:
        print(f"[BOOTSTRAP ERROR] {exc}", file=sys.stderr)
        raise SystemExit(2) from exc


if __name__ == "__main__":
    main()