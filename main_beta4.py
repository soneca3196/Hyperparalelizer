from __future__ import annotations

"""hyperparalelizer.main - bootstrap e orquestração do Hyperparalelizer.

Uso:
    python -m main_beta4 server --host 127.0.0.1 --port 9000 --max-ram-mb 2048 --max-cpu-cores 2
    python -m main_beta4 peer --host 127.0.0.1 --port 9101 --server-port 9000 --max-ram-mb 512 --max-cpu-cores 1
"""

import argparse
import asyncio
import logging
import sys

from hyperparalelizer.runtime.common import (
    AVAILABLE_DATASETS,
    BootstrapError,
    DEFAULT_DATASET_KEY,
    DEFAULT_GRID,
)
from hyperparalelizer.runtime.peer_runtime import run_peer
from hyperparalelizer.runtime.server_runtime import run_server
from utils.logger import set_level


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
    server.add_argument(
        "--debug",
        action="store_true",
        help="Ativa logs detalhados (DEBUG). Por padrão os logs ficam em INFO",
    )
    server.add_argument(
        "--dataset",
        choices=sorted(AVAILABLE_DATASETS.keys()),
        default=None,
        help="Escolhe o dataset sem perguntar interativamente (ver menu no --help)",
    )
    server.add_argument(
        "--non-interactive",
        action="store_true",
        help="Não pergunta dataset/hiperparâmetros; usa --dataset (ou padrão) e os "
        "valores default de cada hiperparâmetro",
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
    peer.add_argument(
        "--debug",
        action="store_true",
        help="Ativa logs detalhados (DEBUG)",
    )

    return parser


def _parse_scalar(raw: str):
    raw = raw.strip()
    try:
        return int(raw)
    except ValueError:
        pass
    try:
        return float(raw)
    except ValueError:
        return raw


def prompt_dataset_choice(preselected: str | None) -> str:
    if preselected:
        return AVAILABLE_DATASETS[preselected][0]

    if len(AVAILABLE_DATASETS) == 1:
        (only_key, (dataset_key, desc)) = next(iter(AVAILABLE_DATASETS.items()))
        print(f"Dataset: {desc}")
        return dataset_key

    print("Datasets disponíveis:")
    for key, (_name, desc) in sorted(AVAILABLE_DATASETS.items()):
        print(f"  [{key}] {desc}")
    default_key = "1"
    choice = input(f"Escolha o dataset [{default_key}]: ").strip() or default_key
    while choice not in AVAILABLE_DATASETS:
        choice = input(
            f"Opção inválida. Escolha entre {sorted(AVAILABLE_DATASETS.keys())}: "
        ).strip()
    return AVAILABLE_DATASETS[choice][0]


def prompt_hyperparameters() -> dict:
    print("Hiperparâmetros do grid search (Enter para usar o padrão):")
    grid: dict = {}
    for param, default_values in DEFAULT_GRID.items():
        raw = input(f"  {param} {default_values}: ").strip()
        if not raw:
            grid[param] = default_values
        else:
            grid[param] = [_parse_scalar(v) for v in raw.split(",")]
    return grid


def prompt_dataset_and_hyperparameters(args: argparse.Namespace) -> None:
    non_interactive = args.non_interactive or not sys.stdin.isatty()

    if non_interactive:
        args.dataset_key = (
            AVAILABLE_DATASETS[args.dataset][0] if args.dataset else DEFAULT_DATASET_KEY
        )
        args.grid = dict(DEFAULT_GRID)
        return

    args.dataset_key = prompt_dataset_choice(args.dataset)
    args.grid = prompt_hyperparameters()


async def async_main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    set_level(logging.DEBUG if args.debug else logging.INFO)

    if args.mode == "server":
        prompt_dataset_and_hyperparameters(args)
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