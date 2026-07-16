# -*- coding: utf-8 -*-
"""
COMO EXECUTAR
  TERMINAL 1: python main_beta2.py server --host 127.0.0.1 --port 9000 --fragments 4 --stop-when-complete
  TERMINAL 2: python main_beta2.py peer --host 127.0.0.1 --port 9101 --server-host 127.0.0.1 --server-port 9000 --reset-storage
  TERMINAL 3: python main_beta2.py peer --host 127.0.0.1 --port 9102 --server-host 127.0.0.1 --server-port 9000 --reset-storage
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import shutil
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

import numpy as np
import pandas as pd

from core.network import P2PNode, send_message, send_once
from hyperparalelizer.global_table import GlobalTable
from hyperparalelizer.ml.dataset_loader import DatasetLoader
from hyperparalelizer.peer.data_thread import DataThread
from hyperparalelizer.peer.peer_messenger import PeerMessenger
from hyperparalelizer.peer.peer_outer_protocol import register_peer_peer_handlers
from hyperparalelizer.peer.trainer import TrainerNode
from hyperparalelizer.server.coordinator import Coordinator
from hyperparalelizer.server.server_messenger import ServerMessenger
from hyperparalelizer.server.server_peer_protocol import register_all_handlers
from hyperparalelizer.sync.bully import BullyElection
from hyperparalelizer.sync.maekawa import MaekawaMutex
from utils.logger import get_logger
from utils.protocol import MSG_ACK, MSG_KEEP_ALIVE, Ack, KeepAlive

log = get_logger("main")

DEFAULT_DATASET_PATH = Path("dataset/dataset_risco_credito_3000.csv")
DEFAULT_TARGET_COLUMN = "inadimplente"

DEFAULT_HYPERPARAMETER_GRID: Dict[str, List[Any]] = {
    "n_estimators": [50, 100, 200],
    "max_depth": [5, 10, 20, None],
    "random_state": [42],
    "n_jobs": [1],
}


class NoOpMutex:

    async def request_access(self) -> None:
        return

    async def release_access(self) -> None:
        return


def load_credit_risk_dataset(
    csv_path: Path,
    target_column: str,
) -> tuple[np.ndarray, np.ndarray]:
    """Carrega o dataset real do projeto (risco de crédito) a partir do CSV."""
    df = pd.read_csv(csv_path)

    if target_column not in df.columns:
        raise ValueError(
            f"Coluna alvo '{target_column}' não encontrada em {csv_path}. "
            f"Colunas disponíveis: {list(df.columns)}"
        )

    y = df[target_column].to_numpy()
    X = df.drop(columns=[target_column]).to_numpy(dtype=float)
    return X, y

def build_maekawa_mutex(node_id: str, known_peers: List[Dict[str, Any]]) -> Any:
    """Constrói o mutex de Maekawa com o quórum de peers conhecidos no join.
    Sem peers conhecidos, usamos um mutex no-op"""
    if not known_peers:
        return NoOpMutex()
    return MaekawaMutex(node_id=node_id, quorum=list(known_peers))


def build_bully_peers_map(known_peers: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    """peers conhecidos vão para o dicionário id_node -> {ip, port}"""
    return {
        peer["id_node"]: {"ip": peer["ip"], "port": peer["port"]}
        for peer in known_peers
        if peer.get("id_node")
    }


async def take_over_as_coordinator(
    *,
    trainer: TrainerNode,
    host: str,
    port: int,
    healthcheck_interval: float,
    healthcheck_request_timeout: float,
    healthcheck_max_failures: int,
    task_check_interval: float,
) -> None:
    """Executa quando este peer vence a eleição de Bully.
    """
    log.warning(
        "Este nó venceu a eleição de Bully. Assumindo papel de coordenador "
        "a partir do estado replicado (Peer Pupilo)."
    )

    new_coordinator = trainer.promote_to_server()

    messenger = ServerMessenger(coordinator=new_coordinator, host=host, port=port)
    register_all_handlers(
        messenger=messenger,
        coordinator=new_coordinator,
        status_queue=messenger.status_queue,
    )

    healthcheck_task = asyncio.create_task(
        server_healthcheck_loop(
            new_coordinator,
            interval=healthcheck_interval,
            request_timeout=healthcheck_request_timeout,
            max_failures=healthcheck_max_failures,
        ),
        name="promoted-peer-healthcheck",
    )
    timeout_task = asyncio.create_task(
        task_timeout_recovery_loop(new_coordinator, interval=task_check_interval),
        name="promoted-peer-task-timeout",
    )

    try:
        await messenger.start()
    finally:
        healthcheck_task.cancel()
        timeout_task.cancel()
        await asyncio.gather(healthcheck_task, timeout_task, return_exceptions=True)


# Loops de background compartilhados (servidor e servidor promovido)


async def server_healthcheck_loop(
    coordinator: Coordinator,
    *,
    interval: float,
    request_timeout: float,
    max_failures: int,
) -> None:
    """Servidor testa os peers via heartbeat; remove e reenfileira
    tarefas de quem não responder após max_failures tentativas"""
    failures: Dict[str, int] = {}

    while True:
        nodes = coordinator.GlobalTable.get_all_nodes()
        active_ids = {node["node_id"] for node in nodes}

        for old_node_id in list(failures):
            if old_node_id not in active_ids:
                failures.pop(old_node_id, None)

        for node in nodes:
            node_id = node["node_id"]
            ip = node["ip"]
            port = node["port"]

            reply = await send_once(
                ip,
                port,
                KeepAlive(id_node="server").to_dict(),
                expect_reply=True,
                timeout=request_timeout,
            )

            valid = (
                isinstance(reply, dict)
                and reply.get("type") == MSG_ACK
                and reply.get("ref_type") == MSG_KEEP_ALIVE
                and reply.get("ref_id") == node_id
            )

            if valid:
                if failures.get(node_id, 0) > 0:
                    log.info("Peer %s voltou a responder", node_id[:8])
                failures[node_id] = 0
                continue

            failures[node_id] = failures.get(node_id, 0) + 1
            log.warning(
                "Peer %s não respondeu ao health check (%s/%s)",
                node_id[:8],
                failures[node_id],
                max_failures,
            )

            if failures[node_id] < max_failures:
                continue

            log.error("Peer %s considerado morto", node_id[:8])
            coordinator.handle_peer_failure(node_id)
            failures.pop(node_id, None)
            await coordinator.dispatch_all_idle()

        await asyncio.sleep(interval)


async def task_timeout_recovery_loop(coordinator: Coordinator, *, interval: float) -> None:
    """Reenfileira tarefas cujo timeout expirou e redistribui para peers ociosos."""
    while True:
        timed_out_ids = coordinator.check_task_status()

        if timed_out_ids:
            for task_id in timed_out_ids:
                log.warning("Task %s excedeu o timeout e foi reenfileirada", task_id[:8])
            await coordinator.dispatch_all_idle()

        await asyncio.sleep(interval)


async def pupil_replication_loop(
    coordinator: Coordinator,
    *,
    interval: float,
) -> None:
    """Escolhe periodicamente o Peer Pupilo e replica a GlobalTable"""
    while True:
        nodes = coordinator.GlobalTable.get_all_nodes()

        if nodes:
            pupil_node = max(nodes, key=lambda node: node["node_id"])
            new_pupil = {"ip": pupil_node["ip"], "port": pupil_node["port"]}

            if new_pupil != coordinator.pupil_peer:
                coordinator.pupil_peer = new_pupil
                log.info(
                    "Peer Pupilo designado: %s (%s:%s)",
                    pupil_node["node_id"][:8],
                    pupil_node["ip"],
                    pupil_node["port"],
                )

            await coordinator.replicate_state_to_pupil()

        await asyncio.sleep(interval)


async def server_status_loop(
    *,
    coordinator: Coordinator,
    status_queue: asyncio.Queue,
    total_tasks: int,
    completion_event: asyncio.Event,
) -> None:
    """Mostra progresso e confirma formalmente o fim do Grid Search."""
    completed_task_ids: Set[str] = set()
    failed_attempts = 0
    completion_logged = False

    while True:
        event = await status_queue.get()

        try:
            if event.get("event") != "task_result":
                continue

            task_id = event.get("task_id")
            status = event.get("status")

            if status == "success" and task_id:
                completed_task_ids.add(task_id)
            elif status == "failed":
                failed_attempts += 1

            with coordinator.GlobalTable.lock:
                queued = len(coordinator.GlobalTable.task_pool)
                running = len(coordinator.GlobalTable.assigned_tasks)

            completed = len(completed_task_ids)
            log.info(
                "Progresso: %s/%s concluídas | %s executando | %s aguardando | %s falhas",
                completed,
                total_tasks,
                running,
                queued,
                failed_attempts,
            )

            if (
                completed == total_tasks
                and queued == 0
                and running == 0
                and not completion_logged
            ):
                completion_logged = True
                best = coordinator.get_best_model() or {}

                log.info("=" * 60)
                log.info("TODAS AS TAREFAS FORAM CONCLUÍDAS")
                log.info("Total: %s", total_tasks)
                log.info("Tentativas com falha: %s", failed_attempts)
                log.info("Melhor F1: %.6f", float(best.get("f1_score", 0.0)))
                log.info("Melhor task: %s", best.get("task_id"))
                log.info("Peer vencedor: %s", best.get("peer_id"))
                log.info("=" * 60)
                completion_event.set()
        finally:
            status_queue.task_done()

# Servidor

async def run_server(args: argparse.Namespace) -> None:
    print("=" * 60)
    print("Inicializando servidor Hyperparalelizer")
    print(f"Endereço: {args.host}:{args.port}")
    print(f"Dataset: {args.dataset}")
    print("=" * 60)

    X, y = load_credit_risk_dataset(Path(args.dataset), args.target_column)

    global_table = GlobalTable()
    coordinator = Coordinator(
        dataset=(X, y),
        model=None,
        global_table=global_table,
        model_type=args.model_type,
        model_config={},
        pubsub_queue=None,
    )

    fragment_ids = coordinator.fragment_dataset(n_fragments=args.fragments)
    combinations = coordinator.generate_grid_search(DEFAULT_HYPERPARAMETER_GRID)
    total_tasks = len(combinations)

    messenger = ServerMessenger(coordinator=coordinator, host=args.host, port=args.port)
    register_all_handlers(
        messenger=messenger,
        coordinator=coordinator,
        status_queue=messenger.status_queue,
    )

    completion_event = asyncio.Event()

    print(f"Dataset: {len(X)} amostras")
    print(f"Fragmentos criados: {fragment_ids}")
    print(f"Tarefas criadas: {total_tasks}")
    print("Servidor pronto para receber peers.")
    print("Pressione Ctrl+C para encerrar.")

    messenger_task = asyncio.create_task(messenger.start(), name="server-messenger")
    healthcheck_task = asyncio.create_task(
        server_healthcheck_loop(
            coordinator,
            interval=args.healthcheck_interval,
            request_timeout=args.healthcheck_request_timeout,
            max_failures=args.healthcheck_max_failures,
        ),
        name="peer-healthcheck",
    )
    timeout_task = asyncio.create_task(
        task_timeout_recovery_loop(coordinator, interval=args.task_check_interval),
        name="task-timeout-recovery",
    )
    pupil_task = asyncio.create_task(
        pupil_replication_loop(coordinator, interval=args.pupil_replication_interval),
        name="pupil-replication",
    )
    status_task = asyncio.create_task(
        server_status_loop(
            coordinator=coordinator,
            status_queue=messenger.status_queue,
            total_tasks=total_tasks,
            completion_event=completion_event,
        ),
        name="server-status",
    )

    background_tasks = [healthcheck_task, timeout_task, pupil_task, status_task]

    try:
        if not args.stop_when_complete:
            await messenger_task
            return

        completion_waiter = asyncio.create_task(
            completion_event.wait(), name="completion-waiter"
        )
        done, _ = await asyncio.wait(
            {messenger_task, completion_waiter},
            return_when=asyncio.FIRST_COMPLETED,
        )

        if messenger_task in done:
            await messenger_task
        else:
            log.info("Encerrando servidor após concluir todas as tasks")
            await messenger.stop()
    finally:
        for task in background_tasks:
            task.cancel()
        await asyncio.gather(*background_tasks, return_exceptions=True)

        if not messenger_task.done():
            await messenger.stop()
            messenger_task.cancel()

        with contextlib.suppress(asyncio.CancelledError):
            await messenger_task


# Peer
def prepare_peer_storage(storage_dir: Path, reset: bool) -> None:
    """Evita que fragmentos de uma execução antiga contaminem o teste."""
    if reset and storage_dir.exists():
        shutil.rmtree(storage_dir)
    storage_dir.mkdir(parents=True, exist_ok=True)


def configure_dataset_ready_once(data_thread: DataThread) -> None:
    """Faz cada fragmento ser confirmado uma única vez ao servidor."""
    original_assemble_many = data_thread.assemble_many
    original_notify = data_thread.notify_dataset_ready
    notified: Set[str] = set()

    async def notify_once(fragment_id: Optional[str]) -> bool:
        if not fragment_id:
            return False
        if fragment_id in notified:
            return True
        ok = await original_notify(fragment_id)
        if ok:
            notified.add(fragment_id)
        return bool(ok)

    async def assemble_many_and_notify(fragment_ids: list[str]) -> bool:
        ok = await original_assemble_many(fragment_ids)
        if not ok:
            return False
        for fragment_id in fragment_ids:
            await notify_once(fragment_id)
        return True

    data_thread.notify_dataset_ready = notify_once  # type: ignore[method-assign]
    data_thread.assemble_many = assemble_many_and_notify  # type: ignore[method-assign]


async def register_peer_healthcheck_handler(p2p_node: P2PNode, node_id: str) -> None:
    """Permite que o servidor (ou o coordenador promovido) verifique se este
    peer ainda está vivo."""

    async def handle_keep_alive(_msg: Dict[str, Any], writer: asyncio.StreamWriter) -> None:
        await send_message(writer, Ack(ref_type=MSG_KEEP_ALIVE, ref_id=node_id).to_dict())

    p2p_node.register_handler(MSG_KEEP_ALIVE, handle_keep_alive)


async def peer_server_heartbeat_loop(
    *,
    node_id: str,
    server_ip: str,
    server_port: int,
    interval: float,
    request_timeout: float,
    max_failures: int,
    bully: BullyElection,
) -> None:
    """Peer detecta quando o servidor deixa de responder e, ao ultrapassar
    max_failures, dispara a eleição de Bully"""
    failures = 0
    server_offline = False

    while True:
        reply = await send_once(
            server_ip,
            server_port,
            KeepAlive(id_node=node_id).to_dict(),
            expect_reply=True,
            timeout=request_timeout,
        )

        valid = (
            isinstance(reply, dict)
            and reply.get("type") == MSG_ACK
            and reply.get("ref_type") == MSG_KEEP_ALIVE
            and reply.get("ref_id") == node_id
        )

        if valid:
            if server_offline:
                log.info("Servidor voltou a responder")
            failures = 0
            server_offline = False
        else:
            failures += 1
            log.warning(
                "Servidor não respondeu ao heartbeat (%s/%s)", failures, max_failures
            )

            if failures >= max_failures and not server_offline:
                server_offline = True
                log.error("Servidor considerado indisponível, iniciando eleição de Bully")
                asyncio.create_task(bully.detect_timeout_and_start())

        await asyncio.sleep(interval)


async def run_peer(args: argparse.Namespace) -> None:
    storage_dir = Path(args.storage_dir or f"data/peer_{args.port}/fragments")
    prepare_peer_storage(storage_dir, reset=args.reset_storage)

    print("=" * 60)
    print("Inicializando peer Hyperparalelizer")
    print(f"Peer: {args.host}:{args.port}")
    print(f"Servidor: {args.server_host}:{args.server_port}")
    print(f"Armazenamento: {storage_dir}")
    print(f"Reset do armazenamento: {args.reset_storage}")
    print("=" * 60)

    data_thread = DataThread(
        ip=args.host,
        listen_port=args.port,
        server_ip=args.server_host,
        server_port=args.server_port,
        storage_dir=str(storage_dir),
    )
    configure_dataset_ready_once(data_thread)

    join_reply = await data_thread.join_network()
    if join_reply is None:
        raise RuntimeError("O peer não conseguiu entrar na rede.")

    node_id = join_reply["node_id"]
    known_peers = join_reply.get("peers") or []

    print(f"Peer registrado com node_id: {node_id}")
    print("Fragmento inicial:", join_reply.get("fragment_id"))
    print("Peers conhecidos:", len(known_peers))

    dataset_loader = DatasetLoader(storage_dir=str(storage_dir))
    current_loop = asyncio.get_running_loop()

    messenger = PeerMessenger(
        node_id=node_id,
        server_ip=args.server_host,
        server_port=args.server_port,
        loop=current_loop,
    )
    maekawa_mutex = build_maekawa_mutex(node_id, known_peers)

    trainer = TrainerNode(
        node_id=node_id,
        messenger=messenger,
        data_thread=data_thread,
        dataset_loader=dataset_loader,
        maekawa_mutex=maekawa_mutex,
    )
    messenger.attach_trainer(trainer)

    p2p_node = P2PNode(host=args.host, port=args.port, node_id=node_id)
    messenger.register_handlers(p2p_node)
    register_peer_peer_handlers(p2p_node=p2p_node, storage_dir=str(storage_dir))
    await register_peer_healthcheck_handler(p2p_node, node_id)

    if hasattr(maekawa_mutex, "register_handlers"):
        maekawa_mutex.register_handlers(p2p_node)

    def _handle_promotion() -> None:
        asyncio.create_task(
            take_over_as_coordinator(
                trainer=trainer,
                host=args.host,
                port=args.server_port,
                healthcheck_interval=args.heartbeat_interval,
                healthcheck_request_timeout=args.heartbeat_request_timeout,
                healthcheck_max_failures=args.heartbeat_max_failures,
                task_check_interval=1.0,
            ),
            name="server-takeover",
        )

    bully = BullyElection(
        my_id=node_id,
        globaltable_peers=build_bully_peers_map(known_peers),
        promote_callback=_handle_promotion,
    )
    bully.register_handlers(p2p_node)

    async def _handle_sync_state(msg: Dict[str, Any], writer: asyncio.StreamWriter) -> None:
        trainer.handle_sync_state(msg)

    from utils.protocol import MSG_SYNC_STATE

    p2p_node.register_handler(MSG_SYNC_STATE, _handle_sync_state)

    messenger.start()

    p2p_task = asyncio.create_task(p2p_node.start(), name=f"p2p-node-{node_id[:8]}")
    await asyncio.sleep(0.2)

    heartbeat_task = asyncio.create_task(
        peer_server_heartbeat_loop(
            node_id=node_id,
            server_ip=args.server_host,
            server_port=args.server_port,
            interval=args.heartbeat_interval,
            request_timeout=args.heartbeat_request_timeout,
            max_failures=args.heartbeat_max_failures,
            bully=bully,
        ),
        name=f"server-heartbeat-{node_id[:8]}",
    )

    print(f"Peer {node_id[:8]} pronto e escutando em {args.host}:{args.port}")

    initial_task = join_reply.get("task")
    if initial_task is not None:
        print("Executando tarefa inicial:", initial_task.get("task_id"))
        asyncio.create_task(
            trainer.handle_training_task(initial_task),
            name=f"initial-training-{initial_task.get('task_id', 'unknown')}",
        )
    else:
        print("Sem tarefa inicial. Aguardando novas tasks.")

    print("Peer em execução. Pressione Ctrl+C para encerrar.")

    try:
        await p2p_task
    finally:
        heartbeat_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await heartbeat_task

        if not p2p_task.done():
            p2p_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await p2p_task

# CLI
def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Hyperparalelizer - Grupo 12")
    subparsers = parser.add_subparsers(dest="mode", required=True)

    server_parser = subparsers.add_parser("server")
    server_parser.add_argument("--host", default="127.0.0.1")
    server_parser.add_argument("--port", type=int, default=9000)
    server_parser.add_argument("--fragments", type=int, default=10)
    server_parser.add_argument("--dataset", default=str(DEFAULT_DATASET_PATH))
    server_parser.add_argument("--target-column", default=DEFAULT_TARGET_COLUMN)
    server_parser.add_argument("--model-type", default="random_forest")
    server_parser.add_argument("--healthcheck-interval", type=float, default=2.0)
    server_parser.add_argument("--healthcheck-request-timeout", type=float, default=2.0)
    server_parser.add_argument("--healthcheck-max-failures", type=int, default=3)
    server_parser.add_argument("--task-check-interval", type=float, default=1.0)
    server_parser.add_argument("--pupil-replication-interval", type=float, default=5.0)
    server_parser.add_argument("--stop-when-complete", action="store_true")

    peer_parser = subparsers.add_parser("peer")
    peer_parser.add_argument("--host", default="127.0.0.1")
    peer_parser.add_argument("--port", type=int, required=True)
    peer_parser.add_argument("--server-host", default="127.0.0.1")
    peer_parser.add_argument("--server-port", type=int, default=9000)
    peer_parser.add_argument("--storage-dir", default=None)
    peer_parser.add_argument(
        "--reset-storage",
        action="store_true",
        help="Apaga fragmentos antigos antes de entrar na rede",
    )
    peer_parser.add_argument("--heartbeat-interval", type=float, default=2.0)
    peer_parser.add_argument("--heartbeat-request-timeout", type=float, default=2.0)
    peer_parser.add_argument("--heartbeat-max-failures", type=int, default=3)

    return parser


async def async_main() -> None:
    args = build_parser().parse_args()

    if args.mode == "server":
        await run_server(args)
        return

    if args.mode == "peer":
        await run_peer(args)
        return

    raise ValueError(f"Modo desconhecido: {args.mode}")


def main() -> None:
    try:
        asyncio.run(async_main())
    except KeyboardInterrupt:
        print("\nAplicação encerrada pelo usuário.")


if __name__ == "__main__":
    main()