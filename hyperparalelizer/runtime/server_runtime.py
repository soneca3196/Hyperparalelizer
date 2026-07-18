from __future__ import annotations

"""ciclo de vida do servidor coordenador
"""

import argparse
import asyncio
import queue
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

from core.network import send_once
from hyperparalelizer.global_table import GlobalTable
from hyperparalelizer.server.coordinator import Coordinator
from hyperparalelizer.server.server_messenger import ServerMessenger
from hyperparalelizer.server.server_peer_protocol import (
    handle_keep_alive,
    register_all_handlers,
)
from utils.protocol import MSG_KEEP_ALIVE, MSG_PUBSUB_PUBLISH, PubSubNotify
from utils.resource_limits import ResourceLimitError, apply_resource_limits

from .common import (
    BootstrapError,
    DEFAULT_DATASET_KEY,
    DEFAULT_GRID,
    DEFAULT_MODEL_TYPE,
    DatasetIdentity,
    ServerHeartbeatTracker,
    ServerProgress,
    SEPARATOR,
    cancel_tasks,
    clear_data_dir,
    create_task,
    grid_size,
    load_reference_dataset,
    short_id,
    wait_for_listener,
    write_run_config,
)


@dataclass
class ServerRuntime:
    coordinator: Coordinator
    messenger: ServerMessenger
    progress: ServerProgress
    heartbeat: ServerHeartbeatTracker
    run_id: str
    dataset_identity: DatasetIdentity
    args: argparse.Namespace
    pubsub_queue: Optional[queue.Queue]
    stop_event: asyncio.Event = field(default_factory=asyncio.Event)
    complete_event: asyncio.Event = field(default_factory=asyncio.Event)
    job_started: asyncio.Event = field(default_factory=asyncio.Event)
    tasks: List[asyncio.Task[Any]] = field(default_factory=list)


def server_progress_snapshot(runtime: ServerRuntime) -> Tuple[int, int, int, int]:
    table = runtime.coordinator.GlobalTable
    with table.lock:
        waiting = len(table.task_pool)
        running = len(table.assigned_tasks)
    return runtime.progress.completed, running, waiting, runtime.progress.failed


def print_progress(runtime: ServerRuntime) -> None:
    completed, running, waiting, failed = server_progress_snapshot(runtime)
    print(
        f"[PROGRESS] {completed}/{runtime.progress.total_tasks} ok | "
        f"{running} rodando | {waiting} fila | {failed} falhas"
    )


def is_formally_complete(runtime: ServerRuntime) -> bool:
    completed, running, waiting, failed = server_progress_snapshot(runtime)
    return (
        runtime.job_started.is_set()
        and waiting == 0
        and running == 0
        and completed + failed == runtime.progress.total_tasks
    )




async def status_consumer(runtime: ServerRuntime) -> None:
    while not runtime.stop_event.is_set():
        event = await runtime.messenger.status_queue.get()
        try:
            event_name = event.get("event")
            node_id = str(
                event.get("node_id")
                or event.get("peer_id")
                or event.get("requester_id")
                or ""
            )
            if node_id:
                runtime.heartbeat.touch(node_id)

            if event_name == "peer_joined":
                print(
                    f"[JOIN] Peer {short_id(node_id)} em "
                    f"{event.get('ip')}:{event.get('port')} | "
                    f"fragmento={event.get('fragment_id')} | "
                    f"task={event.get('task_id')}"
                )
                if not runtime.args.disable_pupil:
                    changed = await runtime.coordinator.pupil_manager.reconcile()
                    pupil = runtime.coordinator.pupil_peer
                    if changed and pupil is not None:
                        print(
                            f"[PUPIL] Peer de backup atual: "
                            f"{short_id(str(pupil.get('id_node')))} "
                            f"(epoch={runtime.coordinator.pupil_manager.epoch})"
                        )

            elif event_name == "task_result":
                task_id = str(event.get("task_id") or "")
                if event.get("status") == "success":
                    runtime.progress.register_success(task_id)
                else:
                    accepted_retry, retry = runtime.progress.register_failure_or_retry(
                        task_id, runtime.args.max_task_retries
                    )
                    if accepted_retry:
                        print(
                            f"[RETRY] Tarefa {short_id(task_id)} reenfileirada "
                            f"(tentativa registrada: {retry})"
                        )
                    else:
                        print(
                            f"[PROGRESSO] Tarefa {short_id(task_id)} falhou "
                            f"definitivamente após {runtime.args.max_task_retries} tentativas."
                        )
                        
                print_progress(runtime)

            elif event_name == "dataset_ready":
                print(
                    f"[DATASET] {short_id(node_id)} possui "
                    f"{event.get('fragment_id')}"
                )

            elif event_name == "fragment_backup_served":
                print(
                    f"[FRAGMENT] Backup de {event.get('fragment_id')} enviado "
                    f"a {short_id(node_id)}"
                )
        finally:
            runtime.messenger.status_queue.task_done()


async def scheduler_service(runtime: ServerRuntime, interval: float = 1.0) -> None:
    await runtime.job_started.wait()
    cleaned_up = False
    while not runtime.stop_event.is_set():
        timed_out = runtime.coordinator.check_task_status()
        for task_id in timed_out:
            retry = runtime.progress.register_retry(task_id)
            print(
                f"[TIMEOUT] Tarefa {short_id(task_id)} excedeu "
                f"{runtime.coordinator.task_timeout:.1f}s e voltou à fila "
                f"(retry {retry})"
            )

        await runtime.coordinator.dispatch_all_idle()

        if is_formally_complete(runtime):
            was_complete = runtime.complete_event.is_set()
            runtime.complete_event.set()
            if not was_complete:
                print_completion_summary(runtime)
            if not cleaned_up:
                cleaned_up = True
                clear_data_dir()
            if runtime.args.stop_when_complete:
                runtime.stop_event.set()
                return
        await asyncio.sleep(interval)


async def peer_failure_monitor(runtime: ServerRuntime) -> None:
    interval = runtime.args.heartbeat_interval
    timeout = runtime.args.heartbeat_timeout
    max_failures = runtime.args.heartbeat_failures

    while not runtime.stop_event.is_set():
        await asyncio.sleep(interval)
        now = time.monotonic()
        for node_id, health in runtime.heartbeat.items():
            if health.declared_dead:
                continue
            if now - health.last_seen <= timeout:
                health.misses = 0
                continue

            health.misses += 1
            if health.misses < max_failures:
                print(
                    f"[WARNING] Peer {short_id(node_id)} não respondeu "
                    f"({health.misses}/{max_failures})"
                )
                continue

            health.declared_dead = True
            with runtime.coordinator.GlobalTable.lock:
                before_pending = len(runtime.coordinator.GlobalTable.task_pool)
                before_assigned = len(runtime.coordinator.GlobalTable.assigned_tasks)
            print(f"[ERROR] Peer {short_id(node_id)} considerado morto")
            runtime.coordinator.handle_peer_failure(node_id)
            with runtime.coordinator.GlobalTable.lock:
                after_pending = len(runtime.coordinator.GlobalTable.task_pool)
                after_assigned = len(runtime.coordinator.GlobalTable.assigned_tasks)
            recovered = max(0, after_pending - before_pending)
            removed_assignments = max(0, before_assigned - after_assigned)
            print(
                f"[RECOVERY] {max(recovered, removed_assignments)} tarefa(s) "
                "devolvida(s) para a fila"
            )
            print("[RECOVERY] Localizações de fragmentos removidas")
            runtime.heartbeat.remove(node_id)


async def pupil_sync_service(runtime: ServerRuntime, interval: float = 2.0) -> None:
    await runtime.job_started.wait()
    last_confirmed: Optional[str] = None
    while not runtime.stop_event.is_set():
        await asyncio.sleep(interval)

        nodes = [
            node
            for node in runtime.coordinator.GlobalTable.get_all_nodes()
            if node.get("ready") is True
        ]

        if not nodes:

            continue
            
        await runtime.coordinator.pupil_manager.reconcile()
        pupil = runtime.coordinator.pupil_peer
        if pupil is None:
            continue
        try:
            synced = await runtime.coordinator.replicate_state_to_pupil()
            pupil_id = str(pupil.get("id_node") or "")
            if synced and pupil_id != last_confirmed:
                last_confirmed = pupil_id
                print(
                    f"[PUPIL] Snapshot confirmado em "
                    f"{short_id(pupil_id)}"
                )
        except Exception as exc:
            print(f"[WARNING] Falha ao sincronizar Peer Pupilo: {exc}")


async def pubsub_broadcast_service(runtime: ServerRuntime) -> None:
    """Bridge beta do tópico global enquanto não há runtime Pub/Sub exposto."""
    if runtime.pubsub_queue is None:
        return
    while not runtime.stop_event.is_set():
        try:
            publish = runtime.pubsub_queue.get_nowait()
        except queue.Empty:
            await asyncio.sleep(0.1)
            continue

        try:
            if publish.get("type") != MSG_PUBSUB_PUBLISH:
                continue
            notification = PubSubNotify(
                topic=publish.get("topic", "global_best_score"),
                payload=publish.get("payload") or {},
                lamport_clock=int(publish.get("lamport_clock") or 0),
            ).to_dict()
            nodes = runtime.coordinator.GlobalTable.get_all_nodes()
            sends = [
                send_once(
                    node["ip"],
                    node["port"],
                    notification,
                    expect_reply=False,
                    timeout=5.0,
                )
                for node in nodes
            ]
            if sends:
                await asyncio.gather(*sends, return_exceptions=True)
            payload = notification.get("payload") or {}
            print(
                f"[PUBSUB] Novo melhor score global: "
                f"{float(payload.get('f1_score') or 0.0):.4f}"
            )
        finally:
            runtime.pubsub_queue.task_done()


async def wait_for_minimum_peers(runtime: ServerRuntime) -> None:
    minimum = runtime.args.min_peers
    if minimum <= 1:
        runtime.job_started.set()
        return

    deadline = time.monotonic() + runtime.args.peer_wait_timeout
    last_count = -1
    while not runtime.stop_event.is_set():
        count = len(runtime.coordinator.GlobalTable.get_all_nodes())
        if count != last_count:
            print(f"[WAITING] {count}/{minimum} peers conectados")
            last_count = count
        if count >= minimum:
            break
        if time.monotonic() >= deadline:
            print(
                f"[WARNING] Timeout aguardando {minimum} peers; "
                f"iniciando com {count}"
            )
            break
        await asyncio.sleep(0.25)

    # No modo min-peers o grid é criado somente aqui, portanto nenhum JoinAck
    # anterior reservou tarefa. O scheduler fará o primeiro dispatch por TCP.
    runtime.coordinator.generate_grid_search(getattr(runtime.args, "grid", None) or DEFAULT_GRID)
    runtime.job_started.set()
    print("[START] Iniciando distribuição das tarefas")


async def tracked_keep_alive_handler(
    msg: Dict[str, Any],
    writer: asyncio.StreamWriter,
    runtime: ServerRuntime,
) -> None:
    node_id = str(msg.get("id_node") or "")
    runtime.heartbeat.touch(node_id)
    await handle_keep_alive(
        msg,
        writer,
        coordinator=runtime.coordinator,
        status_queue=runtime.messenger.status_queue,
    )


async def start_server_runtime(runtime: ServerRuntime) -> None:
    register_all_handlers(
        runtime.messenger,
        runtime.coordinator,
        runtime.messenger.status_queue,
    )
    runtime.messenger.register_handler(
        MSG_KEEP_ALIVE,
        lambda msg, writer: tracked_keep_alive_handler(msg, writer, runtime),
    )

    create_task(runtime.tasks, runtime.messenger.start(), "server-tcp")
    await wait_for_listener(runtime.args.host, runtime.args.port)
    print(f"[READY] Servidor escutando em {runtime.args.host}:{runtime.args.port}")

    create_task(runtime.tasks, status_consumer(runtime), "server-status-consumer")
    create_task(runtime.tasks, scheduler_service(runtime), "server-scheduler")
    create_task(runtime.tasks, peer_failure_monitor(runtime), "server-heartbeat-monitor")

    if not runtime.args.disable_pupil:
        create_task(runtime.tasks, pupil_sync_service(runtime), "server-pupil-sync")
    if runtime.pubsub_queue is not None:
        create_task(runtime.tasks, pubsub_broadcast_service(runtime), "server-pubsub")

    if runtime.args.min_peers > 1:
        create_task(
            runtime.tasks,
            wait_for_minimum_peers(runtime),
            "server-wait-min-peers",
        )
    else:
        runtime.job_started.set()


async def shutdown_server_runtime(runtime: ServerRuntime) -> None:
    print("[SHUTDOWN] Encerrando serviços do servidor...")
    runtime.stop_event.set()
    await runtime.messenger.stop()
    await cancel_tasks(runtime.tasks)
    print("[SHUTDOWN] Mensageiro e rotinas do servidor encerrados")


def _limits_desc(args: argparse.Namespace) -> str:
    ram = f"{args.max_ram_mb:.0f}MiB" if args.max_ram_mb else "-"
    cpu = f"{args.max_cpu_cores}cores" if args.max_cpu_cores else "-"
    return f"ram={ram} cpu={cpu}"


def print_server_header(
    args: argparse.Namespace,
    run_id: str,
    identity: DatasetIdentity,
    task_count: int,
) -> None:
    pubsub = "off" if args.disable_pubsub else "on"
    pupil = "off" if args.disable_pupil else "on"
    print(SEPARATOR)
    print(f"Hyperparalelizer · servidor em {args.host}:{args.port} (run {run_id[:8]})")
    print(
        f"dataset={identity.sample_count} amostras ({identity.short_id}) "
        f"fragmentos={args.fragments} tarefas={task_count} modelo={DEFAULT_MODEL_TYPE}"
    )
    print(f"pubsub={pubsub} pupilo={pupil} {_limits_desc(args)}")
    print(SEPARATOR)


def print_completion_summary(runtime: ServerRuntime) -> None:
    best = runtime.coordinator.get_best_model() or {}
    elapsed = time.monotonic() - runtime.progress.started_at
    print(SEPARATOR)
    print(
        f"Treino finalizado (run {runtime.run_id[:8]}) em {elapsed:.1f}s — "
        f"{runtime.progress.completed}/{runtime.progress.total_tasks} ok, "
        f"{runtime.progress.failed} falhas"
    )
    print(
        f"melhor f1={float(best.get('f1_score') or 0.0):.4f} "
        f"task={short_id(str(best.get('task_id') or ''))} "
        f"peer={short_id(str(best.get('peer_id') or ''))}"
    )
    print(f"hiperparâmetros: {best.get('hyperparameters')}")
    print(SEPARATOR)


async def run_server(args: argparse.Namespace) -> None:
    try:
        apply_resource_limits(
            max_ram_mb=args.max_ram_mb,
            max_cpu_cores=args.max_cpu_cores,
            label=f"server:{args.port}",
        )
    except ResourceLimitError as exc:
        raise BootstrapError(str(exc)) from exc

    dataset_key = getattr(args, "dataset_key", None) or DEFAULT_DATASET_KEY
    grid = getattr(args, "grid", None) or DEFAULT_GRID
    write_run_config(dataset_key, grid)

    X, y, identity = load_reference_dataset(dataset_key)
    run_id = uuid.uuid4().hex
    total_tasks = grid_size(grid)
    pubsub_queue: Optional[queue.Queue] = (
        None if args.disable_pubsub else queue.Queue()
    )

    global_table = GlobalTable()
    coordinator = Coordinator(
        dataset=(X, y),
        model=None,
        global_table=global_table,
        model_type=DEFAULT_MODEL_TYPE,
        model_config={},
        pubsub_queue=pubsub_queue,
        task_timeout=args.task_timeout,
        run_id=run_id,
        max_task_retries=args.max_task_retries,
    )
    fragment_ids = coordinator.fragment_dataset(n_fragments=args.fragments)
    if len(fragment_ids) != args.fragments:
        raise BootstrapError(
            f"Fragmentação gerou {len(fragment_ids)}, esperado {args.fragments}"
        )

    # Fluxo normal: tarefa inicial reservada por add_peer e enviada no JoinAck.
    # Com min_peers > 1, o grid é adiado para impedir reserva prematura.
    if args.min_peers <= 1:
        coordinator.generate_grid_search(grid)

    messenger = ServerMessenger(
        coordinator=coordinator,
        host=args.host,
        port=args.port,
    )
    runtime = ServerRuntime(
        coordinator=coordinator,
        messenger=messenger,
        progress=ServerProgress(total_tasks=total_tasks),
        heartbeat=ServerHeartbeatTracker(),
        run_id=run_id,
        dataset_identity=identity,
        args=args,
        pubsub_queue=pubsub_queue,
    )

    print_server_header(args, run_id, identity, total_tasks)
    await start_server_runtime(runtime)

    try:
        await runtime.stop_event.wait()
    except asyncio.CancelledError:
        raise
    finally:
        await shutdown_server_runtime(runtime)