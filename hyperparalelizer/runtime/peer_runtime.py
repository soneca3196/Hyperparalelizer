from __future__ import annotations

"""ciclo de vida do peer de treinamento
"""

import argparse
import asyncio
import contextlib
import queue
import uuid
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

from core.network import P2PNode, send_message, send_once
from hyperparalelizer.peer.peer_inner_protocol import (
    EVT_BEST_MODEL_UPDATED,
    EVT_FRAGMENT_ACQUIRED,
    EVT_FRAGMENT_ASSEMBLY_FAILED,
    EVT_TRAINING_FAILED,
    EVT_TRAINING_FINISHED,
    EVT_TRAINING_STARTED,
    InternalEventBus,
)
from hyperparalelizer.peer.peer_outer_protocol import register_peer_peer_handlers
from hyperparalelizer.peer.trainer import TrainerNode
from hyperparalelizer.server.server_messenger import ServerMessenger
from hyperparalelizer.sync.bully import BullyElection
from hyperparalelizer.sync.maekawa import MaekawaMutex
from utils.protocol import (
    Ack,
    ErrorMsg,
    KeepAlive,
    MSG_KEEP_ALIVE,
    MSG_MEMBERSHIP_UPDATE,
    MSG_PEER_READY,
    MSG_PUBSUB_NOTIFY,
    MSG_SYNC_STATE,
    PeerReady,
)
from utils.resource_limits import ResourceLimitError, apply_resource_limits

from .common import (
    BootstrapError,
    DatasetIdentity,
    NoOpMutex,
    ReliablePeerMessenger,
    SEPARATOR,
    ValidatedJoin,
    ValidatingDataThread,
    ValidatingDatasetLoader,
    cancel_tasks,
    create_task,
    load_reference_dataset,
    normalize_peers,
    prepare_storage,
    resolve_storage_dir,
    short_id,
    validate_ack,
    validate_join_reply,
    wait_for_listener,
)
from .server_runtime import (
    ServerHeartbeatTracker,
    ServerProgress,
    ServerRuntime,
    shutdown_server_runtime,
    start_server_runtime,
)


def _limits_desc(args: argparse.Namespace) -> str:
    ram = f"{args.max_ram_mb:.0f}MiB" if args.max_ram_mb else "-"
    cpu = f"{args.max_cpu_cores}cores" if args.max_cpu_cores else "-"
    return f"ram={ram} cpu={cpu}"


def print_peer_header(
    args: argparse.Namespace,
    local_run_id: str,
    storage_dir: Path,
    identity: DatasetIdentity,
) -> None:
    maekawa = "off" if args.disable_maekawa else "on"
    bully = "off" if args.disable_bully else "on"
    pubsub = "off" if args.disable_pubsub else "on"
    print(SEPARATOR)
    print(f"Hyperparalelizer · peer {args.host}:{args.port} -> servidor {args.server_host}:{args.server_port}")
    print(
        f"run={local_run_id} dataset={identity.short_id} "
        f"storage={storage_dir}{' (reset)' if args.reset_storage else ''}"
    )
    print(f"maekawa={maekawa} bully={bully} pubsub={pubsub} {_limits_desc(args)}")
    print(SEPARATOR)


def print_join_summary(join: ValidatedJoin, identity: DatasetIdentity) -> None:
    task_id = join.initial_task.get("task_id") if join.initial_task else None
    print(
        f"[JOIN] node={short_id(join.node_id)} fragmento={join.fragment_id} "
        f"peers={len(join.peers)} task={short_id(task_id) if task_id else '-'}"
    )


def build_bully_peer_map(peers: Iterable[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    return {
        peer["id_node"]: {"ip": peer["ip"], "port": peer["port"]}
        for peer in normalize_peers(peers)
    }


def update_endpoints_after_election(
    bully: BullyElection,
    messenger: ReliablePeerMessenger,
    data_thread: ValidatingDataThread,
) -> bool:
    ip = bully.current_coordinator_ip
    port = bully.current_coordinator_port
    if not isinstance(ip, str) or not ip or not isinstance(port, int):
        return False
    messenger.server_ip = ip
    messenger.server_port = port
    data_thread.server_ip = ip
    data_thread.server_port = port
    print(f"[ELECTION] Novo servidor configurado em {ip}:{port}")
    return True


async def peer_heartbeat_loop(
    args: argparse.Namespace,
    node_id: str,
    messenger: ReliablePeerMessenger,
    data_thread: ValidatingDataThread,
    bully: Optional[BullyElection],
    stop_event: asyncio.Event,
) -> None:
    failures = 0
    unavailable = False
    election_started = False

    while not stop_event.is_set():
        await asyncio.sleep(args.heartbeat_interval)
        try:
            reply = await send_once(
                messenger.server_ip,
                messenger.server_port,
                KeepAlive(id_node=node_id).to_dict(),
                expect_reply=True,
                timeout=args.heartbeat_timeout,
            )
            ok = validate_ack(
                reply,
                expected_ref_type=MSG_KEEP_ALIVE,
                expected_ref_id=node_id,
            )
        except Exception as exc:
            print(f"[WARNING] Erro no heartbeat: {exc}")
            ok = False

        if ok:
            if unavailable:
                print("[HEARTBEAT] Conexão com o servidor restaurada")
            failures = 0
            unavailable = False
            election_started = False
            continue

        failures += 1
        print(
            f"[WARNING] Servidor não respondeu ao heartbeat "
            f"({failures}/{args.heartbeat_failures})"
        )
        if failures < args.heartbeat_failures:
            continue

        if not unavailable:
            print("[ERROR] Servidor considerado indisponível")
        unavailable = True

        if bully is None:
            continue

        if not election_started:
            election_started = True
            print("[ELECTION] Iniciando algoritmo Bully")
            try:
                await bully.detect_timeout_and_start()
            except Exception as exc:
                print(f"[ELECTION ERROR] {exc}")

        if update_endpoints_after_election(bully, messenger, data_thread):
            failures = 0


async def promoted_server_from_peer(
    args: argparse.Namespace,
    node_id: str,
    trainer: TrainerNode,
    p2p_node: P2PNode,
    p2p_task: asyncio.Task[Any],
    messenger: ReliablePeerMessenger,
    peer_tasks: List[asyncio.Task[Any]],
) -> None:
    """Promoção beta usando o snapshot recebido pelo TrainerNode
    """
    if not trainer.replica_global_table_snapshot:
        print(
            "[PROMOTION ERROR] Este peer venceu a eleição, mas não possui "
            "snapshot da GlobalTable. A promoção operacional foi abortada."
        )
        return

    print("[PROMOTION] Encerrando papel P2P para assumir como servidor")
    await cancel_tasks(peer_tasks)
    with contextlib.suppress(Exception):
        await p2p_node.stop()
    if not p2p_task.done():
        p2p_task.cancel()
        await asyncio.gather(p2p_task, return_exceptions=True)
    await asyncio.to_thread(messenger.stop)

    coordinator = trainer.promote_to_server()
    # O peer promovido deixa de ser executor. Sua tarefa em andamento, se
    # ainda constar no snapshot, é reenfileirada pelo método existente.
    coordinator.handle_peer_failure(node_id)

    promoted_args = argparse.Namespace(**vars(args))
    promoted_args.host = args.host
    promoted_args.port = args.port
    promoted_args.task_timeout = 30.0
    promoted_args.stop_when_complete = False
    promoted_args.min_peers = 1
    promoted_args.peer_wait_timeout = 0.0
    promoted_args.disable_pupil = False

    remaining = 0
    with coordinator.GlobalTable.lock:
        remaining = (
            len(coordinator.GlobalTable.task_pool)
            + len(coordinator.GlobalTable.assigned_tasks)
        )

    promoted_messenger = ServerMessenger(
        coordinator=coordinator,
        host=args.host,
        port=args.port,
    )
    runtime = ServerRuntime(
        coordinator=coordinator,
        messenger=promoted_messenger,
        progress=ServerProgress(total_tasks=remaining),
        heartbeat=ServerHeartbeatTracker(),
        run_id=f"promoted-{uuid.uuid4().hex}",
        dataset_identity=load_reference_dataset()[2],
        args=promoted_args,
        pubsub_queue=None if args.disable_pubsub else queue.Queue(),
    )
    runtime.job_started.set()
    print(
        f"[PROMOTION] Novo servidor será iniciado em {args.host}:{args.port} "
        f"com {remaining} tarefa(s) remanescente(s)"
    )
    await start_server_runtime(runtime)
    try:
        await runtime.stop_event.wait()
    finally:
        await shutdown_server_runtime(runtime)


async def run_peer(args: argparse.Namespace) -> None:
    try:
        apply_resource_limits(
            max_ram_mb=args.max_ram_mb,
            max_cpu_cores=args.max_cpu_cores,
            label=f"peer:{args.port}",
        )
    except ResourceLimitError as exc:
        raise BootstrapError(str(exc)) from exc

    _, _, identity = load_reference_dataset()
    local_run_id = f"peer-{args.port}-{uuid.uuid4().hex[:8]}"
    storage_dir = prepare_storage(resolve_storage_dir(args), args.reset_storage)
    spool_dir = storage_dir.parent / "pending_results"
    print_peer_header(args, local_run_id, storage_dir, identity)

    event_bus = InternalEventBus()
    data_thread = ValidatingDataThread(
        ip=args.host,
        listen_port=args.port,
        server_ip=args.server_host,
        server_port=args.server_port,
        storage_dir=str(storage_dir),
        event_bus=event_bus,
    )

    join_reply = await data_thread.join_network(timeout=args.join_timeout)
    join = validate_join_reply(join_reply)
    data_thread.node_id = join.node_id
    data_thread.fragment_id = join.fragment_id
    data_thread.known_peers = join.peers
    data_thread.initial_task = join.initial_task
    print_join_summary(join, identity)

    dataset_loader = ValidatingDatasetLoader(str(storage_dir), expected=identity)
    data_thread.attach_dataset_loader(dataset_loader)

    if args.disable_maekawa:
        mutex: Any = NoOpMutex()
    else:
        mutex = MaekawaMutex(node_id=join.node_id, quorum=join.peers)
        print(f"[MAEKAWA] Quórum inicial com {len(join.peers)} peer(s)")

    loop = asyncio.get_running_loop()
    messenger = ReliablePeerMessenger(
        node_id=join.node_id,
        server_ip=args.server_host,
        server_port=args.server_port,
        spool_dir=spool_dir,
        loop=loop,
    )
    trainer = TrainerNode(
        node_id=join.node_id,
        messenger=messenger,
        data_thread=data_thread,
        dataset_loader=dataset_loader,
        maekawa_mutex=mutex,
        event_bus=event_bus,
        run_id=join.run_id,
    )
    messenger.attach_trainer(trainer)

    p2p_node = P2PNode(host=args.host, port=args.port, node_id=join.node_id)
    messenger.register_handlers(p2p_node)
    register_peer_peer_handlers(p2p_node, storage_dir=str(storage_dir))
    if not args.disable_maekawa:
        mutex.register_handlers(p2p_node)

    promotion_requested = asyncio.Event()

    def request_promotion() -> None:
        promotion_requested.set()

    bully: Optional[BullyElection]
    if args.disable_bully:
        bully = None
    else:
        bully = BullyElection(
            my_id=join.node_id,
            globaltable_peers=build_bully_peer_map(join.peers),
            promote_callback=request_promotion,
        )
        bully.register_handlers(p2p_node)

    async def handle_sync_state(
        msg: Dict[str, Any], writer: asyncio.StreamWriter
    ) -> None:
        snapshot_id = str(msg.get("snapshot_id") or "")
        pupil_id = str(msg.get("pupil_id") or "")
        pupil_epoch = int(msg.get("pupil_epoch") or 0)

        if pupil_epoch < trainer.pupil_epoch:
            await send_message(
                writer,
                ErrorMsg(code="STALE_SYNC_STATE", detail=snapshot_id).to_dict(),
            )
            return

        trainer.pupil_epoch = pupil_epoch

        if pupil_id != join.node_id:
            trainer.is_pupil = False
            trainer.replica_global_table_snapshot = {}
            with contextlib.suppress(Exception):
                await send_message(
                    writer,
                    Ack(ref_type=MSG_SYNC_STATE, ref_id=snapshot_id).to_dict(),
                )
            return

        trainer.is_pupil = True
        trainer.handle_sync_state(msg)
        await send_message(
            writer, Ack(ref_type=MSG_SYNC_STATE, ref_id=snapshot_id).to_dict()
        )

    async def handle_membership_update(
        msg: Dict[str, Any], writer: asyncio.StreamWriter
    ) -> None:
        peers = msg.get("peers") or []
        normalized = normalize_peers(peers, exclude_node_id=join.node_id)
        data_thread.update_known_peers(normalized)
        if hasattr(mutex, "replace_quorum"):
            mutex.replace_quorum(normalized)
        if bully is not None:
            bully.peers = build_bully_peer_map(normalized)
        with contextlib.suppress(Exception):
            await send_message(
                writer,
                Ack(
                    ref_type=MSG_MEMBERSHIP_UPDATE,
                    ref_id=str(msg.get("epoch") or ""),
                ).to_dict(),
            )

    async def handle_pubsub_notify(
        msg: Dict[str, Any], writer: asyncio.StreamWriter
    ) -> None:
        payload = msg.get("payload") or {}
        score = float(payload.get("f1_score") or -1.0)
        if score > trainer.best_score:
            trainer.best_score = score
        print(f"[PUBSUB] Novo melhor score global: {score:.4f}")
        producer = payload.get("id_node")
        if producer:
            print(f"[PUBSUB] Modelo produzido pelo peer {short_id(str(producer))}")
        # O bridge publica com expect_reply=False; não responde.
        del writer

    p2p_node.register_handler(MSG_SYNC_STATE, handle_sync_state)
    p2p_node.register_handler(MSG_MEMBERSHIP_UPDATE, handle_membership_update)
    if not args.disable_pubsub:
        p2p_node.register_handler(MSG_PUBSUB_NOTIFY, handle_pubsub_notify)

    def on_fragment_acquired(event: Any) -> None:
        print(
            f"[FRAGMENT] {event.fragment_id} disponível via {event.source}"
        )

    def on_fragment_failed(event: Any) -> None:
        print(
            f"[FRAGMENT ERROR] {event.fragment_id}: {event.reason}"
        )

    def on_training_started(event: Any) -> None:
        print(f"[TRAINING] Iniciada task {short_id(event.task_id)}")

    def on_training_finished(event: Any) -> None:
        print(f"[TRAINING] Finalizada task {short_id(event.task_id)}")

    def on_training_failed(event: Any) -> None:
        print(
            f"[TRAINING ERROR] Task {short_id(event.task_id)}: {event.error}"
        )

    def on_local_best(event: Any) -> None:
        print(f"[LOCAL BEST] Score local atualizado para {event.score:.4f}")

    event_bus.subscribe(EVT_FRAGMENT_ACQUIRED, on_fragment_acquired)
    event_bus.subscribe(EVT_FRAGMENT_ASSEMBLY_FAILED, on_fragment_failed)
    event_bus.subscribe(EVT_TRAINING_STARTED, on_training_started)
    event_bus.subscribe(EVT_TRAINING_FINISHED, on_training_finished)
    event_bus.subscribe(EVT_TRAINING_FAILED, on_training_failed)
    event_bus.subscribe(EVT_BEST_MODEL_UPDATED, on_local_best)

    messenger.restore_pending_results()
    messenger.start()

    peer_tasks: List[asyncio.Task[Any]] = []
    p2p_task = create_task(peer_tasks, p2p_node.start(), "peer-p2p-server")
    await wait_for_listener(args.host, args.port)
    print(f"[READY] Peer P2P escutando em {args.host}:{args.port}")

    ready_reply = await send_once(
        args.server_host,
        args.server_port,
        PeerReady(id_node=join.node_id).to_dict(),
        expect_reply=True,
        timeout=10.0,
    )
    if validate_ack(
        ready_reply, expected_ref_type=MSG_PEER_READY, expected_ref_id=join.node_id
    ):
        print("[READY] PeerReady confirmado; apto a receber tarefas do servidor")
    else:
        print("[WARNING] Servidor não confirmou PeerReady")

    stop_event = asyncio.Event()
    heartbeat_task = create_task(
        peer_tasks,
        peer_heartbeat_loop(
            args,
            join.node_id,
            messenger,
            data_thread,
            bully,
            stop_event,
        ),
        "peer-server-heartbeat",
    )
    del heartbeat_task

    if join.initial_task is not None:
        create_task(
            peer_tasks,
            trainer.try_submit_task(join.initial_task),
            "peer-initial-training-task",
        )

    try:
        if bully is None:
            await stop_event.wait()
        else:
            await promotion_requested.wait()
            stop_event.set()
            await promoted_server_from_peer(
                args=args,
                node_id=join.node_id,
                trainer=trainer,
                p2p_node=p2p_node,
                p2p_task=p2p_task,
                messenger=messenger,
                peer_tasks=peer_tasks,
            )
    except asyncio.CancelledError:
        raise
    finally:
        print("[SHUTDOWN] Encerrando serviços do peer...")
        stop_event.set()
        await cancel_tasks(peer_tasks)
        with contextlib.suppress(Exception):
            await p2p_node.stop()
        await asyncio.to_thread(messenger.stop)
        print("[SHUTDOWN] Finalizado com segurança")