"""
server-peer_protocol.py - Handlers de protocolo server ↔ peer.

Define uma função de handler para cada tipo de mensagem trocada entre
o servidor e os peers. Cada handler:
  - Interpreta a mensagem recebida (já roteada pelo ServerMessenger)
  - Delega a lógica de negócio ao Coordinator
  - Envia a resposta TCP ao peer via send_message
  - Publica eventos de status no status_queue do ServerMessenger (opcional)

Fluxo de chamada:
    Peer (TCP)
      ↓
    ServerMessenger._handle_connection()   ← transporte e roteamento
      ↓
    handler do protocolo (este módulo)     ← processamento da mensagem
      ↓
    Coordinator                            ← lógica de negócio

Uso típico (em main.py ou na inicialização do servidor):

    from hyperparalelizer.server_peer_protocol import register_all_handlers
    register_all_handlers(messenger, coordinator, messenger.status_queue)

Isso substitui os handlers built-in do ServerMessenger pelos handlers
deste módulo, mantendo a publicação de eventos no status_queue.
"""

import asyncio
import functools
import pickle
from typing import Any, Dict, Optional

from core.network import send_message
from hyperparalelizer.server.coordinator import Coordinator, Peer
from utils.logger import get_logger
from utils.protocol import (
    MSG_DATASET_READY,
    MSG_JOIN_NETWORK,
    MSG_KEEP_ALIVE,
    MSG_PEER_READY,
    MSG_REQUEST_BEST,
    MSG_REQUEST_FRAGMENT_BACKUP,
    MSG_TASK_RESULT,
    Ack,
    FragmentBackupData,
    JoinAck,
    SendBestModel,
)

log = get_logger("server_peer_protocol")

# JoinNetwork                                                     
async def handle_join_network(msg: Dict[str, Any], writer: asyncio.StreamWriter, coordinator: Coordinator, status_queue: Optional[asyncio.Queue] = None,) -> str:
    ip = msg.get("ip")
    port = msg.get("porta")

    if not ip or not isinstance(port, int):
        await send_message(
            writer,
            {
                "type": "Error",
                "code": "INVALID_JOIN_REQUEST",
                "detail": "Os campos ip e porta são obrigatórios.",
            },
        )
        return ""

    peer = Peer(ip=ip, port=port,)
    node_id, fragment_id, task = (coordinator.add_peer(peer))

    # Lista apenas estruturas simples, serializáveis em JSON.
    known_peers = []

    for node in coordinator.GlobalTable.get_all_nodes():
        other_node_id = node.get("node_id")

        if other_node_id == node_id:
            continue

        known_peers.append(
            {
                "id_node": other_node_id,
                "ip": node.get("ip"),
                "port": node.get("port"),
            }
        )

    response = JoinAck(
        node_id=node_id,
        fragment_id=fragment_id,
        peers=known_peers,
        task=task.to_dict() if task else None,
        run_id=coordinator.run_id,
    )

    await send_message(writer, response.to_dict(),)

    asyncio.create_task(coordinator.broadcast_membership_update())

    if status_queue is not None:
        await status_queue.put(
            {
                "event": "peer_joined",
                "node_id": node_id,
                "ip": ip,
                "port": port,
                "fragment_id": fragment_id,
                "task_id": (
                    task.task_id
                    if task is not None
                    else None
                ),
            }
        )

    log.info(
        f"JoinNetwork: peer registrado "
        f"{node_id[:8]}… ({ip}:{port}), "
        f"fragmento={fragment_id}, "
        f"task={task.task_id if task else None}"
    )

    return node_id

# TaskResult                                                         
async def handle_task_result(
    msg: Dict[str, Any],
    writer: asyncio.StreamWriter,
    coordinator: Coordinator,
    status_queue: Optional[asyncio.Queue] = None,
) -> None:
    # Roteado por: ServerMessenger → MSG_TASK_RESULT
    task_id = msg.get("task_id", "")
    msg_run_id = msg.get("run_id") or ""

    if msg_run_id and coordinator.run_id and msg_run_id != coordinator.run_id:
        log.warning(
            f"TaskResult: run_id divergente para task {task_id[:8]}… "
            f"(msg={msg_run_id[:8]}…, servidor={coordinator.run_id[:8]}…) — rejeitado"
        )
        await send_message(writer, {
            "type": "Error",
            "code": "RUN_MISMATCH",
            "detail": task_id,
        })
        return

    result = coordinator.receive_task_result(task_id, msg)

    ack = Ack(ref_type=MSG_TASK_RESULT, ref_id=task_id)
    await send_message(writer, ack.to_dict())

    if result is not None:
        peer, task = result
        failed = msg.get("status") == "failed" or msg.get("error") is not None
        f1_score = msg.get("f1_score") if not failed else None
        best_model = coordinator.get_best_model()
        is_new_best = (
            not failed
            and best_model is not None
            and best_model.get("task_id") == task_id
        )
        if status_queue is not None:
            await status_queue.put({
                "event": "task_result",
                "task_id": task_id,
                "peer_id": peer.id_node,
                "status": "failed" if failed else "success",
                "f1_score": f1_score,
                "is_new_best": is_new_best,
                "hyperparameters": dict(task.parametros or {}),
            })
        if failed:
            log.info(
                f"TaskResult de {peer.id_node[:8]}… — FALHOU "
                f"({msg.get('error')}), tarefa reenfileirada"
            )
        else:
            log.info(
                f"TaskResult de {peer.id_node[:8]}… — "
                f"f1={(f1_score or 0.0):.4f}"
                + (" [NOVO MELHOR]" if is_new_best else "")
            )
    else:
        log.warning(f"TaskResult: task_id desconhecido '{task_id}'")

# RequestBestModel                                                    
async def handle_request_best_model(
    msg: Dict[str, Any],
    writer: asyncio.StreamWriter,
    coordinator: Coordinator,
    status_queue: Optional[asyncio.Queue] = None,
) -> None:
    #Roteado por: ServerMessenger → MSG_REQUEST_BEST
    requester_id = msg.get("id_node", "")
    best = coordinator.get_best_model()

    if best is None:
        await send_message(writer, {
            "type": "Error",
            "reason": "Nenhum modelo disponível ainda.",
        })
        log.info(
            f"RequestBestModel de {requester_id[:8] if requester_id else '?'}…: "
            "sem modelo ainda"
        )
        return

    model_bytes = best["model_bytes"]
    metricas = {
        k: float(v)
        for k, v in best.get("metrics", {}).items()
        if isinstance(v, (int, float))
    }
    response = SendBestModel(
        id_node="server",
        model_bytes=model_bytes,
        metricas=metricas,
    )
    await send_message(writer, response.to_dict())

    if status_queue is not None:
        await status_queue.put({
            "event": "best_model_requested",
            "requester_id": requester_id,
            "best_task_id": best.get("task_id"),
            "f1_score": best.get("f1_score"),
        })

    log.info(
        f"RequestBestModel de {requester_id[:8] if requester_id else '?'}…: "
        f"enviado (f1={best.get('f1_score', 0.0):.4f})"
    )

# DatasetReady                                                        
async def handle_dataset_ready(
    msg: Dict[str, Any],
    writer: asyncio.StreamWriter,
    coordinator: Coordinator,
    status_queue: Optional[asyncio.Queue] = None,
) -> None:
    """
    O servidor considera que o peer realmente possui o fragmento em disco a partir daqui
    """
    node_id = msg.get("id_node", "")
    fragment_id = msg.get("fragment_id", "")
    msg_run_id = msg.get("run_id") or ""

    if not node_id or not fragment_id:
        await send_message(writer, {
            "type": "Error",
            "code": "INVALID_DATASET_READY",
            "detail": "id_node e fragment_id são obrigatórios.",
        })
        return

    if msg_run_id and coordinator.run_id and msg_run_id != coordinator.run_id:
        log.warning(
            f"DatasetReady: run_id divergente de {node_id[:8]}… "
            f"(msg={msg_run_id[:8]}…, servidor={coordinator.run_id[:8]}…)"
        )

    coordinator.GlobalTable.add_fragment_location(fragment_id, node_id)

    ack = Ack(ref_type=MSG_DATASET_READY, ref_id=fragment_id)
    await send_message(writer, ack.to_dict())

    if status_queue is not None:
        await status_queue.put({
            "event": "dataset_ready",
            "node_id": node_id,
            "fragment_id": fragment_id,
        })

    log.info(
        f"DatasetReady: {node_id[:8]}… confirmou posse de '{fragment_id}'"
    )

# RequestFragmentBackup                                               
async def handle_request_fragment_backup(
    msg: Dict[str, Any],
    writer: asyncio.StreamWriter,
    coordinator: Coordinator,
    status_queue: Optional[asyncio.Queue] = None,
) -> None:
    """
    Quando um peer não encontra o fragmento em nenhum outro peer
    """
    requester = msg.get("id_node", "")
    fragment_id = msg.get("fragment_id", "")

    payload = coordinator.GlobalTable.fragments_payloads.get(fragment_id)

    if payload is None:
        log.warning(
            f"RequestFragmentBackup: '{fragment_id}' não existe no servidor "
            f"(pedido de {requester[:8] if requester else '?'}…)"
        )
        await send_message(writer, {
            "type": "Error",
            "code": "FRAGMENT_NOT_FOUND",
            "detail": fragment_id,
        })
        return

    reply = FragmentBackupData(id_node=requester, fragment_id=fragment_id, data=payload)
    await send_message(writer, reply.to_dict())

    if status_queue is not None:
        await status_queue.put({
            "event": "fragment_backup_served",
            "requester_id": requester,
            "fragment_id": fragment_id,
        })

    log.info(
        f"RequestFragmentBackup: '{fragment_id}' ({len(payload)} bytes) "
        f"enviado para {requester[:8] if requester else '?'}…"
    )

async def handle_peer_ready(
    msg: Dict[str, Any],
    writer: asyncio.StreamWriter,
    coordinator: Coordinator,
    status_queue: Optional[asyncio.Queue] = None,
) -> None:
    """Só a partir daqui o peer é elegível em dispatch_all_idle().

    Antes de PeerReady, o servidor pode já ter registrado o peer (via
    JoinNetwork), mas o listener P2P dele ainda pode não estar de pé.
    """
    node_id = msg.get("id_node", "")
    coordinator.GlobalTable.set_node_ready(node_id, True)
    ack = Ack(ref_type=MSG_PEER_READY, ref_id=node_id)
    await send_message(writer, ack.to_dict())
    if status_queue is not None:
        await status_queue.put({"event": "peer_ready", "node_id": node_id})
    log.info(f"PeerReady: {node_id[:8] if node_id else '?'}… apto a receber tarefas")

# KeepAlive                                                       
async def handle_keep_alive(
    msg: Dict[str, Any],
    writer: asyncio.StreamWriter,
    coordinator: Coordinator,
    status_queue: Optional[asyncio.Queue] = None,
) -> None:
    node_id = msg.get("id_node", "")
    ack = Ack(ref_type=MSG_KEEP_ALIVE, ref_id=node_id)
    await send_message(writer, ack.to_dict())
    log.debug(f"KeepAlive de {node_id[:8] if node_id else '?'}…")

# Registro centralizado                                                
def register_all_handlers(
    messenger,
    coordinator: Coordinator,
    status_queue: Optional[asyncio.Queue] = None,
) -> None:
    # Registra todos os handlers de protocolo no ServerMessenger, substituindoos handlers built-in definidos em ServerMessenger.__init__.
    bindings = [
        (MSG_JOIN_NETWORK,           handle_join_network),
        (MSG_TASK_RESULT,            handle_task_result),
        (MSG_REQUEST_BEST,           handle_request_best_model),
        (MSG_KEEP_ALIVE,             handle_keep_alive),
        (MSG_DATASET_READY,          handle_dataset_ready),
        (MSG_REQUEST_FRAGMENT_BACKUP, handle_request_fragment_backup),
        (MSG_PEER_READY,             handle_peer_ready),
    ]
    for msg_type, handler_fn in bindings:
        # functools.partial vincula coordinator e status_queue,
        # produzindo a assinatura final: async (msg, writer) -> None
        messenger.register_handler(
            msg_type,
            functools.partial(handler_fn, coordinator=coordinator, status_queue=status_queue),
        )
    log.info("Handlers de protocolo server-peer registrados no ServerMessenger.")