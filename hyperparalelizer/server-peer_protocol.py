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
from hyperparalelizer.server.middleware import Coordinator, Peer
from utils.logger import get_logger
from utils.protocol import (
    MSG_JOIN_NETWORK,
    MSG_KEEP_ALIVE,
    MSG_REQUEST_BEST,
    MSG_TASK_RESULT,
    Ack,
    SendBestModel,
)

log = get_logger("server_peer_protocol")

# JoinNetwork                                                     
async def handle_join_network(
    msg: Dict[str, Any],
    writer: asyncio.StreamWriter,
    coordinator: Coordinator,
    status_queue: Optional[asyncio.Queue] = None,
) -> str:
    peer = Peer(ip=msg["ip"], port=msg["porta"])
    node_id = coordinator.add_peer(peer)

    ack = Ack(ref_type=MSG_JOIN_NETWORK, ref_id=node_id)
    await send_message(writer, ack.to_dict())

    if status_queue is not None:
        await status_queue.put({
            "event": "peer_joined",
            "node_id": node_id,
            "ip": msg["ip"],
            "port": msg["porta"],
        })

    log.info(f"JoinNetwork: peer registrado {node_id[:8]}… ({msg['ip']}:{msg['porta']})")
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
    result = coordinator.receive_task_result(task_id, msg)

    ack = Ack(ref_type=MSG_TASK_RESULT, ref_id=task_id)
    await send_message(writer, ack.to_dict())

    if result is not None:
        peer, task = result
        is_new_best = (
            coordinator.best_model is not None
            and coordinator.best_model.get("task_id") == task_id
        )
        if status_queue is not None:
            await status_queue.put({
                "event": "task_result",
                "task_id": task_id,
                "peer_id": peer.id_node,
                "f1_score": msg.get("f1_score", 0.0),
                "is_new_best": is_new_best,
            })
        log.info(
            f"TaskResult de {peer.id_node[:8]}… — "
            f"f1={msg.get('f1_score', 0.0):.4f}"
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
    best = coordinator.best_model

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

    model_bytes = pickle.dumps(best)
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
        (MSG_JOIN_NETWORK, handle_join_network),
        (MSG_TASK_RESULT,  handle_task_result),
        (MSG_REQUEST_BEST, handle_request_best_model),
        (MSG_KEEP_ALIVE,   handle_keep_alive),
    ]
    for msg_type, handler_fn in bindings:
        # functools.partial vincula coordinator e status_queue,
        # produzindo a assinatura final: async (msg, writer) -> None
        messenger.register_handler(
            msg_type,
            functools.partial(handler_fn, coordinator=coordinator, status_queue=status_queue),
        )
    log.info("Handlers de protocolo server-peer registrados no ServerMessenger.")