"""
 ServerMessenger 
 - receber mensagens TCP dos peers;
 - identificar o tipo de mensagem recebida;
 - encaminhar para o Coordinator;
 - responder ao peer;
 - publicar eventos de status.
"""

import asyncio
import pickle
from typing import Any, Dict, Optional

from core.network import recv_message, send_message
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

log = get_logger("server_messenger")


class ServerMessenger:
    """
    Servidor TCP assíncrono que recebe mensagens dos peers e as roteia
    para o Coordinator.

    Cada conexão é tratada em uma corrotina independente. Após o
    processamento, um evento é publicado em `status_queue` para
    consumo por outros componentes (ex: monitor, PubSub bridge).
    """

    def __init__(self, coordinator: Coordinator, host: str, port: int):
        self.coordinator = coordinator
        self.host = host
        self.port = port

        self._server: Optional[asyncio.AbstractServer] = None

        # Fila de eventos de status: {"event": str, ...dados relevantes...}
        # Consumível por outros componentes sem acoplamento direto.
        self.status_queue: asyncio.Queue = asyncio.Queue()

    # Ciclo de vida                                                      
    async def start(self) -> None:
        """Inicia o servidor e entra no loop de aceitação de conexões."""
        self._server = await asyncio.start_server(
            self._handle_connection,
            self.host,
            self.port,
        )
        log.info(f"ServerMessenger escutando em {self.host}:{self.port}")
        async with self._server:
            await self._server.serve_forever()

    async def stop(self) -> None:
        if self._server:
            self._server.close()
            await self._server.wait_closed()
            log.info("ServerMessenger encerrado.")

    # Dispatcher central                                                   
    async def _handle_connection(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        """Lê uma mensagem, despacha para o handler correto e fecha a conexão."""
        peer_addr = writer.get_extra_info("peername")
        try:
            msg = await recv_message(reader)
            msg_type = msg.get("type", "")
            log.debug(f"Recebido '{msg_type}' de {peer_addr}")

            handlers = {
                MSG_JOIN_NETWORK: self._handle_join_network,
                MSG_TASK_RESULT:  self._handle_task_result,
                MSG_REQUEST_BEST: self._handle_request_best_model,
                MSG_KEEP_ALIVE:   self._handle_keep_alive,
            }

            handler = handlers.get(msg_type)
            if handler:
                await handler(msg, writer)
            else:
                log.warning(f"Tipo de mensagem desconhecido: '{msg_type}' de {peer_addr}")

        except Exception as exc:
            log.error(f"Erro ao processar conexão de {peer_addr}: {exc}")
        finally:
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass

    # Handlers                                                             
    async def _handle_join_network(
        self,
        msg: Dict[str, Any],
        writer: asyncio.StreamWriter,
    ) -> None:
        """
        JoinNetwork → coordinator.add_peer() → Ack(node_id)

        Cria um Peer com IP/porta do peer entrante e registra no Coordinator,
        que por sua vez registra na DHT e atribui fragmento + hiperparâmetros.
        """
        peer = Peer(ip=msg["ip"], port=msg["porta"])
        node_id = self.coordinator.add_peer(peer)

        ack = Ack(ref_type=MSG_JOIN_NETWORK, ref_id=node_id)
        await send_message(writer, ack.to_dict())

        await self.status_queue.put({
            "event": "peer_joined",
            "node_id": node_id,
            "ip": msg["ip"],
            "port": msg["porta"],
        })
        log.info(f"Peer entrou na rede: {node_id[:8]}… ({msg['ip']}:{msg['porta']})")

    async def _handle_task_result(
        self,
        msg: Dict[str, Any],
        writer: asyncio.StreamWriter,
    ) -> None:
        
        # TaskResult → coordinator.receive_task_result() → Ack

        task_id = msg.get("task_id", "")
        result = self.coordinator.receive_task_result(task_id, msg)

        ack = Ack(ref_type=MSG_TASK_RESULT, ref_id=task_id)
        await send_message(writer, ack.to_dict())

        if result is not None:
            peer, task = result
            is_new_best = (
                self.coordinator.best_model is not None
                and self.coordinator.best_model.get("task_id") == task_id
            )
            await self.status_queue.put({
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
            log.warning(f"TaskResult para task_id desconhecido: '{task_id}'")

    async def _handle_request_best_model(
        self,
        msg: Dict[str, Any],
        writer: asyncio.StreamWriter,
    ) -> None:
        # RequestBestModel → coordinator.best_model → SendBestModel
        
        requester_id = msg.get("id_node", "")
        best = self.coordinator.best_model

        if best is None:
            await send_message(writer, {
                "type": "Error",
                "reason": "Nenhum modelo disponível ainda.",
            })
            log.info(f"RequestBestModel de {requester_id[:8] if requester_id else '?'}…: sem modelo ainda")
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

        await self.status_queue.put({
            "event": "best_model_requested",
            "requester_id": requester_id,
            "best_task_id": best.get("task_id"),
            "f1_score": best.get("f1_score"),
        })
        log.info(
            f"RequestBestModel de {requester_id[:8] if requester_id else '?'}…: "
            f"enviado (f1={best.get('f1_score', 0.0):.4f})"
        )

    async def _handle_keep_alive(
        self,
        msg: Dict[str, Any],
        writer: asyncio.StreamWriter,
    ) -> None:
        node_id = msg.get("id_node", "")
        ack = Ack(ref_type=MSG_KEEP_ALIVE, ref_id=node_id)
        await send_message(writer, ack.to_dict())
        log.debug(f"KeepAlive de {node_id[:8] if node_id else '?'}…")
