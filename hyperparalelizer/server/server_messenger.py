""" ServerMessenger - Roteador TCP assíncrono.
 - Aceitar conexões TCP dos peers.
 - Ler e deserializar a mensagem recebida.
 - Identificar o tipo da mensagem e despachar para o handler registrado.
 - Fechar a conexão após o processamento.
 - Expor status_queue para publicação de eventos por handlers externos.
Os handlers concretos de protocolo são registrados externamente via register_all_handlers() (server-peer_protocol.py), mantendo este módulo restrito ao transporte e roteamento.
"""
import asyncio
from typing import Any, Callable, Coroutine, Dict, Optional

from core.network import recv_message
from hyperparalelizer.server.coordinator import Coordinator
from utils.logger import get_logger

log = get_logger("server_messenger")
class ServerMessenger:

    def __init__(self, coordinator: Coordinator, host: str, port: int):
        self.coordinator = coordinator
        self.host = host
        self.port = port

        self._server: Optional[asyncio.AbstractServer] = None

        # Tabela de dispatch — populada externamente via register_handler()
        self._handlers: Dict[str, Any] = {}

        # Fila de eventos de status: {"event": str, ...dados relevantes...}
        self.status_queue: asyncio.Queue = asyncio.Queue()
                                                    
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

    def register_handler(self, msg_type: str, handler: Callable[..., Coroutine]) -> None:
        # Registra ou substitui o handler para msg_type na tabela de dispatch
        self._handlers[msg_type] = handler

    # Dispatcher central                                                   
    async def _handle_connection(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        # Lê uma mensagem, despacha para o handler correto e fecha a conexão
        peer_addr = writer.get_extra_info("peername")
        try:
            msg = await recv_message(reader)
            msg_type = msg.get("type", "")
            log.debug(f"Recebido '{msg_type}' de {peer_addr}")

            handler = self._handlers.get(msg_type)
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


