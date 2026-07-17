from __future__ import annotations
 
import asyncio
from typing import TYPE_CHECKING, Any, Dict, Optional
 
from core.network import send_once
from utils.protocol import MSG_SYNC_STATE
 
if TYPE_CHECKING:  # pragma: no cover - apenas para tipagem
    from hyperparalelizer.server.coordinator import Coordinator
 
 
class PupilManager:
    def __init__(self, coordinator: "Coordinator") -> None:
        self.coordinator = coordinator
        self._pupil_id: Optional[str] = None
        self._epoch = 0
        self._lock = asyncio.Lock()
 
    @property
    def pupil_id(self) -> Optional[str]:
        return self._pupil_id
 
    @property
    def epoch(self) -> int:
        return self._epoch
 
    def _select_candidate(self) -> Optional[Dict[str, Any]]:
        nodes = self.coordinator.GlobalTable.get_all_nodes()
        if not nodes:
            return None
        winner = max(nodes, key=lambda node: str(node.get("node_id") or ""))
        return {
            "id_node": winner.get("node_id"),
            "ip": winner.get("ip"),
            "port": winner.get("port"),
        }
 
    async def reconcile(self) -> bool:
        """Reavalia quem deveria ser o Pupilo.
 
        Retorna True se houve mudança de titular (nova época).
        """
        async with self._lock:
            candidate = self._select_candidate()
            candidate_id = candidate.get("id_node") if candidate else None
 
            if candidate_id == self._pupil_id:
                return False
 
            previous_id = self._pupil_id
            previous_node = (
                self.coordinator.GlobalTable.get_node(previous_id)
                if previous_id
                else None
            )
 
            self._pupil_id = candidate_id
            self._epoch += 1
 
            table = self.coordinator.GlobalTable
            with table.lock:
                table.pupil_id = candidate_id
                table.pupil_epoch = self._epoch
 
            self.coordinator.pupil_peer = candidate
 
            if previous_node is not None:
                await self._revoke(previous_node)
 
            if candidate is not None:
                await self._assign(candidate)
 
            return True
 
    async def _revoke(self, previous_node: Dict[str, Any]) -> None:
        """Avisa o Pupilo anterior de que ele foi destituído.
 
        Envia um SyncState "vazio" apontando o novo pupil_id/epoch: o peer
        que recebe compara com seu próprio node_id e, ao ver que não é mais
        o titular, zera is_pupil e descarta a réplica antiga.
        """
        ip = previous_node.get("ip")
        port = previous_node.get("port")
        if not ip or not port:
            return
        msg = {
            "type": MSG_SYNC_STATE,
            "id_node": "server",
            "snapshot_id": "revoke",
            "run_id": self.coordinator.run_id,
            "pupil_id": self._pupil_id,
            "pupil_epoch": self._epoch,
            "global_table_snapshot": {},
        }
        try:
            await send_once(ip, port, msg, expect_reply=False, timeout=5.0)
        except Exception:
            pass
 
    async def _assign(self, candidate: Dict[str, Any]) -> None:
        """Empurra o primeiro snapshot para o novo Pupilo imediatamente."""
        del candidate
        try:
            await self.coordinator.replicate_state_to_pupil()
        except Exception:
            pass