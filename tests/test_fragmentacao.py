import asyncio
import os
import shutil
import sys

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from hyperparalelizer.global_table import GlobalTable
from hyperparalelizer.server.coordinator import Coordinator
from hyperparalelizer.ml.dataset_loader import DatasetLoader
from hyperparalelizer.peer.peer_outer_protocol import register_peer_peer_handlers
from hyperparalelizer.peer.download_worker import fetch_fragment
from core.network import P2PNode

CSV_PATH = "dataset/dataset_risco_credito_3000.csv"
TARGET_COL = "inadimplente"

DONOR_DIR = "test_fragmentacao/donor_fragments"     # onde o peer "dono" guarda os fragmentos
RECEIVER_DIR = "test_fragmentacao/receiver_fragments"  # onde o peer que baixa vai salvar

DONOR_PORT = 8901


async def main():
    # limpa execucoes anteriores
    for d in (DONOR_DIR, RECEIVER_DIR):
        shutil.rmtree(d, ignore_errors=True)
        os.makedirs(d, exist_ok=True)

    # Carrega o CSV real do projeto
    df = pd.read_csv(CSV_PATH)
    X_full = df.drop(columns=[TARGET_COL]).to_numpy()
    y_full = df[TARGET_COL].to_numpy()
    print(f"CSV carregado: {len(X_full)} linhas, {X_full.shape[1]} features")

    # Fragmenta usando o Coordinator de verdade (mesma logica do sistema)
    table = GlobalTable()
    coordinator = Coordinator(dataset=(X_full, y_full), model=None, global_table=table)
    N_FRAGMENTS = 4
    fragment_names = coordinator.fragment_dataset(n_fragments=N_FRAGMENTS)
    print(f"fragment_dataset() gerou: {fragment_names}")

    # O peer "dono" grava os fragmentos em disco, exatamente como o
    # download_worker/peer_outer_protocol esperam encontrar (<id>.bin)
    for frag_name in fragment_names:
        payload_bytes = table.fragments_payloads[frag_name]
        with open(os.path.join(DONOR_DIR, f"{frag_name}.bin"), "wb") as f:
            f.write(payload_bytes)

    # Sobe um P2PNode real (o "dono") servindo RequestFragment
    donor_node = P2PNode(host="127.0.0.1", port=DONOR_PORT, node_id="donor-node")
    register_peer_peer_handlers(donor_node, storage_dir=DONOR_DIR)
    server_task = asyncio.create_task(donor_node.start())
    await asyncio.sleep(0.3)  # da tempo do servidor subir

    try:
        # Outro peer ("receiver") baixa cada fragmento via TCP de verdade,
        # usando o download_worker.fetch_fragment (o mesmo que o DataThread usa)
        for frag_name in fragment_names:
            ok = await fetch_fragment(
                peer_ip="127.0.0.1",
                peer_port=DONOR_PORT,
                fragment_id=frag_name,
                node_id="receiver-node",
                storage_dir=RECEIVER_DIR,
            )
            assert ok, f"Falha ao baixar '{frag_name}' via TCP"
        print(f"Todos os {len(fragment_names)} fragmentos baixados via TCP real")

        # Remonta o dataset usando o DatasetLoader real
        loader = DatasetLoader(storage_dir=RECEIVER_DIR)
        X_rebuilt, y_rebuilt = loader.load(fragment_names)
        print(f"DatasetLoader remontou: {X_rebuilt.shape}, {y_rebuilt.shape}")

        # Confere que bate exatamente com o original
        assert X_rebuilt.shape == X_full.shape, "shape de X diferente!"
        assert y_rebuilt.shape == y_full.shape, "shape de y diferente!"
        assert np.array_equal(X_rebuilt, X_full), "valores de X divergem!"
        assert np.array_equal(y_rebuilt, y_full), "valores de y divergem!"
        print("OK: dataset remontado e' byte-a-byte identico ao original")

        # Testa tambem o caso de fragmento ausente (pra confirmar o erro
        # correto do DatasetLoader, sem quebrar o processo)
        try:
            loader.load(["fragment_9999"])
            print("FALHOU: deveria ter levantado FileNotFoundError")
        except FileNotFoundError:
            print("OK: fragmento inexistente levanta FileNotFoundError como esperado")

    finally:
        server_task.cancel()
        try:
            await server_task
        except asyncio.CancelledError:
            pass


if __name__ == "__main__":
    asyncio.run(main())