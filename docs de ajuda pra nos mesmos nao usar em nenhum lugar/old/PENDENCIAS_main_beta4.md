# Pendências no código para completar a `main_beta3.py`

A `main_beta3.py` refatorada não cria subclasses ou wrappers para corrigir componentes. Os recursos abaixo precisam ser incorporados aos módulos oficiais.

## 1. `hyperparalelizer/peer/runtime.py` — prioridade máxima

### 1.1 Storage e `--reset-storage`

Adicionar ao construtor:

```python
reset_storage: bool = False
storage_manager: Optional[StorageManager] = None
```

Antes de `DataThread.join_network()`:

1. validar que o caminho está dentro de `data/peer_<porta>/`;
2. apagar o diretório somente quando `reset_storage=True`;
3. recriar o diretório;
4. registrar o que foi apagado.

A limpeza não deve ser implementada na main.

### 1.2 Validar integralmente o `JoinAck`

Remover o fallback atual:

```python
reply.get("node_id") or self.node_id or "peer"
```

O runtime deve abortar quando:

- o servidor não responde;
- `type != JoinAck`;
- existe `Error`;
- `node_id` é vazio;
- `fragment_id`, `peers` ou `task` possuem formato inválido.

### 1.3 Ordem do bootstrap

A ordem correta deve ser:

1. `join_network()`;
2. usar o `node_id` definitivo;
3. criar mensageiro, trainer, Maekawa, Bully e Pub/Sub;
4. criar e registrar handlers no `P2PNode`;
5. iniciar o mensageiro;
6. iniciar o P2P;
7. aguardar readiness real;
8. iniciar heartbeat;
9. executar a tarefa inicial.

Hoje a tarefa inicial é criada antes de existir uma confirmação real de que o P2P começou a escutar.

### 1.4 Maekawa real

O método `_build_maekawa_mutex()` não pode usar:

```python
quorum=[]
```

Construir o quórum com `DataThread.known_peers`, registrar handlers e atualizar o quórum quando o snapshot/membership mudar.

Adicionar opção oficial:

```python
enable_maekawa: bool = True
```

`NoOpMutex` deve ser usado somente quando essa opção for explicitamente falsa.

### 1.5 Bully e promoção

O método `_build_bully()` não pode usar:

```python
globaltable_peers={}
promote_callback=lambda: None
```

Criar `PupilManager` ou `FailoverRuntime`, responsável por:

- guardar snapshots;
- iniciar eleição;
- promover o peer;
- iniciar `Coordinator`, `ServerMessenger`, scheduler, Pub/Sub e replicação;
- atualizar o endereço do servidor em todos os componentes.

### 1.6 Pub/Sub do peer

Adicionar ao runtime:

```python
enable_pubsub: bool = True
pubsub_client_cls: Type[PubSubClient] = PubSubClient
```

Depois do `JoinAck`:

1. criar `PubSubClient` com o `node_id` definitivo;
2. registrar `handle_notify` no `P2PNode`;
3. executar `p2p_node.attach_pubsub_client(client)`;
4. assinar `TOPIC_GLOBAL_BEST_SCORE`;
5. entregar a atualização ao `TrainerNode` por um método público.

Remover o handler vazio atual de `MSG_PUBSUB_NOTIFY`.

### 1.7 KeepAlive

Usar o `KeepAliveManager` existente em `core/network.py`.

Antes de iniciar o P2P:

```python
p2p_node.keepalive = KeepAliveManager(...)
```

O peer deve registrar o servidor como alvo e acionar Bully no callback `on_dead`.

Remover `_server_heartbeat_loop()` quando o `KeepAliveManager` assumir essa função.

## 2. `core/network.py`

### 2.1 Readiness

Adicionar ao `P2PNode`:

```python
self.ready_event = asyncio.Event()
```

Depois de `asyncio.start_server()`:

```python
self.ready_event.set()
```

Expor:

```python
async def wait_until_ready(self):
    await self.ready_event.wait()
```

Fazer o mesmo no `ServerMessenger`.

### 2.2 Rastrear a task do KeepAlive

Atualmente `P2PNode.start()` usa `asyncio.create_task(self.keepalive.run())` sem guardar a task. Guardá-la e aguardá-la/cancelá-la no shutdown.

## 3. `utils/protocol.py`

### 3.1 Manifesto no `JoinAck`

Adicionar:

```python
run_id: str
dataset_id: str
expected_samples: int
expected_features: int
fragment_ids: list[str]
fragment_hashes: dict[str, str]
```

Adicionar `run_id`/`dataset_id` também a:

- `TrainingTask`;
- `TaskResult`;
- `DatasetReady`;
- pedidos de fragmento;
- `SyncState`;
- mensagens Pub/Sub.

## 4. `hyperparalelizer/ml/dataset_loader.py`

Criar um `DatasetManifest` e validar:

- hash de cada arquivo antes do `pickle.loads`;
- IDs repetidos;
- fragmentos ausentes;
- quantidade total de amostras;
- número de features;
- hash completo do dataset.

O loader deve abortar antes do treino quando o breast cancer não tiver 569 amostras.

## 5. `hyperparalelizer/peer/data_thread.py`

- armazenar o manifesto recebido no `JoinAck`;
- validar um fragmento local antes de reutilizá-lo;
- apagar arquivo local inválido;
- validar o hash depois de cada download;
- notificar `DatasetReady` para cada fragmento adquirido, uma única vez;
- oferecer `update_server_endpoint()` e `update_membership()`.

## 6. `hyperparalelizer/peer/peer_messenger.py`

A fila atual remove a mensagem mesmo quando o envio falha.

Criar uma outbox persistente no componente oficial:

```text
data/peer_<porta>/<dataset_id>/outbox/
```

A mensagem só deve ser removida depois de um ACK que confirme aceitação ou duplicidade.

Também:

- rastrear tasks criadas pelos handlers;
- rejeitar `TrainingTask` quando o Trainer está ocupado;
- não enviar ACK de tarefa antes de validar a mensagem;
- oferecer `update_server_endpoint()`.

## 7. `hyperparalelizer/server/coordinator.py` e `global_table.py`

### 7.1 Reserva atômica

Mover a reserva de tarefa para um método único da `GlobalTable` que verifique e reserve dentro do mesmo lock.

### 7.2 Única fonte de despacho

Depois da tarefa inicial enviada pelo `JoinAck`, usar somente o scheduler para tarefas seguintes. Remover o despacho extra do handler de `TaskResult`.

### 7.3 Melhor modelo atômico

Criar:

```python
def update_best_if_better(candidate) -> bool
```

A leitura, comparação e escrita devem ocorrer dentro do mesmo lock.

### 7.4 Progresso e retries

Persistir na `GlobalTable`:

```python
total_tasks
completed_tasks
failed_tasks
task_attempts
processed_results
```

Adicionar limite de tentativas para impedir reenfileiramento infinito.

## 8. `hyperparalelizer/server/server_peer_protocol.py`

### 8.1 ACK de resultado

O servidor não deve responder um `Ack` de sucesso quando a tarefa não existe.

Criar status explícitos:

```text
accepted
duplicate
unknown_task
wrong_run
invalid_result
```

### 8.2 Heartbeat

O handler de `KeepAlive` deve registrar a atividade no gerenciador/estado oficial, e não apenas responder.

## 9. Criar `hyperparalelizer/server/runtime.py`

A main ainda contém os loops genéricos de ciclo de vida do servidor porque não existe um `ServerRuntime` oficial.

Mover para ele:

- criação do broker/publisher Pub/Sub;
- scheduler;
- persistência periódica;
- consumo de eventos;
- seleção e sincronização do Pupilo;
- monitor de conclusão;
- shutdown.

Depois disso, `run_server()` poderá se limitar a:

```python
runtime = ServerRuntime(config)
await runtime.run()
```