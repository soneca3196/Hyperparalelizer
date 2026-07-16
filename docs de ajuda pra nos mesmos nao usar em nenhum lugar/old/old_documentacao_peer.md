# Documentação técnica da pasta `hyperparalelizer/peer/`

## 1. Objetivo da camada Peer

A pasta `peer/` reúne os componentes executados em cada nó trabalhador do Hyperparalelizer.

Um peer possui quatro responsabilidades principais:

1. Entrar na rede e obter informações do servidor.
2. Obter e disponibilizar fragmentos do dataset.
3. Receber e executar tarefas de treinamento.
4. Enviar resultados e modelos ao servidor.

A arquitetura atual está dividida assim:

```text
peer/
├── data_thread.py
├── download_worker.py
├── peer_inner_protocol.py
├── peer_messenger.py
├── peer_outer_protocol.py
└── trainer.py
```

A relação pretendida entre os componentes é:

```text
                         Servidor central
                               │
              JoinNetwork / TrainingTask / TaskResult
                               │
                               ▼
                        PeerMessenger
                         │          │
                         │          └── envia mensagens ao servidor
                         ▼
                      TrainerNode
                         │
                         ▼
                       DataThread
                         │
             ┌───────────┴────────────┐
             ▼                        ▼
      DownloadWorker          PeerOuterProtocol
      baixa fragmentos        fornece fragmentos
             │                        │
             └────────── P2PNode ─────┘

Todos os componentes podem emitir eventos para:

                   InternalEventBus
```

O `P2PNode`, localizado em `core/network.py`, funciona como servidor TCP do peer. Ele recebe mensagens, identifica o campo `type` e chama o handler correspondente registrado pelos componentes da pasta `peer/`.

---

# 2. `data_thread.py`

## 2.1. Responsabilidade

O `DataThread` é responsável pelo ciclo de aquisição dos dados usados no treinamento.

Apesar do nome, ele não cria uma thread própria. Trata-se de um componente assíncrono que:

* entra na rede;
* recebe informações sobre fragmentos;
* verifica arquivos locais;
* tenta baixar fragmentos de outros peers;
* usa o servidor como fonte de backup;
* informa ao servidor quando um fragmento está disponível;
* publica eventos internos sobre sucesso ou falha.

A principal classe é:

```python
class DataThread
```

---

## 2.2. Dependências

O módulo depende de:

```python
JoinNetwork
DatasetReady
MSG_JOIN_ACK
send_once
fetch_fragment
fetch_fragment_from_backup
InternalEventBus
FragmentAcquired
FragmentAssemblyFailed
```

Portanto, ele depende diretamente de:

* contratos definidos em `utils/protocol.py`;
* transporte TCP de `core/network.py`;
* download de fragmentos;
* protocolo interno do peer.

---

## 2.3. Construtor

```python
DataThread(
    node_id,
    ip,
    listen_port,
    server_ip,
    server_port,
    storage_dir="data/fragments",
    event_bus=None,
)
```

### Parâmetros

| Parâmetro     | Função                                            |
| ------------- | ------------------------------------------------- |
| `node_id`     | Identificador do peer                             |
| `ip`          | Endereço anunciado ao servidor e aos outros peers |
| `listen_port` | Porta em que o `P2PNode` escuta                   |
| `server_ip`   | IP do servidor central                            |
| `server_port` | Porta do servidor central                         |
| `storage_dir` | Diretório onde os fragmentos são armazenados      |
| `event_bus`   | Barramento opcional para eventos internos         |

### Estado interno

Depois da construção, o componente mantém:

```python
self.fragment_id
self.known_peers
self.initial_task
```

Esses campos deveriam ser preenchidos após a entrada na rede.

---

## 2.4. Entrada na rede

### Método

```python
async def join_network(
    memoria_total_mb=0.0,
    memoria_disponivel_mb=0.0,
    timeout=15.0,
)
```

O método envia:

```python
JoinNetwork(
    id_node=self.node_id,
    ip=self.ip,
    porta=self.listen_port,
    memoria_total_mb=...,
    memoria_disponivel_mb=...,
)
```

Depois, espera uma resposta cujo tipo seja `MSG_JOIN_ACK`.

Da resposta, tenta extrair:

```python
self.fragment_id = reply.get("fragment_id")
self.known_peers = reply.get("peers", [])
self.initial_task = reply.get("task")
```

### Problema atual

No protocolo atual:

```python
MSG_JOIN_ACK = MSG_ACK
```

O servidor responde apenas com um `Ack` contendo o `node_id` em `ref_id`. Não envia `fragment_id`, `peers` ou `task`.
Consequentemente, depois do join:

```python
fragment_id = None
known_peers = []
initial_task = None
```

### Alteração necessária

Criar uma resposta própria:

```python
@dataclass
class JoinAck:
    node_id: str
    required_fragment_ids: list[str]
    fragment_sources: dict[str, list[dict]]
    peers: list[dict]
    task: dict | None
```

O `DataThread` também deve atualizar seu `node_id` caso o servidor continue responsável por gerá-lo.

---

## 2.5. Localização de fragmentos

### `fragment_path()`

```python
def fragment_path(fragment_id=None) -> str
```

Produz um caminho no formato:

```text
data/fragments/<fragment_id>.bin
```

### `has_fragment_locally()`

```python
def has_fragment_locally(fragment_id=None) -> bool
```

Verifica se o arquivo existe.

### Problema atual

O diretório padrão é compartilhado:

```text
data/fragments
```

Ao executar vários peers na mesma máquina, todos podem acessar os mesmos arquivos. Isso mascara a transferência P2P durante os testes.

### Estrutura recomendada

```text
data/
└── nodes/
    ├── <node_id_1>/
    │   └── fragments/
    └── <node_id_2>/
        └── fragments/
```

Cada peer deve usar:

```python
storage_dir = f"data/nodes/{node_id}/fragments"
```

---

## 2.6. Montagem do dataset

### `assemble_dataset()`

Monta o fragmento principal atribuído ao peer.

Caso `fragment_id` não tenha sido recebido, retorna `None`.

### `assemble_dataset_for(fragment_id)`

O método usa esta ordem de aquisição:

```text
1. Arquivo local
2. Outros peers
3. Backup do servidor
4. Falha
```

Fluxo:

```python
if fragmento_existe_localmente:
    return caminho

for peer in known_peers:
    tentar fetch_fragment(...)

tentar fetch_fragment_from_backup(...)

return None
```

### `assemble_many(fragment_ids)`

Executa `assemble_dataset_for()` sequencialmente para todos os fragmentos.

A função termina no primeiro erro:

```python
for fragment_id in fragment_ids:
    if await assemble_dataset_for(fragment_id) is None:
        return False
return True
```

### Pontos de melhoria

* Os downloads são sequenciais.
* Não há atualização dinâmica da lista de peers.
* Não há indicação de quais peers possuem cada fragmento.
* Todos os peers conhecidos são consultados, independentemente do fragmento solicitado.
* Não há checksum.
* Não há validação do tamanho do fragmento.
* Não há limite de tentativas.
* Não há download parcial ou retomada.

Para o MVP, o comportamento sequencial é aceitável. A prioridade deve ser corrigir o contrato com o servidor.

---

## 2.7. Eventos emitidos

Em caso de sucesso, publica:

```python
FragmentAcquired(
    fragment_id=...,
    node_id=...,
    source="local" | "peer" | "server_backup",
)
```

Em caso de falha, publica:

```python
FragmentAssemblyFailed(
    fragment_id=...,
    node_id=...,
    reason="no_source_available",
)
```

Esses eventos são locais e não trafegam pela rede.

---

## 2.8. Notificação `DatasetReady`

### Método

```python
async def notify_dataset_ready(fragment_id=None) -> bool
```

Envia:

```python
DatasetReady(
    id_node=self.node_id,
    fragment_id=fid,
)
```

A função considera sucesso se receber qualquer mensagem do tipo:

```python
"type": "Ack"
```

### Problemas atuais

O servidor não registra um handler para `DatasetReady`. Seus handlers atuais são apenas:

* `JoinNetwork`;
* `TaskResult`;
* `RequestBestModel`;
* `KeepAlive`.

Além disso, o trainer chama `notify_dataset_ready()` apenas para o primeiro fragmento:

```python
fragment_ids[0]
```

Mesmo quando a tarefa exige múltiplos fragmentos.

### Alteração recomendada

Criar:

```python
async def notify_fragments_ready(
    fragment_ids: list[str],
) -> bool:
```

E enviar:

```python
{
    "type": "DatasetReady",
    "node_id": "...",
    "fragment_ids": [
        "fragment_0000",
        "fragment_0001",
    ],
}
```

O servidor deve registrar as localizações somente após essa confirmação.

---

# 3. `download_worker.py`

## 3.1. Responsabilidade

O `download_worker.py` implementa o lado cliente da transferência de fragmentos.

Ele pode buscar dados em:

1. outro peer;
2. servidor central como backup.

O módulo não cria uma thread ou worker permanente. Suas funções são corrotinas chamadas pelo `DataThread`.

---

## 3.2. `_save_fragment()`

```python
def _save_fragment(
    storage_dir,
    fragment_id,
    data,
) -> str
```

Cria o diretório, grava os bytes e retorna o caminho.

### Problemas atuais

A gravação ocorre diretamente no arquivo definitivo:

```python
with open(path, "wb") as f:
    f.write(data)
```

Se o processo morrer durante a gravação, um fragmento incompleto pode ser considerado válido porque `has_fragment_locally()` verifica apenas a existência do arquivo.

### Alteração recomendada

Usar gravação atômica:

```python
temp_path = path + ".part"

with open(temp_path, "wb") as file:
    file.write(data)

os.replace(temp_path, path)
```

Também deve ser armazenado ou validado um checksum.

---

## 3.3. Download de outro peer

### Função

```python
async def fetch_fragment(
    peer_ip,
    peer_port,
    fragment_id,
    node_id,
    storage_dir,
    timeout=30.0,
) -> bool
```

Envia:

```python
RequestFragment(
    id_node=node_id,
    fragment_id=fragment_id,
)
```

Respostas aceitas:

```text
FragmentData
FragmentNotFound
```

Caso receba `FragmentData`, verifica se `data` é `bytes` ou `bytearray`, salva o arquivo e retorna `True`.

---

## 3.4. Download do servidor

### Função

```python
async def fetch_fragment_from_backup(
    server_ip,
    server_port,
    fragment_id,
    node_id,
    storage_dir,
    timeout=30.0,
) -> bool
```

Envia:

```python
RequestFragmentBackup(
    id_node=node_id,
    fragment_id=fragment_id,
)
```

Espera:

```text
FragmentBackup
```

### Problema atual

O servidor não possui handler registrado para `RequestFragmentBackup`. Portanto, o fallback final do `DataThread` não funciona atualmente.

### Handler necessário no servidor

```python
async def handle_request_fragment_backup(
    msg,
    writer,
    coordinator,
):
    fragment_id = msg["fragment_id"]
    data = coordinator.get_fragment_bytes(fragment_id)

    if data is None:
        await send_message(writer, FragmentNotFound(...).to_dict())
        return

    await send_message(
        writer,
        FragmentBackup(
            fragment_id=fragment_id,
            data=data,
        ).to_dict(),
    )
```

---

## 3.5. Melhorias futuras

Depois do MVP:

* checksum SHA-256;
* tamanho máximo de payload;
* escrita atômica;
* tentativas com backoff;
* download em chunks;
* retomada de download;
* timeout proporcional ao tamanho do fragmento;
* priorização de peers com menor latência;
* lista de fontes por fragmento.

---

# 4. `peer_inner_protocol.py`

## 4.1. Responsabilidade

Esse arquivo define eventos que circulam somente dentro do processo do peer.

Eles não são mensagens de rede.

O objetivo é desacoplar componentes como:

* `DataThread`;
* `TrainerNode`;
* logs;
* métricas;
* interface;
* lógica de promoção do pupilo.

---

## 4.2. Eventos definidos

### `FragmentAcquired`

Indica que um fragmento passou a existir localmente.

Campos:

```python
fragment_id
node_id
source
timestamp
```

Valores esperados para `source`:

```text
local
peer
server_backup
```

---

### `FragmentAssemblyFailed`

Indica que não foi possível obter um fragmento.

Campos:

```python
fragment_id
node_id
reason
timestamp
```

---

### `TrainingStarted`

Indica o início de um treinamento.

Campos:

```python
task_id
node_id
fragment_ids
timestamp
```

---

### `TrainingFinished`

Indica que um treinamento terminou com sucesso.

Campos:

```python
task_id
node_id
metrics
timestamp
```

---

### `TrainingFailed`

Indica uma falha durante o treinamento.

Campos:

```python
task_id
node_id
error
timestamp
```

---

### `BestModelUpdatedLocally`

Indica que o resultado superou o melhor modelo local do peer.

Isso não significa que o peer tenha superado o melhor modelo global.

Campos:

```python
task_id
node_id
score
timestamp
```

---

### `PupilPromoted`

Indica que o peer pupilo foi promovido.

Campos:

```python
coordinator
timestamp
```

Esse evento carrega um objeto `Coordinator`, o que é aceitável para comunicação interna, mas ele não é serializável para rede ou persistência.

---

## 4.3. `InternalEventBus`

A classe implementa um pequeno publish/subscribe local:

```python
class InternalEventBus
```

### Métodos

```python
subscribe(event_type, callback)
unsubscribe(event_type, callback)
publish(event)
drain_history()
```

### Funcionamento

Os assinantes são armazenados por tipo:

```python
{
    "TrainingStarted": [callback_1, callback_2],
    "TrainingFinished": [callback_3],
}
```

Ao publicar um evento:

1. o evento é colocado no histórico;
2. os callbacks são copiados sob um lock;
3. cada callback é executado;
4. erros de callbacks são registrados, mas não interrompem os demais.

### Limitações

* Os callbacks são síncronos.
* Um callback lento bloqueia o publicador.
* O histórico cresce indefinidamente enquanto `drain_history()` não for chamado.
* Não há suporte direto a callbacks `async`.
* O tipo do evento é validado apenas em tempo de execução.

### Uso recomendado no MVP

Utilizar o barramento somente para:

* logs;
* testes;
* métricas internas;
* atualização de uma interface.

Não deve ser usado como parte necessária do fluxo de treinamento.

---

# 5. `peer_messenger.py`

## 5.1. Responsabilidade

O `PeerMessenger` é a ponte entre o peer e o servidor.

Ele possui dois lados:

### Entrada

Recebe mensagens através do `P2PNode` e encaminha para o `TrainerNode`.

### Saída

Recebe mensagens dos componentes internos por uma fila thread-safe e as envia ao servidor.

---

## 5.2. Construtor

```python
PeerMessenger(
    node_id,
    server_ip,
    server_port,
    loop,
)
```

Mantém:

```python
self.trainer
self._outbound_queue
self._outbound_thread
self._stop_event
```

---

## 5.3. Ligação com o trainer

```python
def attach_trainer(self, trainer)
```

Esse método deve ser chamado antes que uma tarefa chegue.

Caso contrário, o messenger confirma o recebimento da tarefa, mas registra:

```text
TrainingTask recebida sem trainer
```

Isso significa que o servidor pode acreditar que a tarefa foi aceita mesmo que ela seja descartada.

### Correção recomendada

Verificar o trainer antes de enviar o ACK:

```python
if self.trainer is None:
    await send_message(
        writer,
        ErrorMsg(
            code="TRAINER_NOT_READY",
            detail="Trainer não inicializado",
        ).to_dict(),
    )
    return
```

---

## 5.4. Handlers de entrada

### `TrainingTask`

Registrado com:

```python
p2p_node.register_handler(
    MSG_TRAINING_TASK,
    self._handle_training_task,
)
```

O handler:

1. envia um `Ack`;
2. cria uma nova task assíncrona;
3. chama `trainer.handle_training_task(msg)`.

O treinamento não bloqueia a conexão TCP do servidor.

### `RequestBestModel`

Registrado com:

```python
p2p_node.register_handler(
    MSG_REQUEST_BEST,
    self._handle_request_best_model,
)
```

O handler confirma a mensagem e pede ao trainer que envie o melhor modelo local.

### Ambiguidade de protocolo

O mesmo tipo `RequestBestModel` é usado em sentidos diferentes:

* no peer: servidor pede ao peer seu melhor modelo local;
* no servidor: peer pede ao servidor o melhor modelo global.
  Isso deve ser separado em dois contratos:

```text
RequestLocalBestModel
RequestGlobalBestModel
```

---

## 5.5. Fila de saída

### `send(message)`

Adiciona uma mensagem à fila:

```python
self._outbound_queue.put(message)
```

### `start()`

Cria uma thread daemon chamada:

```text
peer-messenger-outbound
```

### `_outbound_worker()`

A thread:

1. retira mensagens da fila;
2. agenda `_send_async()` no event loop principal;
3. aguarda o resultado por até 15 segundos;
4. marca o item como concluído.

### `_send_async()`

Usa:

```python
send_once(
    server_ip,
    server_port,
    message,
    expect_reply=True,
)
```

---

## 5.6. Problemas atuais

### Qualquer resposta é considerada confirmação

O código rejeita apenas:

* ausência de resposta;
* resposta com `"type": "Error"`.

Não valida:

```python
reply["type"] == "Ack"
reply["ref_type"] == message["type"]
reply["ref_id"] == task_id
```

### Mensagens perdidas

Se o envio falhar:

* a mensagem não volta para a fila;
* não há retry;
* não há dead-letter queue;
* o servidor pode nunca receber o resultado.

### Endereço do servidor é fixo

`server_ip` e `server_port` são definidos no construtor.

Na promoção de um novo coordenador, seria necessário atualizar:

* `PeerMessenger`;
* `DataThread`;
* Pub/Sub;
* KeepAlive;
* demais componentes.

### Melhoria mínima

Adicionar:

```python
def update_server_address(
    self,
    ip: str,
    port: int,
) -> None:
```

E implementar retry limitado para `TaskResult`.

---

# 6. `peer_outer_protocol.py`

## 6.1. Responsabilidade

Esse módulo implementa o lado servidor da transferência P2P.

Quando outro peer solicita um fragmento, esse arquivo:

1. verifica se o arquivo existe;
2. lê os bytes;
3. envia `FragmentData`;
4. ou responde `FragmentNotFound`.

---

## 6.2. `handle_request_fragment()`

Assinatura:

```python
async def handle_request_fragment(
    msg,
    writer,
    storage_dir,
)
```

A mensagem esperada contém:

```python
{
    "type": "RequestFragment",
    "id_node": "...",
    "fragment_id": "...",
}
```

O arquivo procurado é:

```text
<storage_dir>/<fragment_id>.bin
```

Se existir:

```python
FragmentData(
    id_node=requester,
    fragment_id=fragment_id,
    data=data,
)
```

Caso contrário:

```python
FragmentNotFound(
    id_node=requester,
    fragment_id=fragment_id,
)
```

---

## 6.3. Registro no `P2PNode`

A função:

```python
register_peer_peer_handlers(
    p2p_node,
    storage_dir,
)
```

registra o handler para:

```python
MSG_REQUEST_FRAGMENT
```

Ela deve ser chamada antes de `P2PNode.start()`.

---

## 6.4. Irregularidade de nome

O arquivo se chama:

```text
peer_outer_protocol.py
```

Mas internamente usa:

```python
log = get_logger("peer_peer_protocol")
```

E a função se chama:

```python
register_peer_peer_handlers()
```

Isso indica que o módulo foi pensado como `peer_peer_protocol.py`.

### Nome recomendado

```text
peer_fragment_protocol.py
```

Ou:

```text
peer_peer_protocol.py
```

O importante é adotar um único nome nos imports, logs e documentação.

---

## 6.5. Limitações

* Carrega o fragmento inteiro na memória.
* Não há checksum.
* Não há controle de tamanho.
* Não há autenticação do solicitante.
* Não há envio em chunks.
* Não há limitação de downloads simultâneos.
* Não informa ao servidor que um peer transferiu uma réplica.

Para o MVP, o envio completo em uma única mensagem é aceitável, desde que os fragmentos sejam pequenos.

---

# 7. `trainer.py`

## 7.1. Responsabilidade

O `TrainerNode` coordena o ciclo de uma tarefa dentro do peer:

1. recebe `TrainingTask`;
2. valida o ID da tarefa;
3. garante os fragmentos;
4. carrega o dataset;
5. divide treino e teste;
6. cria e treina o modelo;
7. calcula as métricas;
8. atualiza o melhor modelo local;
9. envia o resultado ao servidor.

O arquivo também contém lógica de:

* envio de melhor modelo;
* réplica de estado;
* promoção do peer pupilo.

Essas últimas responsabilidades deveriam estar em componentes separados.

---

## 7.2. Construtor

```python
TrainerNode(
    node_id,
    messenger,
    data_thread,
    dataset_loader,
    maekawa_mutex,
    event_bus=None,
)
```

### Dependências

| Dependência      | Função                          |
| ---------------- | ------------------------------- |
| `messenger`      | Envio de resultados ao servidor |
| `data_thread`    | Aquisição dos fragmentos        |
| `dataset_loader` | Leitura e montagem de `X` e `y` |
| `maekawa_mutex`  | Exclusão mútua distribuída      |
| `event_bus`      | Eventos internos                |

### Estado local

```python
self.best_model
self.best_score
```

Estado de backup:

```python
self.replica_global_table
self.replica_queue
self.replica_best_model
```

---

## 7.3. Recebimento de tarefa

### Método

```python
async def handle_training_task(task)
```

Primeiro valida:

```python
task["task_id"]
```

Caso seja inválido, envia um resultado com erro.

Depois determina os fragmentos:

```python
task.get("dataset_fragmentos")
```

Caso a tarefa não contenha fragmentos, usa:

```python
self.data_thread.fragment_id
```

Em seguida chama:

```python
await self.data_thread.assemble_many(fragment_ids)
```

---

## 7.4. Execução fora do event loop

O treinamento é bloqueante. Para não interromper a rede, ele roda em executor:

```python
metrics, is_new_best = await loop.run_in_executor(
    None,
    self._train_task,
    task,
    fragment_ids,
    task_id,
)
```

Essa é uma decisão correta para impedir que `model.fit()` bloqueie o event loop.

### Limitação

O executor padrão pode executar mais de um treinamento simultaneamente no mesmo peer caso o servidor envie várias tarefas.

É necessário controlar a concorrência com:

```python
self._training_lock = asyncio.Lock()
```

ou:

```python
self._training_semaphore = asyncio.Semaphore(max_parallel_tasks)
```

Para o primeiro MVP, usar uma tarefa por peer.

---

## 7.5. Carregamento do dataset

O trainer espera uma dependência com esta interface:

```python
X, y = self.dataset_loader.load(fragment_ids)
```

Entretanto, nenhum `dataset_loader.py` foi fornecido.

### Componente necessário

Criar:

```text
peer/
└── dataset_loader.py
```

Exemplo de interface:

```python
class DatasetLoader:
    def __init__(self, storage_dir: str):
        self.storage_dir = storage_dir

    def load(
        self,
        fragment_ids: list[str],
    ):
        ...
        return X, y
```

### Formato recomendado dos fragmentos

Cada fragmento deve armazenar:

```python
{
    "X": X_fragment,
    "y": y_fragment,
}
```

O loader deve concatenar os fragmentos:

```python
X = np.concatenate(all_X)
y = np.concatenate(all_y)
```

---

## 7.6. Treinamento

O método `_train_task()`:

1. carrega `X` e `y`;
2. executa `train_test_split`;
3. cria o modelo usando `get_model`;
4. chama `fit`;
5. gera predições;
6. calcula métricas;
7. compara o F1 com o melhor score local.

Código conceitual:

```python
model = get_model(
    task["model_type"],
    task["parametros"],
)

model.fit(X_train, y_train)

metrics = evaluate(
    y_test,
    model.predict(X_test),
    model.predict_proba(X_test),
)
```

---

## 7.7. Melhor modelo local

O trainer mantém:

```python
self.best_score
self.best_model
```

Se:

```python
metrics["f1"] > self.best_score
```

ele atualiza o recorde local e emite:

```python
BestModelUpdatedLocally
```

### Problema conceitual

O melhor modelo local não necessariamente supera o melhor modelo global.

Atualmente, Maekawa é solicitado quando o peer supera seu próprio recorde:

```python
if is_new_best:
    await self.maekawa_mutex.request_access()
```

Isso não é suficiente para decidir se o resultado deve entrar na região crítica global.

O peer deve comparar o resultado com o último `global_best_score` recebido pelo Pub/Sub. Mesmo assim, o servidor precisa repetir a comparação atomicamente.

---

## 7.8. Envio de resultado

O método `_send_result()` cria:

```python
{
    "type": "TaskResult",
    "task_id": ...,
    "id_node": ...,
    "accuracy": ...,
    "precision": ...,
    "recall": ...,
    "f1_score": ...,
    "roc_auc": ...,
    "error": ...,
}
```

E envia usando:

```python
self.messenger.send(message)
```

### Problemas

* Não envia `tempo_treino_s`.
* Não envia status explícito.
* Não envia `attempt`.
* Em caso de erro, métricas ficam `None`.
* O servidor tenta converter algumas dessas métricas para `float`.
* O modelo treinado não acompanha o resultado.
* Não existe idempotência para resultados duplicados.

### Contrato recomendado

```python
{
    "type": "TaskResult",
    "task_id": "...",
    "node_id": "...",
    "attempt": 1,
    "status": "success",
    "metrics": {
        "accuracy": 0.91,
        "precision": 0.90,
        "recall": 0.89,
        "f1_score": 0.895,
        "roc_auc": 0.94,
    },
    "training_time_s": 8.42,
    "model_bytes": b"...",
    "error": None,
}
```

Em caso de falha:

```python
{
    "status": "failed",
    "metrics": None,
    "model_bytes": None,
    "error": {
        "code": "DATASET_UNAVAILABLE",
        "detail": "...",
    },
}
```

---

## 7.9. Envio do melhor modelo local

O método:

```python
handle_request_best_model()
```

serializa:

```python
pickle.dumps(self.best_model)
```

E envia:

```python
{
    "type": "SendBestModel",
    "id_node": ...,
    "model_bytes": ...,
    "metricas": {
        "f1": self.best_score,
    },
}
```

### Problema atual

O servidor não registra handler para `SendBestModel`.

### Estratégia mais simples

Para o MVP, incluir o modelo em `TaskResult`.

Posteriormente, otimizar:

```text
1. Peer envia apenas métricas.
2. Servidor decide se é novo recorde.
3. Servidor envia RequestLocalBestModel.
4. Peer responde SendBestModel.
```

---

## 7.10. Estado do pupilo

### `handle_sync_state()`

Espera campos:

```python
global_table_snapshot
task_queue_snapshot
best_model_metrics
```

E armazena cópias locais.

### Problema atual

O `PeerMessenger` não registra um handler para `SyncState`. Portanto, esse método nunca será chamado pela rede na configuração atual.

---

## 7.11. Promoção para servidor

O método:

```python
promote_to_server()
```

tenta:

1. criar uma nova `GlobalTable`;
2. restaurar os nós;
3. construir um novo `Coordinator`;
4. restaurar fila e melhor modelo;
5. emitir `PupilPromoted`.

### Problemas

* Import incorreto:

```python
from peer.peer_inner_protocol import PupilPromoted
```

* Argumento incorreto:

```python
GlobalTable=tabela_recuperada
```

O construtor espera:

```python
global_table=tabela_recuperada
```

* O snapshot inteiro é atribuído apenas a `nodes`.
* Fila e melhor modelo são colocados no `Coordinator`, embora estejam na tabela.
* Não cria nem inicia um `ServerMessenger`.
* Não atualiza os outros peers.
* Não muda o endereço do servidor nos componentes locais.
* Mistura treinamento e recuperação de servidor no mesmo objeto.

### Refatoração recomendada

Remover do `TrainerNode`:

```python
handle_sync_state()
promote_to_server()
```

Criar:

```text
peer/
├── pupil_manager.py
└── leader_manager.py
```

O trainer deve cuidar somente de treinamento.

---

# 8. Dependências entre os módulos

## 8.1. Grafo simplificado

```text
peer_inner_protocol
       ▲       ▲
       │       │
 data_thread  trainer
       ▲       ▲
       │       │
download_worker
               │
         peer_messenger
               │
            P2PNode
               │
      peer_outer_protocol
```

## 8.2. Dependências externas

```text
DataThread
├── core.network
├── utils.protocol
├── download_worker
└── peer_inner_protocol

DownloadWorker
├── core.network
└── utils.protocol

PeerMessenger
├── core.network
└── utils.protocol

PeerOuterProtocol
├── core.network
└── utils.protocol

TrainerNode
├── ml.models
├── ml.evaluator
├── DataThread
├── PeerMessenger
├── DatasetLoader
├── MaekawaMutex
├── Coordinator
├── GlobalTable
└── peer_inner_protocol
```

O `TrainerNode` atualmente depende do código do servidor devido à lógica de promoção. Essa dependência deve ser removida.

---

# 9. Fluxo pretendido de inicialização do peer

Atualmente não há um ponto de composição completo ligando todos os componentes.

A inicialização deveria seguir esta ordem:

```python
async def start_peer(config):
    node_id = generate_node_id(
        config.host,
        config.port,
    )

    storage_dir = (
        f"data/nodes/{node_id}/fragments"
    )

    event_bus = InternalEventBus()

    p2p_node = P2PNode(
        host=config.host,
        port=config.port,
        node_id=node_id,
    )

    data_thread = DataThread(
        node_id=node_id,
        ip=config.advertised_ip,
        listen_port=config.port,
        server_ip=config.server_ip,
        server_port=config.server_port,
        storage_dir=storage_dir,
        event_bus=event_bus,
    )

    messenger = PeerMessenger(
        node_id=node_id,
        server_ip=config.server_ip,
        server_port=config.server_port,
        loop=asyncio.get_running_loop(),
    )

    dataset_loader = DatasetLoader(
        storage_dir=storage_dir,
    )

    trainer = TrainerNode(
        node_id=node_id,
        messenger=messenger,
        data_thread=data_thread,
        dataset_loader=dataset_loader,
        maekawa_mutex=NoOpMutex(),
        event_bus=event_bus,
    )

    messenger.attach_trainer(trainer)

    messenger.register_handlers(p2p_node)

    register_peer_peer_handlers(
        p2p_node,
        storage_dir,
    )

    p2p_task = asyncio.create_task(
        p2p_node.start()
    )

    messenger.start()

    join_response = await data_thread.join_network()

    if join_response is None:
        raise RuntimeError(
            "Não foi possível entrar na rede"
        )

    initial_task = join_response.get("task")
    if initial_task:
        await trainer.handle_training_task(
            initial_task
        )

    await p2p_task
```

Para o MVP, `NoOpMutex` pode ser:

```python
class NoOpMutex:
    async def request_access(self):
        return None

    async def release_access(self):
        return None
```

Assim, a integração pode funcionar antes da correção completa de Maekawa.

---

# 10. Matriz de integração Peer ↔ Server

| Mensagem                | Origem   | Destino  | Peer                         | Servidor                          | Situação                      |
| ----------------------- | -------- | -------- | ---------------------------- | --------------------------------- | ----------------------------- |
| `JoinNetwork`           | Peer     | Servidor | Implementado                 | Implementado                      | Resposta incompatível         |
| `JoinAck`               | Servidor | Peer     | Esperado                     | Não existe de fato                | Bloqueador                    |
| `DatasetReady`          | Peer     | Servidor | Implementado                 | Handler ausente                   | Bloqueador                    |
| `TrainingTask`          | Servidor | Peer     | Handler implementado         | Não há envio real completo        | Bloqueador                    |
| `Ack TrainingTask`      | Peer     | Servidor | Implementado                 | Pode ser recebido por `send_once` | Parcial                       |
| `TaskResult`            | Peer     | Servidor | Implementado                 | Handler implementado              | Schema incompatível em falhas |
| `RequestFragment`       | Peer     | Peer     | Implementado                 | Não se aplica                     | Funcional isoladamente        |
| `FragmentData`          | Peer     | Peer     | Implementado                 | Não se aplica                     | Funcional isoladamente        |
| `FragmentNotFound`      | Peer     | Peer     | Implementado                 | Não se aplica                     | Funcional isoladamente        |
| `RequestFragmentBackup` | Peer     | Servidor | Implementado                 | Handler ausente                   | Bloqueador                    |
| `FragmentBackup`        | Servidor | Peer     | Esperado                     | Handler ausente                   | Bloqueador                    |
| `RequestBestModel`      | Ambos    | Ambos    | Implementado com ambiguidade | Implementado com ambiguidade      | Precisa separar               |
| `SendBestModel`         | Peer     | Servidor | Implementado                 | Handler ausente                   | Não funcional                 |
| `SyncState`             | Servidor | Pupilo   | Método no trainer            | Envio parcial                     | Handler peer ausente          |
| `PubSubNotify`          | Broker   | Peer     | Fora de `peer/`              | Broker parcial                    | Não integrado                 |
| `Maekawa*`              | Peer     | Peer     | Fora de `peer/`              | Não se aplica                     | Não registrado                |
| `Bully*`                | Peer     | Peer     | Fora de `peer/`              | Não se aplica                     | Não registrado                |

Os handlers atualmente registrados no servidor confirmam que `DatasetReady`, `RequestFragmentBackup` e `SendBestModel` ainda não possuem tratamento.

---

# 11. Próximos passos para integrar o Peer com o Server

## Etapa 1 — Corrigir os contratos

Alterar primeiro:

```text
utils/protocol.py
```

Criar contratos claros:

```text
JoinNetwork
JoinAck
DatasetReady
TrainingTask
TaskAccepted
TaskResult
RequestFragment
FragmentData
FragmentNotFound
RequestFragmentBackup
FragmentBackup
RequestLocalBestModel
SendLocalBestModel
RequestGlobalBestModel
SendGlobalBestModel
```

Padronizar nomes:

```text
node_id
port
fragment_ids
metrics
training_time_s
model_bytes
```

Não alternar entre:

```text
id_node / node_id
porta / port
f1 / f1_score
```

---

## Etapa 2 — Corrigir o join

O servidor deve responder com:

```python
{
    "type": "JoinAck",
    "node_id": node_id,
    "peers": [...],
    "required_fragment_ids": [...],
    "fragment_sources": {...},
    "task": None,
}
```

O peer deve:

1. validar o tipo;
2. armazenar o ID definitivo;
3. atualizar a lista de peers;
4. armazenar os fragmentos necessários;
5. iniciar o download.

O servidor não deve declarar que o peer possui um fragmento antes do recebimento de `DatasetReady`.

---

## Etapa 3 — Implementar os handlers de dataset no servidor

Adicionar ao `server_peer_protocol.py`:

```text
handle_dataset_ready
handle_request_fragment_backup
```

Registrar:

```python
messenger.register_handler(
    MSG_DATASET_READY,
    ...,
)

messenger.register_handler(
    MSG_REQUEST_FRAGMENT_BACKUP,
    ...,
)
```

### `DatasetReady`

Deve:

1. validar peer e fragmentos;
2. registrar a localização na `GlobalTable`;
3. marcar o peer como pronto;
4. responder com `Ack`.

### `RequestFragmentBackup`

Deve:

1. localizar os bytes no servidor;
2. responder `FragmentBackup`;
3. ou responder `FragmentNotFound`.

---

## Etapa 4 — Criar o `DatasetLoader`

Criar:

```text
hyperparalelizer/peer/dataset_loader.py
```

Ele deve:

* localizar os arquivos;
* desserializar os fragmentos;
* validar o schema;
* concatenar `X`;
* concatenar `y`;
* devolver `(X, y)`.

Sem esse componente, o `TrainerNode` não consegue executar nenhuma tarefa.

---

## Etapa 5 — Criar o ponto de inicialização do peer

O `main.py` deve ser responsável por:

1. criar o ID;
2. criar diretório próprio;
3. criar `P2PNode`;
4. criar `DataThread`;
5. criar `PeerMessenger`;
6. criar `DatasetLoader`;
7. criar `TrainerNode`;
8. ligar messenger e trainer;
9. registrar handlers;
10. iniciar o servidor P2P;
11. iniciar a fila do messenger;
12. executar o join.

A ordem é importante. O peer precisa estar escutando antes de anunciar sua porta ao servidor.

---

## Etapa 6 — Implementar o envio real de tarefas no servidor

O `Coordinator` já retira tarefas da fila, mas o servidor precisa efetivamente enviá-las.

Fluxo recomendado:

```python
task = coordinator.reserve_next_task(
    node_id,
)

reply = await send_once(
    peer_ip,
    peer_port,
    task.to_dict(),
    expect_reply=True,
)

if not valid_task_ack(reply, task.task_id):
    coordinator.requeue_task(task.task_id)
else:
    coordinator.mark_task_running(
        task.task_id
    )
```

O servidor deve enviar a tarefa somente depois de o peer estar:

```text
REGISTERED
DATASET_READY
IDLE
```

---

## Etapa 7 — Corrigir o ciclo de resultados

O servidor deve tratar separadamente:

```text
status = success
status = failed
```

Em sucesso:

1. validar o resultado;
2. comparar com o melhor modelo;
3. atualizar o estado;
4. marcar a tarefa como concluída;
5. enviar uma nova tarefa.

Em falha:

1. registrar o erro;
2. incrementar `attempt`;
3. reintroduzir na fila;
4. ou marcar como falha definitiva.

O peer deve enviar `TaskResult` com um contrato único e não métricas dispersas no nível principal.

---

## Etapa 8 — Enviar o modelo vencedor

Para a primeira integração, incluir:

```python
model_bytes
```

dentro de `TaskResult`.

Depois que o sistema estiver funcionando, migrar para o fluxo otimizado:

```text
TaskResult com métricas
        ↓
Servidor detecta novo recorde
        ↓
RequestLocalBestModel
        ↓
SendLocalBestModel
```

---

## Etapa 9 — Criar testes de integração

### Teste 1 — Join

* iniciar servidor;
* iniciar peer;
* verificar registro;
* verificar ID;
* verificar lista de fragmentos.

### Teste 2 — Backup do servidor

* iniciar servidor com fragmento;
* iniciar peer sem arquivo;
* baixar via `RequestFragmentBackup`;
* validar bytes;
* receber `DatasetReady`.

### Teste 3 — P2P

* peer A baixa do servidor;
* peer B recebe A como fonte;
* peer B baixa de A;
* verificar arquivos separados.

### Teste 4 — Treinamento

* servidor envia uma tarefa;
* peer confirma;
* peer treina;
* servidor recebe `TaskResult`.

### Teste 5 — Distribuição

* iniciar dois peers;
* criar múltiplas combinações;
* garantir que ambos recebam tarefas;
* garantir que nenhuma tarefa seja perdida.

### Teste 6 — Falha

* matar um peer durante o treinamento;
* devolver tarefa à fila;
* enviar para outro peer.

---

# 12. Ordem recomendada de implementação

Executar nesta ordem:

```text
1. Corrigir imports do trainer.py
2. Padronizar utils/protocol.py
3. Criar JoinAck
4. Corrigir DataThread.join_network()
5. Criar DatasetLoader
6. Criar handler RequestFragmentBackup
7. Criar handler DatasetReady
8. Criar inicialização completa do peer
9. Implementar envio de TrainingTask no servidor
10. Corrigir TaskResult
11. Armazenar o modelo real
12. Testar servidor + um peer
13. Testar servidor + dois peers
14. Adicionar retry e timeout
15. Integrar Pub/Sub
16. Integrar Maekawa
17. Separar e implementar PupilManager
18. Integrar Bully
```

---

# 13. Escopo do primeiro MVP

O primeiro marco funcional deve ser:

```text
Um servidor
Dois peers
Um dataset
Random Forest
Várias combinações de hiperparâmetros
Transferência de fragmentos
Execução paralela
Resultados devolvidos
Melhor modelo armazenado
```

Deixar temporariamente desabilitados:

```text
Maekawa
Bully
Peer pupilo
Pub/Sub
Conexões persistentes
XGBoost
LightGBM
```

O servidor pode usar inicialmente o lock local da `GlobalTable` para proteger a atualização do melhor modelo.

Depois que esse fluxo estiver estável, os recursos distribuídos avançados podem ser adicionados sem tentar depurar toda a arquitetura ao mesmo tempo.

---

# 14. Resultado esperado da integração

O fluxo completo deve terminar assim:

```text
Servidor inicia
    ↓
Servidor fragmenta dataset
    ↓
Servidor gera tarefas
    ↓
Peer inicia P2PNode
    ↓
Peer entra na rede
    ↓
Peer baixa fragmentos
    ↓
Peer envia DatasetReady
    ↓
Servidor envia TrainingTask
    ↓
Peer envia TaskAccepted
    ↓
Peer treina
    ↓
Peer envia TaskResult
    ↓
Servidor atualiza best_model
    ↓
Servidor envia próxima tarefa
    ↓
Todas as tarefas são concluídas
```

Quando esse ciclo funcionar, a pasta `peer/` estará efetivamente integrada ao servidor para o MVP do Hyperparalelizer.
