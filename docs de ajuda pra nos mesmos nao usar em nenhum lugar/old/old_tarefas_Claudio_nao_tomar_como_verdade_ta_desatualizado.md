# Hyperparalelizer — Levantamento de Requisitos de Integração

> Documento gerado a partir de engenharia reversa dos 7 arquivos entregues (`download_worker.py`, `peer_inner_protocol.py`, `peer_messenger.py`, `peer_outer_protocol.py`, `trainer.py`, `data_thread.py`, `server_peer_protocol.py`), cruzados com a especificação e os dois diagramas (arquitetura e sequência).
>
> Nenhum destes arquivos foi editado — este documento só descreve **o que cada um espera que exista** do lado de fora dele.

---

## TL;DR — 5 coisas para olhar primeiro

1. **`JoinNetwork` não bate**: `data_thread.py` espera que a resposta do servidor já venha com `fragment_id`, `peers` e a **primeira task**. `server_peer_protocol.py` hoje só devolve um `Ack` genérico, sem nenhum desses campos. O diagrama de sequência confirma que o comportamento esperado é o do `data_thread.py` — ou seja, é o `handle_join_network` que está incompleto.
2. **Falta o "loop" de tasks**: pelo diagrama de sequência, depois de um `TaskResult` o servidor deveria mandar **outra** `TrainingTask` na hora. `handle_task_result` hoje só manda um `Ack` de volta e não dispara a próxima task.
3. **Dois arquivos concorrentes para a mesma coisa**: a árvore do projeto tem `hyperparalelizer/peer_peer_protocol.py` (raiz) *e* `hyperparalelizer/peer/peer_outer_protocol.py`. O arquivo que vocês me passaram como `peer_outer_protocol.py` se autonomeia `"peer_peer_protocol"` no logger (`log = get_logger("peer_peer_protocol")`, mensagens `"peer-peer: ..."`). Muito provavelmente são a mesma coisa escrita duas vezes por pessoas diferentes — precisa decidir qual fica.
4. **Import quebrado em `trainer.py`**: dentro de `promote_to_server()` tem `from peer.peer_inner_protocol import PupilPromoted`, mas o import no topo do arquivo usa `from hyperparalelizer.peer.peer_inner_protocol import ...`. Um dos dois caminhos está errado (falta o prefixo `hyperparalelizer.`).
5. **`dataset_loader` não existe em lugar nenhum**: `trainer.py` recebe um `dataset_loader` no construtor e chama `dataset_loader.load(fragment_ids)`, mas não existe nenhum arquivo na árvore que pareça ser isso. Alguém precisa criar essa classe (provavelmente em `hyperparalelizer/ml/`).

Detalhamento de tudo isso mais abaixo (seção 5).

---

## 1. O que o código pressupõe sobre o `Coordinator`

Ninguém enviou `coordinator.py`, mas dois arquivos diferentes o usam de formas distintas — um constrói/reconstrói o objeto (`trainer.py`, no cenário de promoção do Pupilo), o outro só chama métodos assumindo que ele já existe (`server_peer_protocol.py`). Juntando as duas visões:

### 1.1 Construtor esperado

Só aparece uma vez, em `trainer.py → promote_to_server()`:

```python
Coordinator(
    dataset=[],
    model=None,
    GlobalTable=tabela_recuperada,   # instância de GlobalTable
    model_type="generic",
)
```

⚠️ Reparem que o parâmetro se chama `GlobalTable` (maiúsculo, igual ao nome da classe importada) — isso é osso de dar bug/confusão (`from hyperparalelizer.global_table import GlobalTable` já ocupa esse nome no módulo). Vale renomear o parâmetro real para algo como `global_table` quando o `coordinator.py` for escrito.

Também não dá pra saber com certeza se `dataset` e `model_type` fazem parte do "estado normal" do Coordinator ou são só relevantes nesse caminho de recuperação (o Pupilo promovido não tem o dataset completo nem sabe o `model_type` de verdade — por isso os valores "vazios"/genéricos aqui). Recomendo que quem escrever `coordinator.py` decida isso explicitamente e talvez torne esses parâmetros opcionais.

### 1.2 Atributos públicos esperados (setados diretamente, não via método)

```python
coordinator.task_pool   # trainer.py atribui uma lista (self.replica_queue)
coordinator.best_model  # os dois arquivos concordam: é um dict (ou None)
```

- `task_pool`: em `trainer.py` é atribuído como `novo_coordenador.task_pool = self.replica_queue`, e `self.replica_queue` vem de `msg.get("task_queue_snapshot", [])` — uma lista de dicts (provavelmente combinações de hiperparâmetros ainda não atribuídas).
- **Esse nome (`task_pool`) só aparece uma vez.** É a suposição de uma única pessoa — quando `coordinator.py` for escrito, ou o nome bate, ou alguém ajusta `trainer.py`.

⚠️ **Requisito estrutural mais importante, e que não é só nomenclatura:** para `receive_task_result(task_id, msg)` (ver 1.3) conseguir devolver "qual peer/task pertence a esse `task_id`", o Coordinator precisa manter **duas estruturas diferentes**, não uma só:
1. Uma fila/pool de combinações de hiperparâmetros **ainda não atribuídas** (isso é o `task_pool`).
2. Um mapa de tasks **em andamento**, tipo `Dict[task_id, Tuple[Peer, task_dict]]`, para conseguir resolver de quem é o resultado quando ele chega.

Isso não aparece nomeado em nenhum arquivo — é uma inferência de "para o método funcionar, essa estrutura *tem* que existir em algum lugar".

### 1.3 Métodos públicos esperados (usados em `server_peer_protocol.py`)

```python
coordinator.add_peer(peer: Peer) -> str
# Recebe um Peer(ip, port) recém-criado, gera/atribui o node_id
# (hash de ip+porta, conforme a especificação) e devolve esse id como string.

coordinator.receive_task_result(task_id: str, msg: dict) -> Optional[Tuple[Peer, dict]]
# msg é literalmente o TaskResult inteiro: 
#   {task_id, id_node, accuracy, precision, recall, f1_score, roc_auc, error}
# Devolve None se task_id é desconhecido.
# Se conhecido, devolve (peer, task) — peer precisa ter atributo `.id_node`.
# ESPERA-SE que, como efeito colateral, esse método já tenha comparado o
# resultado com o recorde atual e atualizado `coordinator.best_model` se for
# melhor — quem chama só verifica depois se aconteceu, comparando task_id:
#   is_new_best = (coordinator.best_model is not None
#                  and coordinator.best_model.get("task_id") == task_id)
```

Como o servidor roda em asyncio single-thread (diferente do peer, que usa uma thread separada para enviar), esse método não precisa se preocupar com lock próprio *desde que não faça `await` no meio da comparação+escrita* — a atomicidade "grátis" do event loop já resolve isso no lado servidor. (A exclusão mútua de Maekawa, pelo que dá pra ver, protege o lado dos **peers decidindo entre si quem manda o resultado antes**, não o servidor processando.)

### 1.4 Formato esperado de `coordinator.best_model`

Juntando os dois usos (`server_peer_protocol.py` e `trainer.py`):

```python
coordinator.best_model: Optional[dict] = {
    "task_id": str,
    "f1_score": float,
    "metrics": {
        "accuracy": float,
        "precision": float,
        "recall": float,
        "roc_auc": float,
        # possivelmente "f1_score" também, duplicado
    },
    # "model": <objeto treinado?>  <- ver bug #6 na seção 5, isso é incerto
}
```

`None` até o primeiro resultado chegar (ver `handle_request_best_model`, que trata `best is None` como "nenhum modelo ainda").

---

## 2. Outras peças que os arquivos citam mas não vieram anexadas

| Peça | Onde é usada | O que se espera dela |
|---|---|---|
| `GlobalTable` (`global_table.py`) | `trainer.py` | Construtor sem argumentos `GlobalTable()`; atributo público `.nodes` (dict, node_id → metadados). Só há um uso, baixa confiança no nome exato. |
| `Peer` (dentro de `coordinator.py`) | `server_peer_protocol.py` | `Peer(ip=str, port=int)`; ganha um atributo `.id_node` (string) em algum momento — provavelmente dentro de `add_peer`. |
| `ServerMessenger` (`server_messenger.py`) | `server_peer_protocol.py` | `.register_handler(msg_type: str, handler: async fn(msg, writer))`; atributo `.status_queue` (uma `asyncio.Queue`, usada para publicar eventos tipo `peer_joined`, `task_result`, `best_model_requested`). |
| `P2PNode` (`core/network.py`) | `peer_messenger.py`, `peer_outer_protocol.py` | Mesmo padrão do `ServerMessenger`: `.register_handler(msg_type, handler)`. Um único `P2PNode` por peer parece atender tanto mensagens vindas do servidor (`TrainingTask`, `RequestBestModel`) quanto de outros peers (`RequestFragment`) — não há listener separado para cada origem. |
| `send_once` / `send_message` (`core/network.py`) | quase todos | `send_once(ip, port, msg: dict, expect_reply=True, timeout=float) -> Optional[dict]` para conexão temporária; `send_message(writer, msg: dict)` para conexão já aberta/durável. **Precisam suportar `bytes` cru dentro do dict** (fragmentos `.bin`, modelo com `pickle.dumps`) — isso descarta um serializador JSON puro sem tratamento especial. Ver pergunta aberta na seção 6. |
| Classes de mensagem (`utils/protocol.py`) | todos | Pelo padrão já usado em `peer_inner_protocol.py` (que **está completo** e serve de referência), cada mensagem deve ser um `@dataclass` com `type: str = field(default=MSG_X, init=False)` e `.to_dict() = asdict(self)`. Classes citadas: `RequestFragment`, `RequestFragmentBackup`, `FragmentData`, `FragmentNotFound`, `JoinNetwork`, `DatasetReady`, `Ack`, `SendBestModel`, + as constantes `MSG_*` correspondentes. |
| `get_model(model_type, params)` (`ml/models.py`) | `trainer.py` | Devolve um estimador estilo sklearn: precisa de `.fit(X, y)`, `.predict(X)`, `.predict_proba(X)`. |
| `evaluate(y_test, y_pred, y_prob)` (`ml/evaluator.py`) | `trainer.py` | Devolve `dict` com chaves `accuracy`, `precision`, `recall`, `f1`, `roc_auc` (note: `f1`, não `f1_score` — o rename para `f1_score` acontece manualmente dentro de `_send_result`). |
| `dataset_loader` | `trainer.py` (parâmetro do construtor) | `.load(fragment_ids: List[str]) -> (X, y)`. **Não existe nenhum arquivo candidato na árvore do projeto** — precisa ser criado. |
| `maekawa_mutex` (`sync/maekawa.py`) | `trainer.py` | `async def request_access(self) -> None` / `async def release_access(self) -> None`, sem parâmetros — presume-se que ele já sabe quem é o peer e gerencia quórum e timestamps de Lamport internamente. |
| `sync/bully.py`, `sync/lamport.py` | citados na spec, não usados diretamente em nenhum dos 7 arquivos | A lógica de "quem detecta a queda do servidor e chama `promote_to_server()`" não está em nenhum arquivo enviado — provavelmente vive em `main.py` ou num módulo ainda não escrito que orquestra o bully. |

---

## 3. Coisas que os diagramas confirmam (e que ajudam a resolver as dúvidas acima)

- O diagrama de arquitetura junta em uma única caixa **"Data Thread"** o que no código são **três arquivos**: `data_thread.py` (orquestra), `download_worker.py` (busca em outro peer), `peer_outer_protocol.py` (serve fragmento para outro peer que pede). Isso é só uma divisão de responsabilidades, não é um bug — mas ajuda a explicar por que existem tantos arquivos pequenos.
- A caixa "Peer D (Server Pupil)" do diagrama de arquitetura é exatamente o que `trainer.py` implementa em `handle_sync_state` / `promote_to_server` — bate certinho.
- O diagrama de sequência mostra, na entrada de um peer na rede: `Server → Peer: "Envia endereços dataset + 1a Task"` — ou seja, **um único envio** já contendo fragmento(s) + primeira tarefa. Isso confirma que o formato esperado por `data_thread.py` (fragment_id + peers + task na resposta do join) é o comportamento correto, e é `handle_join_network` que ficou incompleto.
- O mesmo diagrama mostra, depois de um resultado: `Server: Atualiza best_model` → `Server → Peer: Envia outra Task de treinamento`. Isso confirma o item #2 do TL;DR: falta esse envio de nova task em `handle_task_result`.

---

## 4. Coisas que já batem certinho entre os arquivos (não mexer)

Vale destacar também o que **já está alinhado**, para não gastar tempo revisando à toa:

- `PeerMessenger.attach_trainer(trainer)` espera exatamente os dois métodos que `TrainerNode` tem: `handle_training_task(msg)` e `handle_request_best_model()`, ambos `async`. Encaixe perfeito.
- `DataThread.fragment_id`, `.assemble_many()`, `.notify_dataset_ready()` são usados por `trainer.py` exatamente como `data_thread.py` os implementa.
- O formato do `TaskResult` que `trainer._send_result` monta bate com o que `server_peer_protocol.handle_task_result` espera ler de `msg`.
- O padrão `Ack(ref_type, ref_id).to_dict()` é usado de forma consistente nos dois lados (peer e servidor).

---

## 5. Inconsistências e bugs concretos encontrados

1. **`JoinNetwork` incompleto** (detalhado no TL;DR #1). `data_thread.py`:
   ```python
   self.fragment_id = reply.get("fragment_id")
   self.known_peers = reply.get("peers", []) or []
   self.initial_task = reply.get("task")
   ```
   `server_peer_protocol.handle_join_network` hoje só devolve `Ack(ref_type=MSG_JOIN_NETWORK, ref_id=node_id)` — sem `fragment_id`, `peers` ou `task`. Além disso, o `type` de um `Ack` genérico provavelmente é `"Ack"`, mas `data_thread.py` confere contra `MSG_JOIN_ACK` (uma constante diferente) — então mesmo o "tipo" da mensagem não bate. Provavelmente precisa de uma classe de mensagem própria (`JoinNetworkAck` ou similar) em vez de reaproveitar `Ack`.

2. **Falta o disparo da próxima task** (TL;DR #2). `handle_task_result` só envia um `Ack`. Precisa, depois de atualizar o coordinator, buscar a próxima combinação de hiperparâmetros e mandar uma nova `TrainingTask` para o peer.

3. **Arquivo duplicado/mal localizado** (TL;DR #3): `peer_outer_protocol.py` (dentro de `hyperparalelizer/peer/`) se identifica internamente como `"peer_peer_protocol"`, e a árvore do projeto também lista um `hyperparalelizer/peer_peer_protocol.py` separado, na raiz do pacote. Quase certo que são duas pessoas resolvendo o mesmo problema.

4. **Import quebrado em `trainer.py`** (TL;DR #4):
   ```python
   # topo do arquivo:
   from hyperparalelizer.peer.peer_inner_protocol import (
       InternalEventBus, TrainingStarted, TrainingFinished,
       TrainingFailed, BestModelUpdatedLocally,
   )
   # dentro de promote_to_server():
   from peer.peer_inner_protocol import PupilPromoted
   ```
   `PupilPromoted` deveria estar na lista de import do topo (ela já existe em `peer_inner_protocol.py`!) em vez de reimportada com caminho diferente e provavelmente errado.

5. **`memoria_total_mb` / `memoria_disponivel_mb` são descartados**: `data_thread.py` manda esses dois campos no `JoinNetwork`, e a especificação diz explicitamente que a tabela hash deveria guardar "capacidade de memória, memória disponível, etc" por nó — mas `handle_join_network` só lê `msg["ip"]` e `msg["porta"]`, e o `Peer(ip=..., port=...)` não tem onde guardar isso. Precisa decidir onde esses dados vão parar (provavelmente `Peer` precisa ganhar esses campos).

6. **`RequestBestModel`/`SendBestModel`: o que é "o modelo", de verdade?** A especificação diz que `RequestBestModel` serve tanto para (a) um peer novo sincronizar o estado inicial, quanto para (b) o coordenador, ao final, resgatar o modelo definitivo — mas nesse segundo caso, quem realmente tem o objeto treinado (o `.pkl` do sklearn) é o **peer vencedor** (`self.best_model` dentro de `TrainerNode`), não o servidor. O servidor só guarda métricas. Só que `server_peer_protocol.handle_request_best_model` faz `pickle.dumps(best)` em cima do **dict de métricas do coordinator**, e manda isso no campo `model_bytes` — como se fosse o modelo de verdade. Se algum peer tentar dar `pickle.loads()` nisso esperando um estimador com `.predict()`, vai quebrar, porque é só um dict. Vale decidir: o servidor alguma vez guarda o modelo de verdade, ou toda solicitação de modelo final precisa ser redirecionada/repassada ao peer que o treinou?

7. **Inconsistência de nome de campo `f1` vs `f1_score`** dentro de `metricas` do `SendBestModel`: o lado peer (`trainer.handle_request_best_model`) manda `{"f1": self.best_score}`, enquanto o lado servidor (`server_peer_protocol.handle_request_best_model`) monta `metricas` a partir de `best.get("metrics", {})` (que, por 1.4, tende a ter `f1_score`, não `f1`). Quem for consumir essas mensagens vai precisar tratar os dois nomes, ou padronizar um.

8. **Falta handler para `DatasetReady`**: `data_thread.notify_dataset_ready()` manda uma mensagem `DatasetReady` esperando um `Ack` de volta, mas `register_all_handlers` em `server_peer_protocol.py` só registra `MSG_JOIN_NETWORK`, `MSG_TASK_RESULT`, `MSG_REQUEST_BEST` e `MSG_KEEP_ALIVE`. Falta escrever (e registrar) um `handle_dataset_ready`.

9. **Mistura de idioma nas chaves de mensagem**: a maioria dos campos está em inglês (`task_id`, `fragment_id`, `model_type`), mas `TrainingTask` usa `dataset_fragmentos` e `parametros` em português. Não é um bug funcional, mas vale padronizar antes de fechar `utils/protocol.py` de vez, porque é fácil alguém digitar errado depois.

---

## 6. Perguntas em aberto para o grupo decidir

- **Serialização de mensagens**: como `send_once`/`send_message` precisam trafegar `bytes` cru (fragmentos, modelo com `pickle.dumps`) dentro dos dicts, JSON puro não resolve sem um passo extra de base64. Vão usar `pickle` como formato de transporte (mais simples, mas exige confiar nos dois lados) ou JSON com bytes em base64 (mais portável, mais código)? Isso é decisão de `core/network.py` + `utils/serializer.py`.
- **Quem chama `promote_to_server()`?** O algoritmo de Bully em si (`sync/bully.py`) não aparece referenciado em nenhum dos 7 arquivos — falta o "cola" que detecta timeout de heartbeat do servidor e decide disparar a eleição/promoção.
- **Os Relógios de Lamport** estão previstos na especificação junto com Maekawa, mas `trainer.py` chama `maekawa_mutex.request_access()`/`release_access()` sem passar nenhum timestamp — o `MaekawaMutex` vai gerenciar isso internamente, ou falta integrar `sync/lamport.py` explicitamente nessas chamadas?
- **Task_pool vs. mapa de tasks em andamento**: como descrito na seção 1.2, `Coordinator` provavelmente precisa de duas estruturas internas (pendentes vs. em andamento), não só uma. Vale desenhar isso explicitamente antes de escrever `coordinator.py`.

---

## 7. Sugestão de por onde começar

Pela quantidade de peças faltando, a ordem que parece minimizar retrabalho:

1. Fechar `utils/protocol.py` primeiro (todas as mensagens + constantes `MSG_*`), porque **todo o resto** depende dele.
2. Escrever `coordinator.py` e `global_table.py` juntos, usando a seção 1 e 2 deste documento como contrato — e já resolvendo os bugs #1, #2, #5, #6, #8 nesse processo (eles são todos "responsabilidade do coordinator/servidor").
3. Resolver a duplicidade `peer_outer_protocol.py` / `peer_peer_protocol.py` (bug #3) antes de mais ninguém importar um dos dois por engano.
4. Escrever `dataset_loader` (não existe ainda) e conferir `ml/models.py` / `ml/evaluator.py`.
5. Só depois mexer em `main.py` para plugar tudo (ordem de inicialização: `PeerMessenger.attach_trainer()` → `register_handlers()` → `messenger.start()`, conforme visto em `peer_messenger.py`).

Este documento não cobre `core/network.py`, `sync/*.py`, `main.py` nem `server_messenger.py` em detalhe porque nenhum dos 7 arquivos enviados expõe o suficiente sobre eles além do que já está na seção 2 — se vocês tiverem rascunhos desses, vale eu olhar depois.
