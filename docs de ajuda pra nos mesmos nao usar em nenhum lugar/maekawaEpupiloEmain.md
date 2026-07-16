Sim, **a lista anterior precisa ser atualizada**. A `main_beta4.py` já corrige parcialmente alguns pontos que eu havia mencionado, mas também introduz problemas adicionais que explicam diretamente:

* mais de um peer aparentando ser Pupilo;
* peers não começando ou não continuando os treinamentos;
* Maekawa podendo bloquear com mais de um peer;
* tarefas sendo reservadas antes de o peer estar pronto;
* promoção por Bully podendo ocorrer no peer errado.

Abaixo está a lista completa revisada, separando o que já foi corrigido provisoriamente na main e o que ainda deve ser consertado nos módulos do projeto. A análise se baseia na `main_beta4.py` enviada. 

---

# 1. O problema de múltiplos Pupilos continua existindo

## O que a main faz atualmente

A eleição do Pupilo acontece em dois lugares.

No `status_consumer()`:

```python
if event_name == "peer_joined":
    pupil = select_pupil(runtime.coordinator)
    runtime.coordinator.pupil_peer = pupil
```

E novamente, a cada dois segundos, no `pupil_sync_service()`:

```python
pupil = select_pupil(runtime.coordinator)
runtime.coordinator.pupil_peer = pupil
```

Isso significa que dois serviços escrevem diretamente em:

```python
coordinator.pupil_peer
```

Embora ambos normalmente selecionem o mesmo peer, não existe sincronização, versão da eleição nem destituição do Pupilo anterior.

## Por que aparecem vários Pupilos

Quando entra um novo peer com `node_id` maior:

```text
Peer A entra → vira Pupilo
Peer A recebe SyncState

Peer B entra → passa a ser Pupilo
Peer B recebe SyncState
```

O Peer A:

* não recebe uma mensagem de destituição;
* mantém `replica_global_table_snapshot`;
* continua apto a chamar `promote_to_server()`;
* continua parecendo Pupilo nos logs;
* pode tentar se promover futuramente.

Além disso, o handler atual faz isto para qualquer `SyncState` recebido:

```python
trainer.handle_sync_state(msg)
```

Não há nenhuma validação de que:

```python
msg["pupil_id"] == join.node_id
```

Portanto, o papel de Pupilo não é formalmente representado.

## O que mudar na main

Remover a seleção do Pupilo de `status_consumer()`:

```python
if event_name == "peer_joined":
    print(...)
    # NÃO selecionar Pupilo aqui
```

O `pupil_sync_service()` também não deveria escolher o Pupilo diretamente. Ele deveria apenas pedir ao componente responsável para reconciliar o papel.

## O que mudar no código

Criar:

```text
hyperparalelizer/server/pupil_manager.py
```

Exemplo:

```python
class PupilManager:
    def __init__(self, coordinator: Coordinator) -> None:
        self.coordinator = coordinator
        self._pupil_id: str | None = None
        self._epoch = 0
        self._lock = asyncio.Lock()

    @property
    def pupil_id(self) -> str | None:
        return self._pupil_id

    async def reconcile(self) -> bool:
        async with self._lock:
            candidate = self._select_candidate()

            if candidate == self._pupil_id:
                return False

            previous = self._pupil_id
            self._pupil_id = candidate
            self._epoch += 1

            if previous is not None:
                await self._revoke(previous)

            if candidate is not None:
                await self._assign(candidate)

            return True
```

A `GlobalTable` deve manter:

```python
self.pupil_id: str | None = None
self.pupil_epoch: int = 0
```

O snapshot deve carregar:

```python
{
    "pupil_id": self.pupil_id,
    "pupil_epoch": self.pupil_epoch,
    ...
}
```

---

# 2. O `SyncState` transforma qualquer destinatário em Pupilo

O handler na main é:

```python
async def handle_sync_state(msg, writer):
    trainer.handle_sync_state(msg)
```

Ele não verifica:

* qual peer é o Pupilo;
* a época da eleição;
* se o snapshot é mais recente;
* se o peer foi destituído;
* se a mensagem pertence à execução atual.

## Correção necessária

O `SyncState` deve incluir:

```python
{
    "type": "SyncState",
    "snapshot_id": "...",
    "run_id": "...",
    "pupil_id": "...",
    "pupil_epoch": 3,
    "global_table_snapshot": {...},
}
```

No peer:

```python
async def handle_sync_state(
    msg: Dict[str, Any],
    writer: asyncio.StreamWriter,
) -> None:
    pupil_id = str(msg.get("pupil_id") or "")
    pupil_epoch = int(msg.get("pupil_epoch") or 0)

    if pupil_epoch < trainer.pupil_epoch:
        await send_error(...)
        return

    trainer.pupil_epoch = pupil_epoch

    if pupil_id != join.node_id:
        trainer.is_pupil = False
        trainer.replica_global_table_snapshot = None
        await send_ack(...)
        return

    trainer.is_pupil = True
    trainer.handle_sync_state(msg)

    await send_ack(...)
```

O `TrainerNode` precisa ter:

```python
self.is_pupil = False
self.pupil_epoch = 0
```

E `promote_to_server()` deve validar:

```python
if not self.is_pupil:
    raise RuntimeError("Peer não é o Pupilo ativo")
```

---

# 3. A “confirmação” do snapshot ainda não é uma confirmação real

A própria main reconhece isso:

```python
# replicate_state_to_pupil envia com expect_reply=False; não responde.
del writer
```

Entretanto, o servidor imprime:

```python
[PUPIL] Snapshot confirmado
```

quando `replicate_state_to_pupil()` retorna `True`.

Esse `True` provavelmente significa somente:

```text
a tentativa de envio não lançou exceção imediatamente
```

Não significa que o peer:

* recebeu a mensagem;
* processou o snapshot;
* persistiu a réplica;
* atualizou o quórum;
* está apto à promoção.

## Correção no `Coordinator`

O envio deve utilizar:

```python
reply = await send_once(
    pupil["ip"],
    pupil["port"],
    message,
    expect_reply=True,
    timeout=5.0,
)
```

E validar:

```python
return validate_ack(
    reply,
    expected_ref_type=MSG_SYNC_STATE,
    expected_ref_id=snapshot_id,
)
```

## Correção na main

O handler não deve ignorar o `writer`.

Substituir:

```python
del writer
```

por uma resposta:

```python
await send_message(
    writer,
    Ack(
        ref_type=MSG_SYNC_STATE,
        ref_id=snapshot_id,
    ).to_dict(),
)
```

O ACK deve ser enviado **depois** de atualizar e guardar o snapshot.

---

# 4. O Maekawa vazio foi parcialmente corrigido na main

Este item da lista anterior mudou.

A main criou:

```python
class SafeMaekawaMutex(MaekawaMutex):
    async def request_access(...):
        if not self.quorum:
            self.state = "HELD"
            return
```

Portanto, o problema específico de:

```text
quorum == [] → timeout inevitável
```

foi contornado.

## Mas a correção está no lugar errado

Ela deve ser transferida para:

```text
hyperparalelizer/sync/maekawa.py
```

A main não deveria precisar criar uma subclasse para corrigir o comportamento base.

O `MaekawaMutex` oficial deve tratar quórum vazio como acesso trivial:

```python
async def request_access(...) -> None:
    if not self.quorum:
        self.state = "HELD"
        return
```

```python
async def release_access() -> None:
    if not self.quorum:
        self.state = "RELEASED"
        return
```

Depois disso, remover da main:

```python
class SafeMaekawaMutex(...)
```

e usar diretamente:

```python
mutex = MaekawaMutex(
    node_id=join.node_id,
    quorum=join.peers,
)
```

---

# 5. A atualização do quórum está ligada incorretamente ao Pupilo

Este é um novo problema importante.

A main atualiza os peers conhecidos e o quórum apenas no handler:

```python
handle_sync_state()
```

Porém, `SyncState` deveria ser enviado apenas ao Pupilo.

Logo:

* o Pupilo recebe uma lista atualizada;
* peers comuns não recebem atualizações de membership;
* o quórum do Maekawa fica diferente em cada peer;
* o mapa do Bully fica diferente em cada peer;
* `DataThread.known_peers` fica desatualizado nos peers comuns.

Exemplo:

```text
Peer A entra primeiro
quorum A = []

Peer B entra
quorum B = [A]

Servidor envia SyncState apenas ao Pupilo B
B continua atualizado

Peer C entra
C conhece [A, B]
A talvez continue conhecendo ninguém
B talvez seja atualizado
```

Isso viola a premissa do Maekawa e do Bully.

## Correção necessária no protocolo

Criar uma mensagem separada:

```python
MSG_MEMBERSHIP_UPDATE = "MembershipUpdate"
```

Payload:

```python
{
    "type": "MembershipUpdate",
    "epoch": membership_epoch,
    "peers": [...],
}
```

O servidor deve enviar para **todos os peers** sempre que houver:

* entrada de peer;
* morte de peer;
* alteração de endereço;
* promoção de coordenador.

Cada peer atualiza:

```python
data_thread.update_known_peers(peers)
mutex.replace_quorum(peers)
bully.peers = build_bully_peer_map(peers)
```

O `SyncState` deve ficar restrito à réplica do Pupilo.

---

# 6. O `GuardedTrainerNode` corrige parcialmente o timeout, mas pode enviar resultados duplicados

A main criou:

```python
class GuardedTrainerNode(TrainerNode):
    async def handle_training_task(self, task):
        try:
            await super().handle_training_task(task)
        except MaekawaTimeoutError:
            self._send_result(...)
```

Isso é melhor do que deixar a exceção destruir silenciosamente a task.

Entretanto, há risco de duplicação caso o `TrainerNode` base:

1. envie o resultado;
2. lance uma exceção posteriormente;
3. a subclasse envie outro resultado no `except`.

Também existe o risco de o `TrainerNode` base já ter emitido um evento de falha e a subclasse emitir outro `TaskResult`.

## Correção correta

A proteção deve ser implementada dentro de `TrainerNode`, onde é possível saber precisamente se o resultado já foi emitido.

Exemplo:

```python
result_sent = False

try:
    ...
    self._send_result(...)
    result_sent = True

except MaekawaTimeoutError as exc:
    if not result_sent:
        self._send_result(
            task,
            metrics=None,
            model_bytes=None,
            error="maekawa_timeout",
        )

except Exception as exc:
    if not result_sent:
        self._send_result(
            task,
            metrics=None,
            model_bytes=None,
            error=str(exc),
        )

finally:
    if self.maekawa_mutex.state == "HELD":
        await self.maekawa_mutex.release_access()
```

Depois remover da main:

```python
GuardedTrainerNode
```

---

# 7. A main ainda não impede o peer de aceitar mais de uma tarefa simultaneamente

A main protege o despacho no servidor, mas não protege o recebimento no peer.

O `PeerMessenger` continua sendo o handler das novas `TrainingTask`:

```python
messenger.register_handlers(p2p_node)
```

O `GuardedTrainerNode` não possui:

* lock por treinamento;
* `current_task`;
* verificação de peer ocupado;
* deduplicação por `task_id`.

Assim, ainda é possível ocorrer:

```text
TrainingTask A chega
→ cria asyncio task A

TrainingTask B chega antes de A terminar
→ cria asyncio task B
```

Isso pode:

* misturar `best_score`;
* fazer duas operações Maekawa concorrentes no mesmo objeto;
* carregar e treinar o dataset duas vezes;
* enviar resultados fora de ordem;
* causar aparência de que “não está treinando”, devido a deadlocks ou excesso de trabalho.

## Correção em `TrainerNode`

Adicionar:

```python
self._training_lock = asyncio.Lock()
self._current_task_id: str | None = None
self._processed_task_ids: set[str] = set()
```

Criar:

```python
async def try_submit_task(
    self,
    task: Dict[str, Any],
) -> bool:
    task_id = str(task.get("task_id") or "")

    if not task_id:
        return False

    if task_id in self._processed_task_ids:
        return False

    if self._training_lock.locked():
        return False

    asyncio.create_task(self._run_guarded(task))
    return True
```

E:

```python
async def _run_guarded(self, task):
    async with self._training_lock:
        task_id = task["task_id"]
        self._current_task_id = task_id

        try:
            await self.handle_training_task(task)
            self._processed_task_ids.add(task_id)
        finally:
            self._current_task_id = None
```

## Correção em `PeerMessenger`

O ACK só deve ser enviado depois de a tarefa ser aceita:

```python
accepted = await self.trainer.try_submit_task(msg)

if not accepted:
    await send_message(
        writer,
        ErrorMessage(
            code="PEER_BUSY",
            ref_id=task_id,
        ).to_dict(),
    )
    return

await send_message(
    writer,
    Ack(
        ref_type=MSG_TRAINING_TASK,
        ref_id=task_id,
    ).to_dict(),
)
```

---

# 8. O guard de despacho na main é apenas parcial

A main criou:

```python
install_dispatch_guard(coordinator)
```

Ela utiliza um lock por peer e verifica se já existe uma tarefa atribuída.

Isso reduz bastante a corrida entre:

* `handle_task_result()`;
* `scheduler_service()`.

Portanto, este item da lista anterior foi parcialmente corrigido.

## O que ainda está errado

A lógica continua sendo aplicada por monkey patch:

```python
coordinator.dispatch_next_task = guarded_dispatch
```

Isso é frágil porque:

* outros métodos podem reservar tarefas sem usar esse wrapper;
* uma nova instância do Coordinator pode esquecer de instalar o patch;
* testes do Coordinator não cobrem o comportamento real;
* o check e a reserva ainda não formam necessariamente uma operação atômica.

## Correção definitiva

Mover a lógica para `GlobalTable`:

```python
def reserve_next_task_for_peer(
    self,
    peer: Peer,
) -> TrainingTask | None:
    with self.lock:
        if self._peer_has_assigned_task_locked(peer.id_node):
            return None

        if not self.task_pool:
            return None

        task = self.task_pool.pop(0)

        self.assigned_tasks[task.task_id] = {
            "peer": peer,
            "task": task,
            "timestamp": time.monotonic(),
        }

        return task
```

O `Coordinator.dispatch_next_task()` usa somente esse método.

Depois remover da main:

```python
install_dispatch_guard()
```

---

# 9. Ainda existem duas fontes de despacho

A main tem:

```python
await runtime.coordinator.dispatch_all_idle()
```

dentro do scheduler.

Pelo comportamento analisado anteriormente, o handler de `TaskResult` também agenda:

```python
coordinator.dispatch_next_task(peer)
```

O guard reduz a possibilidade de reserva dupla, mas não elimina a duplicidade arquitetural.

## Correção recomendada

Escolher apenas um fluxo.

A opção mais limpa é:

* `TaskResult` apenas libera o peer e registra o resultado;
* o scheduler distribui a próxima tarefa.

Remover de `server_peer_protocol.py`:

```python
asyncio.create_task(
    coordinator.dispatch_next_task(peer)
)
```

Depois disso, o scheduler será a única fonte de redistribuição.

---

# 10. Existe uma condição de corrida grave no bootstrap do peer

Este é um dos pontos mais importantes para explicar por que os peers não treinam.

A ordem atual é:

```python
join_reply = await data_thread.join_network(...)
```

Somente muito depois:

```python
p2p_task = create_task(peer_tasks, p2p_node.start(), ...)
await wait_for_listener(...)
```

Ou seja:

1. o peer envia `JoinNetwork`;
2. o servidor registra o peer;
3. o servidor pode começar a enviar mensagens para ele;
4. mas a porta P2P do peer ainda não está aberta.

Além disso, no caso de tarefa inicial, ela é reservada no `JoinAck`, mas o peer ainda precisa:

* criar loader;
* criar mutex;
* criar messenger;
* criar trainer;
* registrar handlers;
* iniciar listener;
* montar os fragmentos.

Durante esse intervalo, o servidor já enxerga o peer como existente.

## Correção arquitetural

O join deve ser dividido em duas fases:

```text
JoinNetwork
→ servidor fornece node_id, fragment_id e peers

peer sobe listener e componentes

PeerReady
→ servidor marca peer como apto a receber tarefas
```

Adicionar à `GlobalTable`:

```python
node["ready"] = False
```

Quando receber `PeerReady`:

```python
node["ready"] = True
```

`get_idle_peers()` deve retornar apenas:

```python
peer.ready is True
```

## Correção temporária na main

A tarefa inicial só funciona porque veio no `JoinAck` e é executada localmente depois do listener subir. Porém o servidor não deveria considerar o peer ocioso até receber `PeerReady`.

Não é correto resolver isso com mais `sleep()`.

---

# 11. O modo `--min-peers` contém um problema no estado do progresso

Quando:

```python
args.min_peers > 1
```

o grid só é criado dentro de:

```python
wait_for_minimum_peers()
```

Isso está correto para impedir que os primeiros peers recebam tarefas prematuramente.

Mas existe outro problema: os peers entram antes do grid e podem ser tratados como Pupilo antes de estarem prontos. O serviço:

```python
pupil_sync_service()
```

já está ativo e pode replicar um snapshot incompleto.

## Correção

O serviço de Pupilo deve aguardar:

```python
await runtime.job_started.wait()
```

ou, melhor, a eleição do Pupilo deve exigir peers `ready`.

Exemplo:

```python
async def pupil_sync_service(runtime, interval=2.0):
    await runtime.job_started.wait()

    while not runtime.stop_event.is_set():
        ...
```

A seleção também deve filtrar:

```python
nodes = [
    node
    for node in table.get_all_nodes()
    if node.get("ready") is True
]
```

---

# 12. O progresso nunca registra falhas definitivas

`ServerProgress` possui:

```python
failed_task_ids
```

Porém, no `status_consumer()`, quando há falha:

```python
retry = runtime.progress.register_retry(task_id)
```

A tarefa é sempre considerada retry.

Não existe:

* limite de tentativas;
* inserção em `failed_task_ids`;
* critério de falha definitiva.

Logo:

```python
completed + failed == total_tasks
```

pode nunca ficar verdadeiro quando uma tarefa falha permanentemente.

Isso também pode dar a impressão de que o sistema parou de treinar.

## Correção

Adicionar:

```python
--max-task-retries
```

No `ServerProgress`:

```python
def register_failure_or_retry(
    self,
    task_id: str,
    max_retries: int,
) -> bool:
    retries = self.register_retry(task_id)

    if retries > max_retries:
        self.failed_task_ids.add(task_id)
        return False

    return True
```

O `Coordinator` também não deve reenfileirar uma tarefa marcada como falha definitiva.

---

# 13. A tarefa inicial não usa o mesmo caminho das tarefas normais

A tarefa inicial é executada assim:

```python
create_task(
    peer_tasks,
    trainer.handle_training_task(join.initial_task),
    "peer-initial-training-task",
)
```

Já tarefas posteriores passam pelo `PeerMessenger`.

Isso gera dois caminhos diferentes:

```text
Tarefa inicial:
main → trainer.handle_training_task()

Tarefas seguintes:
P2PNode → PeerMessenger → trainer
```

Consequências:

* o lock de aceitação pode ser ignorado;
* validações podem ser diferentes;
* deduplicação pode ser ignorada;
* ACK de aceitação não existe para tarefa inicial;
* observabilidade fica diferente.

## Correção

O `TrainerNode` deve expor um único método:

```python
await trainer.submit_task(task, source="join_ack")
```

Tanto a main quanto o messenger usam esse método.

Na main:

```python
accepted = await trainer.submit_task(
    join.initial_task,
    source="join_ack",
)

if not accepted:
    raise BootstrapError(
        "Tarefa inicial não foi aceita pelo Trainer"
    )
```

---

# 14. O Pupilo e o Bully estão escolhendo líderes com critérios independentes

O Pupilo usa:

```python
max(nodes, key=node_id)
```

O Bully também normalmente favorece o maior ID, mas cada peer possui um mapa de peers potencialmente diferente.

Além disso, **todo peer** recebe:

```python
promote_callback=request_promotion
```

Quando um peer acredita que venceu, ele chama:

```python
promoted_server_from_peer(...)
```

O método verifica apenas:

```python
if not trainer.replica_global_table_snapshot:
    aborta
```

Ele não verifica se o peer é o Pupilo atual.

Um antigo Pupilo que ainda guarda snapshot pode se promover.

## Correção

Antes da promoção:

```python
if not trainer.is_pupil:
    print(
        "[PROMOTION ERROR] Peer venceu o Bully, "
        "mas não é o Pupilo ativo."
    )
    return
```

Também validar o epoch:

```python
if trainer.replica_pupil_epoch != trainer.pupil_epoch:
    abort
```

Idealmente:

```text
Bully escolhe coordenador
PupilManager define quem pode possuir réplica promovível
```

Se o vencedor do Bully não for o Pupilo, o sistema precisa definir uma política:

1. somente o Pupilo pode participar da eleição;
2. o Pupilo recebe prioridade máxima;
3. qualquer vencedor solicita o snapshot ao Pupilo;
4. Pupilo e coordenador substituto são o mesmo papel.

Para o projeto atual, a opção mais simples é:

```text
somente o Pupilo pode assumir como servidor
```

---

# 15. A promoção reutiliza a porta P2P, mas os demais peers podem continuar apontando para o servidor antigo

Na promoção:

```python
promoted_args.port = args.port
```

E o peer vencedor encerra o listener P2P para usar aquela porta como servidor.

Isso pode funcionar, mas os demais peers só atualizam os endpoints se receberem corretamente a mensagem do Bully.

Problemas possíveis:

* peers com mapas desatualizados;
* peer vencedor não conhecido por todos;
* `DataThread.server_ip` atualizado, mas outros componentes não;
* resultados pendentes no spool ainda apontando para o servidor antigo até a atualização;
* mensagens já enfileiradas no messenger usando endpoint mutável.

## Correção

Criar um objeto compartilhado de endpoint:

```python
@dataclass
class CoordinatorEndpoint:
    host: str
    port: int
    epoch: int
```

Todos os componentes devem consultar esse objeto no momento do envio:

* `PeerMessenger`;
* `DataThread`;
* heartbeat;
* DatasetReady;
* TaskResult.

Não atualizar atributos separadamente:

```python
messenger.server_ip = ip
data_thread.server_ip = ip
```

---

# 16. A recuperação de `TaskResult` pode reenviar resultado de uma execução antiga

A main cria o spool em:

```python
spool_dir = storage_dir.parent / "pending_results"
```

Quando usa:

```bash
--reset-storage
```

apaga apenas:

```text
fragments/
```

O diretório:

```text
pending_results/
```

continua existindo.

Depois:

```python
messenger.restore_pending_results()
```

reenvia todos os resultados armazenados, inclusive de execuções anteriores.

Isso pode:

* enviar `task_id` antigo;
* fazer o servidor atual receber resultado desconhecido;
* atualizar métricas incorretamente;
* criar duplicações;
* deixar o worker em retry infinito se o servidor rejeitar o task antigo.

## Correção imediata na main

Quando `--reset-storage` estiver ativo, limpar também o spool:

```python
if args.reset_storage and spool_dir.exists():
    shutil.rmtree(spool_dir)

spool_dir.mkdir(parents=True, exist_ok=True)
```

## Correção correta

Cada mensagem precisa conter:

```python
run_id
```

O arquivo de spool deve ser:

```text
pending_results/<run_id>/task_result_<task_id>.pkl
```

O problema é que o peer atualmente cria:

```python
local_run_id
```

mas não recebe o `run_id` do servidor no `JoinAck`.

Portanto, adicionar `run_id` ao protocolo de join:

```python
JoinAck(
    run_id=server_run_id,
    ...
)
```

O peer deve rejeitar ou arquivar spools de outro `run_id`.

---

# 17. O `run_id` local do peer não identifica a execução distribuída

A main imprime:

```python
local_run_id = f"peer-{args.port}-{uuid...}"
```

Isso é apenas um ID local, não o ID do trabalho no servidor.

O servidor possui outro:

```python
run_id = uuid.uuid4().hex
```

Mas ele não é transmitido ao peer.

Consequentemente, não é possível validar corretamente:

* fragmentos de uma execução anterior;
* resultados pendentes;
* snapshots;
* tarefas;
* promoção;
* mensagens atrasadas.

O hash do dataset ajuda, mas duas execuções podem usar o mesmo dataset com grids ou fragmentações diferentes.

## Correção

Adicionar `run_id` a:

* `JoinAck`;
* `TrainingTask`;
* `TaskResult`;
* `DatasetReady`;
* `SyncState`;
* `MembershipUpdate`;
* spool;
* metadados dos fragmentos.

O peer deve validar:

```python
if msg["run_id"] != self.run_id:
    rejeitar
```

---

# 18. A limpeza de fragmentos está boa, mas ainda é opcional

A main implementou corretamente uma limpeza segura:

```python
prepare_storage(..., reset=args.reset_storage)
```

E restringe o caminho a:

```text
data/.../fragments
```

Essa parte está melhor do que na lista anterior.

Porém, sem `--reset-storage`, fragmentos antigos continuam presentes. O `ValidatingDatasetLoader` detecta hash incompatível, mas o peer apenas falha no treinamento.

## Melhoria recomendada

Guardar um manifesto:

```text
data/peer_9101/manifest.json
```

Exemplo:

```json
{
  "run_id": "...",
  "dataset_id": "...",
  "fragment_count": 10
}
```

No bootstrap:

```python
if manifest.run_id != join.run_id:
    limpar fragments
```

Assim, a limpeza deixa de depender de o usuário lembrar de passar uma flag.

---

# 19. O `DatasetReady` é enviado uma vez por fragmento, o que está correto, mas a validação pode exigir o dataset completo cedo demais

No `assemble_many()`:

```python
await asyncio.to_thread(
    self.dataset_loader.load,
    fragment_ids,
)
```

O loader exige:

```python
len(X) == 569
```

Isso só funciona se toda tarefa usar todos os fragmentos do dataset.

Caso uma tarefa de treinamento seja projetada para usar apenas parte do dataset, a validação irá acusar erro mesmo que os fragmentos estejam corretos.

Pelo código atual, aparentemente cada tarefa usa o dataset completo, então isso não parece ser o bloqueio imediato. Mas a validação está acoplada a essa premissa.

## Correção futura

A tarefa deve informar:

```python
expected_sample_count
expected_dataset_hash
expected_fragment_set_hash
```

O loader valida de acordo com a tarefa, não por constantes globais da main.

---

# 20. O `wait_for_listener()` abre uma conexão vazia

A função testa a porta assim:

```python
reader, writer = await asyncio.open_connection(...)
writer.close()
```

Isso cria uma conexão sem mensagem tanto no servidor quanto no peer.

Dependendo da implementação de `P2PNode` e `ServerMessenger`, isso pode gerar:

* erro de EOF;
* log de mensagem inválida;
* exceção em handler;
* contador incorreto;
* task de conexão falhando.

Provavelmente não é a causa principal, mas deve ser garantido que os listeners tratem conexão vazia normalmente.

## Correção

No loop de conexão:

```python
try:
    message = await receive_message(reader)
except asyncio.IncompleteReadError:
    return

if message is None:
    return
```

---

# 21. O heartbeat do servidor não descobre peers silenciosos corretamente em todas as situações

O tracker só conhece um peer depois de:

```python
runtime.heartbeat.touch(node_id)
```

Isso ocorre via evento ou heartbeat.

Se o evento `peer_joined` não for consumido corretamente e o peer nunca enviar heartbeat, ele não entra no tracker.

Além disso, o tracker é separado da `GlobalTable`, criando duas fontes de verdade.

## Correção

Guardar `last_seen`, `misses` e estado do peer na própria `GlobalTable`.

Exemplo:

```python
{
    "node_id": "...",
    "status": "READY",
    "last_seen": time.monotonic(),
    "misses": 0,
}
```

O monitor percorre os nodes da tabela, não um dicionário paralelo na main.

---

# 22. O Pupilo morto não é imediatamente substituído

Em `peer_failure_monitor()`:

```python
runtime.coordinator.handle_peer_failure(node_id)
runtime.heartbeat.remove(node_id)
```

Mas não há uma reconciliação imediata do Pupilo.

A troca só ocorrerá quando o loop periódico de dois segundos rodar novamente.

Pior: `last_confirmed` em `pupil_sync_service()` não é apagado quando o Pupilo morre.

## Correção

Depois da falha:

```python
await runtime.pupil_manager.reconcile()
```

O estado do Pupilo deve ser atualizado atomicamente junto da remoção do peer.

---

# 23. `last_confirmed` pode manter estado incorreto

No serviço:

```python
last_confirmed: Optional[str] = None
```

Ele só muda quando:

```python
synced and pupil_id != last_confirmed
```

Se o Pupilo A falha, retorna e é selecionado novamente, o log não refletirá uma nova confirmação porque:

```python
last_confirmed == A
```

Além disso, ele guarda apenas o ID do Pupilo, não o ID do snapshot.

## Correção

Usar:

```python
last_confirmed_snapshot_id
```

Cada sincronização precisa ter um ID próprio e um ACK correspondente.

---

# 24. O `ReliablePeerMessenger` pode bloquear indefinidamente a fila

Para um `TaskResult`, ele tenta até ser entregue:

```python
while not stop_event and not delivered:
```

Se o servidor responder permanentemente com erro, o worker continua tentando a mesma mensagem e nenhuma mensagem posterior sai da fila.

Isso pode bloquear:

* outro `TaskResult`;
* mensagens de status;
* comunicações posteriores.

## Correção

Separar:

* spool persistente;
* fila de tentativas;
* agendamento por `next_retry_at`.

Uma solução mínima é recolocar a mensagem no final da fila após uma tentativa falha, em vez de manter o worker preso nela:

```python
if not delivered:
    envelope["attempt"] += 1
    envelope["next_retry_at"] = ...
    self._outbound_queue.put(envelope)
```

Também distinguir erros:

```text
temporário → retry
permanente/UNKNOWN_TASK/RUN_MISMATCH → quarantine
```

---

# 25. A main não chama `bootstrap()` duas vezes

Este item da lista anterior pode ser removido para esta `main_beta4.py`.

Ela não usa `PeerRuntime` e não executa:

```python
await runtime.bootstrap()
await runtime.run()
```

A beta4 monta os componentes manualmente em `run_peer()`.

Portanto, **a duplicação de Pupilo não vem de bootstrap duplicado nessa main**.

Contudo, montar todos os componentes manualmente recria boa parte da responsabilidade que deveria estar no `PeerRuntime`.

A longo prazo, a main deveria voltar a ser fina:

```python
runtime = PeerRuntime(config)
await runtime.run()
```

Mas somente depois de o runtime oficial receber:

* join em duas fases;
* endpoint compartilhado;
* membership update;
* trainer guard;
* Maekawa corrigido;
* spool por run;
* papel formal de Pupilo.

---

# Ordem exata de correção

## Bloco 1 — Fazer os peers treinarem

1. Adicionar estado `ready` aos peers.
2. Criar mensagem `PeerReady`.
3. O servidor só despacha para peers prontos.
4. Adicionar lock e `current_task` ao `TrainerNode`.
5. Fazer `PeerMessenger` rejeitar tarefa quando o trainer estiver ocupado.
6. Fazer tarefa inicial e tarefas normais usarem o mesmo `submit_task()`.
7. Mover o tratamento de exceções do `GuardedTrainerNode` para `TrainerNode`.
8. Garantir exatamente um `TaskResult` por tentativa.
9. Manter a correção de quórum vazio, mas movê-la para `MaekawaMutex`.
10. Remover o despacho automático de `handle_task_result()` e deixar apenas o scheduler.

## Bloco 2 — Corrigir o Maekawa e membership

11. Criar `MembershipUpdate`.
12. Enviar membership para todos os peers.
13. Atualizar `DataThread`, Maekawa e Bully em todos os peers.
14. Não usar `SyncState` como mensagem de membership.
15. Versionar membership com `membership_epoch`.

## Bloco 3 — Garantir um único Pupilo

16. Criar `PupilManager`.
17. Remover seleção de Pupilo do `status_consumer`.
18. Remover seleção direta do `pupil_sync_service`.
19. Guardar `pupil_id` e `pupil_epoch` na `GlobalTable`.
20. Adicionar `pupil_id`, `pupil_epoch` e `snapshot_id` ao `SyncState`.
21. Exigir ACK real do snapshot.
22. Enviar destituição ao Pupilo anterior.
23. Apagar ou invalidar o snapshot do antigo Pupilo.
24. Fazer `promote_to_server()` exigir `is_pupil=True`.
25. Reconciliar imediatamente o Pupilo após falha de peer.

## Bloco 4 — Corrigir execução e persistência

26. Transmitir `run_id` no `JoinAck`.
27. Incluir `run_id` em tarefas, resultados, snapshots e fragmentos.
28. Separar spool por `run_id`.
29. Limpar `pending_results` no `--reset-storage`.
30. Criar manifesto do storage.
31. Rejeitar mensagens pertencentes a outra execução.
32. Definir limite máximo de retry por tarefa.
33. Registrar falhas definitivas em `failed_task_ids`.

## Bloco 5 — Remover correções hard-coded da main

34. Mover `SafeMaekawaMutex` para `MaekawaMutex`.
35. Mover `GuardedTrainerNode` para `TrainerNode`.
36. Mover `install_dispatch_guard` para `GlobalTable`/`Coordinator`.
37. Mover heartbeat para o estado oficial dos peers.
38. Mover gestão do Pupilo para `PupilManager`.
39. Mover persistência de resultados para o `PeerMessenger` oficial.
40. Deixar a main somente configurando e iniciando os runtimes.

---

# Causa mais provável dos sintomas atuais

## “Está criando mais de um Pupilo”

A causa mais direta é:

```text
não existe papel formal de Pupilo
+ não existe destituição
+ qualquer peer que recebe SyncState guarda snapshot
+ o antigo Pupilo continua com réplica promovível
```

A dupla chamada a `select_pupil()` na main piora a falta de uma fonte única de verdade.

## “Eles não estão treinando”

As causas mais prováveis, em ordem, são:

1. o peer é registrado antes de estar completamente pronto;
2. não existe `PeerReady`;
3. peer pode receber tarefas concorrentes;
4. Maekawa possui memberships inconsistentes;
5. somente o Pupilo recebe atualização do quórum;
6. duas fontes tentam despachar tarefas;
7. TaskResult pode ficar bloqueado no worker de retry;
8. tarefas podem falhar indefinidamente sem atingir estado final.

Portanto, **não use apenas a lista anterior**. Esta lista revisada substitui a anterior para a `main_beta4.py` enviada.
