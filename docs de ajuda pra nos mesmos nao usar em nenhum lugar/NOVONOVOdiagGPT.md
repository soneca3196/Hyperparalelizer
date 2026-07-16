## Diagnóstico geral

Os 20 arquivos enviados passam na compilação sintática isolada. Porém, **o projeto ainda não consegue completar um único treinamento ponta a ponta**.

Os bloqueios principais são:

* Sua `main` chama `join_network()` sem `await`; portanto, `reply` será uma coroutine e `reply.get(...)` falhará. 
* O servidor reserva uma tarefa no `JoinAck`, mas depois tenta reservar e enviar outra tarefa para o mesmo peer.
* O `PeerRuntime` cria o `TrainerNode` com `dataset_loader=None` e `maekawa_mutex=None`; qualquer treinamento necessariamente quebra. 
* O `node_id` recebido do servidor não é atualizado corretamente em todos os componentes.
* O peer não registra todos os handlers necessários.
* O scheduler pode encerrar enquanto ainda existem tarefas sendo executadas.
* Heartbeat, Pupilo, Bully, Maekawa e Pub/Sub existem parcialmente, mas ainda não estão integrados ao ciclo principal.

O primeiro objetivo deve ser bem menor que a especificação completa:

> **Subir um servidor, subir um peer, baixar um fragmento, executar uma tarefa Random Forest, enviar o resultado e atualizar o `best_model`.**

Só depois disso devem entrar Maekawa, Pub/Sub e eleição.

---

# Lista de tarefas em ordem

## P0 — Corrigir o que impede o projeto de iniciar

### 1. Padronizar a estrutura de pacotes e imports (DEIXA QUIETO)

Atualmente sua `main` importa:

```python
from peer.data_thread import DataThread
```

Mas os próprios arquivos importam:

```python
from hyperparalelizer.peer.data_thread import DataThread
```

Escolha apenas um padrão. Recomendo:

```text
project/
├── main_peer.py
├── main_server.py
├── core/
├── utils/
└── hyperparalelizer/
    ├── global_table.py
    ├── ml/
    ├── peer/
    ├── server/
    └── sync/
```

E use sempre:

```python
from hyperparalelizer.peer.data_thread import DataThread
```

Também adicione `__init__.py` em todos os diretórios Python.

**Teste de conclusão:**

```bash
python -c "from hyperparalelizer.peer.trainer import TrainerNode"
python -c "from hyperparalelizer.server.coordinator import Coordinator"
```

Os dois comandos devem terminar sem erro.

---

### 2. Criar um `requirements.txt` (FIXED) 

Dependências identificadas:

```text
numpy
scikit-learn
xgboost
lightgbm
```

Por enquanto, teste apenas com `random_forest`.

Os imports de XGBoost e LightGBM são feitos no início de `models.py`. Isso significa que, mesmo usando somente Random Forest, o programa quebra se essas bibliotecas não estiverem instaladas. 

Melhor solução posterior: fazer imports opcionais dentro de `XGBWrapper` e `LGBMWrapper`.

**Teste de conclusão:**

```bash
pip install -r requirements.txt
python -c "from hyperparalelizer.ml.models import get_model"
```

---

### 3. Transformar a inicialização do peer em fluxo assíncrono (FIXED)

A sua `main` atual não pode funcionar porque `DataThread.join_network()` é `async`. 

Não continue montando todos os componentes diretamente na `main`. Use o `PeerRuntime`, depois de corrigi-lo.

A futura entrada deve ser aproximadamente:

```python
import asyncio

async def main():
    runtime = PeerRuntime(
        node_id=None,
        host=own_ip,
        listen_port=own_port,
        server_ip=server_ip,
        server_port=server_port,
    )
    await runtime.run()

if __name__ == "__main__":
    asyncio.run(main())
```

O `PeerRuntime` precisa ganhar um método `run()`. O `bootstrap()` atual cria a task do `P2PNode`, dorme `0.1` segundo e retorna. Se a `main` terminar nesse ponto, o event loop fecha e o peer para.

**Teste de conclusão:** o processo do peer deve continuar rodando e escutando na porta configurada.

---

## P1 — Corrigir o ciclo de atribuição de tarefas

### 4. Escolher apenas uma forma de entregar a primeira tarefa

Hoje existem duas formas ao mesmo tempo:

1. `Coordinator.add_peer()` retira uma tarefa da fila e coloca no `JoinAck`.
2. Depois do `JoinAck`, `handle_join_network()` chama `dispatch_next_task(peer)`, que retira uma segunda tarefa.

Isso deixa a primeira tarefa em `assigned_tasks`, mas nunca a executa. A segunda ainda pode ser enviada antes de o peer iniciar seu `P2PNode`.

A correção mais simples é:

* Manter a primeira tarefa dentro do `JoinAck`.
* Remover isto de `handle_join_network()`:

```python
if task is not None:
    asyncio.create_task(coordinator.dispatch_next_task(peer))
```

* Depois que o peer construir todos os componentes e iniciar sua porta P2P, executar:

```python
if self.data_thread.initial_task:
    asyncio.create_task(
        self.trainer.handle_training_task(
            self.data_thread.initial_task
        )
    )
```

As tarefas seguintes continuam sendo enviadas pelo servidor após o recebimento de cada `TaskResult`.

**Teste de conclusão:**

Depois do primeiro peer entrar:

```text
task_pool diminui em 1
assigned_tasks aumenta em 1
```

Não pode diminuir em 2.

---

### 5. Não registrar posse de fragmento durante o join

O `Coordinator.add_peer()` já chama:

```python
self.GlobalTable.add_fragment_location(fragment_id, node_id)
```

Mas nesse momento o peer ainda não baixou o arquivo. Isso cria uma localização falsa. Outro peer pode tentar buscar o fragmento nesse nó e receber `FragmentNotFound`. 

Remova esse registro do `add_peer()`.

A localização deve ser registrada apenas quando o peer enviar `DatasetReady`, fluxo que já existe no `server_peer_protocol.py`. 

**Teste de conclusão:**

* Antes de `DatasetReady`, o peer não aparece em `fragments_locations`.
* Depois de `DatasetReady`, passa a aparecer.

---

### 6. Corrigir o encerramento precoce do scheduler

O scheduler atualmente encerra quando:

```python
if not dispatched and not self.GlobalTable.task_pool:
    break
```

Isso é incorreto se `task_pool` estiver vazia, mas ainda existirem tarefas em `assigned_tasks`.

Nesse cenário, o scheduler para de verificar timeouts. Se um peer morrer, a tarefa nunca será reenfileirada.

Troque a condição por algo equivalente a:

```python
with self.GlobalTable.lock:
    no_pending = not self.GlobalTable.task_pool
    no_running = not self.GlobalTable.assigned_tasks

if no_pending and no_running:
    break
```

**Teste de conclusão:**

* Uma tarefa atribuída deve manter o scheduler ativo.
* Ao exceder o timeout, deve voltar para `task_pool`.

---

### 7. Tornar o timeout configurável

`TASK_TIMEOUT = 30.0` é muito baixo para treinamento de ML.

Transforme em configuração do `Coordinator`, por exemplo:

```python
Coordinator(..., task_timeout=300)
```

Além disso, registre o momento em que o peer confirmou o recebimento da tarefa, e não apenas o momento em que ela foi retirada da fila.

**Teste de conclusão:** um treinamento que demora mais de 30 segundos não pode ser duplicado indevidamente.

---

## P2 — Completar o `PeerRuntime`

### 8. Atualizar o `node_id` definitivo em todos os componentes

Depois do join:

```python
self.node_id = reply["node_id"]
```

Esse ID deve ser usado em:

* `DataThread`;
* `PeerMessenger`;
* `TrainerNode`;
* `P2PNode`;
* `MaekawaMutex`;
* `BullyElection`;
* Pub/Sub.

Hoje o `PeerRuntime` usa o ID do `reply` apenas no `PeerMessenger`, mas passa `self.node_id` antigo ao `P2PNode` e ao `TrainerNode`. 

**Teste de conclusão:** o mesmo `node_id` deve aparecer nos logs do DataThread, Messenger, Trainer e P2PNode.

---

### 9. Instanciar o `DatasetLoader`

O runtime atualmente passa:

```python
dataset_loader=None
```

Mas o trainer executa:

```python
self.dataset_loader.load(fragment_ids)
```

Portanto, o primeiro treinamento quebra obrigatoriamente. 

Implemente:

```python
dataset_loader = DatasetLoader(self.storage_dir)
```

E passe a instância para o `TrainerNode`.

**Teste de conclusão:** teste unitário salvando um fragmento `.bin` e carregando-o novamente.

---

### 10. Usar um mutex simples no primeiro teste

O runtime também passa:

```python
maekawa_mutex=None
```

E o primeiro treinamento bem-sucedido sempre será o melhor modelo local inicial, fazendo o trainer executar:

```python
await self.maekawa_mutex.request_access()
```

Isso provoca `AttributeError`. 

Para testar o pipeline antes do Maekawa, crie temporariamente:

```python
class NoOpMutex:
    async def request_access(self):
        return

    async def release_access(self):
        return
```

Depois que o treinamento ponta a ponta funcionar, substitua pelo `MaekawaMutex`.

Não tente depurar rede, dataset, treinamento e Maekawa simultaneamente.

---

### 11. Registrar todos os handlers do peer

Atualmente o runtime registra somente os handlers do `PeerMessenger`.

Também precisam ser registrados:

```python
messenger.register_handlers(p2p_node)
register_peer_peer_handlers(p2p_node, storage_dir)
maekawa_mutex.register_handlers(p2p_node)
bully.register_handlers(p2p_node)
```

Também faltam handlers para:

* `SyncState`;
* `PubSubNotify`;
* possivelmente atualização da lista de peers;
* promoção do Pupilo.

Sem `register_peer_peer_handlers`, nenhum peer fornece fragmentos a outro peer, apesar de o download P2P estar implementado. 

**Teste de conclusão:** enviar manualmente `RequestFragment` para um peer e receber `FragmentData`.

---

### 12. Implementar shutdown correto

O `PeerMessenger` pode reutilizar o event loop principal. Nesse caso, seu `stop()` não deve fechar esse loop.

Adicione uma flag indicando se o loop foi:

* criado internamente pelo messenger; ou
* recebido/reutilizado do runtime.

Feche o loop apenas no primeiro caso.

O runtime também precisa cancelar ou aguardar:

* task do `P2PNode`;
* worker do messenger;
* heartbeat;
* Pub/Sub;
* replicação;
* eleição.

---

## P3 — Corrigir contratos entre peer e servidor

### 13. Atualizar o contrato de `TaskResult`

O `TaskResult` declarado em `protocol.py` não possui os campos realmente enviados pelo trainer:

* `status`;
* `error`;
* `model_bytes`.

O trainer monta um dicionário manual com esses campos, mas o contrato oficial não os aceita.

Atualize a dataclass para refletir a mensagem real, com campos opcionais para falha:

```python
@dataclass
class TaskResult:
    task_id: str
    id_node: str
    status: str
    accuracy: Optional[float] = None
    precision: Optional[float] = None
    recall: Optional[float] = None
    f1_score: Optional[float] = None
    roc_auc: Optional[float] = None
    model_bytes: Optional[bytes] = None
    error: Optional[str] = None
```

Depois faça o trainer usar `TaskResult(...).to_dict()`.

**Teste de conclusão:** serializar e desserializar um resultado de sucesso e um de falha.

---

### 14. Validar o ACK correto

O servidor e o peer verificam, em vários lugares, apenas:

```python
reply.get("type") == "Ack"
```

Também valide:

```python
reply["ref_type"]
reply["ref_id"]
```

Exemplo: o ACK de uma tarefa deve ter:

```text
ref_type = TrainingTask
ref_id = task_id
```

Isso evita considerar como confirmação um ACK atrasado ou referente a outra operação.

---

### 15. Fazer a atualização de `best_model` realmente atômica

Hoje o fluxo é:

1. ler `best_model`;
2. comparar score;
3. adquirir novamente o lock;
4. escrever `best_model`.

Duas respostas concorrentes podem ler o mesmo valor antigo e a resposta de menor score pode sobrescrever a de maior score.

Implemente na `GlobalTable` algo como:

```python
def try_update_best_model(self, candidate) -> bool:
    with self.lock:
        current_score = ...
        if candidate["f1_score"] <= current_score:
            return False

        self.best_model = candidate
        return True
```

A operação de leitura, comparação e escrita deve acontecer sob o mesmo lock. Isso é necessário independentemente do Maekawa. 

**Teste de conclusão:** enviar resultados concorrentes com F1 `0.70`, `0.92` e `0.81`; o resultado final deve ser sempre `0.92`.

---

### 16. Notificar posse de todos os fragmentos

Uma tarefa pode pedir vários fragmentos, mas o trainer atualmente notifica apenas:

```python
fragment_ids[0]
```

Assim, o peer baixa todos os fragmentos, mas o servidor registra somente o primeiro.

Faça:

```python
for fragment_id in fragment_ids:
    await self.data_thread.notify_dataset_ready(fragment_id)
```

**Teste de conclusão:** todos os fragmentos presentes em disco devem aparecer na `fragments_locations` do peer.

---

### 17. Unificar `model_config` e `parametros`

A tarefa contém:

```python
model_config
parametros
```

Mas o trainer usa apenas:

```python
get_model(task["model_type"], task["parametros"])
```

Defina claramente:

* `model_config`: parâmetros fixos;
* `parametros`: combinação variável do Grid Search.

No trainer:

```python
params = {
    **task.get("model_config", {}),
    **task.get("parametros", {}),
}
```

---

### 18. Adicionar retry limitado por tarefa

Quando uma tarefa falha, ela é reenfileirada e imediatamente pode voltar para o mesmo peer. Se o erro for determinístico, cria um ciclo infinito.

Adicione:

```text
attempt
max_attempts
last_error
```

Depois de, por exemplo, três falhas, mova a tarefa para `failed_tasks`.

---

## P4 — Testes que devem ser executados primeiro

Execute exatamente nesta ordem.

### 19. Testes unitários sem rede

1. `serializer`: bytes → frame → bytes.
2. `DatasetLoader`: um e vários fragmentos.
3. `get_model("random_forest")`.
4. `evaluate()` com classificação binária.
5. snapshot da `GlobalTable`.
6. criação do Grid Search.
7. atribuição de uma tarefa sem atribuição duplicada.
8. atualização atômica do melhor modelo.

O serializer já suporta bytes em Base64, necessário para fragmentos e modelos. 

---

### 20. Primeiro teste integrado: servidor + um peer + uma tarefa

Use:

* dataset pequeno, como `load_breast_cancer()` do scikit-learn;
* um único fragmento;
* `random_forest`;
* uma única combinação;
* `NoOpMutex`;
* sem Pub/Sub;
* sem Bully;
* sem Pupilo.

Condições de sucesso:

```text
Peer recebe JoinAck
Peer recebe node_id
Peer baixa fragment_0000
Peer envia DatasetReady
Trainer carrega o fragmento
Random Forest termina
Peer envia TaskResult
Servidor remove a tarefa de assigned_tasks
Servidor atualiza best_model
Servidor fica com task_pool vazia
Servidor fica com assigned_tasks vazia
```

---

### 21. Segundo teste integrado: um peer e várias tarefas

Grid pequeno:

```python
{
    "n_estimators": [5, 10],
    "max_depth": [2, 4],
}
```

Resultado esperado:

* quatro tarefas executadas;
* nenhuma tarefa perdida;
* nenhuma tarefa duplicada;
* `best_model` corresponde ao maior F1.

---

### 22. Terceiro teste integrado: dois peers

Valide:

* distribuição de tarefas;
* execução simultânea;
* cada peer com porta própria;
* resultados concorrentes;
* atualização atômica do melhor modelo.

---

### 23. Testar transferência P2P de verdade

Para garantir que o teste não está silenciosamente usando o backup do servidor:

1. Peer A baixa o fragmento.
2. Peer A envia `DatasetReady`.
3. Peer B entra.
4. Temporariamente desabilite `RequestFragmentBackup`.
5. Peer B deve receber o fragmento de A.

A camada de rede já possui conexão temporária e uma classe de conexão persistente, mas o fluxo de fragmentos atual utiliza `send_once`. 

---

## P5 — Somente depois do MVP funcionar

### 24. Integrar Maekawa

Ainda faltam:

* geração real do quórum;
* tratamento de quórum vazio para sistema com um nó;
* atualização do quórum quando novos peers entram;
* captura de `MaekawaTimeoutError` no trainer;
* `release_access()` dentro de `finally`;
* tratamento de deadlock.

A implementação atual espera todos os membros da lista `quorum`, mas nenhum componente constrói essa lista no runtime. 

---

### 25. Integrar Pub/Sub

Faltam:

* instanciar `PubSubBroker`;
* registrar handlers de subscribe, unsubscribe e publish no servidor;
* instanciar `PubSubClient` nos peers;
* registrar `PubSubNotify` no `P2PNode`;
* assinar `global_best_score`;
* atualizar o recorde local ao receber notificação.

O código do broker e cliente existe, mas não está conectado ao runtime atual. 

---

### 26. Integrar heartbeat e remoção de peers

A camada de rede possui `KeepAliveManager`, mas o runtime não o cria nem registra os participantes. 

Implemente:

* servidor monitorando peers;
* peer monitorando servidor;
* callback do servidor para `Coordinator.handle_peer_failure`;
* callback do peer para iniciar Bully;
* atualização de listas de peers e quóruns após remoção.

---

### 27. Implementar o Pupilo de verdade

Atualmente:

* `Coordinator.pupil_peer` não é escolhido automaticamente;
* `SyncState` não é registrado no peer;
* não há loop periódico de replicação;
* `promote_to_server()` cria somente um `Coordinator`;
* a promoção não inicia `ServerMessenger`;
* não registra handlers do novo servidor;
* não reinicia o scheduler;
* não informa aos peers o novo endereço.

Portanto, o Pupilo ainda não consegue substituir o servidor.

---

### 28. Completar o Bully

O Bully atual consegue trocar mensagens de eleição, mas vencer a eleição não transforma efetivamente o peer em servidor operacional. 

A promoção precisa:

1. recuperar snapshot;
2. parar o modo peer ou separar portas;
3. criar `ServerMessenger`;
4. registrar handlers;
5. iniciar scheduler;
6. anunciar IP e porta do novo servidor;
7. fazer os peers atualizarem `server_ip` e `server_port`.

---

### 29. Executar o benchmark final

Somente depois dos testes funcionais:

* mesmo dataset;
* mesmo Grid Search;
* mesmo modelo;
* mesmo `random_state`;
* mesmo split de treino/teste;
* medir treinamento sequencial;
* medir treinamento distribuído;
* separar:

  * tempo de transferência;
  * tempo de treinamento;
  * tempo total;
  * número de falhas/retries.

Também use `stratify=y` no `train_test_split` para tornar a comparação mais estável.

---

## Ordem resumida de implementação

A sequência que eu seguiria é:

```text
1. Imports e estrutura
2. main assíncrona
3. Server main
4. Corrigir tarefa duplicada no Join
5. Corrigir scheduler
6. Corrigir node_id
7. Instanciar DatasetLoader
8. Usar NoOpMutex
9. Registrar handler de fragmentos
10. Executar 1 peer + 1 tarefa
11. Executar 1 peer + várias tarefas
12. Executar 2 peers
13. Testar P2P
14. Atualização atômica do best_model
15. Timeout e crash de peer
16. Maekawa
17. Pub/Sub
18. Heartbeat
19. Pupilo e Bully
20. Benchmark
```

**A tarefa número 1 prática agora é corrigir `PeerRuntime` e o fluxo da primeira tarefa.** Enquanto `DatasetLoader`, mutex, `node_id`, handlers e `initial_task` não estiverem ligados ali, continuar aumentando a sua `main` só duplicará a lógica de inicialização e tornará a integração mais difícil.
