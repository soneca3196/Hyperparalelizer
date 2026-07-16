# Diagnóstico geral

O projeto tem várias peças aproveitáveis, mas **ainda não existe um sistema executável de ponta a ponta**. Os arquivos compilam isoladamente, porém os componentes foram construídos com contratos e fluxos diferentes.

O problema principal não é “um bug”: é a ausência de uma **composição central** que determine:

* quem inicia cada componente;
* quem é responsável por cada estado;
* qual mensagem cada lado envia;
* quando uma tarefa é considerada reservada, executada ou concluída;
* quando um peer realmente passa a possuir um fragmento.

A boa notícia é que não precisa jogar tudo fora. A camada TCP, serialização de bytes, wrappers de modelos, carregamento de fragmentos e boa parte dos handlers são uma base aproveitável.    

---

# Irregularidades críticas

## 1. O `main.py` não inicia o sistema

O arquivo atual:

* usa `own_ip`, `own_port`, `server_ip` e `server_port` sem defini-los;
* importa `peer.data_thread`, mas o pacote real está em `hyperparalelizer.peer`;
* chama `join_network()` sem `await`;
* tenta usar `.get()` em uma coroutine;
* instancia `PeerMessenger` sem o argumento obrigatório `loop`;
* não cria `P2PNode`;
* não registra handlers;
* não cria `DatasetLoader`;
* não cria `TrainerNode`;
* não cria `MaekawaMutex`;
* não inicia o listener TCP do peer;
* não possui nenhum fluxo para iniciar o servidor.

Portanto, neste momento, não há comando capaz de levantar nem um servidor nem um peer.  

---

## 2. A primeira tarefa é reservada duas vezes

Quando um peer entra:

1. `Coordinator.add_peer()` retira uma tarefa da fila e coloca em `assigned_tasks`;
2. essa tarefa é enviada dentro do `JoinAck`;
3. depois, `handle_join_network()` chama `dispatch_next_task(peer)`;
4. `dispatch_next_task()` retira **outra tarefa** da fila.

Resultado: o mesmo peer pode ficar com duas tarefas simultaneamente, enquanto a primeira enviada no `JoinAck` talvez nem seja executada.

Esse é um dos bugs mais graves do fluxo atual.  

A solução mais simples é:

* manter a primeira tarefa no `JoinAck`;
* remover o `dispatch_next_task(peer)` do final de `handle_join_network`;
* no peer, depois do join, o `main` chama `trainer.handle_training_task(initial_task)`.

Deve existir **um único caminho** para entregar a tarefa inicial.

---

## 3. Todo treinamento bem-sucedido falha ao criar o resultado

O avaliador retorna:

```python
metrics["f1_score"]
```

Mas o `TrainerNode._send_result()` tenta acessar:

```python
metrics["f1"]
```

Isso gera `KeyError: "f1"` depois do modelo já ter sido treinado.

Na prática, nenhum resultado bem-sucedido chega corretamente ao servidor.  

A correção imediata é:

```python
"f1_score": metrics["f1_score"] if metrics else None,
```

A documentação do próprio `evaluator.py` também fala em `f1`, enquanto a implementação retorna `f1_score`. Escolham apenas um nome — recomendo `f1_score` em todo o sistema.

---

## 4. O contrato de `TaskResult` não representa as mensagens reais

A classe `TaskResult` define:

* métricas obrigatórias;
* `tempo_treino_s`;
* nenhum `status`;
* nenhum `error`;
* nenhum `model_bytes`.

Mas o trainer envia:

* `status`;
* `error`;
* métricas possivelmente `None`;
* `model_bytes`;
* e não envia `tempo_treino_s`.

Assim, o protocolo documentado e a mensagem efetivamente enviada são diferentes. Se `from_dict()` for usado para interpretar um resultado real, ele falhará.  

O contrato deveria ficar aproximadamente assim:

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
    tempo_treino_s: float = 0.0
    model_bytes: Optional[bytes] = None
    error: Optional[str] = None
```

---

## 5. Tarefa que falha fica simultaneamente atribuída e reenfileirada

Em `receive_task_result()`, quando uma tarefa falha, ela volta para `task_pool`, mas não é removida de `assigned_tasks`.

Isso cria este estado impossível:

```text
task X está na fila
task X ainda está atribuída ao peer
```

Consequências:

* o peer continua sendo considerado ocupado;
* a mesma tarefa pode ser executada duas vezes;
* o timeout pode reenfileirá-la novamente;
* `dispatch_all_idle()` pode nunca considerar o peer livre.



A correção precisa ser feita dentro do mesmo lock:

```python
task_info = assigned_tasks.pop(task_id)
task_pool.insert(0, task_info["task"])
```

---

## 6. A atualização do melhor modelo não é atômica

Atualmente o coordenador faz:

1. `get_best_model()`;
2. compara o score;
3. `set_best_model()`.

Cada operação possui seu próprio lock, mas a sequência inteira não está protegida.

Dois resultados podem chegar quase simultaneamente:

```text
Resultado A lê best = 0.80
Resultado B lê best = 0.80
A grava 0.95
B grava 0.90
```

O resultado final poderia ser `0.90`, mesmo tendo existido um `0.95`.

A `GlobalTable` precisa de um método único:

```python
update_best_if_better(candidate) -> bool
```

Ele deve comparar e atualizar dentro do mesmo lock. A consistência não pode depender apenas do Maekawa.  

---

## 7. O Maekawa está protegendo o critério errado

O trainer solicita acesso ao Maekawa quando o modelo supera o **melhor modelo local daquele peer**.

Mas o que interessa é saber se ele pode superar o **melhor modelo global**.

Exemplo:

```text
Peer A:
melhor local = 0.95

Melhor global atual = 0.90

Novo resultado do A = 0.93
```

O resultado `0.93` não supera o melhor local do A, então ele não solicita Maekawa. Porém ele supera o melhor global e deveria disputar a atualização.

Além disso:

* quórum vazio causa espera infinita;
* não existe timeout no `grant_event.wait()`;
* os handlers Maekawa não são registrados no `P2PNode`;
* o código espera `len(quorum)` grants;
* não há tratamento completo de deadlock do Maekawa;
* o `release_access()` não está protegido por `finally`.

 

Para o primeiro MVP, o Maekawa deve ficar desativado. Primeiro façam o servidor atualizar atomicamente o melhor modelo. Depois integrem o algoritmo distribuído.

---

## 8. O servidor registra fragmentos antes de eles existirem no peer

O coordenador adiciona o peer em `fragments_locations`:

* durante `fragment_dataset()`;
* durante `add_peer()`.

Isso acontece antes de o peer baixar ou montar o fragmento. O próprio comentário no código reconhece a inconsistência. 

A localização só deveria ser registrada depois de um `DatasetReady` confirmado.

Fluxo correto:

```text
Servidor atribui fragmento
Peer baixa fragmento
Peer salva fragmento
Peer envia DatasetReady
Servidor adiciona peer em fragments_locations
```

---

## 9. Um peer que baixa vários fragmentos só confirma o primeiro

O trainer executa:

```python
assemble_many(fragment_ids)
```

Mas depois chama:

```python
notify_dataset_ready(fragment_ids[0])
```

Se ele baixou cinco fragmentos, o servidor fica sabendo apenas do primeiro.  

A confirmação deve ocorrer para cada fragmento adquirido, preferencialmente dentro de `assemble_dataset_for()` logo após o arquivo ser salvo.

---

## 10. Todos os peers podem compartilhar acidentalmente a mesma pasta

O diretório padrão é:

```text
data/fragments
```

Se vários peers forem executados na mesma máquina, todos enxergarão os mesmos arquivos.

Assim, um peer pode acreditar que baixou ou recebeu um fragmento P2P quando, na verdade, o arquivo foi criado por outro processo na pasta compartilhada.

Use algo como:

```text
data/peers/8001/fragments
data/peers/8002/fragments
data/peers/8003/fragments
```

ou:

```text
data/peers/{node_id}/fragments
```

 

---

## 11. As informações de roteamento ficam desatualizadas

O peer recebe uma lista de peers apenas durante o join. Depois disso:

* peers antigos não descobrem peers novos;
* peers novos não recebem atualizações posteriores;
* não existe consulta funcional de localização por fragmento;
* `FindNode` está definido, mas não possui handler;
* `DataThread` tenta cada peer conhecido sem saber qual possui o fragmento.

Na prática, o P2P tende a falhar e cair no backup central.  

Para o MVP P2P, criem uma mensagem semelhante a:

```text
LocateFragment(fragment_id)
FragmentLocations(fragment_id, peers)
```

O peer consulta o servidor antes do download e tenta apenas os peers listados.

---

## 12. Pub/Sub existe, mas não está conectado ao restante do sistema

Há implementação de broker e cliente, porém:

* o `PubSubBroker` não é criado no servidor;
* seus handlers não são registrados no `ServerMessenger`;
* o `PubSubClient.handle_notify()` não é registrado no `P2PNode`;
* o peer não se inscreve no tópico;
* o listener outbound não é iniciado;
* não existe consumo prático da `inbound_queue`.

Assim, o coordenador pode colocar algo na fila, mas ninguém necessariamente publica ou recebe.  

---

## 13. Vários tipos de mensagem não possuem handler conectado

Existem classes ou métodos para as mensagens abaixo, mas elas não estão completamente registradas:

* `PubSubSubscribe`;
* `PubSubUnsubscribe`;
* `PubSubPublish`;
* `PubSubNotify`;
* `MaekawaRequest`;
* `MaekawaGrant`;
* `MaekawaRelease`;
* `BullyElection`;
* `BullyCoordinator`;
* `SyncState`;
* `SendBestModel`;
* `FindNode`.

O código existe, mas nunca é chamado pela camada de rede.    

---

## 14. `RequestBestModel` possui dois significados diferentes

No servidor, significa:

```text
Peer pede ao servidor o melhor modelo global.
```

No peer, significa:

```text
Servidor pede ao peer o melhor modelo local.
```

Usar o mesmo tipo para duas operações semanticamente diferentes deixa o protocolo ambíguo.

Além disso, o peer responde com `SendBestModel`, mas o servidor não registra handler para `SendBestModel`.   

Para simplificar:

* mantenha `RequestBestModel` apenas para peer → servidor;
* remova temporariamente o fluxo servidor → peer;
* o servidor já recebe `model_bytes` pelo `TaskResult`.

---

## 15. Heartbeat e recuperação não estão ativos

Existem partes de heartbeat na camada de rede, mas o fluxo não foi montado:

* ninguém configura o `KeepAliveManager`;
* o servidor apenas responde ao `KeepAlive`;
* não registra `last_seen` na `GlobalTable`;
* não existe loop chamando `check_task_status()`;
* `remove_node()` não reenfileira tarefas daquele peer;
* o Pub/Sub não remove automaticamente o peer morto da tabela principal.

  

Também há um problema conceitual: `TASK_TIMEOUT = 30` segundos é muito baixo para treinamento de machine learning. Um treinamento legítimo pode ser marcado como morto e enviado a outro peer.

---

## 16. Resultados atrasados podem ser atribuídos ao peer errado

Considere:

1. Peer A recebe tarefa X;
2. ocorre timeout;
3. tarefa X volta para a fila;
4. Peer B recebe tarefa X;
5. o resultado atrasado de A chega.

Como o `task_id` é o mesmo e a tabela agora aponta para B, o resultado de A pode ser tratado como se fosse de B.

O servidor precisa validar:

```python
msg["id_node"] == assigned_task["peer"].id_node
```

Também é recomendável adicionar um `attempt_id` ou `assignment_id`.

---

## 17. A promoção do peer pupilo não inicia um servidor funcional

`promote_to_server()` apenas:

* recria uma `GlobalTable`;
* cria um `Coordinator`;
* retorna o objeto.

Ele não:

* inicia um `ServerMessenger`;
* registra handlers;
* inicia Pub/Sub;
* assume uma porta conhecida;
* anuncia IP e porta do novo servidor;
* atualiza `server_ip` nos peers;
* impede dois líderes simultâneos.

 

O `BullyCoordinator` transmite somente o ID. Os outros peers precisam receber também:

```text
coordinator_id
coordinator_ip
coordinator_port
term ou epoch
```

---

## 18. O snapshot possui tipos inconsistentes

`system_state` começa como `ServerState`, depois é gravado como strings como:

```text
"DATASET_DISTRIBUTION"
"MODEL_DISTRIBUTION"
```

No snapshot, um enum pode virar:

```text
"ServerState.HASHING"
```

Na restauração, ele permanece uma string.

Logo, o tipo muda durante o ciclo de vida.  

Escolham uma representação única. A mais simples é usar strings em toda parte:

```python
HASHING = "HASHING"
OPEN = "OPEN"
DATASET_DISTRIBUTION = "DATASET_DISTRIBUTION"
MODEL_DISTRIBUTION = "MODEL_DISTRIBUTION"
```

---

## 19. `metadata` às vezes é `dict` e às vezes é `Peer`

A `GlobalTable` aceita qualquer coisa como metadata. Porém, ao restaurar snapshot, qualquer `dict` é convertido forçadamente em `Peer`.

Isso falha quando metadata é algo como:

```python
{"role": "worker"}
```

porque `Peer` não possui o argumento `role`. 

Escolham uma opção:

* metadata é sempre um `PeerState` bem definido;
* ou metadata é sempre um dicionário simples.

Misturar os dois torna snapshots instáveis.

---

## 20. Os testes estão desatualizados em relação ao código

Em uma montagem temporária, com a camada de transporte apenas simulada para permitir os imports, **5 dos 10 testes anexados terminaram em erro**.

Entre as inconsistências dos testes:

* passam uma lista como dataset, enquanto o coordenador espera `(X, y)`;
* acessam `global_table.fragments`, que não existe;
* esperam que `add_peer()` retorne apenas `node_id`, mas agora retorna uma tupla;
* enviam resultado sem `model_bytes`;
* usam imports antigos;
* usam metadata incompatível com a restauração.



Neste momento, os testes não podem ser usados como autoridade sobre o comportamento esperado. Primeiro é necessário atualizar o contrato e só então reescrevê-los.

---

# Ordem recomendada de trabalho

## Fase 1 — Congelar uma arquitetura única

Antes de corrigir algoritmos, definam esta separação:

### Servidor

```text
server_main
 ├── GlobalTable
 ├── Coordinator
 ├── ServerMessenger
 ├── server protocol handlers
 └── PubSubBroker
```

### Peer

```text
peer_main
 ├── P2PNode
 ├── PeerMessenger
 ├── DataThread / DatasetManager
 ├── DatasetLoader
 ├── TrainerNode
 ├── PubSubClient
 └── sincronização — somente depois
```

Responsabilidades:

* `ServerMessenger` e `P2PNode`: somente transporte e dispatch;
* arquivos de protocolo: interpretar mensagens;
* `Coordinator`: lógica de tarefas;
* `GlobalTable`: estado compartilhado;
* `DataThread`: aquisição e armazenamento de dataset;
* `TrainerNode`: somente treinamento;
* `main`: criação e ligação das dependências.

### Organização de arquivos sugerida

```text
hyperparalelizer/
├── main.py
├── global_table.py
├── ml/
│   ├── evaluator.py
│   └── models.py
├── peer/
│   ├── bootstrap.py
│   ├── data_manager.py
│   ├── dataset_loader.py
│   ├── messenger.py
│   ├── network_handlers.py
│   └── trainer.py
├── server/
│   ├── coordinator.py
│   ├── messenger.py
│   └── network_handlers.py
└── sync/
    ├── bully.py
    ├── lamport.py
    └── maekawa.py
```

Removam ou movam:

* `server_peer_protocol.py` para `server/network_handlers.py`;
* `peer_peer_protocol.py` e `peer_outer_protocol.py` devem virar um único arquivo;
* `server_inner_protocol.py` está praticamente vazio e pode ser removido;
* `peer_inner_protocol.py` pode permanecer como barramento interno, mas não é necessário para o MVP.  

---

## Fase 2 — Reescrever completamente o `main.py`

Criem dois modos:

```bash
python -m hyperparalelizer.main server
python -m hyperparalelizer.main peer
```

### Inicialização do servidor

Ordem:

1. carregar `(X, y)`;
2. criar `GlobalTable`;
3. criar `Coordinator`;
4. executar `fragment_dataset()`;
5. executar `generate_grid_search()`;
6. criar `ServerMessenger`;
7. criar `PubSubBroker`, inicialmente opcional;
8. registrar todos os handlers;
9. iniciar o servidor;
10. iniciar monitor de tarefas em background.

### Inicialização do peer

Ordem:

1. ler IP, porta e endereço do servidor;
2. definir um diretório exclusivo;
3. calcular ou obter `node_id`;
4. criar o `P2PNode`;
5. criar `DataThread`;
6. criar `DatasetLoader`;
7. criar `PeerMessenger`;
8. criar `TrainerNode`;
9. ligar messenger ↔ trainer;
10. registrar handlers de tarefa e fragmento;
11. iniciar `PeerMessenger`;
12. iniciar listener `P2PNode`;
13. executar `join_network()`;
14. executar a tarefa inicial do `JoinAck`.

O listener do peer precisa estar ativo **antes** de o servidor tentar enviar qualquer tarefa.

---

## Fase 3 — Corrigir os contratos bloqueadores

Faça estas alterações antes de qualquer teste distribuído:

1. trocar `metrics["f1"]` por `metrics["f1_score"]`;
2. atualizar a classe `TaskResult`;
3. decidir um único fluxo para a tarefa inicial;
4. remover o segundo despacho no join;
5. padronizar imports absolutos;
6. padronizar `system_state`;
7. padronizar metadata;
8. adicionar `node_id` explicitamente ao `DataThread`;
9. validar `Ack.ref_type` e `Ack.ref_id`, não apenas `type == "Ack"`;
10. configurar um `model_type` válido, como `random_forest`, em vez de `generic`.

O valor `"generic"` não é aceito pelo `get_model()`.  

---

## Fase 4 — Fazer o menor fluxo possível funcionar

Desative temporariamente:

* Pub/Sub;
* Maekawa;
* Lamport;
* Bully;
* peer pupilo;
* heartbeat avançado;
* conexões persistentes.

O primeiro objetivo deve ser somente:

```text
1 servidor
1 peer
1 fragmento
1 tarefa Random Forest
1 resultado
1 best_model
```

### Critério de conclusão

O log deve demonstrar:

```text
Servidor iniciou
Peer entrou
Peer recebeu node_id
Peer recebeu tarefa
Peer baixou fragmento
Peer confirmou DatasetReady
Peer treinou
Servidor recebeu TaskResult
Servidor atualizou best_model
Fila ficou vazia
assigned_tasks ficou vazio
```

Enquanto isso não funcionar, não avance para sincronização distribuída.

---

## Fase 5 — Consertar o ciclo de vida das tarefas

Crie operações atômicas na `GlobalTable`:

```python
reserve_next_task(peer_id)
complete_task(task_id, peer_id)
fail_task(task_id, peer_id)
requeue_timed_out_task(task_id)
update_best_if_better(candidate)
```

Não permita que o `Coordinator` modifique diretamente cinco dicionários diferentes.

Estados recomendados:

```text
QUEUED
ASSIGNED
RUNNING
COMPLETED
FAILED
TIMED_OUT
```

Cada atribuição deveria armazenar:

```python
{
    "task": task,
    "peer_id": peer_id,
    "assignment_id": uuid,
    "assigned_at": timestamp,
    "last_progress_at": timestamp,
}
```

### Regras

* sucesso remove de `assigned_tasks`;
* falha remove de `assigned_tasks` e reenfileira;
* timeout remove de `assigned_tasks` e reenfileira;
* resultado duplicado é ignorado;
* resultado de outro peer é rejeitado;
* resultado atrasado de uma atribuição antiga é rejeitado;
* atualização do melhor modelo ocorre atomicamente.

---

## Fase 6 — Consertar o ciclo de vida dos fragmentos

A `GlobalTable` deve separar:

```text
fragment_payloads:
    fragment_id -> bytes mantidos pelo servidor

fragment_locations:
    fragment_id -> peers que confirmaram o arquivo em disco
```

### Regras

* fragmentar não significa distribuir;
* atribuir não significa possuir;
* somente `DatasetReady` registra localização;
* remoção de peer elimina suas localizações;
* cada fragmento adquirido gera um `DatasetReady`;
* os arquivos devem ser gravados atomicamente.

Para gravação segura:

```text
fragment_0001.bin.tmp
→ validar
→ renomear para fragment_0001.bin
```

Também vale adicionar hash SHA-256 ao fragmento para detectar corrupção.

---

## Fase 7 — Fazer P2P real funcionar

Depois do fluxo de um peer:

1. iniciar dois peers com pastas diferentes;
2. colocar fragmento A no peer 1;
3. registrar a localização somente após `DatasetReady`;
4. peer 2 consulta `LocateFragment(A)`;
5. servidor responde endereço do peer 1;
6. peer 2 solicita o fragmento;
7. peer 1 envia;
8. peer 2 salva;
9. peer 2 envia `DatasetReady`;
10. desligar peer 1;
11. validar fallback para servidor.

Não use uma lista global de peers para tentar todos indiscriminadamente.

---

## Fase 8 — Reescrever os testes

A ordem recomendada:

### Testes unitários

1. serialização e desserialização de bytes;
2. `TrainingTask` round-trip;
3. `TaskResult` de sucesso;
4. `TaskResult` de erro;
5. fragmentação `(X, y)`;
6. reserva de tarefa;
7. conclusão;
8. falha e requeue;
9. timeout;
10. atualização atômica do melhor modelo;
11. snapshot e restauração.

### Testes de integração

1. servidor + peer entrando;
2. download de backup;
3. treinamento completo;
4. dois peers recebendo tarefas diferentes;
5. transferência peer-to-peer;
6. queda durante treinamento;
7. resultado atrasado;
8. resultado duplicado;
9. Pub/Sub;
10. promoção do pupilo.

---

## Fase 9 — Integrar Pub/Sub

O servidor deve criar um único `PubSubBroker`.

Registrar no `ServerMessenger`:

```python
MSG_PUBSUB_SUBSCRIBE
MSG_PUBSUB_UNSUBSCRIBE
MSG_PUBSUB_PUBLISH
```

No peer, registrar:

```python
MSG_PUBSUB_NOTIFY
```

Fluxo:

```text
Peer se inscreve em global_best_score
Servidor atualiza best_model
Coordinator coloca publicação na fila
PubSubClient do servidor publica
Broker envia notify
Peer atualiza cópia local do score
```

O Pub/Sub deve transmitir métricas, não necessariamente o modelo completo.

---

## Fase 10 — Adicionar recuperação de peers

Criem um monitor no servidor:

```python
async def task_monitor():
    while True:
        coordinator.check_task_status()
        await asyncio.sleep(5)
```

Mas substituam o timeout fixo de 30 segundos por configuração.

Uma tarefa de ML deveria enviar progresso ou heartbeat enquanto treina. O heartbeat de rede e o timeout de tarefa não devem ser exatamente a mesma coisa:

* peer vivo + tarefa longa: não reenfileirar;
* peer morto: remover nó e reenfileirar;
* peer vivo, mas tarefa travada: timeout da tarefa.

---

## Fase 11 — Integrar Maekawa e Lamport

Somente depois do sistema básico estável.

Trabalho necessário:

1. construir quóruns;
2. registrar handlers no `P2PNode`;
3. adicionar timeout ao pedido;
4. definir comportamento para quórum vazio;
5. adicionar o próprio nó ao cálculo corretamente;
6. usar timestamps Lamport nas mensagens;
7. liberar acesso em `finally`;
8. lidar com nó do quórum morto;
9. impedir espera infinita;
10. testar dois candidatos simultâneos.

Mesmo com Maekawa, o servidor continua precisando de `update_best_if_better()` atômico.

---

## Fase 12 — Integrar pupilo e Bully

Etapas:

1. selecionar explicitamente um peer pupilo;
2. informar o papel no `JoinAck` ou em mensagem separada;
3. replicar snapshot com versão monotônica;
4. receber ACK do snapshot;
5. não mandar snapshot completo a cada pequena mudança;
6. detectar queda real do servidor;
7. iniciar eleição;
8. anunciar ID, IP, porta e epoch do vencedor;
9. iniciar `ServerMessenger` no vencedor;
10. registrar handlers;
11. iniciar monitor de tarefas;
12. atualizar `server_ip/server_port` em todos os peers;
13. reenfileirar tarefas cujo estado seja incerto;
14. impedir split-brain.

O estado do novo coordenador não pode ser apenas retornado por `promote_to_server()`: ele precisa se tornar um processo servidor operacional.

---

## Fase 13 — Benchmark final

Somente após a correção funcional:

1. definir dataset fixo;
2. definir seed;
3. definir grid fixo;
4. medir execução sequencial;
5. medir execução com 1 peer;
6. medir com 2, 3 e 4 peers;
7. medir tempo de transferência;
8. medir tempo de treinamento;
9. medir overhead de coordenação;
10. simular queda de peer;
11. validar que todas as combinações foram executadas exatamente uma vez ou, em caso de retry, contabilizadas corretamente.

---

# Ordem dos primeiros arquivos a alterar

A sequência mais eficiente é:

1. **`main.py`** — reescrever a inicialização;
2. **`trainer.py`** — corrigir `f1_score` e envio de resultado;
3. **`protocol.py`** — alinhar `TaskResult`;
4. **`server_peer_protocol.py`** — remover o despacho duplicado no join;
5. **`coordinator.py`** — corrigir falha, timeout e atualização atômica;
6. **`global_table.py`** — criar operações atômicas e tipos consistentes;
7. **`data_thread.py`** — identidade, pastas exclusivas e `DatasetReady`;
8. **`peer_messenger.py`** — ACKs, retries e handlers;
9. **testes** — reescrever com os contratos novos;
10. **Pub/Sub**;
11. **heartbeat**;
12. **Maekawa/Lamport**;
13. **pupilo/Bully**.

## O que não mexer primeiro

Não comece por:

* eleição Bully;
* replicação do pupilo;
* Maekawa completo;
* otimização de conexões persistentes;
* vários modelos de ML;
* benchmark.

Primeiro façam **uma tarefa atravessar o sistema inteiro corretamente**. Depois ampliem uma dimensão de cada vez.
