# Diagnóstico técnico — Hyperparalelizer

## 1. Situação atual

O projeto tem algumas fundações aproveitáveis:

- enquadramento de mensagens TCP com cabeçalho de tamanho;
    
- suporte a bytes via Base64;
    
- servidor TCP assíncrono;
    
- estrutura inicial de modelos e avaliação;
    
- ideias separadas para coordenação, transferência P2P, Pub/Sub, Maekawa, Lamport e Bully.  
    Entretanto, **o fluxo principal não consegue completar atualmente**:
    

> servidor inicia → peer entra → recebe tarefa → baixa dataset → treina → envia resultado → servidor atualiza melhor modelo.

Não é um problema isolado. Há incompatibilidades de contratos, estado duplicado, handlers ausentes e componentes que foram implementados seguindo arquiteturas diferentes.

---

# 2. Irregularidades críticas

## [FIXED] 2.1. O contrato de entrada na rede está incompatível

O `DataThread` espera que a resposta do servidor contenha:

- `fragment_id`;
    
- lista de `peers`;
    
- primeira `task`.
    

Porém o servidor responde apenas com um `Ack` contendo `ref_type` e `ref_id`. Portanto, o peer entra na rede, mas recebe:

```python
{
    "type": "Ack",
    "ref_type": "JoinNetwork",
    "ref_id": "<node_id>"
}
```

Os campos que o peer procura ficam como `None`, lista vazia e tarefa vazia.

### Consequência

O peer não sabe:

- qual é seu ID definitivo;
    
- quais fragmentos precisa baixar;
    
- de quais peers pode baixar;
    
- qual treinamento deve executar.
    

### Correção necessária

Criar uma mensagem explícita:

```python
@dataclass
class JoinAck:
    node_id: str
    required_fragment_ids: list[str]
    fragment_sources: dict[str, list[dict]]
    task: dict | None
    type: str = field(default="JoinAck", init=False)
```

Não reutilizar o `Ack` genérico para transportar o estado inicial da rede.

---

## 2.2. O ID do peer fica inconsistente

O servidor gera o `node_id` usando hash de `IP:porta`, mas o peer já é construído com outro `node_id` antes de entrar na rede. O servidor retorna o novo ID em `ref_id`, porém o `DataThread` não atualiza o próprio `node_id`, nem o ID do:

- `P2PNode`;
    
- `PeerMessenger`;
    
- `TrainerNode`;
    
- `MaekawaMutex`;
    
- `PubSubClient`.
    

### Consequência

O mesmo peer pode aparecer com IDs diferentes dentro do sistema.

### Solução recomendada

Centralizar a criação do ID:

```python
def generate_node_id(ip: str, port: int) -> str:
    return sha256(f"{ip}:{port}".encode()).hexdigest()
```

O peer calcula o ID antes de construir seus componentes, e o servidor apenas valida o valor recebido. Isso também respeita a especificação.

---

## [FIXED] 2.3. A `GlobalTable` contém dois modelos diferentes de fragmentos 

Existem simultaneamente:

```python
self.fragments_dataset
```

e:

```python
self.fragments
```

O primeiro guarda localizações usando hash do nome do fragmento. O segundo é criado dinamicamente pelo `Coordinator` e guarda os dados dos fragmentos.

O próprio construtor de `GlobalTable` não declara `fragments` nem `system_state`; o `Coordinator` adiciona esses atributos em tempo de execução.

### Consequência

Cada módulo entende “fragmentos” de uma maneira:

- `Coordinator`: `GlobalTable.fragments`;
    
- métodos de localização: `GlobalTable.fragments_dataset`;
    
- snapshot: copia `fragments`, mas não copia `fragments_dataset`;
    
- peer: trabalha com nomes como `fragment_0000`;
    
- tabela: transforma nomes em hashes.
    

### Correção necessária

Usar um único formato:

```python
self.fragment_payloads = {
    "fragment_0000": bytes,
}

self.fragment_locations = {
    "fragment_0000": {"node_a", "node_b"},
}
```

O hash pode continuar existindo como identificador externo, mas não deve criar uma segunda tabela conceitualmente diferente.

---

## [FIXED] 2.4. O snapshot da `GlobalTable` não pode ser enviado

A tabela armazena objetos Python completos:

- instâncias de `Peer`;
    
- instâncias de `TrainingTask`;
    
- estruturas com referências mutáveis.
    

O serializer aceita basicamente:

- dicionários;
    
- listas;
    
- bytes;
    
- tipos JSON simples.
    

Ao tentar serializar um snapshot real, o envio falha porque `Peer` não é serializável em JSON. Além disso, o snapshot não inclui `fragments_dataset`, e uma `GlobalTable` reconstruída a partir dele não possui esse atributo.

### Correção necessária

A tabela deve guardar apenas estruturas simples:

```python
nodes[node_id] = {
    "node_id": node_id,
    "ip": ip,
    "port": port,
    "memory_available_mb": 1024,
    "status": "ready",
}
```

As tarefas também devem ser armazenadas como dicionários, ou convertidas com `to_dict()` antes de entrar na tabela.

---

## [FIXED] 2.5. Uma tarefa é retirada da fila, mas nunca é enviada

Ao adicionar um peer, o `Coordinator` chama:

```python
self.assign_hyperparameters_to_peer(peer)
```

Esse método:

- remove uma tarefa de `task_pool`;
    
- coloca a tarefa em `assigned_tasks`;
    
- marca o peer como ocupado.
    

Porém o retorno é descartado. A tarefa não vai no `JoinAck` e também não é enviada por TCP.

O método `distribute_model_and_hyperparameters()` também apenas retorna pares `(peer, task)`; ele não envia as tarefas.

### Consequência

A tarefa fica presa em `assigned_tasks` até expirar.

### Correção necessária

A atribuição e o envio precisam ser uma única operação controlada:

```python
async def dispatch_next_task(peer_id: str) -> bool:
    task = reserve_task(peer_id)

    reply = await send_once(
        peer.ip,
        peer.port,
        task,
        expect_reply=True,
    )

    if not valid_ack(reply):
        unassign_and_requeue(task)
        return False

    mark_task_running(task.task_id)
    return True
```

---

## 2.6. O dataset nunca é semeado na rede

O peer tenta obter cada fragmento nesta ordem:

1. armazenamento local;
    
2. outro peer;
    
3. backup do servidor.
    

Essa lógica é razoável. Porém:

- nenhum peer recebe inicialmente os bytes do fragmento;
    
- o servidor não possui handler registrado para `RequestFragmentBackup`;
    
- o servidor marca um peer como proprietário do fragmento antes de ele realmente possuir o arquivo;
    
- `DatasetReady` não possui handler no servidor.
    

### Consequência

O primeiro peer pergunta aos outros peers, mas nenhum possui o fragmento. Depois pergunta ao servidor, que não reconhece a mensagem.

### Fluxo correto

1. Servidor fragmenta e guarda todos os fragmentos.
    
2. Primeiro peer baixa do servidor.
    
3. Peer salva o arquivo.
    
4. Peer envia `DatasetReady`.
    
5. Somente nesse momento o servidor adiciona o peer em `fragment_locations`.
    
6. Próximos peers podem baixar dele via P2P.
    

---

## 2.7. O loader do dataset não existe nos anexos

O `TrainerNode` chama:

```python
X, y = self.dataset_loader.load(fragment_ids)
```

Não há implementação anexada de `dataset_loader`.

Também não está definido o formato do conteúdo de cada `.bin`.

### Correção necessária

Definir um único formato. Por exemplo:

```python
fragment_data = {
    "X": X_fragment,
    "y": y_fragment,
}
```

Salvar com `pickle` ou `joblib`, e criar:

```python
class DatasetLoader:
    def load(self, fragment_ids):
        fragments = [...]
        X = concatenate(...)
        y = concatenate(...)
        return X, y
```

Para o projeto, a regra mais coerente é:

> Cada configuração de hiperparâmetros treina sobre o mesmo dataset completo. Os fragmentos servem para transporte e armazenamento distribuído, não para cada peer treinar sobre uma parte diferente dos dados.

---

## 2.8. Resultados de treinamento com erro quebram o servidor

Quando o treino falha, o peer envia:

```python
"f1_score": None,
"error": "..."
```

O servidor executa:

```python
float(metrics.get("f1_score", 0.0))
```

Isso produz `TypeError`.

Pior: a tarefa é removida de `assigned_tasks` antes dessa conversão. Portanto, a tarefa pode ser perdida definitivamente após um erro.

### Correção necessária

Separar sucesso e falha:

```python
if msg["status"] == "failed":
    requeue_task(task_id)
    record_failure(...)
    return
```

Somente remover definitivamente a tarefa após validar o resultado.

---

## [FIXED] 2.9. O servidor acessa um atributo que não existe

O protocolo do servidor usa:

```python
coordinator.best_model
```

Mas o `Coordinator` não possui `best_model`. O valor está em:

```python
coordinator.GlobalTable.best_model
```

Isso ocorre tanto ao verificar um novo melhor resultado quanto ao atender `RequestBestModel`.

### Correção necessária

Nunca acessar o atributo diretamente. Criar uma API única:

```python
coordinator.get_best_model()
coordinator.update_best_model(...)
```

Internamente, esses métodos usam a `GlobalTable`.

---

## 2.10. O “melhor modelo” não contém o modelo

O servidor salva apenas:

- métricas;
    
- ID da tarefa;
    
- ID do peer;
    
- hiperparâmetros.
    

Não salva os bytes do modelo treinado.

Quando alguém solicita o melhor modelo, o servidor serializa esse dicionário de metadados e o envia como se fosse o modelo.

Ao mesmo tempo, o peer possui uma função para enviar seu modelo real, mas o servidor não registra handler para `SendBestModel`.

### Solução mais simples para o MVP

Incluir `model_bytes` no `TaskResult`.

```python
{
    "type": "TaskResult",
    "status": "success",
    "metrics": {...},
    "model_bytes": pickle.dumps(model),
}
```

O servidor descarta os bytes quando o resultado não for o melhor.

Mais tarde, isso pode virar um fluxo de duas etapas:

1. peer envia métricas;
    
2. servidor identifica o vencedor;
    
3. servidor solicita somente o modelo vencedor.
    

---
## [FIXED] 2.11. O `trainer.py` possui import quebrado

Existe:

```python
from peer.peer_inner_protocol import PupilPromoted
```

Na estrutura informada, o correto seria:

```python
from hyperparalelizer.peer.peer_inner_protocol import PupilPromoted
```

Ao reconstruir os diretórios e importar os módulos, esse foi o primeiro import que efetivamente quebrou.

O arquivo também importa `threading`, mas não o utiliza, e mistura responsabilidades de:

- treinamento;
    
- réplica de estado;
    
- promoção de servidor;
    
- criação de `Coordinator`.
    

Essas responsabilidades devem ser separadas.

---

## 2.12. A promoção do pupilo não funciona

O `TrainerNode.promote_to_server()` contém vários problemas:

- copia o snapshot inteiro para `tabela_recuperada.nodes`;
    
- chama o construtor com argumento `GlobalTable=`, mas o argumento se chama `global_table`;
    
- escreve `task_pool` e `best_model` no `Coordinator`, quando esses estados ficam na tabela;
    
- não inicia um novo `ServerMessenger`;
    
- não muda os outros peers para o endereço do novo coordenador.
    

### Correção necessária

A lógica do pupilo deve sair do `TrainerNode` e virar um componente separado, por exemplo:

```text
peer/
└── pupil_manager.py
```

---

## 2.13. Faltam handlers de rede essenciais

O registro principal do servidor contém somente:

- `JoinNetwork`;
    
- `TaskResult`;
    
- `RequestBestModel`;
    
- `KeepAlive`.
    

Não há handlers visíveis para:

- `DatasetReady`;
    
- `RequestFragmentBackup`;
    
- `SendBestModel`;
    
- `SyncState`;
    
- mensagens Pub/Sub;
    
- mensagens Maekawa;
    
- mensagens Bully.
    

O arquivo `server_inner_protocol.py` anexado contém somente um comentário, portanto atualmente não cumpre função operacional.

---

## 2.14. Maekawa pode ficar esperando para sempre

O mutex:

```python
await self.grant_event.wait()
```

não possui timeout.

Outros problemas:

- quórum vazio nunca libera o acesso;
    
- não há geração dos quóruns;
    
- não há garantia de interseção entre quóruns;
    
- exige `len(quorum)` concessões;
    
- não trata peer do próprio nó;
    
- conta concessões, mas não identifica quem concedeu;
    
- concessões duplicadas podem ser contadas;
    
- os handlers não estão visivelmente registrados;
    
- falha de um membro do quórum pode travar todos os resultados.
    

### Inconsistência na própria especificação

A especificação fala em “maioria do quórum”. Para implementar Maekawa propriamente, cada nó deve receber permissão de **todos os membros do seu request set**, e os request sets precisam possuir interseção.

Usar apenas a maioria de cada pequeno quórum pode quebrar a exclusão mútua.

---

## 2.15. O peer solicita Maekawa pelo critério errado (REVIEW)

O peer entra na exclusão mútua quando o resultado supera seu próprio `best_score`.

Isso não significa que ele supere o melhor resultado global. O primeiro treinamento de cada peer quase sempre será seu melhor local, mesmo que seja muito pior do que o recorde global.

### Correção

O peer deve manter o último `global_best_score` recebido pelo Pub/Sub e só iniciar Maekawa quando:

```python
candidate_score > known_global_best_score
```

O servidor ainda deve repetir a comparação atomicamente, porque o valor conhecido pelo peer pode estar desatualizado.

---

## 2.16. Bully responde pelo canal errado

O iniciador abre uma conexão com `expect_reply=True`, esperando receber `BullyAlive` naquela conexão.

Porém o handler abre uma nova conexão para uma porta retirada de:

```python
msg.get("port", 5000)
```

A mensagem de eleição nem sequer contém `port`. Assim, o iniciador provavelmente expira o timeout e pode acreditar que venceu.

### Correção

Responder na própria conexão:

```python
await send_message(writer, alive_msg)
```

Além disso, quando um nó maior responde, o código apenas executa `pass`. Precisa haver um timeout para aguardar o anúncio do novo coordenador e reiniciar a eleição caso ele nunca chegue.

---

## 2.17. O sistema ainda não entrega CP

Atualmente a réplica é enviada de forma assíncrona, sem:

- número de versão;
    
- confirmação;
    
- época do líder;
    
- consenso;
    
- quorum para promoção;
    
- prevenção de dois coordenadores simultâneos.
    

Com Bully e snapshot assíncrono, pode ocorrer:

- promoção com estado antigo;
    
- dois servidores ativos;
    
- duas versões diferentes do melhor modelo.
    

Portanto, a implementação atual não sustenta a afirmação de consistência forte e tolerância a partição.  
Para realmente sacrificar disponibilidade em favor de consistência, o pupilo não deve assumir a liderança sem alcançar o quorum necessário.

---

## 2.18. Pub/Sub não está conectado ao restante do sistema

O broker e o cliente existem, mas não há inicialização visível que:

- registre os handlers do broker;
    
- inscreva os peers;
    
- registre `handle_notify` no `P2PNode`;
    
- atualize o melhor score conhecido pelo trainer.
    

Há também uma inconsistência:

- o `Coordinator` coloca `lamport_clock` na fila;
    
- o listener procura `lamport`.
    

Assim o valor tende a voltar para zero.  
A remoção de subscribers mortos também não funciona corretamente: `send_once` captura erros e retorna `None`, mas o broker remove apenas resultados que sejam instâncias de `Exception`.

---

## 2.19. O timeout de tarefas é muito curto

O timeout é fixado em 30 segundos.

Um treinamento de MLP, XGBoost ou LightGBM pode legitimamente demorar mais. Uma tarefa em execução pode ser redistribuída enquanto o peer original ainda treina.

### Correção

Usar estados e progresso:

```text
ASSIGNED → DOWNLOADING → TRAINING → COMPLETED
```

E atualizar `last_progress_at` com heartbeat ou mensagens de progresso.

---

## 2.20. Os testes locais podem mascarar erros

Todos os peers usam por padrão:

```python
data/fragments
```

Se dois peers forem executados na mesma máquina, ambos podem ler os mesmos arquivos locais. Isso faz parecer que a transferência P2P funcionou quando, na realidade, um peer apenas encontrou o arquivo criado pelo outro processo.

### Correção

Usar:

```text
data/nodes/<node_id>/fragments/
```

---

# 3. Ordem recomendada de trabalho

## Fase 0 — Congelar um MVP

Antes de corrigir código, definir este primeiro objetivo:

> Um servidor e dois peers, executados localmente em portas diferentes, treinam Random Forest sobre o mesmo dataset, cada peer recebe configurações diferentes, e o servidor termina com o melhor modelo real.

Para esse primeiro marco, deixar desativados:

- Maekawa;
    
- Bully;
    
- pupilo;
    
- Pub/Sub;
    
- conexões persistentes;
    
- XGBoost e LightGBM.
    

O servidor pode proteger o melhor modelo com o lock local da `GlobalTable`.

### Critério de conclusão

Uma execução completa deve terminar sem intervenção manual e imprimir:

```text
Total de tarefas: N
Tarefas concluídas: N
Melhor score: ...
Melhores hiperparâmetros: ...
```

---

## Fase 1 — Fazer o projeto importar e iniciar

### Tarefas

1. Corrigir o import de `PupilPromoted`.
    
2. Escolher um único nome entre:
    
    - `peer_outer_protocol.py`;
        
    - `peer_peer_protocol.py`.
        
3. Padronizar `server_peer_protocol.py` dentro de uma única pasta.
    
4. Adicionar `__init__.py` aos pacotes.
    
5. Confirmar `utils/logger.py`.
    
6. Confirmar todas as dependências no `requirements.txt`.
    
7. Criar um `main.py` com comandos claros:
    

```bash
python -m hyperparalelizer.main server ...
python -m hyperparalelizer.main peer ...
```

### Critério de conclusão

Todos estes comandos devem funcionar:

```bash
python -m compileall .
python -c "import hyperparalelizer.peer.trainer"
python -c "import hyperparalelizer.server.coordinator"
```

---

## Fase 2 — Reescrever os contratos de mensagens

Criar contratos explícitos para:

```text
JoinNetwork
JoinAck
DatasetReady
TrainingTask
TaskAccepted
TaskResult
RequestFragment
FragmentData
RequestFragmentBackup
Error
KeepAlive
```

Adicionar campos de controle:

```python
message_id
node_id
timestamp
status
error
```

### Regras

- não misturar `f1` e `f1_score`;
    
- usar sempre `node_id`, não alternar com `id_node`;
    
- usar sempre `port`, não alternar com `porta`;
    
- nenhuma mensagem deve depender de campos informais adicionados manualmente;
    
- respostas devem validar também `ref_type`, não apenas `"type": "Ack"`.
    

### Critério de conclusão

Criar testes de ida e volta:

```python
original == decode_body(encode(original)[4:])
```

Incluindo bytes.

---

## Fase 3 — Reescrever a `GlobalTable`

Estrutura mínima:

```python
class GlobalTable:
    nodes: dict
    fragment_payloads: dict
    fragment_locations: dict
    pending_tasks: list
    assigned_tasks: dict
    completed_tasks: dict
    best_model: dict | None
    system_state: str
```

Cada nó deve ser um dicionário JSON-safe.

Cada tarefa deve ter:

```python
{
    "task_id": "...",
    "status": "pending",
    "assigned_node_id": None,
    "attempt": 0,
    "created_at": ...,
    "last_progress_at": ...,
    "payload": {...},
}
```

### Critério de conclusão

Isto deve funcionar:

```python
snapshot = table.get_snapshot()
frame = encode({"type": "SyncState", "snapshot": snapshot})
restored = GlobalTable.from_snapshot(snapshot)
```

---

## Fase 4 — Fazer a distribuição de dataset funcionar

### No servidor

1. Fragmentar o dataset.
    
2. Serializar cada fragmento.
    
3. Guardar uma cópia de backup.
    
4. Implementar `RequestFragmentBackup`.
    
5. Implementar `DatasetReady`.
    
6. Não registrar localização antes da confirmação.
    

### No peer

1. Iniciar o `P2PNode`.
    
2. Registrar `RequestFragment`.
    
3. Entrar na rede.
    
4. Baixar os fragmentos necessários.
    
5. Salvar em diretório exclusivo.
    
6. Notificar o servidor para cada fragmento adquirido.
    
7. Implementar o `DatasetLoader`.
    

### Critério de conclusão

Teste com dois peers:

- peer A baixa do servidor;
    
- peer B baixa pelo menos um fragmento do peer A;
    
- desligar o peer A;
    
- peer B continua conseguindo montar o dataset usando servidor ou outra réplica.
    

---

## Fase 5 — Fazer o ciclo de tarefas funcionar

### Ordem do fluxo

1. Servidor cria todas as combinações do Grid Search.
    
2. Peer entra e fica `READY`.
    
3. Servidor reserva uma tarefa.
    
4. Servidor envia `TrainingTask`.
    
5. Peer responde `TaskAccepted`.
    
6. Peer baixa dataset.
    
7. Peer treina.
    
8. Peer envia `TaskResult`.
    
9. Servidor valida o resultado.
    
10. Servidor atualiza o melhor modelo.
    
11. Servidor envia a próxima tarefa.
    

### Regras importantes

- a tarefa só vira `assigned` depois do envio confirmado;
    
- falha de envio devolve a tarefa para a fila;
    
- resultado com erro deve reintroduzir a tarefa;
    
- resultado duplicado deve ser ignorado;
    
- resultado atrasado não deve sobrescrever uma tentativa mais nova;
    
- medir `tempo_treino_s`;
    
- o servidor precisa saber quando todas as tarefas terminaram.
    

### Critério de conclusão

Executar dez combinações entre dois peers e terminar com dez tarefas concluídas, sem tarefas presas.

---

## Fase 6 — Armazenar o modelo real

Para o primeiro MVP, colocar `model_bytes` no `TaskResult`.

O `best_model` deve possuir:

```python
{
    "task_id": "...",
    "node_id": "...",
    "model_type": "random_forest",
    "hyperparameters": {...},
    "metrics": {...},
    "model_bytes": b"...",
}
```

### Critério de conclusão

Depois da execução:

```python
model = pickle.loads(best_model["model_bytes"])
model.predict(X_test)
```

deve funcionar.

---

## Fase 7 — Criar os testes fundamentais

### Testes unitários

1. Serializer com bytes.
    
2. Contratos de mensagens.
    
3. Geração determinística do `node_id`.
    
4. Operações da `GlobalTable`.
    
5. Snapshot e restauração.
    
6. Grid Search.
    
7. Avaliação de modelos.
    
8. Loader de fragmentos.
    

### Testes de integração

1. servidor + um peer;
    
2. servidor + dois peers;
    
3. transferência peer-to-peer;
    
4. resultado com erro;
    
5. tarefa acima do timeout antigo;
    
6. peer que cai durante treinamento;
    
7. resultado duplicado;
    
8. fragmento inexistente.
    

### Critério de conclusão

Os testes básicos devem passar antes da introdução de Bully e Maekawa.

---

## Fase 8 — Adicionar recuperação de peers

Ligar o `KeepAliveManager` ao `Coordinator`.

Ao detectar peer morto:

1. remover o nó;
    
2. remover suas localizações de fragmentos;
    
3. reintroduzir sua tarefa na fila;
    
4. publicar evento de falha;
    
5. distribuir a tarefa para outro peer.
    

Também criar um loop periódico de scheduler. O método `check_task_status()` não serve sozinho se ninguém o executar.

### Critério de conclusão

Matar um peer durante um treinamento e observar a tarefa sendo executada por outro.

---

## Fase 9 — Conectar Pub/Sub e Lamport

Somente depois do ciclo principal funcionar:

1. registrar handlers do `PubSubBroker`;
    
2. registrar `handle_notify` nos peers;
    
3. inscrever todos em `global_best_score`;
    
4. corrigir `lamport` versus `lamport_clock`;
    
5. incrementar Lamport antes de publicar;
    
6. atualizar Lamport ao receber;
    
7. guardar no peer o melhor score global conhecido.
    

### Critério de conclusão

Quando um peer gerar novo recorde, os demais devem receber a atualização sem consultar o servidor.

---

## Fase 10 — Implementar Maekawa corretamente

### Tarefas

1. definir geração de quóruns intersectantes;
    
2. adicionar self-vote ou remover o próprio nó do quórum;
    
3. exigir todos os votos do request set;
    
4. guardar IDs dos nós que concederam voto;
    
5. ignorar grants duplicados;
    
6. adicionar timeout;
    
7. adicionar cancelamento;
    
8. tratar membro morto;
    
9. registrar os três handlers no `P2PNode`;
    
10. liberar voto corretamente após `Release`.
    

### Fluxo

```text
score local > global_best conhecido
        ↓
solicita Maekawa
        ↓
obtém todos os grants
        ↓
envia resultado ao servidor
        ↓
servidor compara novamente e atualiza atomicamente
        ↓
peer libera quórum
```

### Critério de conclusão

Dois peers tentando atualizar simultaneamente nunca podem entrar na região crítica ao mesmo tempo.

---

## Fase 11 — Implementar pupilo e Bully

Criar componentes separados:

```text
peer/
├── pupil_manager.py
└── leader_election.py
```

### Replicação

O snapshot deve possuir:

```python
{
    "epoch": 3,
    "version": 81,
    "leader_id": "...",
    "state": {...},
}
```

O pupilo precisa confirmar a versão recebida.

### Bully

1. responder `BullyAlive` pela conexão existente;
    
2. aguardar anúncio do coordenador com timeout;
    
3. reiniciar eleição se o anúncio não chegar;
    
4. atualizar endereço do servidor em todos os componentes;
    
5. iniciar `ServerMessenger` no vencedor;
    
6. impedir promoção sem quorum;
    
7. usar `epoch` para rejeitar mensagens de líderes antigos.
    

### Critério de conclusão

Após derrubar o servidor:

- apenas um peer assume;
    
- tarefas pendentes continuam;
    
- tarefas atribuídas são recuperadas;
    
- o melhor modelo permanece;
    
- peers deixam de enviar mensagens ao servidor antigo.
    

---

# 4. Ordem sugerida de arquivos para alteração

Trabalhar nesta sequência:

1. `utils/protocol.py`
    
2. `hyperparalelizer/global_table.py`
    
3. `utils/serializer.py`
    
4. `hyperparalelizer/server/coordinator.py`
    
5. `hyperparalelizer/server_peer_protocol.py`
    
6. `hyperparalelizer/server/server_messenger.py`
    
7. `hyperparalelizer/peer/data_thread.py`
    
8. `hyperparalelizer/peer/download_worker.py`
    
9. `hyperparalelizer/peer/peer_outer_protocol.py`
    
10. novo `dataset_loader.py`
    
11. `hyperparalelizer/peer/peer_messenger.py`
    
12. `hyperparalelizer/peer/trainer.py`
    
13. `hyperparalelizer/main.py`
    
14. testes do MVP
    
15. `core/pubsub.py`
    
16. `sync/maekawa.py`
    
17. `sync/bully.py`
    
18. lógica do pupilo
    

Não começar corrigindo Bully ou Maekawa. Eles dependem de identidade, mensageria, estado e ciclo de tarefas funcionando corretamente.

---

# 5. Primeiro conjunto de commits

## Commit 1 — Organização

```text
fix: normalize package structure and imports
```

- corrigir imports;
    
- renomear protocolos;
    
- adicionar `__init__.py`;
    
- criar configuração básica;
    
- garantir importação completa.
    

## Commit 2 — Contratos

```text
refactor: define canonical network message contracts
```

- `JoinAck`;
    
- nomes padronizados;
    
- resultados com `status`;
    
- erros estruturados.
    

## Commit 3 — Estado

```text
refactor: rebuild GlobalTable with serializable state
```

- remover objetos `Peer` e `TrainingTask` da tabela;
    
- unificar fragmentos;
    
- corrigir snapshot.
    

## Commit 4 — Dataset

```text
feat: implement server-backed fragment distribution
```

- handler de backup;
    
- `DatasetReady`;
    
- loader;
    
- armazenamento por node ID.
    

## Commit 5 — Treinamento

```text
feat: complete task dispatch and result lifecycle
```

- envio real;
    
- ACK;
    
- resultado;
    
- próxima tarefa;
    
- modelo serializado.
    

## Commit 6 — Integração

```text
test: add one-server two-peer end-to-end test
```

Somente depois desses seis commits o restante da arquitetura distribuída deve ser religado.

---

# 6. Fluxo mínimo que deve funcionar primeiro

```text
Servidor
  ├─ carrega dataset
  ├─ cria fragmentos
  ├─ gera Grid Search
  └─ começa a escutar

Peer A
  ├─ começa a escutar
  ├─ entra na rede
  ├─ baixa fragmentos do servidor
  ├─ confirma DatasetReady
  ├─ recebe tarefa
  ├─ treina
  └─ envia resultado + modelo

Peer B
  ├─ começa a escutar
  ├─ entra na rede
  ├─ baixa fragmentos do Peer A
  ├─ confirma DatasetReady
  ├─ recebe tarefa
  ├─ treina
  └─ envia resultado + modelo

Servidor
  ├─ compara os resultados
  ├─ salva o melhor modelo
  ├─ distribui novas tarefas
  └─ encerra quando a fila estiver vazia
```

Esse é o ponto em que o sistema deixa de ser um conjunto de módulos e passa a ser um produto executável.