# Arquitetura do Sistema

- **Componentes**: Leader HTTP API, Peers subscritos via pubsub, nó IPFS (add/pin + pubsub), índice FAISS em memória nos peers (simulado via Map), pipeline de embeddings.
- **Fluxo principal**: Cliente envia ficheiro → Leader adiciona ao IPFS, gera embeddings e nova versão do vetor → Enfileira `document-update` e envia em blocos/heartbeats → Peers validam versão, guardam vetor/embeddings pendentes e enviam `vector-confirmation` com hash → Leader recolhe confirmações, atinge quorum e envia `vector-commit` → Peers aplicam commit e atualizam índice FAISS em memória.
- **Fail-stop do leader**: Leader envia blocos/heartbeats periódicos (`LEADER_HEARTBEAT_INTERVAL_MS`, default 5s). Peers monitorizam `lastHeartbeatAt`; se exceder `PEER_HEARTBEAT_TIMEOUT_MS` (default 15s) sem heartbeat, assumem falha.
- **Quorum**: Confirmações com hash correto. Pode ser fixo (`PEER_QUORUM`) ou maioria dos peers observados/`PEER_COUNT_HINT`.

## Diagrama de Classes
```mermaid
classDiagram
  class Leader {
    +documentVectors:Array
    +confirmationLog:Map
    +pendingLeaderUpdates:Array
    +flushLeaderBlock()
    +computeQuorum()
    +commitVector()
    +startLeaderHeartbeat()
    +startLeaderConfirmationListener()
  }

  class Peer {
    +vectorVersions:Array
    +pendingEmbeddings:Map
    +faissIndex:Map
    +lastHeartbeatAt:Number
    +storeIncomingVersion()
    +handleDocumentUpdate()
    +applyCommit()
    +handleLeaderBlock()
    +touchHeartbeat()
    +startHeartbeatMonitor()
  }

  class IPFS {
    +add(file)
    +pubsubPub(topic,msg)
    +pubsubSub(topic)
  }

  Leader --> IPFS : pubsub pub/sub
  Peer --> IPFS : pubsub sub
  Leader ..> Peer : vector-update/commit blocks
  Peer ..> Leader : confirmations
```

## Diagrama de Sequência
```mermaid
sequenceDiagram
  participant Client
  participant Leader
  participant IPFS
  participant Peer1
  participant Peer2

  Client->>Leader: POST /files (filename, file)
  Leader->>IPFS: add(file)
  IPFS-->>Leader: CID
  Leader->>Leader: gera embeddings, nova versão vetor
  Leader->>IPFS: pubsub pub leader-block (document-update)
  loop cada peer
    IPFS-->>Peer1: leader-block
    Peer1->>Peer1: valida versão, guarda vetor/embeddings pendentes
    Peer1->>IPFS: pubsub pub vector-confirmation(hash)
  end
  IPFS-->>Leader: vector-confirmation (Peer1)
  IPFS-->>Leader: vector-confirmation (Peer2)
  Leader->>Leader: verifica quorum
  Leader->>IPFS: pubsub pub vector-commit
  IPFS-->>Peer1: vector-commit
  Peer1->>Peer1: aplica commit, atualiza índice
  IPFS-->>Peer2: vector-commit
  Peer2->>Peer2: aplica commit, atualiza índice
  Note over Peer1,Peer2: Se heartbeats cessarem > timeout, peers assumem falha do líder
```
