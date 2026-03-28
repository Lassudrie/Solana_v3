# Crate `detection`

## Rôle

`detection` ingère des évènements marché depuis plusieurs sources et les transforme en
`domain::NormalizedEvent`. La crate contient à la fois des sources simples de replay/UDP
et le pipeline live gRPC ShredStream avec réduction locale d'états de pools.

## Sous-systèmes

- Sources simples: replay JSONL et UDP JSON
- Source UDP ShredStream avec gestion de séquence et replay sur gap
- Source gRPC live `GrpcEntriesEventSource`
- Batcher `getMultipleAccounts` et cache d'address lookup tables
- Proxy gRPC ShredStream scaffold
- Helpers RPC avec gestion de backoff rate-limit

## Invariants

- Le schéma `NormalizedEvent` n'est pas possédé ici: il est ré-exporté depuis `domain`.
- Les sources exposent toutes le trait `MarketEventSource`.
- Les chemins live maintiennent des barrières `slot/write_version` pour ne pas régresser l'état.
- Le reducer live peut tourner en `Disabled`, `Shadow` ou `Active` selon la config par venue.

## Inventaire des fichiers

| Fichier | Rôle | Eléments clés |
| --- | --- | --- |
| `crates/detection/src/lib.rs` | Ré-export de la crate | Surface publique de toutes les sources et helpers |
| `crates/detection/src/events.rs` | Ré-export du contrat d'évènements | `pub use domain::events::*` |
| `crates/detection/src/ingestor.rs` | Interface commune d'ingestion | `MarketEventSource`, `IngestError` |
| `crates/detection/src/sources.rs` | Sources de replay et UDP JSON | `JsonlFileEventSource`, `UdpJsonEventSource`, parsing de wire events |
| `crates/detection/src/shredstream.rs` | Source UDP ShredStream | `ShredStreamSource`, séquencement, détection de trous, replay |
| `crates/detection/src/live.rs` | Pipeline gRPC live et reducer | `GrpcEntriesEventSource`, tracked pools, hooks de repair, réduction d'états, invalidation, refresh |
| `crates/detection/src/account_batcher.rs` | Batcher `getMultipleAccounts` | `GetMultipleAccountsBatcher`, `FetchedAccounts`, cache ALT, `decode_lookup_table` |
| `crates/detection/src/rpc.rs` | Client RPC générique | `rpc_call`, `RpcError`, `RpcRateLimitBackoff` |
| `crates/detection/src/proxy.rs` | Serveur gRPC proxy ShredStream | `ShredstreamProxyService`, `ShredstreamProxyHandle`, `serve_shredstream_proxy` |
| `crates/detection/src/proto.rs` | Inclusion des stubs gRPC générés | `tonic::include_proto!` |
| `crates/detection/build.rs` | Génération des stubs protobuf | compile `proto/shared.proto` et `proto/shredstream.proto` |
| `crates/detection/src/bin/shredstream_proxy.rs` | CLI du proxy gRPC | parsing des flags, runtime Tokio, lancement serveur |
| `crates/detection/proto/shared.proto` | Types protobuf partagés | socket, timestamps, structures utilitaires |
| `crates/detection/proto/shredstream.proto` | API protobuf ShredStream | heartbeat, stream d'entries, service proxy |

## `live.rs`: ce que fait le gros module

`live.rs` concentre le chemin le plus sophistiqué du dépôt:

- définition des hooks de repair et de déstabilisation
- stockage live d'`ExecutablePoolState` et de quote models concentrés
- description des pools suivis via `TrackedPool` et `TrackedPoolRegistry`
- connexion gRPC au proxy ShredStream et parsing des entries Solana legacy/v0
- résolution dynamique des ALTs nécessaires pour interpréter les transactions
- réduction exacte ou shadow des mutations de swap sur pools simples et CLMM
- publication d'évènements normalisés:
  - `PoolSnapshotUpdate`
  - `PoolQuoteModelUpdate`
  - `PoolInvalidation`
  - `SlotBoundary`
  - `DestabilizingTransaction`
- orchestration d'un worker de repair/refresh pour recharger l'état quand le reducer ne suffit pas

## Config live

`GrpcEntriesConfig` configure:

- endpoint gRPC
- capacités de buffers et backoff de reconnexion
- seuils de price impact/dislocation
- limite de repairs simultanées
- univers de `TrackedPool`
- lookup tables à précharger pour la résolution des transactions

## Contrats importants avec le reste du système

- `bot::live` adapte les hooks live vers l'observer et la route health policy.
- `state` consomme les `NormalizedEvent` produits ici, pas l'état live interne.
- `builder` et `strategy` partagent les mêmes primitives CLMM via `domain`.
