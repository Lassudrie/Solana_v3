# Crate `bot`

## Rôle

`bot` assemble toutes les autres crates en un runtime exploitable:

- parsing de config
- bootstrap du hot path et du cold path
- daemon principal et boucle d'évènements
- dispatch asynchrone build/sign puis submit
- monitoring HTTP et CLI/TUI
- health policy des pools/routes live

## Points d'entrée

- `main.rs`: binaire `bot`
- `daemon.rs`: boucle runtime principale
- `runtime.rs`: moteur hot path
- `bootstrap.rs`: construction de tous les composants à partir de `BotConfig`

## Invariants

- `state.max_snapshot_slot_lag` et `strategy.max_snapshot_slot_lag` doivent matcher.
- Le runtime seed initial (blockhash, wallet, ALT cache) est injecté avant de traiter le flux.
- Les chemins build/sign et submit peuvent être asynchrones et bornés par des files de capacité finie.
- Le monitoring et la route health policy ne doivent pas perturber le hot path.

## Inventaire des fichiers

| Fichier | Rôle | Eléments clés |
| --- | --- | --- |
| `crates/bot/src/lib.rs` | Ré-export central | expose bootstrap, config, daemon, observer, runtime, route health |
| `crates/bot/src/main.rs` | Entrée binaire `bot` | résolution du chemin de config, lancement du daemon |
| `crates/bot/src/bootstrap.rs` | Assemblage du runtime | validation config, résolution routes, signer, cold path services |
| `crates/bot/src/config.rs` | Schéma de config | `BotConfig` et toutes les sections TOML/JSON |
| `crates/bot/src/runtime.rs` | Hot path | `BotRuntime`, `HotPathPipeline`, `HotPathReport`, intégration stratégie/build/sign/submit |
| `crates/bot/src/daemon.rs` | Boucle daemon | `BotDaemon`, workers de réconciliation, intégration refresh et dispatchers |
| `crates/bot/src/control.rs` | Health HTTP simple | `RuntimeStatus`, `RuntimeIssue`, métriques Prometheus |
| `crates/bot/src/observer.rs` | Observabilité métier | snapshots monitor, rejections, trades, edge capture, serveur HTTP du monitor |
| `crates/bot/src/route_health.rs` | Politique de santé live | `RouteHealthRegistry`, quarantines, `ShadowOnly`, synthèse pool/route |
| `crates/bot/src/live.rs` | Adaptateur live detection -> bot | hooks repairs/déstabilisation, construction `TrackedPool`, source gRPC live |
| `crates/bot/src/sources.rs` | Sélection de la source d'évènements | JSONL, UDP JSON, gRPC ShredStream |
| `crates/bot/src/refresh.rs` | Cold path asynchrone | refresh blockhash, slot, ALT, wallet |
| `crates/bot/src/execution_context.rs` | Snapshot exécution courant | blockhash, leader, ALTs, wallet, kill switch |
| `crates/bot/src/build_sign_dispatch.rs` | Dispatcher build/sign | file bornée, workers, détection de congestion |
| `crates/bot/src/submit_dispatch.rs` | Dispatcher submit | workers, coordination des retries, ordre stable par séquence |
| `crates/bot/src/submit_factory.rs` | Construction du submitter | mapping config -> Jito/RPC/router |
| `crates/bot/src/account_batcher.rs` | Ré-export helpers detection | batcher RPC et cache ALT dans le namespace `bot` |
| `crates/bot/src/rpc.rs` | Ré-export RPC helpers | `rpc_call`, `RpcError`, `RpcRateLimitBackoff` |
| `crates/bot/src/bin/botctl.rs` | CLI/TUI de monitor | appels HTTP au monitor, vues terminales, export snapshot/live manifest |

## `config.rs`

`BotConfig` regroupe les sections suivantes:

- `shredstream`
- `state`
- `routes`
- `strategy`
- `builder`
- `signing`
- `submit`
- `jito`
- `rpc_submit`
- `reconciliation`
- `risk`
- `runtime`

Le module gère aussi:

- fusion overlay TOML/JSON sur défauts
- profils runtime, dont `UltraFast`
- schéma détaillé des legs d'exécution Orca/Raydium
- sélection de la source d'évènements
- paramètres de monitor et de health live set

## `bootstrap.rs`

Le bootstrap:

- enregistre les décodeurs `state`
- convertit les routes de config en `RouteDefinition` et `RouteExecutionConfig`
- valide les invariants des routes et de l'environnement d'exécution
- résout le signer réel
- seed blockhash, ALT cache et wallet via RPC
- construit `BotRuntime` avec le `submit_mode` demandé

## `runtime.rs`

`BotRuntime` maintient:

- `StatePlane`
- `ExecutionContext`
- `StrategyPlane`
- builder
- signer
- submitter
- tracker de réconciliation
- telemetry

Le hot path produit un `HotPathReport` qui sert à la fois au monitoring, au submit async
et au suivi post-trade.

## `daemon.rs`

Le daemon:

- choisit la source d'évènements
- draine les refreshs asynchrones
- traite les évènements en boucle
- distribue build/sign et submit sur des dispatchers bornés
- alimente le worker de réconciliation
- met à jour `RuntimeStatus` et l'observer
- gère les issues de runtime: route warmup, congestion, wallet, source défaillante, kill switch

## `observer.rs` et `botctl`

L'observer expose une API HTTP locale pour:

- overview runtime
- pools
- routes
- signals de pipeline
- rejections
- trades
- edge capture
- snapshot agrégé

`botctl` consomme cette API en CLI simple ou via une TUI Ratatui.
