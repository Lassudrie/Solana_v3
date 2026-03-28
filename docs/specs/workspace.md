# Vue workspace

## Pipeline principal

Le workspace implémente un pipeline séquentiel mais découplé par crates:

1. `detection` ingère des évènements marché et les normalise en `domain::NormalizedEvent`.
2. `state` applique ces évènements pour maintenir snapshots, quote models et warmup route.
3. `strategy` quote les routes impactées, applique les garde-fous et choisit une opportunité.
4. `builder` convertit un candidat en message Solana non signé.
5. `signing` signe localement ou via socket Unix sécurisé.
6. `submit` soumet via Jito, RPC ou routage hybride.
7. `reconciliation` suit le devenir on-chain et classe les issues.
8. `bot` orchestre l'ensemble, gère les refreshs, le monitoring et la health policy.
9. `telemetry` fournit métriques et journal structuré en mémoire.

## Dépendances internes

Le graphe cible du dépôt est le suivant:

```text
domain
├── detection
├── state
├── strategy
├── builder
├── signing
├── submit
└── reconciliation

bot -> detection/state/strategy/builder/signing/submit/reconciliation/domain/telemetry
signerd -> signing
```

Le script `scripts/check_crate_boundaries.py` vérifie ces frontières via `cargo metadata`.

## Invariants transverses

- Les contrats marché partagés vivent dans `domain`, pas dans `detection`.
- Le hot path n'ajoute pas d'appel RPC synchrone opportuniste au moment du trading.
- Les états et quote models sont versionnés par `slot` et `write_version`.
- La stratégie ne traite qu'un sous-ensemble de routes impactées par l'évènement courant.
- Le builder refuse explicitement les plans non profitables, trop vieux, trop gros ou incomplets.
- Le submit est idempotent par signature côté Jito et RPC.
- La réconciliation sépare le résultat de soumission du résultat d'inclusion on-chain.

## Crates

| Crate | Rôle | Contrats clés |
| --- | --- | --- |
| `domain` | Types partagés et contrats de données | `NormalizedEvent`, `PoolSnapshot`, `ExecutionSnapshot`, `ExecutablePoolState` |
| `detection` | Ingestion, ShredStream, live reducer, sources replay/UDP | `MarketEventSource`, `GrpcEntriesEventSource`, `ShredStreamSource` |
| `state` | Store mémoire des comptes, snapshots, quote models et warmup | `StatePlane`, `DecoderRegistry`, `WarmupManager` |
| `strategy` | Registre de routes, quote local, guards, sélection | `StrategyPlane`, `RouteDefinition`, `OpportunityCandidate` |
| `builder` | Construction de transaction versionnée | `AtomicArbTransactionBuilder`, `UnsignedTransactionEnvelope` |
| `signing` | Signers locaux et distants | `Signer`, `LocalWalletSigner`, `SecureUnixWalletSigner` |
| `signerd` | Service séparé de signature Unix socket | `SecureSignerService` |
| `submit` | Jito, RPC, routage et retry | `Submitter`, `JitoSubmitter`, `RpcSubmitter`, `RoutedSubmitter` |
| `reconciliation` | Tracking post-submit et lecture on-chain | `ExecutionTracker`, `OnChainReconciler`, `FailureClass` |
| `telemetry` | Métriques et logs structurés | `TelemetryStack`, `PipelineMetrics`, `MemoryLogSink` |
| `bot` | Bootstrap, runtime, daemon, observer, control plane | `BotRuntime`, `BotDaemon`, `ObserverHandle`, `BotConfig` |

## Binaries

| Binaire | Source | Usage |
| --- | --- | --- |
| `bot` | `crates/bot/src/main.rs` | Runtime principal du bot |
| `botctl` | `crates/bot/src/bin/botctl.rs` | CLI/TUI locale de monitoring |
| `signerd` | `crates/signerd/src/main.rs` | Service de signature Unix socket |
| `shredstream_proxy` | `crates/detection/src/bin/shredstream_proxy.rs` | Proxy gRPC ShredStream scaffold |

## Config et manifests

- La config runtime complète est portée par `bot::config::BotConfig`.
- Les manifests `.toml` à la racine décrivent l'univers de routes et d'exécution.
- Les scripts Python génèrent ou filtrent ces manifests pour les profils live.
- `render_bot_config.sh` transforme un manifest source en config de déploiement du bot.

## Documents d'architecture utiles

- `../architecture/crate-boundaries.md`
- `../architecture/normalized-event.md`
- `../architecture/triangular-arbitrage.md`

Les specs de cette arborescence décrivent la mise en oeuvre concrète de ces choix.
