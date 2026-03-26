# HFT Bot Scaffold

> Archive: decrit l'etat initial du depot au stade scaffold.

## Vue d’ensemble

Ce workspace pose la fondation d’un bot Solana d’arbitrage atomique 2-leg orienté low-latency. Le design part d’un principe simple : le hot path ne doit lire que des données locales en RAM et transformer un événement normalisé en décision d’exécution sans RPC synchrone.

Pipeline cible :

`ShredStream -> normalisation -> state RAM -> invalidation ciblée -> quote local -> sélection -> build tx atomique 2-leg -> signature -> submit Jito -> suivi d’inclusion`

Le dépôt initial était vide. Le scaffold crée donc un workspace Rust multi-crates où chaque plane expose des contrats explicites, des enums métier et un wiring minimal de bout en bout.

## Cartographie du workspace

- `crates/detection`: ingestion et normalisation des événements. `ShredStreamSource` lit maintenant les événements ShredStream depuis UDP JSON et produit des `NormalizedEvent`.
- `crates/state`: state plane en RAM. `AccountStore`, `DecoderRegistry`, `DependencyGraph`, `PoolSnapshotStore`, `ExecutionState`, `WarmupManager`, `StatePlane`.
- `crates/strategy`: registre des routes, quote local sur `PoolSnapshot`, guards, selector, raisons de rejet.
- `crates/builder`: template de transaction atomique 2-leg, `BuildRequest`, `BuildResult`, envelope non signé.
- `crates/signing`: wallet runtime local, préconditions de signature, enveloppe signée.
- `crates/submit`: abstraction `Submitter`, stub `JitoSubmitter`, types de soumission et d’échec.
- `crates/reconciliation`: `ExecutionTracker`, `InclusionStatus`, `ExecutionOutcome`, historique, classification d’échec.
- `crates/telemetry`: compteurs pipeline, latences par stage, logs structurés mémoire.
- `crates/bot`: config globale, bootstrap et orchestrateur runtime.

## Hot Path vs Cold Path

### Hot path

Le hot path est formalisé dans [`runtime.rs`](/root/bot/Solana_v3/crates/bot/src/runtime.rs) via `HotPathPipeline` :

1. `StatePlane::apply_event`
2. `StrategyPlane::evaluate`
3. `AtomicArbTransactionBuilder::build`
4. `LocalWalletSigner::sign`
5. `Submitter::submit`
6. `ExecutionTracker::register_submission`

Contraintes codées dans cette ossature :

- pas de RPC synchrone dans quote/build ;
- la stratégie lit uniquement des `PoolSnapshot` dérivés ;
- les routes non warmées sont rejetées explicitement ;
- les updates obsolètes sont rejetées avant tout recalcul ;
- les raisons de rejet restent structurées.

### Cold path

Le cold path est explicité par `ColdPathServices` :

- `BlockhashService`
- `AltCacheService`
- `WalletRefreshService`
- `WarmupCoordinator`

Ces services injectent l’état nécessaire dans le hot path via `apply_cold_path_seed()`. Dans ce scaffold, ils restent simples et synchrones, mais l’interface prépare l’intégration future de services fondés sur RPC, Geyser, snapshots ou caches internes, hors boucle critique.

## Flux de données

1. `detection` produit un `NormalizedEvent`.
2. `state` applique l’update dans `AccountStore`.
3. `DependencyGraph` trouve les pools et routes impactés.
4. `DecoderRegistry` dérive les `PoolSnapshot`.
5. `WarmupManager` met à jour `WarmupStatus`.
6. `strategy` récupère les snapshots nécessaires, quote localement et applique les guards.
7. `builder` produit une `UnsignedTransactionEnvelope`.
8. `signing` produit une `SignedTransactionEnvelope`.
9. `submit` renvoie un `SubmitResult`.
10. `reconciliation` enregistre un `ExecutionRecord`.
11. `telemetry` compte les événements, rejets, builds, submits et latences par étape.

## Contrats métier déjà scaffoldés

Types principaux présents dans le code :

- `MarketEvent`
- `AccountUpdate`
- `PoolSnapshot`
- `RouteId`
- `RouteDefinition`
- `RouteState`
- `WarmupStatus`
- `NormalizedEvent` ([documentation dédiée](/root/bot/Solana_v3/docs/architecture/normalized-event.md))
- `FreshnessState`
- `OpportunityCandidate`
- `OpportunityDecision`
- `RejectionReason`
- `BuildRequest`
- `BuildResult`
- `SigningRequest`
- `SignedTransactionEnvelope`
- `SubmitRequest`
- `SubmitResult`
- `InclusionStatus`
- `ExecutionOutcome`

## Extensions prévues

### Nouvelles venues

Pour ajouter une venue :

1. ajouter un décodeur dans `state::decoder` si la structure de compte diffère ;
2. enrichir les `RouteLeg` avec les contraintes propres à la venue ;
3. compléter `AtomicTwoLegTemplate` pour générer les instructions réelles ;
4. ajouter les comptes/ALTs nécessaires dans le builder ;
5. compléter la classification d’échecs chain-side si la venue a des erreurs spécifiques.

### Nouvelles routes

Les routes se déclarent côté config dans `routes.definitions`. Le bootstrap :

1. enregistre la route dans `StrategyPlane` ;
2. relie `route -> pools` dans `StatePlane` ;
3. relie `account -> pool` avec le `decoder_key` ;
4. laisse ensuite le hot path recalculer uniquement les routes touchées.

## Ce qui reste à implémenter

- client ShredStream natif et gestion de gaps ;
- décodage réel des comptes Solana par venue ;
- snapshots pricing-ready fidèles aux pools cibles ;
- sizing dynamique et net profit complet incluant CU price, tip et inventory/risk ;
- builder Solana réel avec messages versionnés, ALTs et compute budget exact ;
- submit Jito réel avec retry policy, bundle mode avancé et stratégie multi-canaux ;
- tracker d’inclusion alimenté par sources chain-side réelles ;
- métriques exportables Prometheus/OTLP ;
- kill-switch et healthchecks pilotés par supervision externe.

## Tests fournis

Les tests valident le squelette et ses contrats :

- rejet d’un update obsolète dans `AccountStore` ;
- propagation `account -> pool -> route` dans `DependencyGraph` ;
- warmup d’une route après réception de tous les pools requis ;
- rejet d’une route non warmée dans `strategy` ;
- production d’un `OpportunityCandidate` structuré ;
- builder renvoyant un résultat structuré ;
- submitter Jito stub renvoyant un résultat structuré ;
- tracker d’exécution enregistrant une transition ;
- wiring minimal end-to-end depuis un événement jusqu’à une soumission acceptée.

## Notes d’implémentation

- Le scaffold privilégie des frontières nettes plutôt qu’une pseudo-implémentation on-chain incomplète.
- Les TODO implicites ont été évités. Les points restants sont localisables par module.
- La structure est conçue pour être complétée incrémentalement sans refonte majeure du pipeline.

## Activer Shredstream en production

Définir le mode de source runtime sur `shredstream` et utiliser le bloc `shredstream.endpoint` (port UDP local) :

```toml
[runtime.event_source]
mode = "shredstream"

[shredstream]
endpoint = "udp://127.0.0.1:10000"
```
