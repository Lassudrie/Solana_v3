# Audit détaillé: le bot est-il à l'état de l'art pour l'arb Solana HFT ?

Date: 2026-03-25  
Base auditée: arbre courant du workspace `/root/bot/Solana_v3`  
Validation locale: `cargo test --workspace` et `cargo check --workspace` passent sur l'arbre courant

Note 2026-03-27: ce document est un audit date. L'implementation a deja diverge sur plusieurs points; il faut donc le lire comme une photo historique. En particulier, le submit live passe maintenant par un dispatcher async, et `source_latency` est desormais source-dependent au lieu d'etre uniformement nulle.

## Verdict exécutif

Verdict: **compétitif sur la plomberie**, **pas encore state of the art sur l'alpha et le profit live**.

Le dépôt ne souffre plus des blocages structurels du scaffold initial. Les briques suivantes sont déjà réelles:

- builder Solana avec compilation `Legacy`/`V0`;
- instructions venue-native Orca/Raydium;
- signature cryptographique locale ou via signer Unix;
- refresh asynchrone blockhash/slot/ALT/wallet;
- soumission Jito réelle;
- reconciliation RPC/WebSocket réelle.

L'écart restant vers un bot très compétitif se situe surtout sur:

1. **la fidélité du state marché**;
2. **le moteur de quote et de sizing**;
3. **la sécurité d'exécution autour du builder**;
4. **la mesure opérationnelle du live edge**.

## Scorecard

| Domaine | Note | Niveau |
|---|---:|---|
| Architecture générale | 8.5/10 | solide |
| Ingestion / ShredStream | 7/10 | crédible mais encore minimale |
| State plane / invalidation | 6.5/10 | propre, représentation marché encore légère |
| Quoting / alpha | 3.5/10 | non compétitif en live |
| Builder / exécution on-chain | 8/10 | réel, pas encore blindé |
| Signing | 8/10 | bon niveau pour hot wallet / secure unix |
| Submit / Jito | 7.5/10 | bon minimum public-prod |
| Reconciliation | 7/10 | réelle, encore simple |
| Observabilité / ops | 6.5/10 | utile, pas encore SLO-grade |
| Validation / tests | 7/10 | tests workspace ok, pas de preuve live/perf |

## Ce qui est déjà au bon niveau

### 1. Pipeline hot path cohérent

Le runtime conserve une séquence HFT propre: state -> sélection -> build -> sign -> submit -> tracking, sans RPC synchrone dans la décision chaude. Voir [runtime.rs](../../crates/bot/src/runtime.rs).

### 2. Builder venue-native réel

Le builder dans [transaction_builder.rs](../../crates/builder/src/transaction_builder.rs):

- compile des `Instruction` Solana réelles;
- gère `compute budget`, tip Jito et `VersionedMessage`;
- supporte `OrcaSimplePool`, `OrcaWhirlpool`, `RaydiumSimplePool` et `RaydiumClmm`;
- consomme de vraies ALT rafraîchies depuis le cold path.

Le blocage n'est donc plus "mémos au lieu de swaps". Il est maintenant dans la qualité du sizing, la validation des comptes et la robustesse d'exécution.

### 3. Signature réelle

[signer.rs](../../crates/signing/src/signer.rs) charge un vrai keypair ou parle à un signer Unix, signe réellement `compiled_message_bytes`, puis vérifie localement la signature avant sérialisation.

### 4. Cold path dynamique

[refresh.rs](../../crates/bot/src/refresh.rs) met à jour:

- `getLatestBlockhash`;
- `getSlot`;
- `getMultipleAccounts` pour les ALT;
- `getBalance`.

Ces mises à jour sont réinjectées dans l'état d'exécution par [runtime.rs](../../crates/bot/src/runtime.rs).

### 5. Reconciliation on-chain réelle

[onchain.rs](../../crates/reconciliation/src/onchain.rs) combine:

- expiration des soumissions;
- polling `getSignatureStatuses`;
- polling `getInflightBundleStatuses`;
- WebSocket `signatureSubscribe`.

Ce n'est plus un simple tracker local.

### 6. Validation locale rétablie

Le workspace compile et teste à nouveau proprement. Les décalages de fixtures entre types de prod et tests ont été corrigés dans `strategy`, `builder` et `signing`.

## Ce qui empêche encore le statut "state of the art"

### 1. Le modèle de marché reste trop simplifié

Le quote engine dans [quote.rs](../../crates/strategy/src/quote.rs) utilise encore:

- une taille unique `default_trade_size`;
- un pricing affine à partir de `price_bps` et `fee_bps`;
- aucune optimisation de taille;
- aucun coût d'échec ou de contention.

Pour du live trading compétitif, il manque:

- ladder de tailles entre `default_trade_size` et `max_trade_size`;
- coût complet `network fee + CU + Jito tip + risque d'échec`;
- expected value par route plutôt que profit brut;
- calibrage par venue et par congestion.

### 2. Le state plane est bien architecturé, mais trop pauvre pour le pricing

`PoolSnapshot` dans [types.rs](../../crates/state/src/types.rs) reste compact. Les décodeurs venue-specific de [decoder.rs](../../crates/state/src/decoder.rs) existent, mais le snapshot final ne transporte pas assez d'information pour un pricing CLMM/Whirlpool robuste sur plusieurs tailles.

Le chantier principal n'est pas l'invalidation, déjà propre, mais l'enrichissement des données de marché réellement exploitées par la stratégie.

### 3. L'exécution manque de garde-fous autour des comptes runtime

Le builder dérive des ATA utilisateur, mais il n'y a pas encore:

- création/prévalidation des ATA nécessaires;
- shadow simulation hors hot path;
- contrôle de complétude des comptes avant mise en production;
- calibration dynamique `CU/tip` par route.

### 4. L'ingestion reste minimale

[shredstream.rs](../../crates/detection/src/shredstream.rs) gère bien:

- UDP non bloquant;
- auth token;
- replay sur gap;
- reorder buffer minimal.

Mais il manque encore:

- couverture homogène de `source_latency` sur toutes les sources;
- redondance multi-feed/région;
- politique de reconnexion/backoff mieux structurée.

### 5. L'observabilité n'est pas encore au niveau d'un desk HFT

Le projet a:

- compteurs et latences cumulées dans [metrics.rs](../../crates/telemetry/src/metrics.rs);
- percentiles mémoire `p50/p95/p99` dans [observer.rs](../../crates/bot/src/observer.rs);
- endpoints opératoires dans [control.rs](../../crates/bot/src/control.rs).

Il manque encore:

- export primaire des percentiles au niveau control plane;
- série temporelle claire `source -> decision -> submit -> terminal`;
- ventilation par leader/région/canal;
- preuves live de landing rate, reject mix et qualité d'edge.

## Domaine par domaine

### Ingestion / ShredStream

Niveau: **crédible mais encore minimal**

Points forts:

- auth et replay sur gap réels;
- dédup/reorder de base;
- intégration simple dans le runtime.

Limites:

- `source_latency` dépend encore de la source: gRPC live peut la renseigner, UDP shredstream reste aujourd'hui sans timestamp amont;
- pas de redondance régionale;
- pas de preuve du gain d'avance informationnelle.

### State / invalidation

Niveau: **propre, extensible, pas encore market-ready**

Points forts:

- stale rejection;
- invalidation ciblée `account -> pool -> route`;
- warmup/freshness explicites.

Limites:

- snapshot trop léger pour pricing robuste;
- composition multi-comptes encore insuffisante.

### Quoting / risk

Niveau: **insuffisant pour profit live robuste**

Points forts:

- garde-fous corrects: wallet, blockhash, inflight, minimum de profit.

Limites:

- pas de sizing adaptatif;
- pas de coût complet;
- pas d'inventory/conflict risk;
- pas d'EV routing.

### Builder / signing

Niveau: **réel et exploitable**

Points forts:

- instructions Solana réelles;
- compilation `Legacy`/`V0`;
- vraie signature et vérification locale.

Limites:

- pas de simulation/validation runtime;
- pas de lifecycle de comptes token complet;
- pas de calibration live `CU/tip`.

### Submit / reconciliation

Niveau: **bon minimum public-prod**

Points forts:

- Jito HTTP réel, retry, idempotence, classification utile;
- suivi on-chain réel.

Limites:

- pas de stratégie multi-canal;
- pas de policy sophistiquée de replacement/réémission.

### Observabilité / opérations

Niveau: **utile pour itération, insuffisant pour desk HFT**

Points forts:

- métriques et endpoints opératoires;
- historique mémoire des signaux et trades.

Limites:

- export SLO incomplet;
- pas de preuve de perf/landing live.

## Priorités de remédiation

### P0
1. Remplacer la quote fixe par un sizing adaptatif avec coût complet.
2. Enrichir `PoolSnapshot` et les décodeurs pour porter un état marché réellement exécutable.
3. Ajouter des garde-fous d'exécution hors hot path: prévalidation comptes, ATA, shadow simulation.

### P1
1. Instrumenter `source_latency` et les métriques `source_to_submit` / `submit_to_terminal`.
2. Exporter les percentiles et ratios d'inclusion au niveau control plane.
3. Renforcer ingestion et submit par des politiques plus robustes par feed/canal.

### P2
1. Ajouter des preuves live/staging de landing rate et qualité d'edge.
2. Étendre la stratégie de routing/costing par congestion et par leader.

## Références internes utilisées

- [Runtime](../../crates/bot/src/runtime.rs)
- [Builder](../../crates/builder/src/transaction_builder.rs)
- [Quote](../../crates/strategy/src/quote.rs)
- [State types](../../crates/state/src/types.rs)
- [Refresh](../../crates/bot/src/refresh.rs)
- [Reconciliation](../../crates/reconciliation/src/onchain.rs)
