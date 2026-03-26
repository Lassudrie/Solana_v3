# Revue "State of the Art" de l’architecture bot Solana

> Archive: revue synthetique remplacee par l'audit detaille du 2026-03-25.

Date: 2026-03-25  
Version: révision après remise au vert de `cargo test --workspace`

## Verdict court

Le workspace n'est **pas encore state of the art** pour un bot Solana HFT réellement compétitif, mais il n'est plus au stade scaffold.

Le pipeline critique existe déjà de bout en bout:

- ingestion UDP/ShredStream avec auth et replay sur gap;
- state/invalidation/warmup;
- quote et sélection;
- build Solana réel avec messages `Legacy` et `V0`;
- signature cryptographique locale ou via signer Unix sécurisé;
- submit Jito réel;
- reconciliation RPC/WebSocket réelle.

L'écart principal n'est donc plus "avoir une transaction réelle", mais **avoir un moteur marché/exécution suffisamment fidèle pour gagner en live**.

## Ce qui est solide aujourd'hui

- séparation claire par crates et hot path sans RPC synchrone dans quote/build;
- builder venue-native dans [transaction_builder.rs](../../../crates/builder/src/transaction_builder.rs):
  - compute budget;
  - tip Jito;
  - compilation `VersionedMessage::V0` ou fallback legacy;
  - instructions Orca simple pool, Orca Whirlpool, Raydium simple pool et Raydium CLMM;
- signature réelle dans [signer.rs](../../../crates/signing/src/signer.rs):
  - chargement keypair fichier ou base58;
  - vérification locale de la signature;
  - support d'un signer Unix sécurisé;
- refresh asynchrone réel dans [refresh.rs](../../../crates/bot/src/refresh.rs):
  - `getLatestBlockhash`;
  - `getSlot`;
  - `getMultipleAccounts` pour les ALT;
  - `getBalance`;
- reconciliation réelle dans [onchain.rs](../../../crates/reconciliation/src/onchain.rs):
  - expiration des pending;
  - `getSignatureStatuses`;
  - `getInflightBundleStatuses`;
  - `signatureSubscribe`;
- observabilité opératoire utile:
  - `/status`, `/ready`, `/live`, `/metrics`;
  - historique mémoire de signaux avec `p50/p95/p99` dans [observer.rs](../../../crates/bot/src/observer.rs).

## Écarts encore bloquants

### 1) Quote / sizing encore trop simplifiés

- Le moteur de quote dans [quote.rs](../../../crates/strategy/src/quote.rs) reste affine:
  - une seule taille = `default_trade_size`;
  - pas de ladder de tailles;
  - pas de coût complet `CU + tip + probabilité d'échec`;
  - pas de sélection par expected value.
- C'est aujourd'hui le principal frein à la profitabilité live.

### 2) State marché trop pauvre

- `PoolSnapshot` dans [types.rs](../../../crates/state/src/types.rs) ne porte qu'un sous-ensemble de l'information réellement utile au pricing:
  - `price_bps`;
  - `fee_bps`;
  - `reserve_depth`;
  - mints, tick spacing, current tick.
- Les décodeurs `orca-whirlpool-v1` et `raydium-clmm-v1` existent dans [decoder.rs](../../../crates/state/src/decoder.rs), mais le snapshot agrégé reste trop réduit pour un sizing fiable.
- Il manque encore la composition multi-comptes réellement exploitable pour la décision live.

### 3) Sécurité d'exécution incomplète

- Le builder dérive des ATA utilisateur mais ne gère pas leur création ni leur prévalidation.
- Pas de shadow simulation ou calibration live avant exécution.
- Pas de contrôle explicite "route prête à trader" sur complétude des comptes runtime au-delà du build.
- Pas de politique avancée de remplacement/réémission.

### 4) Ingestion et mesure d'avance encore minimales

- `auth_token` et `replay_on_gap` sont bien actifs dans [shredstream.rs](../../../crates/detection/src/shredstream.rs).
- En revanche:
  - `source_latency` reste à `Duration::ZERO`;
  - pas de redondance multi-source/région;
  - pas de reconnexion/backoff structurés côté feed.

### 5) Observabilité encore partielle pour un desk HFT

- Les percentiles existent dans l'observer mémoire, mais pas comme export primaire Prometheus/SLO.
- Le control plane expose surtout des compteurs et des latences cumulées.
- Il manque encore:
  - `source -> decision -> submit -> terminal` comme série SLO exploitable;
  - ventilation par leader/région/canal;
  - suivi clair landing rate vs expired vs failed.

## Niveau actuel par domaine

| Domaine | Niveau | Note |
|---|---:|---|
| Architecture | 8.5/10 | bonne base, propre et extensible |
| Ingestion | 7/10 | crédible, encore minimale |
| State plane | 6.5/10 | bonne plomberie, représentation marché trop pauvre |
| Quoting / alpha | 3.5/10 | insuffisant pour profit live robuste |
| Builder / signing | 8/10 | réel et exploitable, pas encore blindé |
| Submit / reconciliation | 7.5/10 | base crédible public-prod |
| Observabilité | 6.5/10 | meilleure que le scaffold, encore incomplète |
| Validation | 7/10 | `cargo test --workspace` passe, pas de preuve live/perf |

## Priorités de remédiation

### P0
1. Remplacer la quote fixe par un sizing adaptatif avec coût total complet.
2. Enrichir `PoolSnapshot` et les décodeurs pour porter un état marché exécutable.
3. Ajouter prévalidation runtime des comptes et calibration d'exécution hors hot path.

### P1
1. Instrumenter `source_latency` et les métriques `source_to_submit` / `submit_to_terminal`.
2. Exporter percentiles et ratios d'inclusion au niveau control plane/Prometheus.
3. Renforcer le feed avec redondance, reconnect et backoff.

### P2
1. Ajouter validation live/staging et preuves de landing rate.
2. Étendre la politique de submit/réconciliation par canal et par congestion.

## Références internes utilisées

- [Runtime](../../../crates/bot/src/runtime.rs)
- [Builder](../../../crates/builder/src/transaction_builder.rs)
- [Signing](../../../crates/signing/src/signer.rs)
- [Refresh](../../../crates/bot/src/refresh.rs)
- [Reconciliation](../../../crates/reconciliation/src/onchain.rs)
