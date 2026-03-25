# Audit détaillé: le bot est-il à l'état de l'art pour l'arb Solana HFT ?

Date: 2026-03-25  
Base auditée: arbre courant du workspace `/root/bot/Solana_v3`  
Référentiel: hybride

- Standard 1: capacités publiques vérifiables via docs Solana et Jito
- Standard 2: estimation de l'écart restant vers les meilleurs searchers privés

## Verdict exécutif

Verdict: **partiellement compétitif**, mais **pas state of the art** pour un bot Solana arb HFT de production.

Le dépôt a dépassé le simple scaffold. Il possède déjà un pipeline cohérent, des garde-fous utiles, un submitter Jito HTTP réel, une signature Solana réelle, un refresh asynchrone du state d'exécution, et une reconciliation on-chain minimale. En revanche, les deux points qui empêchent encore le statut "state of the art" sont structurels:

1. **Le moteur de marché reste synthétique**: le décodage de pools, les snapshots et le quoting ne modélisent pas les DEX Solana réels.
2. **Le builder n'exécute pas de swaps venue-native**: il compile bien un `VersionedMessage`, mais les deux "legs" sont encore des mémos, pas des instructions Orca/Raydium/Phoenix/Whirlpool exécutables.

En pratique, l'architecture est bonne, la discipline hot-path est correcte, mais l'alpha engine et l'exécution venue-specific ne sont pas encore au niveau d'un searcher HFT moderne.

## Méthodologie

L'évaluation s'appuie sur:

- lecture du code courant du workspace, avec références fichier/ligne;
- validation locale via `cargo test`, qui passe sur l'arbre courant au 2026-03-25;
- comparaison avec les attentes publiques Solana/Jito pour:
  - versioned transactions et Address Lookup Tables;
  - refresh blockhash/balance et suivi `getSignatureStatuses` / `signatureSubscribe`;
  - low latency transaction sending, bundles, ShredStream.

Références officielles utilisées:

- Jito Docs: <https://docs.jito.wtf/>
- Jito Low Latency Transaction Send: <https://docs.jito.wtf/lowlatencytxnsend/>
- Jito ShredStream: <https://docs.jito.wtf/lowlatencytxnfeed/>
- Solana Versioned Transactions: <https://solana.com/developers/guides/advanced/versions>
- Solana Address Lookup Tables: <https://solana.com/developers/guides/advanced/lookup-tables>
- Solana `getLatestBlockhash`: <https://solana.com/docs/rpc/http/getlatestblockhash>
- Solana `getBalance`: <https://solana.com/docs/rpc/http/getbalance>
- Solana `getSignatureStatuses`: <https://solana.com/docs/rpc/http/getsignaturestatuses>
- Solana `signatureSubscribe`: <https://solana.com/docs/rpc/websocket/signaturesubscribe>

## Scorecard

| Domaine | Note | Niveau |
|---|---:|---|
| Architecture générale | 8.5/10 | solide |
| Ingestion / ShredStream | 7/10 | crédible mais encore minimal |
| State plane / invalidation | 7/10 | propre mais synthétique |
| Quoting / alpha | 3/10 | non HFT réel |
| Builder / exécution on-chain | 3.5/10 | message Solana réel, legs non réels |
| Signing | 7.5/10 | correct pour un hot wallet local |
| Submit / Jito | 7.5/10 | bon minimum public-prod |
| Reconciliation | 6.5/10 | minimale mais réelle |
| Observabilité / ops | 6/10 | utile mais pas SLO-grade |
| Validation / tests | 6.5/10 | cohérent, peu de preuves perf/live |

## Ce qui est déjà au bon niveau

### 1. Le pipeline hot-path est cohérent

Le runtime garde une séquence HFT propre: state -> sélection -> build -> sign -> submit -> tracking, sans RPC synchrone dans le coeur du quote/build. Cela se voit dans [`runtime.rs`](/root/bot/Solana_v3/crates/bot/src/runtime.rs#L255) à [`runtime.rs`](/root/bot/Solana_v3/crates/bot/src/runtime.rs#L369).

Points positifs:

- rejet précoce des updates stales avant recalcul, [`runtime.rs`](/root/bot/Solana_v3/crates/bot/src/runtime.rs#L267);
- décision uniquement sur routes impactées, [`state/src/lib.rs`](/root/bot/Solana_v3/crates/state/src/lib.rs#L196);
- garde-fous wallet, blockhash, inflight et profit minimal, [`guards.rs`](/root/bot/Solana_v3/crates/strategy/src/guards.rs#L66).

### 2. Le submit Jito n'est plus un stub

Le module `submit` fait réellement des appels HTTP JSON-RPC vers les endpoints Jito, avec:

- `sendTransaction` et `sendBundle`, [`jito.rs`](/root/bot/Solana_v3/crates/submit/src/jito.rs#L336);
- auth header `x-jito-auth`, [`jito.rs`](/root/bot/Solana_v3/crates/submit/src/jito.rs#L26), [`jito.rs`](/root/bot/Solana_v3/crates/submit/src/jito.rs#L353);
- retry/backoff, [`jito.rs`](/root/bot/Solana_v3/crates/submit/src/jito.rs#L81);
- cache d'idempotence par signature, [`jito.rs`](/root/bot/Solana_v3/crates/submit/src/jito.rs#L263), [`jito.rs`](/root/bot/Solana_v3/crates/submit/src/jito.rs#L427);
- classification partielle des rejets et exploitation de `Retry-After` / `x-wait-to-retry-ms`, [`jito.rs`](/root/bot/Solana_v3/crates/submit/src/jito.rs#L137), [`jito.rs`](/root/bot/Solana_v3/crates/submit/src/jito.rs#L513).

Pour un bot encore en montée de maturité, c'est une base sérieuse.

### 3. La signature est une vraie signature Solana

Le dépôt n'utilise plus de signature factice. `LocalWalletSigner` charge un keypair depuis fichier ou base58 et signe `message_bytes` via `try_sign_message`, [`signer.rs`](/root/bot/Solana_v3/crates/signing/src/signer.rs#L75), [`signer.rs`](/root/bot/Solana_v3/crates/signing/src/signer.rs#L159).

Le module vérifie aussi:

- préconditions wallet, [`signer.rs`](/root/bot/Solana_v3/crates/signing/src/signer.rs#L141);
- cohérence du pubkey attendu, [`signer.rs`](/root/bot/Solana_v3/crates/signing/src/signer.rs#L152);
- sérialisation de transaction signée, [`signer.rs`](/root/bot/Solana_v3/crates/signing/src/signer.rs#L117).

### 4. Le cold path n'est plus figé

Le runtime applique des refresh asynchrones pour:

- `getLatestBlockhash`, [`refresh.rs`](/root/bot/Solana_v3/crates/bot/src/refresh.rs#L132);
- `getSlot` comme révision d'ALT, [`refresh.rs`](/root/bot/Solana_v3/crates/bot/src/refresh.rs#L142);
- `getBalance`, [`refresh.rs`](/root/bot/Solana_v3/crates/bot/src/refresh.rs#L153).

Les mises à jour sont réinjectées dans l'état d'exécution du hot path via [`runtime.rs`](/root/bot/Solana_v3/crates/bot/src/runtime.rs#L191). Ce point rapproche le bot d'un standard public-prod crédible.

### 5. La reconciliation on-chain existe réellement

Le module `reconciliation` gère:

- expiration de soumissions trop anciennes, [`onchain.rs`](/root/bot/Solana_v3/crates/reconciliation/src/onchain.rs#L109);
- polling RPC `getSignatureStatuses`, [`onchain.rs`](/root/bot/Solana_v3/crates/reconciliation/src/onchain.rs#L120), [`onchain.rs`](/root/bot/Solana_v3/crates/reconciliation/src/onchain.rs#L253);
- polling `getInflightBundleStatuses`, [`onchain.rs`](/root/bot/Solana_v3/crates/reconciliation/src/onchain.rs#L161), [`onchain.rs`](/root/bot/Solana_v3/crates/reconciliation/src/onchain.rs#L281);
- souscriptions websocket `signatureSubscribe`, [`onchain.rs`](/root/bot/Solana_v3/crates/reconciliation/src/onchain.rs#L334).

Ce n'est pas encore une boucle d'exécution "institutionnelle", mais ce n'est plus un tracker purement local.

## Ce qui empêche le statut "state of the art"

### 1. Le builder compile un message Solana réel, mais pas une transaction d'arb réelle

Le builder crée un `VersionedMessage::V0` avec compute budget, ALTs et tip system transfer, [`transaction_builder.rs`](/root/bot/Solana_v3/crates/builder/src/transaction_builder.rs#L91), [`transaction_builder.rs`](/root/bot/Solana_v3/crates/builder/src/transaction_builder.rs#L133), [`transaction_builder.rs`](/root/bot/Solana_v3/crates/builder/src/transaction_builder.rs#L148).

Le problème critique est ailleurs: les deux legs sont des instructions `Memo`, pas des swaps réels:

- programme Memo comme pseudo-leg, [`transaction_builder.rs`](/root/bot/Solana_v3/crates/builder/src/transaction_builder.rs#L22), [`transaction_builder.rs`](/root/bot/Solana_v3/crates/builder/src/transaction_builder.rs#L105);
- contenu des legs généré comme texte template/route/pool/min_out, [`templates.rs`](/root/bot/Solana_v3/crates/builder/src/templates.rs#L38).

Conséquence:

- pas d'instruction venue-native;
- pas de comptes SPL réels;
- pas d'orchestration token accounts / ATA / vaults;
- pas de CU calibration par venue;
- pas de simulation de swap réelle;
- pas de validation on-chain du chemin d'arb.

Tant que ce point n'est pas traité, le bot ne peut pas être considéré HFT arb state of the art.

### 2. Le modèle de quote n'est pas un modèle de marché Solana réel

Le quote engine applique un simple `price_bps` puis `fee_bps` sur deux snapshots, [`quote.rs`](/root/bot/Solana_v3/crates/strategy/src/quote.rs#L45), [`quote.rs`](/root/bot/Solana_v3/crates/strategy/src/quote.rs#L97).

Le décodeur de pool ne lit que:

- `price_bps`;
- `fee_bps`;
- `reserve_depth`;

depuis un buffer brut, [`decoder.rs`](/root/bot/Solana_v3/crates/state/src/decoder.rs#L47).

Il manque tout ce qui fait la difficulté réelle de l'arb Solana:

- courbes AMM spécifiques par venue;
- tick arrays / concentrated liquidity;
- virtual reserves / oracle guards;
- impact slippage sur taille marginale;
- frais multiples;
- risque d'échec selon comptes intermédiaires;
- sizing adaptatif;
- inventory risk et conflict risk.

En d'autres termes, le moteur décide aujourd'hui sur un proxy pédagogique, pas sur une représentation financière exploitable contre le marché réel.

### 3. Les Address Lookup Tables sont synthétiques

Le builder fabrique un `AddressLookupTableAccount` local à partir de `derived_pubkey`, [`transaction_builder.rs`](/root/bot/Solana_v3/crates/builder/src/transaction_builder.rs#L77), [`transaction_builder.rs`](/root/bot/Solana_v3/crates/builder/src/transaction_builder.rs#L91), [`transaction_builder.rs`](/root/bot/Solana_v3/crates/builder/src/transaction_builder.rs#L221).

Le refresh ALT, lui, assimile l'ALT revision au slot courant, [`refresh.rs`](/root/bot/Solana_v3/crates/bot/src/refresh.rs#L142).

Cela donne une bonne interface logicielle, mais pas une gestion réelle des ALT on-chain:

- pas de fetch et cache du contenu des tables;
- pas de validation de présence des comptes nécessaires;
- pas de gestion de vieillissement / rotation / fallback no-ALT.

### 4. La télémétrie n'est pas encore au niveau SLO/perf HFT

Le projet exporte des compteurs et des latences cumulées par stage, [`metrics.rs`](/root/bot/Solana_v3/crates/telemetry/src/metrics.rs#L25), et les expose en Prometheus via le control plane, [`control.rs`](/root/bot/Solana_v3/crates/bot/src/control.rs#L159).

Mais il manque encore:

- distributions P50/P95/P99;
- séparation hot-path CPU vs latence réseau;
- latence "market data -> quote -> sign -> submit -> land";
- budget de jitter;
- corrélation slot/leader/region;
- alerting d'inclusion, de drop, de stale blockhash, de rate limit.

Autre limite importante: `source_latency` est encore forcée à `Duration::ZERO` dans l'événement normalisé, [`shredstream.rs`](/root/bot/Solana_v3/crates/detection/src/shredstream.rs#L405), donc la mesure d'avance informationnelle reste incomplète.

### 5. Le state plane est propre mais encore synthétique

Le state plane applique bien:

- invalidation ciblée `account -> pool -> route`, [`state/src/lib.rs`](/root/bot/Solana_v3/crates/state/src/lib.rs#L171);
- refresh de fraîcheur sur boundaries de slot, [`state/src/lib.rs`](/root/bot/Solana_v3/crates/state/src/lib.rs#L127);
- warmup route par dépendances, [`state/src/lib.rs`](/root/bot/Solana_v3/crates/state/src/lib.rs#L196).

Mais l'information stockée dans `PoolSnapshot` reste très pauvre. Pour du HFT réel, la qualité d'un state plane se juge surtout sur la fidélité des snapshots, pas sur la seule propreté de l'invalidation. Ici, la plomberie est bonne, la représentation marché ne l'est pas encore.

## Domaine par domaine

### Ingestion / ShredStream

Niveau: **crédible mais encore minimal**

Points forts:

- socket UDP non bloquante, [`shredstream.rs`](/root/bot/Solana_v3/crates/detection/src/shredstream.rs#L169);
- auth token validé, [`shredstream.rs`](/root/bot/Solana_v3/crates/detection/src/shredstream.rs#L417);
- gestion des gaps avec replay request si activé, [`shredstream.rs`](/root/bot/Solana_v3/crates/detection/src/shredstream.rs#L147), [`shredstream.rs`](/root/bot/Solana_v3/crates/detection/src/shredstream.rs#L180);
- reorder buffer minimal, [`shredstream.rs`](/root/bot/Solana_v3/crates/detection/src/shredstream.rs#L229).

Limites:

- pas de multi-source ni redondance régionale;
- pas de stratégie explicite de reconnexion/backoff socket;
- pas de mesure réelle du gain d'avance vs RPC/Geyser;
- `source_latency` non renseignée.

Comparaison state of the art public:

- bon alignement avec l'idée Jito ShredStream;
- encore loin d'une infra de searcher avec régionalisation, déduplication multi-feed et mesure d'avance micro/millisecond.

### State / invalidation

Niveau: **propre, extensible, pas encore market-ready**

Points forts:

- graphe de dépendances propre;
- stale rejection;
- freshness gating;
- warmup explicite.

Limites:

- un seul décodeur `pool-price-v1`, [`decoder.rs`](/root/bot/Solana_v3/crates/state/src/decoder.rs#L39);
- pas de décodage venue-specific réel;
- pas d'état de comptes SPL ou orderbook;
- pas de multi-account composition par venue.

### Quoting / risk

Niveau: **insuffisant pour HFT**

Points forts:

- garde-fous simples mais corrects: wallet ready, blockhash fraîche, inflight cap, profit minimal, [`guards.rs`](/root/bot/Solana_v3/crates/strategy/src/guards.rs#L66).

Limites:

- quote purement affine;
- pas de side-specific economics malgré `SwapSide` dans le registry;
- pas de sizing dynamique;
- pas de coût complet incluant failure probability, contention, expected landing price.

### Builder

Niveau: **partiellement crédible, non venue-native**

Points forts:

- vrai `VersionedMessage`;
- compute budget explicite;
- `jito_tip_lamports`;
- structure d'enveloppe propre.

Limites bloquantes:

- legs = mémos;
- comptes dérivés synthétiquement;
- ALT synthétique;
- pas de simulation;
- pas de fallback/fanout par canal;
- pas de templates DEX.

### Signing

Niveau: **bon minimum**

Points forts:

- keypair file ou base58;
- validation de cohérence de pubkey;
- vraie signature curve-compatible.

Limites:

- hot wallet local uniquement;
- pas de séparation opératoire HSM/remote signer;
- pas de rotation / zeroization / multi-signer policy;
- pas de gestion avancée du secret lifecycle.

### Submit / Jito

Niveau: **bon minimum public-prod**

Points forts:

- endpoints Jito réels;
- bundle ou single;
- auth, retry, timeout, idempotence;
- interprétation minimale de rejets.

Limites:

- pas de routing multi-region;
- pas de comparaison multi-canaux;
- pas de tuning dynamique tip/CU selon congestion et leader;
- pas de simulation bundle ni preflight policy sophistiquée;
- pas de stratégie concurrente Jito + RPC + privé.

### Reconciliation

Niveau: **réel mais minimal**

Points forts:

- `signatureSubscribe`;
- `getSignatureStatuses`;
- `getInflightBundleStatuses`;
- expiration.

Limites:

- pas de boucle séparée avec budget temporel et métriques dédiées;
- pas de classification venue-specific;
- pas de linking explicite inclusion -> `inclusion_count` métrique;
- pas de réémission / cancel / replacement policy.

### Observabilité / opérations

Niveau: **utile pour itération, insuffisant pour desk HFT**

Points forts:

- `/live`, `/ready`, `/status`, `/metrics`;
- métriques ShredStream et stage totals;
- statut runtime avec issues dérivées.

Limites:

- pas de percentiles;
- pas de runbook d'incident;
- pas de perf budget live;
- pas de dashboard PnL / landed rate / reject mix / latency by leader.

### Tests / validation

Niveau: **cohérent mais peu démonstratif**

Constat:

- `cargo test` passe sur l'arbre courant le 2026-03-25;
- les tests valident surtout les contrats internes;
- il n'y a pas de preuve dans le dépôt d'un test live contre Jito/Solana;
- il n'y a pas de benchmark de latence ni test de charge.

Pour un claim "state of the art", l'absence de preuve perf mesurée est bloquante.

## Écart vers le standard public Solana/Jito

Par rapport à un bot public-prod moderne, les manques majeurs sont:

1. **DEX adapters réels**
- intégrer au moins 2 venues réelles bout en bout;
- construire des instructions swap réelles;
- gérer les comptes SPL, vaults, tick arrays, metas exacts.

2. **Pricing réel**
- décodage venue-specific;
- slippage réel;
- coût net tenant compte de CU, tip, rent, échec probable.

3. **Execution quality**
- simulation transaction/bundle;
- tuning dynamique CU/tip;
- stratégie régionale et fallback canal.

4. **Observabilité**
- percentiles;
- landed rate;
- time-to-land;
- by route / by venue / by leader.

5. **Validation live**
- smoke tests sur test infra;
- replay datasets réels;
- bench latency end-to-end.

## Écart vers les meilleurs searchers privés

Cette section est **partiellement inférée**. Le dépôt ne peut pas prouver les standards privés, mais l'écart probable est important.

Les meilleurs searchers privés disposent typiquement de:

- infra multi-région et peering optimisé;
- moteurs de quote par venue très fins;
- conflict detection et expected value de landing;
- calibration par leader schedule;
- stratégies multi-canaux et bundles sophistiquées;
- simulation massive et gating adaptatif;
- modèles de contention et d'inclusion;
- instrumentation microseconde et contrôles opérationnels serrés.

Sur cette base, l'écart le plus fort du dépôt n'est pas l'architecture logicielle. C'est la **densité d'intelligence marché** et la **qualité de l'exécution venue-native**.

## Roadmap priorisée

### P0: bloquants pour prétendre au state of the art public

1. Remplacer les legs `Memo` par de vraies instructions de swap pour 2 venues Solana réelles.
2. Remplacer `PoolPriceAccountDecoder` par des décodeurs venue-specific.
3. Refondre le quote engine avec sizing, slippage et coût net réel.
4. Introduire simulation transaction/bundle avant envoi.
5. Gérer de vraies ALT on-chain ou supprimer la couche ALT synthétique tant qu'elle n'est pas réelle.

### P1: compétitivité d'exécution

1. Tip/CU tuning dynamique.
2. Routage régional / multi-canaux.
3. Métriques d'inclusion et percentiles de latence.
4. Reconciliation plus riche avec transitions et compteurs complets.

### P2: niveau searcher avancé

1. Inventory/risk engine.
2. Conflict prediction et probabilités de landing.
3. Replay datasets réalistes et benchmark live.
4. Sécurisation du signing au-delà du hot wallet local.

## Conclusion

Le dépôt est **un bon socle d'exécution Solana low-latency**, avec une architecture sérieuse et plusieurs composants déjà réels. Il n'est **pas** à l'état de l'art pour l'arb Solana HFT aujourd'hui, parce que les couches qui font réellement la performance économique d'un searcher ne sont pas encore implémentées au niveau requis:

- données marché venue-native;
- pricing réaliste;
- exécution swap réelle;
- simulation et tuning d'inclusion;
- observabilité de perf HFT.

La bonne nouvelle est que le projet n'a pas besoin d'une refonte complète. La structure actuelle est assez propre pour supporter une montée de niveau rapide. Le verdict exact est donc:

**architecture solide, exécution partiellement crédible, alpha/execution market layer encore insuffisants pour être state of the art.**

## Annexe: preuves principales dans le code

- Hot path et orchestration: [`runtime.rs`](/root/bot/Solana_v3/crates/bot/src/runtime.rs#L255)
- Refresh async blockhash/ALT/wallet: [`refresh.rs`](/root/bot/Solana_v3/crates/bot/src/refresh.rs#L52)
- Submit Jito HTTP réel: [`jito.rs`](/root/bot/Solana_v3/crates/submit/src/jito.rs#L245)
- Reconciliation RPC/WS: [`onchain.rs`](/root/bot/Solana_v3/crates/reconciliation/src/onchain.rs#L83)
- Builder `VersionedMessage` mais legs `Memo`: [`transaction_builder.rs`](/root/bot/Solana_v3/crates/builder/src/transaction_builder.rs#L98)
- Template de legs texte: [`templates.rs`](/root/bot/Solana_v3/crates/builder/src/templates.rs#L38)
- Quote simplifié: [`quote.rs`](/root/bot/Solana_v3/crates/strategy/src/quote.rs#L45)
- Décodeur synthétique: [`decoder.rs`](/root/bot/Solana_v3/crates/state/src/decoder.rs#L42)
- Guardrails runtime: [`guards.rs`](/root/bot/Solana_v3/crates/strategy/src/guards.rs#L66)
- Métriques et export Prometheus: [`metrics.rs`](/root/bot/Solana_v3/crates/telemetry/src/metrics.rs#L66), [`control.rs`](/root/bot/Solana_v3/crates/bot/src/control.rs#L159)
