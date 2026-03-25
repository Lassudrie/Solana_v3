# Revue "State of the Art" de l’architecture bot Solana

Date: 2026-03-25  
Version: Évaluation du scaffold actuel (avant intégration complète production)

## Verdict court

L’architecture est **bien conçue** (separation en planes, flux hot/cold, chemins d’erreur structurés), mais **pas encore state of the art pour un bot HFT réel**.  
Le pipeline est cohérent, toutefois des briques critiques restent au stade scaffold / partial implementation (transactions réelles, signatures, cold-path dynamique, instrumentation SLO, inclusion on-chain).

## Ce qui est bon aujourd’hui

- séparation claire par crates : détection, state, stratégie, build, signing, submit, reconciliation, telemetry, bot.
- hot-path orienté RAM locale, sans RPC synchrone dans le calcul de quote/route (objectif aligné HFT).
- détection ShredStream/JSON UDP fonctionnelle, dépendance route->pool->snapshot explicite.
- garde-fous de base implémentés : route non `warm`, snapshots stales, max inflight, kill-switch, blockhash stale guard.
- soumission Jito non-bidon : HTTP JSON-RPC, retries, classification d’erreur, idempotence locale basique.
- observabilité de base : métriques de counters/stages, endpoints `/status`, `/ready`, `/live`, `/metrics`.

## Écarts bloquants pour une version production HFT

### 1) Construction de transaction
- Les templates/build ne produisent pas de message Solana compilable réel (instr. factices, `message_bytes` texte).
- Pas de compilation message/Account metas réelles, pas de versioned message réel.
- Pas de gestion détaillée CU budget/slots/ALT par venue.

### 2) Signature et wallet
- Signature non cryptographique (hash deterministic local).
- Pas d’intégration de wallet runtime (clés locales/secure provider), pas de gestion d’erreur keypair fine.

### 3) Quoting / modélisation financière
- Quote AMM simplifiée (`price_bps`, `fee_bps`, `reserve_depth`).
- Pas de pricing venue réel (AMM CPMM/constant-product, slippage dynamique, profondeur réelle, fees supplémentaires, JIT risk).

4) Gestion source de marché / ingestion
- ShredStream minimal :
  - pas de replay sur gap robuste,
  - pas de reconnexion/backoff systématique,
  - pas de pipeline de fiabilité pour trous de séquence.
- `source_latency` non exploité ; observabilité de latence moins exploitable que potentiellement possible.

### 5) Cold path incomplet
- `blockhash`, `wallet`, ALT injectés une fois au bootstrap puis figés.
- Pas d’actualisation continue de blockhash/slot-lag, ni de solde wallet.
- `max_batch` / `primary_source` / `replay_on_gap` existent en config mais restent partiellement inactifs.

### 6) Reconciliation / exécution réelle
- Suivi inclusion on-chain non connecté aux WebSockets/RPC de statut.
- Pas de loop de confirmation, pas de gestion de régression (drop/expired) en production.

### 7) Configuration / gouvernance opératoire
- Certains champs de config sont présents mais ignorés dans le runtime chaud/froid.
- Pas de politique de fail-closed homogène (ex. `risk.fail_closed` non effectif).

## Points de maturité (niveau actuel)

| Domaine | Niveau | Note |
|---|---:|---|
| Architecture | 8/10 | solide, découplée, extensible |
| Hot path | 7/10 | court et déterministe, mais input encore simplifié |
| Quoting | 3/10 | pédagogique, pas réaliste HFT |
| Builder/Signing | 2/10 | non production |
| Submit | 7/10 | correct au niveau réseau/réties, classification utile |
| Ingestion | 6/10 | opérationnelle mais sans robustesse production |
| Reconciliation | 3/10 | statut interne seulement |
| Observabilité | 6/10 | métriques de base + HTTP ok, pas de SLO/P95/P99 |

## Plan de remédiation priorisée

### P0 – Bloquant production
1. Implémenter transaction Solana réelle (message/versioned message + comptes/compilation) pour 2-leg.
2. Remplacer signature simulée par signature Ed25519 vérifiable (wallet sécurisé).
3. Activer refresh dynamique blockhash/slot/solde/ALT en tâche séparée.
4. Ajouter suivi inclusion chain-side + transition tracker basée sur résultat réel.

### P1 – Robustesse marché / exécution
1. Réconciliation gap/reconnect/auth token sur ShredStream.
2. Ajouter buffering et fenêtre de reorder / dedupe avancée par slot/sequence.
3. Contrôle de latence: p95/p99 latence de détection → sign → submit.
4. Remplacer params de config inactifs ou supprimer la dette.

### P2 – Optimisation qualité alpha
1. Quote AMM plus réaliste par venue (modèles dédiés par DEX).
2. Risk/anti-gaming: inventory, MEV/arb conflict, protections timing.
3. Observabilité externe (Prometheus/OTLP + alerting, tableaux PnL/ROI/exclusion).
4. Contrôles de sécurité: key mgmt, secret handling, kill-switch distribué.

## Références internes utilisées

- [Architecture scaffold](./hft-bot-scaffold.md)
- [Audit opérationnel](../bot/audit.md)
- [Contrat NormalizedEvent](./normalized-event.md)
- [Runtime](../..//crates/bot/src/runtime.rs)

