# Audit Bot - 2026-03-25

Note 2026-03-27: ce document est un audit date, pas une spec vivante. Plusieurs constats ont deja evolue depuis, notamment la soumission live qui passe maintenant par un dispatcher async, et `source_latency` qui depend des sources au lieu d'etre uniformement nulle.

Objectif: vérifier ce qui manque avant utilisation production.

## État réel du pipeline

Le bot n'est plus un scaffold incomplet. L'arbre courant fournit déjà:

- une source ShredStream UDP avec auth et replay sur gap;
- un builder Solana réel;
- une signature cryptographique réelle;
- un submitter Jito réel;
- un refresh asynchrone blockhash/ALT/wallet;
- une reconciliation on-chain réelle.

Validation locale sur l'arbre courant:

- `cargo test --workspace`: passe
- `cargo check --workspace`: passe

## Ce qui est déjà opérationnel

### ShredStream

- `EventSourceMode::Shredstream` est supporté dans [sources.rs](../../crates/bot/src/sources.rs).
- [shredstream.rs](../../crates/detection/src/shredstream.rs) gère:
  - lecture UDP non bloquante;
  - validation `auth_token`;
  - replay request sur gap si `replay_on_gap=true`;
  - reorder buffer minimal.

### Build / sign / submit

- [transaction_builder.rs](../../crates/builder/src/transaction_builder.rs) construit de vraies instructions Solana et compile des messages `Legacy`/`V0`.
- [signer.rs](../../crates/signing/src/signer.rs) signe réellement via keypair local ou signer Unix.
- [jito.rs](../../crates/submit/src/jito.rs) effectue de vrais appels HTTP JSON-RPC Jito avec retry, timeout, auth et idempotence.

### Cold path / reconciliation

- [refresh.rs](../../crates/bot/src/refresh.rs) rafraîchit blockhash, slot, ALT et solde wallet.
- [onchain.rs](../../crates/reconciliation/src/onchain.rs) suit l'état on-chain via RPC et WebSocket.

## Manques critiques

1. Quote et sizing encore trop simplifiés
- [quote.rs](../../crates/strategy/src/quote.rs) travaille à taille fixe et sans coût complet d'exécution.
- Pas de ladder de tailles ni d'expected value tenant compte de `CU + tip + risque d'échec`.

2. State marché trop léger
- [types.rs](../../crates/state/src/types.rs) ne transporte pas encore assez d'information pour un pricing live robuste.
- Les décodeurs venue-specific existent, mais la représentation finale reste trop pauvre pour un sizing compétitif.

3. Exécution pas encore assez blindée
- Pas de shadow simulation.
- Pas de prévalidation complète des comptes runtime/ATA avant usage live.
- Pas de calibration dynamique `CU/tip` par route.

## Manques importants

4. Mesure de latence source incomplète
- `source_latency` reste absente dans [shredstream.rs](../../crates/detection/src/shredstream.rs).
- la source gRPC live peut en revanche renseigner une vraie latence amont quand `created_at` est disponible.
- Le bot ne mesure donc pas encore proprement son avance informationnelle réelle sur toutes les sources.

5. Observabilité SLO incomplète
- Le control plane expose des compteurs et des latences cumulées.
- Les percentiles existent en mémoire dans [observer.rs](../../crates/bot/src/observer.rs), mais ne sont pas encore la sortie opératoire principale.

6. Validation live absente
- Les tests internes passent, mais le dépôt ne contient pas encore de preuve live/staging sur:
  - landing rate;
  - reject mix;
  - latence bout en bout;
  - qualité d'edge.

## Risques moyens / dette

7. Ingestion encore minimale
- pas de redondance multi-feed/région;
- pas de politique de reconnexion/backoff plus avancée;
- pas de mesure comparative contre RPC/Geyser.

8. Submit/réconciliation encore simples
- pas de stratégie multi-canal;
- pas de replacement policy avancée;
- pas de tuning fin par congestion/leader.

## Backlog recommandé

- P0: remplacer la quote fixe par un sizing adaptatif avec coût complet.
- P0: enrichir les snapshots marché et les décodeurs pour porter un state exécutable.
- P0: ajouter garde-fous d'exécution hors hot path: prévalidation comptes, ATA, simulation.
- P1: instrumenter `source_latency` et les métriques `source_to_submit` / `submit_to_terminal`.
- P1: exporter percentiles et ratios d'inclusion au niveau control plane.
- P1: renforcer feed et submit avec des politiques plus robustes.
- P2: ajouter validation live/staging et preuves de performance réelle.

## Critères de validation

- Smoke test: un event réel -> build -> sign -> submit -> reconciliation.
- Mesures live/staging: landing rate, expired rate, reject mix, `source_to_submit`.
- Validation sizing: la meilleure taille choisie doit maximiser l'EV nette et non le profit brut théorique.
