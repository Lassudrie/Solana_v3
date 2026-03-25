# Audit Bot - 2026-03-25

Objectif: verifier ce qui manque avant utilisation production.

## Etat de la liaison ShredStream

- La liaison est active au runtime. `EventSourceMode::Shredstream` est supporte dans la factory de sources: [sources.rs](../../crates/bot/src/sources.rs).
- `ShredStreamSource` lit bien des evenements UDP JSON et normalise les events en `NormalizedEvent`: [shredstream.rs](../../crates/detection/src/shredstream.rs).
- Limite: pas de gestion de relecture/reconnexion avec `replay_on_gap`, pas de gestion d'authentification `auth_token` et pas de backoff/retry explicit de socket.

## Manques critiques

1. Soumission Jito non-operationnelle en production
- Dans [jito.rs](../../crates/submit/src/jito.rs), le submitter genere uniquement un `submission_id` local et ne fait aucun appel reseau.
- Aucun retry/retry_backoff, pas de timeout et pas de classification detaillee de rejet.

2. Pipeline de build non concret
- [transaction_builder.rs](../../crates/builder/src/transaction_builder.rs) construit uniquement un bloc binaire faux (`message_bytes` texte) sans tx réelle Solana.
- [templates.rs](../../crates/builder/src/templates.rs) fabrique des `InstructionTemplate` factices (`solana.compute_budget`, `jito.tip`) qui ne correspondent pas a un message/compilation Solana.

3. Signature non cryptographique
- [signer.rs](../../crates/signing/src/signer.rs) derive la signature via hash deterministe local (route+message) au lieu d une signature cryptographique de clef.

4. Reconciliation partielle et incomplète
- [runtime.rs](../../crates/bot/src/runtime.rs) enregistre une soumission dans `ExecutionTracker` puis s arrete; aucune boucle d'inclusion/retry/cancel.
- [tracker.rs](../../crates/reconciliation/src/tracker.rs) gère la transition interne de status, mais aucun callback externe d inclusion on-chain.

## Manques importants

5. Donnees dynamiques du state non rafraichies
- `apply_cold_path_seed` applique un blockhash/wallet/ALT une seule fois au demarrage dans [runtime.rs](../../crates/bot/src/runtime.rs).
- Aucun fetch periodique d'un blockhash frais ou du solde wallet; ces valeurs restent les valeurs bootstrap.

6. Parametres de config non utilises
- `detection.primary_source`, `detection.max_batch` lus dans la config mais jamais pris en compte: [config.rs](../../crates/bot/src/config.rs).
- `shredstream.auth_token`, `shredstream.replay_on_gap` transportes mais pas exploites: [config.rs](../../crates/bot/src/config.rs), [sources.rs](../../crates/bot/src/sources.rs), [shredstream.rs](../../crates/detection/src/shredstream.rs).
- `submit.max_inflight` n est pas applique: le contrôle d'affluence use `strategy.max_inflight_submissions` seulement: [config.rs](../../crates/bot/src/config.rs).
- `telemetry.enable_memory_logs`, `reconciliation.classify_failures`, `risk.fail_closed` sont definis mais non consultes dans le flux: [config.rs](../../crates/bot/src/config.rs).

7. Modeles strategiques incomplets
- Le `SwapSide` est defini dans [route_registry.rs](../../crates/strategy/src/route_registry.rs) mais n est pas utilise dans [quote.rs](../../crates/strategy/src/quote.rs).
- Les champs `input_mint`/`output_mint` sont uniquement stockes et non controles.

## Risques moyens / dette

8. Telesurveillance peu raccordee
- `control` expose `/status`, `/metrics`, `/live`, `/ready`; pas de metrics de latence P95/P99 ni alerte externe.
- `enable_memory_logs` existe mais pas branché conditionnellement dans le pipeline.

9. Observabilite et securite fonctionnelles
- Aucun secret ni endpoint de production n'est present, mais le mode test peut faire croire a une operation complete car tous les etapes semblent reussies.

## Backlog recommande

- P0: remplacer `JitoSubmitter` par un client HTTP/WS reel + retry + erreurs de rejet mappees + idempotence minimale.
- P0: construire des transactions solana reelles dans `builder` (accounts, message, ALTs, fees, signature curve-compatible).
- P0: remplacer la signature locale factice par signing Solana authentique et gestion d erreurs wallet.
- P1: brancher reconciliation on-chain (poll status / websockets) et transitions d inclusion/expirer.
- P1: activer le refresh asynchrone de blockhash, ALT revision et solde wallet.
- P1: implémenter gestion gap/replay et auth token pour source ShredStream.
- P2: appliquer tous les champs de configuration non utilises ou les supprimer si non previsibles.

## Criteres de validation

- Test de smoke: un event reel ShredStream -> build -> sign -> submit -> reconciliation.
- Test de rejections: soumission rejetee par endpoint -> passage `ExecutionRecord.outcome=Failed` et compteur inclusion.
- Test d'un bloc de garde wallet/kill_switch: `kill_switch` active doit empêcher tout submit.
