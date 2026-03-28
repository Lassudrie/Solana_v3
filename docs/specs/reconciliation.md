# Crate `reconciliation`

## Rôle

`reconciliation` suit une soumission après le hot path et transforme le résultat réseau
en résultat d'exécution on-chain exploitable par la stratégie, la health policy et l'observer.

## Sous-systèmes

- `ExecutionTracker`: historique mémoire et machine d'états
- `OutcomeClassifier`: taxonomie des échecs
- `OnChainReconciler`: polling RPC, websocket signatures, statut bundles Jito, extraction de PnL

## Invariants

- Le résultat de soumission et le résultat d'inclusion sont séparés.
- Une transition d'état ne peut pas régresser vers un état moins terminal.
- Les soumissions trop vieilles expirent si elles restent pending trop longtemps.
- Les détails d'échec on-chain sont stockés séparément du `FailureClass`.

## Inventaire des fichiers

| Fichier | Rôle | Eléments clés |
| --- | --- | --- |
| `crates/reconciliation/src/lib.rs` | Ré-export central + tests crate | expose tracker, classifier, onchain |
| `crates/reconciliation/src/classifier.rs` | Taxonomie d'échec | `FailureClass`, `OutcomeClassifier` |
| `crates/reconciliation/src/history.rs` | Store mémoire minimal | `ExecutionHistory` |
| `crates/reconciliation/src/tracker.rs` | Machine d'états exécution | `ExecutionRecord`, `ExecutionTransition`, `ExecutionTracker` |
| `crates/reconciliation/src/onchain.rs` | Réconciliation réseau | `OnChainReconciler`, `ReconciliationConfig`, parsing RPC/WS/Jito |

## `tracker.rs`

Le tracker enregistre:

- identité logique: route, `SubmissionId`, signature chaîne
- contexte de soumission: endpoint, mode, slots, timestamps
- outcome courant: `Pending`, `Included`, `Failed`
- détails additionnels: profit attendu/réalisé, coût estimé, détails d'échec

Les états `InclusionStatus` couvrent:

- `Submitted`
- `Pending`
- `Landed`
- `Dropped`
- `Expired`
- `Failed(FailureClass)`

## `onchain.rs`

Le réconciliateur:

- expire les submissions trop anciennes
- s'abonne en websocket aux signatures si activé
- poll `getSignatureStatuses`
- poll les statuts de bundles inflight côté Jito
- reclassifie les erreurs on-chain en `FailureClass`
- extrait des détails d'erreur programme/instruction/custom code
- tente de calculer un PnL réalisé à partir des balances observées
