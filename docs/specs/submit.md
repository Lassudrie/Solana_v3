# Crate `submit`

## Rôle

`submit` pousse une transaction signée vers le réseau via:

- Jito block engine
- RPC Solana standard
- un routeur multi-lanes qui choisit ou combine ces chemins

## Contrats

- `SubmitRequest`: enveloppe signée, mode (`SingleTransaction` ou `Bundle`), leader optionnel
- `SubmitResult`: accepté/rejeté avec `SubmissionId`
- `SubmitAttemptOutcome`: terminal ou retryable
- `Submitter`: abstraction commune

## Invariants

- Les implémentations sont idempotentes par signature.
- Les rejets sont typés via `SubmitRejectionReason`, jamais par chaîne libre.
- Le routeur peut préférer RPC si un leader est déjà connu (`LeaderAware`).
- Les retries sont pilotés par `SubmitRetryPolicy`.

## Inventaire des fichiers

| Fichier | Rôle | Eléments clés |
| --- | --- | --- |
| `crates/submit/src/lib.rs` | Ré-export central + tests crate | surface publique submit |
| `crates/submit/src/types.rs` | Contrats de soumission | `SubmitMode`, `SubmitRequest`, `SubmitResult`, `SubmitRejectionReason` |
| `crates/submit/src/submitter.rs` | Abstraction et politique de retry | `Submitter`, `SubmitAttemptOutcome`, `SubmitRetryPolicy`, `SubmitError` |
| `crates/submit/src/router.rs` | Routage de lanes | `RoutedSubmitter`, `SingleTransactionRoutingPolicy` |
| `crates/submit/src/rpc.rs` | Chemin RPC Solana | `RpcSubmitter`, classification des rejets JSON-RPC, cache idempotent |
| `crates/submit/src/jito.rs` | Chemin Jito | `JitoSubmitter`, bundle/transaction paths, auth header, cache idempotent |

## `router.rs`

Politiques disponibles:

- `JitoOnly`
- `RpcOnly`
- `Fanout`
- `LeaderAware`

En fanout, le routeur lance plusieurs attempts en parallèle et garde la première acceptation.

## `rpc.rs`

Le submitter RPC:

- n'accepte que `SingleTransaction`
- encode le message signé en base64
- appelle `sendTransaction`
- classe les rejets HTTP/JSON-RPC en raisons structurées
- gère un cache d'idempotence borné

## `jito.rs`

Le submitter Jito:

- sait soumettre transaction simple ou bundle
- supporte l'auth header Jito
- différencie `transactions` et `bundles`
- interprète les headers de retry server-side
- renvoie un `Retry` quand le transport est indisponible mais potentiellement récupérable
