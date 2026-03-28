# Crate `domain`

## Rôle

`domain` porte les contrats de données partagés entre les planes du bot.
La crate ne contient ni I/O, ni logique réseau, ni orchestration runtime; elle définit
le langage commun utilisé par `detection`, `state`, `strategy`, `builder`, `signing`,
`submit` et `reconciliation`.

## Contrats principaux

- Identifiants: `AccountKey`, `PoolId`, `RouteId`
- Contrat marché: `NormalizedEvent`, `MarketEvent`, `PoolSnapshotUpdate`, `PoolQuoteModelUpdate`
- Etats dérivés: `PoolSnapshot`, `ExecutionSnapshot`, `RouteState`, `LookupTableSnapshot`
- Etats exécutables: `ConstantProductPoolState`, `ConcentratedLiquidityPoolState`, `ExecutablePoolState`
- Quote models CLMM: `ConcentratedQuoteModel`, `DirectionalConcentratedQuoteModel`

## Invariants

- `NormalizedEvent` couple un payload métier, une provenance (`SourceMetadata`) et une latence.
- `PoolSnapshot` encode toujours un `FreshnessState`.
- `ExecutablePoolState` est orienté exécution et sait se projeter en `PoolSnapshot`.
- Les quote models concentrés sont séparés des snapshots pour éviter de mélanger état de surface
  et profondeur de tick arrays.

## Inventaire des fichiers

| Fichier | Rôle | Eléments clés |
| --- | --- | --- |
| `crates/domain/src/lib.rs` | Ré-export central | Expose tout le domaine depuis un point unique |
| `crates/domain/src/events.rs` | Contrat d'évènements normalisés | `EventSourceKind`, `LatencyMetadata`, `MarketEvent`, `NormalizedEvent` |
| `crates/domain/src/types.rs` | Types d'état partagés | `PoolVenue`, `PoolConfidence`, `PoolSnapshot`, `ExecutionSnapshot`, `StateApplyOutcome` |
| `crates/domain/src/executable_pool_state.rs` | Store de states exécutables | `ExecutablePoolStateStore`, invalidation/version barrière |
| `crates/domain/src/quote_models.rs` | Quote models concentrés et helpers PDA | ticks initiaux, fenêtres, dérivation des tick arrays Orca/Raydium |

## Notes d'implémentation

- `types.rs` contient la logique utilitaire la plus dense de la crate:
  conversion `sqrt_price_x64 -> price_bps`, projection en snapshot et calcul de slippage heuristique.
- `events.rs` fournit des constructeurs de confort sur `NormalizedEvent` pour produire les payloads
  les plus fréquents.
- `ExecutablePoolStateStore` applique une politique simple de monotonie par `slot` puis `write_version`.
- Les helpers `derive_orca_tick_arrays` et `derive_raydium_tick_arrays` sont consommés par
  `builder` et `detection::live`.
