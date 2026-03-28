# Crate `state`

## Rôle

`state` maintient l'état mémoire du marché consommé par la stratégie:

- store des comptes bruts observés
- décodage d'accounts dépendants des venues
- snapshots de pools
- quote models concentrés
- graphe comptes -> pools -> routes
- warmup de routes

## Contrat principal

`StatePlane` est le point d'entrée. Il reçoit un `NormalizedEvent`, applique les règles
de monotonie `slot/write_version`, met à jour snapshots et quote models, puis renvoie
un `StateApplyOutcome` décrivant les pools et routes impactés.

## Invariants

- Une mise à jour plus vieille que l'état courant est rejetée.
- Les routes deviennent `Ready` uniquement quand tous leurs pools requis sont présents.
- Une route n'est tradable que si ses snapshots sont exécutables, frais et dotés d'un
  quote model exécutable quand la liquidité est concentrée.
- L'invalidation d'un pool supprime aussi son quote model concentré.

## Inventaire des fichiers

| Fichier | Rôle | Eléments clés |
| --- | --- | --- |
| `crates/state/src/lib.rs` | Plane principal | `StatePlane`, `StateError`, application des `MarketEvent` |
| `crates/state/src/account_store.rs` | Store des comptes observés | `AccountStore`, rejet des mises à jour obsolètes |
| `crates/state/src/decoder.rs` | Décodage venue-specific | `AccountDecoder`, `DecoderRegistry`, décodeurs Orca Whirlpool et Raydium CLMM |
| `crates/state/src/dependency_graph.rs` | Graphe de dépendances | comptes -> pools -> routes |
| `crates/state/src/pool_snapshots.rs` | Store de snapshots | `PoolSnapshotStore`, invalidation et refresh de fraîcheur |
| `crates/state/src/quote_models.rs` | Store de quote models concentrés | `ConcentratedQuoteModelStore` et ré-exports `domain::quote_models` |
| `crates/state/src/types.rs` | Ré-exports domaine + types locaux | `AccountRecord`, `DecodedAccount` |
| `crates/state/src/warmup.rs` | Warmup des routes | `WarmupManager`, `RouteWarmupState` |
| `crates/state/src/executable_pool_state.rs` | Ré-export du store domaine | alias pratique vers `ExecutablePoolStateStore` |

## Détails utiles

- `decoder.rs` gère trois voies:
  - `PoolPriceAccountDecoder` pour un format simplifié de snapshot de prix
  - `OrcaWhirlpoolAccountDecoder`
  - `RaydiumClmmPoolDecoder`
- `StatePlane::aligned_concentrated_quote_model` vérifie que le quote model et le snapshot
  sont bien cohérents avant de les exposer à la stratégie.
- `apply_event` traite explicitement:
  - `AccountUpdate`
  - `PoolSnapshotUpdate`
  - `PoolQuoteModelUpdate`
  - `PoolInvalidation`
  - `DestabilizingTransaction`
  - `SlotBoundary`
  - `Heartbeat`
