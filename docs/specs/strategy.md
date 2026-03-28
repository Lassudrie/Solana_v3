# Crate `strategy`

## Rôle

`strategy` transforme l'état marché en décision d'exécution. La crate regroupe:

- la définition et validation des routes
- le quote engine local
- les garde-fous d'exécution
- le sizing et la sélection EV/legacy
- le feedback post-trade pour ajuster buffers et sizing

## Point d'entrée

`StrategyPlane` encapsule le registre de routes, le sélecteur, l'état de sizing par route
et l'état de protection d'exécution. Il expose:

- `register_route`
- `evaluate`
- les hooks de feedback post-submit et post-réconciliation

## Invariants

- Une `RouteDefinition` doit être un cycle fermé.
- Le nombre de legs doit correspondre à `RouteKind`.
- Les legs doivent se chaîner par mint, sans pool dupliqué.
- Les routes triangulaires appliquent des seuils plus stricts que les routes à 2 legs.
- Le choix final peut être piloté par profit brut legacy ou par expected value.

## Inventaire des fichiers

| Fichier | Rôle | Eléments clés |
| --- | --- | --- |
| `crates/strategy/src/lib.rs` | Plane principal | `StrategyPlane`, états de sizing/protection, feedback post-trade |
| `crates/strategy/src/route_registry.rs` | Contrat de route | `RouteDefinition`, `RouteLeg`, `RouteLegSequence`, `RouteKind`, `RouteRegistry` |
| `crates/strategy/src/quote.rs` | Quote engine local | `QuoteEngine`, `LocalTwoLegQuoteEngine`, quote CP/CLMM, conversion coût d'exécution |
| `crates/strategy/src/guards.rs` | Garde-fous | `GuardrailConfig`, `GuardrailSet`, readiness, wallet, blockhash, profit minimum |
| `crates/strategy/src/selector.rs` | Sélection des candidats | `OpportunitySelector`, ladders de tailles, scoring EV, shadow candidate |
| `crates/strategy/src/opportunity.rs` | Objets de décision | `OpportunityCandidate`, `OpportunityDecision`, `SelectionOutcome` |
| `crates/strategy/src/reasons.rs` | Taxonomie de rejet | `RejectionReason` |

## `route_registry.rs`

Ce module formalise les invariants de manifester:

- `SizingMode`: `Legacy`, `EvShadow`, `EvLive`
- `RouteKind`: `TwoLeg` ou `Triangular`
- `RouteLegSequence<T>` pour conserver des invariants forts sans `Vec` dynamique en hot path
- `ExecutionProtectionPolicy` pour le buffer de slippage adaptatif

## `quote.rs`

Le quote engine:

- quote les routes simple constant-product
- simule les swaps concentrés exact input et exact output
- gère le recalage des ticks et le franchissement de tick arrays
- convertit le coût d'exécution SOL en quote atoms si nécessaire
- produit des `LegQuote` et un `RouteQuote`

## `selector.rs`

Le sélecteur:

- ne traite que les routes impactées
- rejette les snapshots manquants ou trop vieux
- construit un ladder de tailles de trade
- quote plusieurs tailles puis garde le meilleur candidat
- compare mode legacy et mode EV
- produit éventuellement un `shadow_candidate` pour observation
