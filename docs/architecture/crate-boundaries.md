# Frontières de crates

Ce document fixe la topologie cible du workspace et les responsabilités de chaque plane.

## Graphe cible

```text
domain
├── detection
├── state
├── strategy
├── builder
├── signing
├── submit
└── reconciliation

bot -> detection/state/strategy/builder/signing/submit/reconciliation/domain/telemetry
```

## Responsabilités

### `domain`

- Types transverses
- Contrats d’événements normalisés
- IDs, snapshots, quote models partagés
- `ExecutionSnapshot`

### `detection`

- Ingestion marché
- Sources wire JSONL/UDP
- ShredStream
- Réémission d’événements normalisés

### `state`

- Account store
- Décodage venue-specific
- Dependency graph
- Warmup
- Application de `NormalizedEvent` vers snapshots marché

`state` ne porte plus l’état d’exécution du runtime.

### `strategy`

- Route registry
- Quote locale
- Guardrails
- Sélection d’opportunité

`strategy` lit explicitement `StatePlane` et `ExecutionSnapshot`.

### `builder`, `signing`, `submit`, `reconciliation`

- Pipeline aval d’exécution
- Dépendent des contrats partagés via `domain`
- Ne doivent pas lire `state` directement pour récupérer des types transverses

### `bot`

- Bootstrap
- Wiring des planes
- Runtime hot path
- Ownership de `ExecutionContext`
- Control plane, observer, refresh

## Dépendances internes autorisées

- `detection -> domain`
- `state -> domain`
- `strategy -> state, domain`
- `builder -> strategy, domain`
- `signing -> builder, domain`
- `submit -> signing, domain`
- `reconciliation -> submit, domain`
- `bot -> orchestration uniquement`

## Garde-fou

Le script `scripts/check_crate_boundaries.py` valide ces arêtes via `cargo metadata`.  
Il doit rester vert à chaque refactor boundary-first.
