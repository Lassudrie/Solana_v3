# Specs du code

Cette arborescence documente le code du workspace tel qu'il existe dans ce dépôt.
Elle complète les notes d'architecture déjà présentes sous `docs/architecture/` avec une vue
orientée implémentation, crate par crate et module par module.

## Portée

- Workspace Rust de bot d'exécution Solana basse latence
- Binaries principaux: `bot`, `botctl`, `signerd`, `shredstream_proxy`
- Scripts de génération de manifestes, d'installation et d'exploitation
- Templates systemd de déploiement

## Lecture recommandée

1. `workspace.md`: vue d'ensemble du pipeline et des dépendances internes
2. `domain.md`: contrats partagés entre les planes
3. `detection.md`, `state.md`, `strategy.md`: coeur du pipeline marché
4. `builder.md`, `signing.md`, `submit.md`, `reconciliation.md`: pipeline d'exécution
5. `bot.md`: assemblage runtime, daemon, monitoring et config
6. `scripts-and-deployment.md`: outillage hors crates

## Index

- [Vue workspace](./workspace.md)
- [domain](./domain.md)
- [detection](./detection.md)
- [state](./state.md)
- [strategy](./strategy.md)
- [builder](./builder.md)
- [signing](./signing.md)
- [signerd](./signerd.md)
- [submit](./submit.md)
- [reconciliation](./reconciliation.md)
- [telemetry](./telemetry.md)
- [bot](./bot.md)
- [scripts et déploiement](./scripts-and-deployment.md)

## Références déjà présentes dans le dépôt

- `../architecture/crate-boundaries.md`
- `../architecture/normalized-event.md`
- `../architecture/triangular-arbitrage.md`
- `../bot/README.md`
- `../routes.md`

## Conventions de cette doc

- "hot path": traitement d'un évènement marché jusqu'à une éventuelle soumission
- "cold path": refresh asynchrones et amorçage runtime
- "spec": rôle du module, contrats exposés, invariants et interactions
- les tables de fichiers couvrent les sources présentes dans `crates/`, `scripts/` et `deploy/`
