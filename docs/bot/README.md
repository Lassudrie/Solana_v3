# Bot Documentation

Ce dossier centralise la documentation du bot (opérations, configuration, architecture d’exécution, runbooks).

## Contenu

- [`../architecture/solana-arb-hft-state-of-the-art-audit-2026-03-25.md`](../architecture/solana-arb-hft-state-of-the-art-audit-2026-03-25.md) : audit d'architecture detaille de reference.
- [`./audit.md`](./audit.md) : audit fonctionnel et operationnel du bot.
- [`../architecture/normalized-event.md`](../architecture/normalized-event.md) : contrat `NormalizedEvent`.
- [`../perf/`](../perf/) : analyses de performance et templates de mesure.
- [`../archives/`](../archives/) : documents historiques qui ne refletent plus l'etat courant.

## Scripts utiles

- [`../../scripts/install_solana_yellowstone.sh`](../../scripts/install_solana_yellowstone.sh) : installe sur une machine Ubuntu 24.04 un noeud Agave `mainnet-beta` prive non votant avec Yellowstone gRPC.

Execution minimale :
```bash
VALIDATOR_KEYPAIR_SRC=/root/validator-keypair.json \
bash scripts/install_solana_yellowstone.sh
```

Variables utiles :
- `YELLOWSTONE_ENABLED=0` pour installer d'abord le noeud sans le plugin.
- `START_SERVICE=0` pour preparer la machine sans lancer le service.
- `RPC_BIND_ADDRESS=0.0.0.0` si le RPC ne doit pas rester en loopback.

## État actuel

- Le bot n'est plus au stade scaffold.
- Le document `hft-bot-scaffold.md` a ete archive car il decrivait l'etat initial du depot.
