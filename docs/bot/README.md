# Bot Documentation

Ce dossier centralise la documentation du bot (opérations, configuration, architecture d’exécution, runbooks).

## Contenu

- [`architecture/`](./architecture/) : vue d’ensemble du pipeline et des composants.
- [`audit.md`](./audit.md) : audit fonctionnel et operationnel (Shredstream, build, sign, submit, reconciliation, config).
- `README-operational.md` : procédure de run, paramètres, supervision (à remplir).
- `README-configuration.md` : guide des paramètres de config et modes de source d’événements.

## État actuel

- La connexion runtime supporte désormais un mode `shredstream` en plus des sources existantes.
- Voir : [architecture/hft-bot-scaffold.md](./architecture/hft-bot-scaffold.md)
