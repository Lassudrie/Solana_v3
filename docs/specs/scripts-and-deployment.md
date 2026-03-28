# Scripts et déploiement

## Rôle

Cette zone couvre l'outillage hors crates:

- génération et sélection de manifests
- installation de services
- rendu de config
- mesure de latence
- templates systemd

## Inventaire des scripts

| Fichier | Rôle | Entrées / sorties principales |
| --- | --- | --- |
| `scripts/check_crate_boundaries.py` | Vérifie le graphe de dépendances internes | lance `cargo metadata`, compare aux dépendances autorisées |
| `scripts/generate_cross_venue_manifest.py` | Génère un manifest multi-venue bas-latence depuis les APIs Orca/Raydium | scrute les APIs publiques, fabrique des routes et legs d'exécution |
| `scripts/generate_triangular_manifest.py` | Génère des routes triangulaires explicites depuis un manifest source | lit TOML, reconstruit routes à 3 legs, écrit un nouveau manifest |
| `scripts/select_live_route_universe.py` | Réduit un manifest vers un sous-ensemble live plus serré | lit manifest + monitor HTTP, score les routes, écrit un manifest filtré |
| `scripts/render_bot_config.sh` | Rend une config de déploiement du bot à partir d'un manifest | applique overrides signing, source live, monitor et builder |
| `scripts/install_bot_systemd.sh` | Build et installation du bot/signerd en service systemd | installe binaires, config, unités, compte système |
| `scripts/install_solana_yellowstone.sh` | Installation d'un noeud Agave RPC + Yellowstone | prépare machine, build Agave/Yellowstone, écrit config système |
| `scripts/measure_bot_latency.sh` | Collecte rapide de signaux de latence depuis le monitor | attend `ready=true`, capture overview et signals |

## Templates systemd

| Fichier | Rôle |
| --- | --- |
| `deploy/systemd/solana-bot.service.in` | Unité systemd du binaire `bot` |
| `deploy/systemd/solana-signerd.service.in` | Unité systemd du binaire `signerd` |

## Détails utiles

### `generate_cross_venue_manifest.py`

- interroge Orca et Raydium
- filtre des pools exclus connus
- dérive les paramètres d'exécution Orca simple/Whirlpool et Raydium simple/CLMM
- choisit des tailles de route selon le mint de quote

### `generate_triangular_manifest.py`

- conserve des invariants explicites sur les mints et la fermeture du cycle
- propage les dépendances de comptes, ALTs et paramètres d'exécution
- durcit au besoin les politiques de protection et de compute pour les triangles

### `select_live_route_universe.py`

- consomme les endpoints monitor du bot
- croise la santé des routes et des pools avec le manifest source
- priorise typiquement les routes live-éligibles et assez profondes

### `render_bot_config.sh`

Le script injecte ou force plusieurs overrides de déploiement:

- `signing.provider = "secure_unix"`
- socket du signer
- source d'évènements live `shredstream`
- forçage de `runtime.event_source.mode = "shredstream"` même si la section existe deja
- forçage de `shredstream.grpc_endpoint` vers l'endpoint fourni au script
- activation du monitor
- paramètres builder/strategy sûrs pour le runtime déployé

## Notes

- Les fichiers `scripts/__pycache__/` sont des artefacts Python et ne font pas partie de la logique source.
- Les manifests TOML racine du dépôt ne sont pas des programmes, mais pilotent directement le comportement
  du bot via `bot::config`.
