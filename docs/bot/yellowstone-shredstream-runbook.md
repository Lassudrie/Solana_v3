# Runbook Yellowstone RPC + Shredstream

Ce document décrit la procédure opératoire complète pour installer un noeud Agave local avec Yellowstone gRPC, brancher le bot sur le RPC local, et clarifier la couche Shredstream réellement attendue par le bot.

## Vue d'ensemble

Le pipeline local comporte 3 couches distinctes :

1. Agave validator local expose le JSON-RPC HTTP sur `127.0.0.1:8899` et le WebSocket RPC sur `127.0.0.1:8900`.
2. Le plugin Yellowstone gRPC expose son serveur gRPC sur `127.0.0.1:10000` et Prometheus sur `127.0.0.1:8999`.
3. Le bot consomme une API gRPC `ShredstreamProxy.SubscribeEntries`, par défaut sur `http://127.0.0.1:50051`.

Point important :

- Le bot ne consomme pas directement le port Yellowstone `10000`.
- Le bot attend aujourd'hui un proxy/adaptateur Shredstream compatible avec [`crates/detection/proto/shredstream.proto`](/root/bot/Solana_v3/crates/detection/proto/shredstream.proto).
- La doc archivee qui parle d'un endpoint UDP `udp://127.0.0.1:10000` n'est plus la reference operative pour le bot actuel.

## Ce que le repo fournit deja

- Le script [`scripts/install_solana_yellowstone.sh`](/root/bot/Solana_v3/scripts/install_solana_yellowstone.sh) installe :
  - Ubuntu packages
  - Rust
  - Agave validator
  - le plugin Yellowstone gRPC
  - `/etc/default/agave-rpc`
  - `/etc/solana/yellowstone-grpc-geyser-config.json`
  - le service `systemd` `agave-rpc`
- Les manifests fast du bot pointent deja le RPC local :
  - [`sol_usdc_routes_amm_fast.toml`](/root/bot/Solana_v3/sol_usdc_routes_amm_fast.toml)
  - [`amm_12_pairs_fast.toml`](/root/bot/Solana_v3/amm_12_pairs_fast.toml)
- Un scaffold de proxy gRPC Shredstream :
  - [`crates/detection/src/proxy.rs`](/root/bot/Solana_v3/crates/detection/src/proxy.rs)
  - [`crates/detection/src/bin/shredstream_proxy.rs`](/root/bot/Solana_v3/crates/detection/src/bin/shredstream_proxy.rs)

Ce que le repo ne fournit pas explicitement :

- le bridge Yellowstone gRPC `10000` -> `ShredstreamProxy` gRPC `50051`
- le mapping d'un flux upstream reel vers `Entry.entries`

## Prerequis

- Ubuntu 24.04
- acces root
- une identity validator existante
- ports locaux libres au minimum :
  - `8899` HTTP RPC
  - `8900` WebSocket RPC
  - `10000` Yellowstone gRPC
  - `8999` Yellowstone Prometheus
  - `50051` proxy Shredstream pour le bot

## Versions validees

Les versions actuellement epinglees et validees par le script d'installation sont :

- `AGAVE_VERSION=v3.1.11`
- `YELLOWSTONE_BASE_TAG=v12.2.0+solana.3.1.10`
- `YELLOWSTONE_INTERFACE_VERSION=3.1.11`
- `YELLOWSTONE_INSTALL_LABEL=v12.2.0-solana-3.1.11-patched`

Interpretation :

- le validator local est compile en Agave `v3.1.11`
- Yellowstone est checkout sur le tag `v12.2.0+solana.3.1.10`
- le script patche ensuite `agave-geyser-plugin-interface` pour l'aligner sur `3.1.11`

Variables de version surchargeables au moment de l'installation :

```bash
AGAVE_VERSION=...
YELLOWSTONE_BASE_TAG=...
YELLOWSTONE_INTERFACE_VERSION=...
YELLOWSTONE_INSTALL_LABEL=...
```

Exemple :

```bash
VALIDATOR_KEYPAIR_SRC=/root/validator-keypair.json \
AGAVE_VERSION=v3.1.11 \
YELLOWSTONE_BASE_TAG=v12.2.0+solana.3.1.10 \
YELLOWSTONE_INTERFACE_VERSION=3.1.11 \
ENABLE_FULL_RPC_API=1 \
bash scripts/install_solana_yellowstone.sh
```

Contrainte importante :

- `YELLOWSTONE_INTERFACE_VERSION` doit rester compatible avec la version d'Agave effectivement compilee
- si vous changez `AGAVE_VERSION`, vous devez revalider la compatibilite ABI du plugin Yellowstone avant de deployer
- ne changez pas une seule de ces variables en isolation sans verifier l'ensemble

## Installation du noeud Agave + Yellowstone

Commande recommandee :

```bash
cd /root/bot/Solana_v3

VALIDATOR_KEYPAIR_SRC=/root/validator-keypair.json \
ENABLE_FULL_RPC_API=1 \
bash scripts/install_solana_yellowstone.sh
```

Variables utiles :

- `YELLOWSTONE_ENABLED=0` pour installer d'abord le noeud sans le plugin
- `START_SERVICE=0` pour preparer la machine sans lancer le service
- `RPC_BIND_ADDRESS=0.0.0.0` si le RPC doit sortir du loopback
- `RPC_PORT=8899` pour garder l'alignement avec la config bot actuelle
- `YELLOWSTONE_GRPC_ADDRESS=127.0.0.1:10000` pour garder l'adresse par defaut du plugin
- `LIMIT_LEDGER_SIZE=50000000` pour garder la retention par defaut du script

Pourquoi `ENABLE_FULL_RPC_API=1` est obligatoire pour le bot :

- le bot utilise notamment `getLatestBlockhash`
- le bot utilise `getMultipleAccounts`
- le bootstrap et les refresh workers en dependent
- sans `--full-rpc-api`, le noeud peut repondre a `getSlot` mais renvoyer `-32601 Method not found` sur ces methodes

## Retention ledger et LIMIT_LEDGER_SIZE

Le script installe par defaut :

```bash
LIMIT_LEDGER_SIZE=50000000
```

Ce parametre est passe a `agave-validator` via `--limit-ledger-size 50000000`.

Ordre de grandeur documente dans le script :

- `50,000,000` shreds vise environ `~1h` de ledger sous forte charge
- cela represente environ `~100 GB` d'ordre de grandeur

Semantique exacte supportee par le script :

- `LIMIT_LEDGER_SIZE=0`
- `LIMIT_LEDGER_SIZE=false`
- `LIMIT_LEDGER_SIZE=no`

Resultat :

- le flag `--limit-ledger-size` n'est pas ajoute

Cas booleen actif :

- `LIMIT_LEDGER_SIZE=1`
- `LIMIT_LEDGER_SIZE=true`
- `LIMIT_LEDGER_SIZE=yes`

Resultat :

- le flag `--limit-ledger-size` est ajoute sans valeur explicite

Cas numerique :

- `LIMIT_LEDGER_SIZE=50000000`
- `LIMIT_LEDGER_SIZE=80000000`
- `LIMIT_LEDGER_SIZE=120000000`

Resultat :

- le script passe `--limit-ledger-size <valeur>`

Recommandation operative pour ce bot :

- garder `LIMIT_LEDGER_SIZE=50000000` par defaut tant qu'un besoin de retention plus long n'est pas etabli
- augmenter la valeur seulement si vous avez une raison claire cote debug, replay, ou historique local
- ne pas desactiver la limite sans budget disque et politique d'exploitation explicites

Exemple avec retention explicite :

```bash
VALIDATOR_KEYPAIR_SRC=/root/validator-keypair.json \
ENABLE_FULL_RPC_API=1 \
LIMIT_LEDGER_SIZE=50000000 \
bash scripts/install_solana_yellowstone.sh
```

## Fichiers installes par le script

- `/etc/default/agave-rpc`
- `/etc/solana/yellowstone-grpc-geyser-config.json`
- `/etc/systemd/system/agave-rpc.service`
- `/home/sol/bin/validator.sh`

Le service utilise ensuite :

```bash
systemctl status agave-rpc --no-pager
journalctl -u agave-rpc -f
```

## Verification du noeud RPC local

Verifier les ports exposes :

```bash
ss -lntp | rg ':8899|:8900|:10000|:8999'
```

Verifier les methodes minimales requises par le bot :

```bash
curl -sS -H 'Content-Type: application/json' \
  -d '{"jsonrpc":"2.0","id":1,"method":"getVersion"}' \
  http://127.0.0.1:8899

curl -sS -H 'Content-Type: application/json' \
  -d '{"jsonrpc":"2.0","id":1,"method":"getHealth"}' \
  http://127.0.0.1:8899

curl -sS -H 'Content-Type: application/json' \
  -d '{"jsonrpc":"2.0","id":1,"method":"getLatestBlockhash","params":[{"commitment":"processed"}]}' \
  http://127.0.0.1:8899

curl -sS -H 'Content-Type: application/json' \
  -d '{"jsonrpc":"2.0","id":1,"method":"getMultipleAccounts","params":[["11111111111111111111111111111111"],{"encoding":"base64","commitment":"processed"}]}' \
  http://127.0.0.1:8899
```

Resultat attendu :

- `getVersion` repond
- `getHealth` repond
- `getLatestBlockhash` repond sans `Method not found`
- `getMultipleAccounts` repond sans `Method not found`
- `8900` est en ecoute pour le WebSocket RPC

## Verification Yellowstone gRPC

Verifier que le plugin est charge :

```bash
ss -lntp | rg ':10000|:8999'
```

Verifier la config plugin :

```bash
cat /etc/solana/yellowstone-grpc-geyser-config.json
```

Par defaut, le script installe :

- gRPC Yellowstone sur `127.0.0.1:10000`
- Prometheus Yellowstone sur `127.0.0.1:8999`

## Clarification Shredstream cote bot

Le bot live utilise :

- `[runtime.event_source] mode = "shredstream"`
- `shredstream.grpc_endpoint`

Dans le code actuel, `shredstream.grpc_endpoint` vaut par defaut :

- `http://127.0.0.1:50051`

Le service attendu par le bot est :

- `ShredstreamProxy.SubscribeEntries`
- `Shredstream.SendHeartbeat`

Reference proto :

- [`crates/detection/proto/shredstream.proto`](/root/bot/Solana_v3/crates/detection/proto/shredstream.proto)

Le repo fournit maintenant le squelette du serveur gRPC correspondant :

- `cargo run -p detection --bin shredstream_proxy`
- ecoute par defaut sur `127.0.0.1:50051`
- expose deja `SubscribeEntries` et `SendHeartbeat`
- n'est pas encore branche a un producteur amont d'`Entry`

Cela signifie qu'il faut encore un composant supplementaire entre Yellowstone et le bot si votre stack n'expose que `127.0.0.1:10000`.

Checklist simple :

- `10000` seul en ecoute : Yellowstone est present, mais le bot ne peut pas encore consommer le flux live
- `50051` en ecoute avec le service `ShredstreamProxy` : le bot peut se connecter en mode live

## Configuration bot recommandee

Les manifests fast du repo pointent deja le RPC local :

- `reconciliation.rpc_http_endpoint = "http://127.0.0.1:8899"`
- `reconciliation.rpc_ws_endpoint = "ws://127.0.0.1:8900"`

Si `rpc_submit.endpoint` est vide, le bot reutilise automatiquement `reconciliation.rpc_http_endpoint`.

Pour un lancement live minimal, utiliser une config derivee du manifest fast avec au moins :

```toml
[runtime]
profile = "ultra_fast"

[runtime.event_source]
mode = "shredstream"

[runtime.monitor_server]
enabled = true

[reconciliation]
rpc_http_endpoint = "http://127.0.0.1:8899"
rpc_ws_endpoint = "ws://127.0.0.1:8900"

[shredstream]
grpc_endpoint = "http://127.0.0.1:50051"

[signing]
owner_pubkey = "..."
validate_execution_accounts = false
```

`validate_execution_accounts = false` est utile pour un bring-up initial. Pour un run operatoire durable, revalider ensuite avec les comptes d'execution reels.

## Deploiement du bot dans tmux

Exemple de lancement :

```bash
tmux new-session -d -s bot
tmux new-window -d -t bot -n bot \
  'cd /root/bot/Solana_v3 && exec env BOT_CONFIG=/tmp/bot-live-yellowstone.toml target/release/bot'
```

Verification :

```bash
ss -lntp | rg ':8080|:8081'
curl -sf http://127.0.0.1:8081/monitor/overview
tmux capture-pane -pt bot:0 -S -80
```

## Symptomes frequents et diagnostic

### `Method not found` sur `getLatestBlockhash` ou `getMultipleAccounts`

Cause probable :

- RPC local lance sans `--full-rpc-api`

Correctif :

```bash
sudoedit /etc/default/agave-rpc
```

Puis definir :

```bash
ENABLE_FULL_RPC_API=1
```

Puis redemarrer :

```bash
sudo systemctl restart agave-rpc
```

### `8899` repond mais `8900` n'ecoute pas

Verifier :

- que le validator a fini son startup
- les logs `journalctl -u agave-rpc -f`
- la ligne de commande effective du process

Rappel :

- `agave-validator --help` indique que `--rpc-port <PORT>` active aussi le port suivant pour le WebSocket RPC

### Le bot ne recoit aucun event live

Verifier dans l'ordre :

1. `10000` ecoute bien
2. `50051` ecoute bien
3. le proxy expose `ShredstreamProxy.SubscribeEntries`
4. la config bot pointe bien vers `shredstream.grpc_endpoint = "http://127.0.0.1:50051"`
5. `runtime.event_source.mode = "shredstream"`

### Le bot reste en `ready=false`

Verifier :

- `curl -sf http://127.0.0.1:8081/monitor/overview`
- les erreurs `repair fetch`
- les erreurs `async refresh blockhash`
- `wallet_ready`
- `warm_routes`

## Etat de reference attendu

Un etat local cohérent pour le bot est :

- Agave RPC HTTP sur `127.0.0.1:8899`
- Agave RPC WebSocket sur `127.0.0.1:8900`
- Yellowstone gRPC sur `127.0.0.1:10000`
- ShredstreamProxy pour le bot sur `127.0.0.1:50051`
- bot monitor sur `127.0.0.1:8081`

Si un de ces maillons manque, le bot peut demarrer mais il ne sera pas operable en live.
