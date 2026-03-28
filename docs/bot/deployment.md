# Deploiement systemd du bot

Cette procedure ajoute un packaging d'exploitation simple pour le bot:

- `bot`
- `botctl`
- `signerd`
- deux services `systemd` (`solana-signerd.service` et `solana-bot.service`)

Le deployement vise une machine Linux avec `systemd`, typiquement Ubuntu 24.04.

## Prerequis

- un noeud Agave RPC local et le flux live deja prepares
- la procedure Yellowstone/Shredstream du repo deja suivie quand vous visez le mode live
- une hot wallet disponible sur la machine cible
- un toolchain Rust capable de builder le workspace, sauf si vous reutilisez deja `target/release`

Reference infrastructure locale:

- RPC HTTP: `127.0.0.1:8899`
- RPC WebSocket: `127.0.0.1:8900`
- Shredstream gRPC cote bot: `127.0.0.1:50051`

Attention: le binaire `shredstream_proxy` present dans ce repo reste un scaffold. Il expose le service gRPC attendu par le bot, mais il n'est pas branche a un producteur amont d'`Entry`. Pour un deployement live, il faut donc un vrai fournisseur Shredstream operable sur `50051`, pas seulement le scaffold.

## Installation

Depuis la racine du repo:

```bash
sudo bash scripts/install_bot_systemd.sh
```

Par defaut, le script:

- build `bot`, `botctl` et `signerd` en `release`
- installe les binaires dans `/usr/local/bin`
- cree l'utilisateur systeme `solbot`
- rend une config derivee de `sol_usdc_routes_amm_fast.toml` vers `/etc/solana-bot/bot.toml`
- installe les unites dans `/etc/systemd/system`
- ajoute automatiquement un drop-in `systemd` pour chaîner `solana-bot.service` apres `agave-rpc.service` et `jito-shredstream-proxy.service` quand ces services existent deja
- active les services sans les demarrer

Options utiles:

- `--source-config /root/bot/Solana_v3/amm_12_pairs_fast.toml`
- `--bot-config /etc/solana-bot/bot.toml`
- `--signer-keypair /etc/solana-bot/signing-keypair.json`
- `--grpc-endpoint http://127.0.0.1:50051`
- `--overwrite-config`
- `--skip-build`
- `--start`

## Ce que le renderer de config applique

Le script `scripts/render_bot_config.sh` transforme le manifest de routes en config de deployement en ajoutant ou forcant:

- `signing.provider = "secure_unix"`
- `signing.owner_pubkey = ""`
- `signing.socket_path = "/run/solana-bot/signerd.sock"`
- `signing.validate_execution_accounts = false`
- `[runtime.event_source] mode = "shredstream"`
- `[runtime.monitor_server] enabled = true`
- `[shredstream] grpc_endpoint = "http://127.0.0.1:50051"`

Le `owner_pubkey` est volontairement vide pour laisser le bot interroger `signerd` et recuperer la vraie cle publique de la hot wallet au boot.

## Mise en service

Copier la cle privee du hot wallet sur la machine cible:

```bash
sudo install -o solbot -g solbot -m 0600 /path/to/hot-wallet.json /etc/solana-bot/signing-keypair.json
```

Verifier ensuite la config rendue:

```bash
sudoedit /etc/solana-bot/bot.toml
```

Points a relire avant premier start:

- le jeu de routes actif
- les protections de taille et de frais
- `validate_execution_accounts = false` pour le bring-up initial, puis remise a `true` quand les comptes reels sont valides
- l'endpoint `shredstream.grpc_endpoint`

Demarrage:

```bash
sudo systemctl restart solana-signerd.service
sudo systemctl restart solana-bot.service
```

## Verification

Verifier l'etat des services:

```bash
systemctl --no-pager --full status solana-signerd.service solana-bot.service
```

Verifier le monitor HTTP:

```bash
curl -sf http://127.0.0.1:8081/monitor/overview
curl -sf http://127.0.0.1:8081/monitor/edge
```

Verifier le resume bot:

```bash
botctl status
botctl edge --sort captured --limit 20
botctl edge --sort realized --limit 20
```

Si `ready=false`, le plus frequent est qu'un maillon live manque encore:

- `127.0.0.1:8899` pour le RPC HTTP
- `127.0.0.1:8900` pour le RPC WebSocket
- `127.0.0.1:50051` pour le flux Shredstream attendu par le bot

## Fichiers installes

- `/usr/local/bin/bot`
- `/usr/local/bin/botctl`
- `/usr/local/bin/signerd`
- `/etc/solana-bot/bot.toml`
- `/etc/systemd/system/solana-signerd.service`
- `/etc/systemd/system/solana-bot.service`
