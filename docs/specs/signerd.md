# Crate `signerd`

## Rôle

`signerd` est un petit service séparé chargé de tenir la keypair hors du process principal
`bot`. Il écoute sur une Unix socket et implémente le protocole défini dans `signing::protocol`.

## Invariants

- La socket est créée dans un répertoire `0700` et le fichier socket est `0600`.
- Un chemin existant n'est remplacé que s'il s'agit déjà d'une socket Unix.
- Chaque client peut envoyer plusieurs requêtes successives sur la même connexion.

## Inventaire des fichiers

| Fichier | Rôle | Eléments clés |
| --- | --- | --- |
| `crates/signerd/src/lib.rs` | Service de signature | `SecureSignerService`, validation du chemin, chargement keypair, boucle client |
| `crates/signerd/src/main.rs` | CLI du daemon | parsing `--socket` et `--keypair-path`, bind puis `serve_forever` |

## Protocole servi

- `GetPublicKey` -> `SignerResponse::PublicKey`
- `SignMessage { message_base64 }` -> `SignerResponse::Signature`
- erreurs -> `SignerResponse::Error`

Le service ne connaît pas `UnsignedTransactionEnvelope`; il signe un blob de message fourni
par le client.
