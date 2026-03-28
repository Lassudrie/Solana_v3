# Crate `signing`

## Rôle

`signing` signe les transactions construites par `builder`, soit directement avec une keypair
chargée dans le process, soit via un service Unix socket séparé (`signerd`).

## Contrats

- entrée: `SigningRequest { envelope: UnsignedTransactionEnvelope }`
- sortie: `SignedTransactionEnvelope`
- abstraction commune: trait `Signer`

## Invariants

- Le wallet doit être prêt et disposer d'un balance positive.
- Le signer distant doit renvoyer une pubkey attendue si elle est configurée.
- La signature renvoyée par le signer distant est revalidée localement.
- Les messages trop gros sont rejetés avant de produire une enveloppe signée invalide.

## Inventaire des fichiers

| Fichier | Rôle | Eléments clés |
| --- | --- | --- |
| `crates/signing/src/lib.rs` | Ré-export et enveloppes | `SigningRequest`, `SignedTransactionEnvelope` |
| `crates/signing/src/wallet.rs` | Etat du wallet chaud | `HotWallet`, `WalletStatus`, `WalletPrecondition` |
| `crates/signing/src/protocol.rs` | Protocole JSON sur socket | `SignerRequest`, `SignerResponse` |
| `crates/signing/src/signer.rs` | Implémentations des signers | `Signer`, `LocalWalletSigner`, `SecureUnixWalletSigner`, `SigningError` |

## `signer.rs`

Le module gère deux providers:

- `LocalWalletSigner`
  - charge une keypair depuis fichier ou base58
  - vérifie la cohérence de config
- `SecureUnixWalletSigner`
  - parle JSON newline-delimited sur socket Unix
  - maintient un petit pool de connexions
  - supporte timeouts de connexion et d'I/O
  - vérifie la pubkey distante et la signature retournée

## Intégration

- `bot::bootstrap` résout le provider de signature à partir de `SigningConfig`.
- `signerd` implémente le serveur correspondant au protocole `protocol.rs`.
