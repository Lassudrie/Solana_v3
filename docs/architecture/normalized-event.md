# Contrat NormalizedEvent

Ce document décrit précisément la structure `NormalizedEvent` produite dans le crate `detection` et consommée par le pipeline `bot` (`runtime` / `state`).

Fichier source unique : [crates/detection/src/events.rs](/root/bot/Solana_v3/crates/detection/src/events.rs).

## Définitions de base

```rust
pub struct NormalizedEvent {
    pub payload: MarketEvent,
    pub source: SourceMetadata,
    pub latency: LatencyMetadata,
}
```

```rust
pub enum MarketEvent {
    AccountUpdate(AccountUpdate),
    SlotBoundary(SlotBoundary),
    Heartbeat(Heartbeat),
}
```

## Champs de `NormalizedEvent` (définition précise)

### `payload: MarketEvent`

`payload` contient le type d’événement marché normalisé.  

- `MarketEvent::AccountUpdate(AccountUpdate)`
  - `pubkey: String` — clé publique du compte mis à jour.
  - `owner: String` — propriétaire (program ID) du compte.
  - `lamports: u64` — solde lamports lu dans le compte.
  - `data: Vec<u8>` — payload brut du compte, bytes décodés depuis le hex wire (`data_hex`).
  - `slot: u64` — slot d’écriture du compte.
  - `write_version: u64` — version d’écriture du compte.

- `MarketEvent::SlotBoundary(SlotBoundary)`
  - `slot: u64` — slot de frontière.
  - `leader: Option<String>` — leader du slot (`None` si non fourni par la source).

- `MarketEvent::Heartbeat(Heartbeat)`
  - `slot: u64` — slot associé au heartbeat.
  - `note: &'static str` — note fixe, actuellement `"wire"`.

### `source: SourceMetadata`

```rust
pub struct SourceMetadata {
    pub source: EventSourceKind,
    pub sequence: u64,
    pub observed_slot: u64,
}
```

- `source: EventSourceKind`
  - `ShredStream`
  - `Replay`
  - `Synthetic`

- `sequence: u64`
  - numéro d’ordre logique de l’événement dans la source.
  - si absent à l’entrée wire, la source assigne une séquence auto-incrémentée (commence à `1`).

- `observed_slot: u64`
  - slot observé coté source d’entrée.
  - si absent à l’entrée wire, il est dérivé du `slot` de l’événement (dépendant du variant).

### `latency: LatencyMetadata`

```rust
pub struct LatencyMetadata {
    pub source_received_at: SystemTime,
    pub normalized_at: SystemTime,
    pub source_latency: Duration,
}
```

- `source_received_at: SystemTime`
  - horodatage de la réception/normalisation côté source de parsing.

- `normalized_at: SystemTime`
  - horodatage quand la forme `NormalizedEvent` est produite.

- `source_latency: Duration`
  - durée de latence calculée pour la source.
  - dans l’implémentation actuelle de normalisation UDP/JSONL, ce champ est initialisé à `Duration::ZERO`.

## Producteurs de `NormalizedEvent`

- `detection::ShredStreamSource` (`crates/detection/src/shredstream.rs`)
- `bot` sources alternatives (`crates/bot/src/sources.rs`) :
  - JSONL
  - UDP JSON
- Les deux implémentations appellent un helper `normalized_event(...)` qui construit systématiquement `payload`, `source`, `latency`.

## Méthodes de construction

### `NormalizedEvent::account_update(...)`

```rust
NormalizedEvent::account_update(source, sequence, observed_slot, update)
```

Construit un événement de type `MarketEvent::AccountUpdate` avec :

- `source` forcé à `SourceMetadata { source, sequence, observed_slot }`
- `latency` rempli avec `source_received_at == normalized_at == SystemTime::now()` et `source_latency == Duration::ZERO`

## Exemple de mapping wire -> normalized

Entrée wire (ShredStream/UDP/JSONL) :

```json
{
  "type": "account_update",
  "source": "replay",
  "sequence": 7,
  "observed_slot": 11,
  "pubkey": "acct-a",
  "owner": "owner",
  "lamports": 0,
  "data_hex": "010203",
  "slot": 11,
  "write_version": 3
}
```

Sortie normalisée (conceptuelle) :

- `payload = MarketEvent::AccountUpdate(AccountUpdate { pubkey: "acct-a", owner: "owner", lamports: 0, data: vec![1,2,3], slot: 11, write_version: 3 })`
- `source = SourceMetadata { source: EventSourceKind::Replay, sequence: 7, observed_slot: 11 }`
- `latency = LatencyMetadata { source_received_at: <t>, normalized_at: <t>, source_latency: 0 }`

## Consommation par le pipeline

- `state::StatePlane::apply_event` lit principalement `payload` :
  - `AccountUpdate` déclenche l’upsert compte + rafraîchissement snapshots/routes.
  - `SlotBoundary` met à jour `latest_slot`.
  - `Heartbeat` met à jour `latest_slot` si nécessaire.
- `source` et `latency` sont utiles pour la traçabilité, la déduplication et les métriques futures ; ils ne sont pas utilisés pour la décision de quote dans le hot path actuellement.

## Contrats implicites importants

- Les timestamps de latence sont cohérents en création aujourd’hui, mais peuvent être enrichis si une métrique source réelle est ajoutée.
- `observed_slot` est un champ de cohérence/freshness et peut différer du `slot` métier suivant la source.
- Le contrat actuel assume une normalisation fidèle au contrat d’entrée wire et un typage strict via `serde`.

