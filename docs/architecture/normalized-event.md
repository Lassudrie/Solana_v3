# Contrat `NormalizedEvent`

Le contrat normalisé du flux marché n’est plus porté par `detection` seul.  
La source de vérité est maintenant `crates/domain/src/events.rs`, et `detection` ne fait que le ré-exporter.

## Propriété du contrat

- Contrat partagé: `crates/domain/src/events.rs`
- Producteurs principaux:
  - `crates/detection/src/shredstream.rs`
  - `crates/detection/src/sources.rs`
  - `crates/bot/src/live.rs` pour le pipeline gRPC live restant encore en migration
- Consommateurs principaux:
  - `crates/state/src/lib.rs`
  - `crates/bot/src/runtime.rs`

## Structure

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
    PoolSnapshotUpdate(PoolSnapshotUpdate),
    PoolQuoteModelUpdate(PoolQuoteModelUpdate),
    PoolInvalidation(PoolInvalidation),
    SlotBoundary(SlotBoundary),
    Heartbeat(Heartbeat),
}
```

## Métadonnées source

```rust
pub struct SourceMetadata {
    pub source: EventSourceKind,
    pub sequence: u64,
    pub observed_slot: u64,
}
```

- `source`: `ShredStream | Replay | Synthetic`
- `sequence`: séquence logique de la source; auto-incrémentée si absente à l’entrée wire
- `observed_slot`: slot observé par la source; peut différer du slot métier embarqué dans le payload

## Métadonnées de latence

```rust
pub struct LatencyMetadata {
    pub source_received_at: SystemTime,
    pub normalized_at: SystemTime,
    pub source_latency: Option<Duration>,
}
```

- `source_latency` est optionnel
- les sources wire JSONL/UDP initialisent aujourd’hui `source_latency` à `None`
- les producteurs peuvent enrichir cette mesure plus tard sans casser le contrat

## Payloads

### `AccountUpdate`

- `pubkey: String`
- `owner: String`
- `lamports: u64`
- `data: Vec<u8>`
- `slot: u64`
- `write_version: u64`

### `PoolSnapshotUpdate`

- `pool_id: String`
- `price_bps: u64`
- `fee_bps: u16`
- `reserve_depth: u64`
- `reserve_a: Option<u64>`
- `reserve_b: Option<u64>`
- `active_liquidity: Option<u64>`
- `sqrt_price_x64: Option<u128>`
- `confidence: SnapshotConfidence`
- `repair_pending: Option<bool>`
- `token_mint_a: String`
- `token_mint_b: String`
- `tick_spacing: u16`
- `current_tick_index: Option<i32>`
- `slot: u64`
- `write_version: u64`

### `PoolQuoteModelUpdate`

- `pool_id: String`
- `liquidity: u128`
- `sqrt_price_x64: u128`
- `current_tick_index: i32`
- `tick_spacing: u16`
- `required_a_to_b: bool`
- `required_b_to_a: bool`
- `a_to_b: Option<DirectionalPoolQuoteModelUpdate>`
- `b_to_a: Option<DirectionalPoolQuoteModelUpdate>`
- `slot: u64`
- `write_version: u64`

### `PoolInvalidation`

- `pool_id: String`

### `SlotBoundary`

- `slot: u64`
- `leader: Option<String>`

### `Heartbeat`

- `slot: u64`
- `note: &'static str`

## Contrats d’architecture

- `domain` porte le contrat et les types transverses
- `detection` produit des `NormalizedEvent` mais ne possède plus le schéma
- `state` applique les événements sans dépendre de `detection`
- `bot` orchestre le hot path, mais ne doit pas redéfinir les contrats du flux
