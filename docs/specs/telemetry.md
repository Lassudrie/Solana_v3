# Crate `telemetry`

## Rôle

`telemetry` fournit les primitives d'observabilité internes du bot:

- compteurs et latences de pipeline
- journal structuré borné en mémoire

La crate reste volontairement simple et ne dépend pas d'un backend externe.

## Inventaire des fichiers

| Fichier | Rôle | Eléments clés |
| --- | --- | --- |
| `crates/telemetry/src/lib.rs` | Façade compacte | `TelemetryStack`, agrège `PipelineMetrics` et `MemoryLogSink` |
| `crates/telemetry/src/metrics.rs` | Compteurs et latences | `PipelineStage`, `PipelineMetrics`, `MetricsSnapshot` |
| `crates/telemetry/src/logging.rs` | Journal structuré borné | `LogLevel`, `StructuredLogEvent`, `MemoryLogSink` |

## Détails

- `PipelineStage` couvre `Detect`, `StateApply`, `Quote`, `Select`, `Build`, `Sign`, `Submit`, `Reconcile`.
- `PipelineMetrics` garde à la fois les compteurs métier du bot et des métriques spécifiques
  au flux ShredStream.
- `MemoryLogSink` applique un niveau minimum et une capacité maximale avec compteur d'évènements perdus.
