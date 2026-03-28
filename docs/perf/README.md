# Perf

Ce dossier stocke les documents de performance du bot:

- analyses de latence;
- analyses de debit et saturation;
- mesures de landing rate et de reject mix;
- mesures de capture d'edge attendu par route;
- mesures de PnL realise et de ratio expected -> realized par route;
- comptes rendus de regression avant/apres changement.

## Convention conseillee

- `YYYY-MM-DD-latency-analysis-<env>.md`
- `YYYY-MM-DD-throughput-analysis-<env>.md`
- `YYYY-MM-DD-regression-note-<scope>.md`

## Point de depart

- [`latency-analysis-template.md`](./latency-analysis-template.md) : template d'analyse de latence avec detail par etape.
- [`2026-03-26-latency-analysis-static.md`](./2026-03-26-latency-analysis-static.md) : analyse statique actuelle de la latence du pipeline.
