# Template d'analyse de latence

Ce document sert de base pour analyser la latence du bot sur une fenetre donnee, avec detail par etape du pipeline.

## Metadonnees

| Champ | Valeur |
|---|---|
| Date | |
| Auteur | |
| Commit / branche | |
| Environnement | |
| Mode runtime | |
| Source d'evenements | |
| Fenetre de mesure | |
| Nombre de signaux observes | |
| Nombre de soumissions / trades observes | |
| Configuration cle | |

## Sources de donnees

- `/monitor/signals`
- `/monitor/trades`
- `/metrics`
- `botctl` / scripts utilises:

```bash
# Exemple
curl -s http://127.0.0.1:PORT/monitor/signals
curl -s http://127.0.0.1:PORT/monitor/trades
curl -s http://127.0.0.1:PORT/metrics
```

## Resume executif

- Objectif de l'analyse:
- Verdict:
- Goulot principal:
- Risque operationnel principal:
- Prochaine action:

## Budget bout en bout

| Segment | Definition | p50 | p95 | p99 | Max | Budget cible | Ecart | Notes |
|---|---|---:|---:|---:|---:|---:|---:|---|
| source_latency | source -> reception locale | | | | | | | Optionnel; depend de la source. gRPC live peut le renseigner, UDP shredstream reste aujourd'hui a `None` |
| ingest | `source_received_at -> normalized_at` | | | | | | | `ingest_nanos` |
| queue_wait | `normalized_at -> process_event` | | | | | | | `queue_wait_nanos` |
| state_apply | application state / invalidation | | | | | | | `state_apply_nanos` |
| quote_select | evaluation strategie + selection | | | | | | | `select_nanos` |
| build | construction transaction | | | | | | | `build_nanos` |
| sign | signature locale / unix signer | | | | | | | `sign_nanos` |
| submit | appel submitter / Jito | | | | | | | `submit_nanos` |
| source_to_submit | source -> retour submit | | | | | | | `total_to_submit_nanos` |
| submit_to_terminal | submit -> statut terminal | | | | | | | `submit_to_terminal_nanos` |
| source_to_terminal | source -> statut terminal | | | | | | | `source_to_terminal_nanos` |

## Detail par etape

| Etape | Champ / metrique | Interprete comme | Observations |
|---|---|---|---|
| source_latency | `MonitorSignalSample.source_latency_nanos` | latence amont fournie par la source | Champ source-dependent: gRPC live peut etre renseigne, UDP shredstream reste aujourd'hui vide |
| ingest | `MonitorSignalSample.ingest_nanos` | normalisation de l'evenement avant hot path | Correspond a `source_received_at -> normalized_at` |
| queue_wait | `MonitorSignalSample.queue_wait_nanos` | attente avant traitement hot path | Bon indicateur de backlog ou de contention CPU |
| state_apply | `MonitorSignalSample.state_apply_nanos` | update state + invalidation | Doit rester tres stable |
| quote_select | `MonitorSignalSample.select_nanos` | evaluation strategie et selection | La quote n'est pas isolee aujourd'hui; elle est incluse ici |
| build | `MonitorSignalSample.build_nanos` | builder transaction | Surveiller les spikes lies aux ALTs, comptes ou contraintes venue |
| sign | `MonitorSignalSample.sign_nanos` | signature locale / secure signer | Verifier la variance si signer externe |
| submit | `MonitorSignalSample.submit_nanos` | duree de la tentative de soumission finalisee par le dispatcher async | Sensible au reseau, au submitter et a la saturation du path async |
| source_to_submit | `MonitorSignalSample.total_to_submit_nanos` | source -> finalisation de la soumission | Metrique cle pour la competitivite; n'implique pas que le thread principal ait bloque jusque-la |
| submit_to_terminal | `MonitorTradeEvent.submit_to_terminal_nanos` | latence de landing / terminalisation | Inclut la phase asynchrone apres soumission |
| source_to_terminal | `MonitorTradeEvent.source_to_terminal_nanos` | vue complete source -> outcome final | Metrique la plus proche de l'experience live |

## Couverture metrique actuelle

| Zone | Etat actuel | Impact sur l'analyse |
|---|---|---|
| `source_latency` | partielle et dependante de la source | gRPC live peut la fournir; UDP shredstream reste aujourd'hui sans timestamp amont |
| `quote` | pas de stage dedie dans les samples | analyser la quote a l'interieur de `select_nanos` |
| `reconcile` | stage asynchrone, pas rattache a chaque signal | utiliser `submit_to_terminal` et `source_to_terminal` pour l'impact live |
| `detect` | compteur present, detail par sample non standardise | utiliser `ingest_nanos` comme meilleur proxy actuel |

## Resultats detailles

### 1. Ingestion et entree hot path

| Metrique | Count | Latest | p50 | p95 | p99 | Notes |
|---|---:|---:|---:|---:|---:|---|
| source_latency | | | | | | |
| ingest | | | | | | |
| queue_wait | | | | | | |

Commentaires:

- 

### 2. Chemin local et soumission

| Metrique | Count | Latest | p50 | p95 | p99 | Notes |
|---|---:|---:|---:|---:|---:|---|
| state_apply | | | | | | |
| quote_select | | | | | | |
| build | | | | | | |
| sign | | | | | | |
| submit | | | | | | |
| source_to_submit | | | | | | |

Commentaires:

- 

### 3. Terminalisation

| Metrique | Count | Latest | p50 | p95 | p99 | Notes |
|---|---:|---:|---:|---:|---:|---|
| submit_to_terminal | | | | | | |
| source_to_terminal | | | | | | |

Commentaires:

- 

## Telemetrie agregee a verifier

| Metrique | Valeur | Notes |
|---|---:|---|
| `bot_stage_latency_nanos_total{stage="state_apply"}` | | |
| `bot_stage_latency_nanos_total{stage="select"}` | | |
| `bot_stage_latency_nanos_total{stage="build"}` | | |
| `bot_stage_latency_nanos_total{stage="sign"}` | | |
| `bot_stage_latency_nanos_total{stage="submit"}` | | |
| `bot_stage_latency_nanos_total{stage="reconcile"}` | | |
| `bot_shredstream_ingest_latency_nanos_total` | | |
| `bot_shredstream_interarrival_latency_nanos_total` | | |

## Rejets et outliers

- Routes ou venues avec la plus forte variance:
- Correlation entre `queue_wait` et pics de rejet:
- Correlation entre `submit_nanos` et echec / expired:
- Echantillons ou outliers a examiner:

## Hypotheses

1. 
2. 
3. 

## Actions recommandees

| Priorite | Action | Proprietaire | Echeance | Impact attendu |
|---|---|---|---|---|
| P0 | | | | |
| P1 | | | | |
| P2 | | | | |

## Annexe brute

- Lien vers export brut:
- Lien vers dashboard:
- Commandes exactes utilisees:
- Notes d'environnement:
