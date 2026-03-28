# Persistance SQLite d'audit

## Objectif

Le bot dispose d'une persistance locale SQLite pour conserver durablement:

- tous les rejets stratégie, build et submit
- toutes les soumissions
- toutes les transitions post-submit issues de la réconciliation
- le dernier état connu de chaque `submission_id`

Le but est d'avoir une base exploitable pour l'analyse de rejet mix, de landing rate,
de qualité d'edge, de latence et de régression de stratégie.

## Principe d'architecture

Le design privilégie explicitement la non-perturbation du hot path:

1. le hot path produit un `HotPathReport` et, plus tard, des `ExecutionRecord`
2. `ObserverHandle` duplique ces objets vers un `PersistenceHandle`
3. `PersistenceHandle` envoie les messages sur une file dédiée en mémoire
4. un thread `bot-persistence-worker` batch les écritures dans SQLite
5. les écritures sont faites en transaction courte avec `journal_mode=WAL`

Conséquence pratique:

- pas d'I/O disque synchrone dans la décision de trading
- pas d'appel SQLite sur le thread principal
- le monitor HTTP et la persistance restent découplés

## Garanties et limites

Garanties:

- chaque démarrage du bot crée un `run_id` dans `bot_runs`
- les rejets sont stockés en append-only dans `rejection_events`
- les trades sont stockés en historique complet dans `trade_events`
- `trade_latest` donne l'état consolidé le plus récent par `submission_id`
- un flush best-effort est demandé à l'arrêt normal du daemon

Limites actuelles:

- la file vers le worker de persistance est non bornée: elle évite de bloquer le bot,
  mais une base bloquée longtemps peut augmenter l'usage mémoire
- l'écriture utilise `synchronous=NORMAL` pour protéger la latence; en cas de crash machine
  ou de coupure brutale, le dernier batch non flushé peut être perdu
- la fenêtre de perte potentielle correspond au batch courant:
  `batch_size` messages ou `flush_interval_millis`, au premier des deux

Si vous avez besoin d'une sémantique plus stricte sur panne hôte, il faudra accepter
un coût supplémentaire côté disque en renforçant la politique SQLite.

## Configuration

Section de config:

```toml
[runtime.persistence]
enabled = true
path = "data/bot_observer.sqlite3"
batch_size = 128
flush_interval_millis = 50
```

Paramètres:

- `enabled`: active ou non le worker de persistance
- `path`: chemin du fichier SQLite
- `batch_size`: nombre maximum d'évènements par transaction
- `flush_interval_millis`: délai maximal avant flush d'un batch incomplet

Recommandations:

- exécution manuelle hors `systemd`: le chemin par défaut relatif convient
- déploiement `systemd`: utiliser un chemin absolu dans `StateDirectory`, par exemple
  `/var/lib/solana-bot/bot_observer.sqlite3`

Sous `systemd`, le service tourne avec `ProtectSystem=strict`. Le chemin relatif par défaut
`data/bot_observer.sqlite3` n'est donc pas le meilleur choix en production.

## Schéma stocké

### `bot_runs`

Une ligne par démarrage du bot:

- `run_id`
- horodatage de démarrage
- PID
- profil runtime
- mode de source d'évènements
- mode de submit
- état du monitor
- chemin de la base

### `rejection_events`

Historique append-only des rejets.

Colonnes principales:

- `event_seq`
- `run_id`
- `stage`
- `route_id`
- `route_kind`
- `leg_count`
- `reason_code`
- `reason_detail`
- `observed_slot`
- `expected_profit_quote_atoms`
- `occurred_at_unix_millis`
- `payload_json`

`payload_json` embarque:

- la ligne `RejectionEvent` normalisée
- la `PipelineTrace`
- le contexte candidat si un meilleur candidat existait

### `trade_events`

Historique append-only des trades et transitions.

Colonnes principales:

- `event_seq`
- `run_id`
- `submission_id`
- `audit_kind` (`submission` ou `transition`)
- `route_id`
- `route_kind`
- `signature`
- `submit_status`
- `outcome`
- `endpoint`
- `quoted_slot`
- `submitted_slot`
- `terminal_slot`
- `expected_pnl_quote_atoms`
- `realized_pnl_quote_atoms`
- `reported_at_unix_millis`
- `payload_json`

`payload_json` contient l'objet `MonitorTradeEvent` complet, et pour l'évènement initial
de soumission la trace pipeline et le leader de submit.

### `trade_latest`

Vue matérialisée minimale du dernier état par `submission_id`.

Usage typique:

- état terminal le plus récent
- requêtes rapides par `route_id`
- dashboard local sans relire tout l'historique

## Requêtes utiles

Mix de rejets par stage et raison:

```sql
SELECT
  stage,
  reason_code,
  COUNT(*) AS reject_count
FROM rejection_events
GROUP BY stage, reason_code
ORDER BY reject_count DESC;
```

Routes avec le plus de submit rejections:

```sql
SELECT
  route_id,
  COUNT(*) AS rejected_submits
FROM trade_latest
WHERE outcome = 'submit_rejected'
GROUP BY route_id
ORDER BY rejected_submits DESC;
```

Historique complet d'une soumission:

```sql
SELECT
  event_seq,
  audit_kind,
  submit_status,
  outcome,
  reported_at_unix_millis
FROM trade_events
WHERE submission_id = 'submission-123'
ORDER BY event_seq;
```

PnL réalisé par route:

```sql
SELECT
  route_id,
  SUM(COALESCE(realized_pnl_quote_atoms, 0)) AS realized_pnl_quote_atoms
FROM trade_latest
GROUP BY route_id
ORDER BY realized_pnl_quote_atoms DESC;
```

## Usage opératoire rapide

Compter les lignes stockées:

```bash
sqlite3 /var/lib/solana-bot/bot_observer.sqlite3 \
  "SELECT 'rejections', COUNT(*) FROM rejection_events
   UNION ALL
   SELECT 'trade_events', COUNT(*) FROM trade_events
   UNION ALL
   SELECT 'trade_latest', COUNT(*) FROM trade_latest;"
```

Vérifier le dernier `run_id`:

```bash
sqlite3 /var/lib/solana-bot/bot_observer.sqlite3 \
  "SELECT run_id, started_at_unix_millis
   FROM bot_runs
   ORDER BY started_at_unix_millis DESC
   LIMIT 5;"
```
