# Playbook d'investigation des rejets et de la profitabilite

## But

Ce document sert a repondre rapidement a trois questions:

1. Quel est le reject mix reel du bot, par code puis par route.
2. Existe-t-il au moins quelques candidats avec profit net positif avant floor.
3. Si oui, sont-ils bloques par calibration des garde-fous, ou plus bas dans le pipeline.

Le point cle est de sortir d'une lecture impressionniste. Il faut pouvoir dire:

- `profit_below_threshold`: X %
- `source_balance_too_low`: Y %
- `quote_failed`: Z %
- `autre`: W %

et savoir si ce mix est global, ou concentre sur quelques `route_id` tres bavardes.

## Sources de verite actuelles

Le depot expose deja plusieurs sources utiles:

- [`rejection_events`](../bot/persistence.md): rejets append-only par stage, route, code et detail.
- [`trade_events` et `trade_latest`](../bot/persistence.md): soumissions, transitions et PnL attendu/realise.
- `/monitor/rejections`, `/monitor/trades`, `/monitor/edge`, consultables via [`botctl`](../../crates/bot/src/bin/botctl.rs).
- Les objets [`OpportunityCandidate`](../../crates/strategy/src/opportunity.rs), [`MonitorTradeEvent`](../../crates/bot/src/observer.rs) et [`PersistedCandidateContext`](../../crates/bot/src/persistence.rs) contiennent deja une partie des donnees economiques.

En pratique, la source principale pour cette investigation doit etre SQLite, pas le monitor HTTP:

- `/monitor/rejections` est borne en memoire et oriente debug live.
- SQLite permet des agregations fiables sur les 100 a 500 derniers rejets, ou sur un `run_id` complet.

## Ce qu'on peut faire maintenant

Avec le schema actuel, on peut faire des analyses fiables sur:

- la distribution des rejets par `stage`, `reason_code` et `route_id`;
- la concentration des rejets sur quelques routes;
- la raison principale et secondaire par route;
- les metriques post-selection et post-submit par route via `trade_latest` et `/monitor/edge`;
- un comparatif partiel `gross -> cost -> net` sur les candidats qui ont atteint le statut `best_candidate`.

## Ce qui manque encore

Le schema actuel ne permet pas de repondre proprement a deux demandes importantes:

1. Le vrai nombre de detections par route.
   Aujourd'hui, le bot persiste les rejets et le seul `best_candidate` retenu pour build/submit. Les candidats acceptes mais non selectionnes ne sont pas journalises durablement. Le `nombre de detections` par route n'est donc pas reconstructible de maniere fiable.

2. La ligne de verite complete pour chaque opportunite rejetee.
   Le payload de rejet ne contient pas toujours:
   - `input atoms`
   - `output atoms`
   - `gross edge bps`
   - `network fee`
   - `jito tip`
   - `shortfall/slippage allowance`
   - `minimum profit floor`
   - `minimum required output`
   - `source balance`
   - `snapshot age`
   - `blockhash slot`

Il y a un point d'attention important: pour les rejets `strategy`, le champ `payload_json.candidate` n'est pas la verite du candidat rejete. Dans [`persisted_candidate_context()`](../../crates/bot/src/persistence.rs), ce payload est rempli a partir de `best_candidate`, pas a partir de la route rejetee. Il ne faut donc pas s'en servir pour faire une analyse economique par rejet strategie.

## Verdict de faisabilite

- Reject mix global: faisable maintenant.
- Reject mix par route: faisable maintenant.
- Route principale et secondaire de rejet: faisable maintenant.
- Comparatif `gross pnl` vs `net pnl` vs `minimum required` pour chaque rejet: partiellement faisable seulement.
- Nombre de detections par route et taux de rejet reel: non faisable proprement sans telemetrie supplementaire.

## Plan d'investigation recommande

### 1. Sortir les 100 a 500 derniers rejets avec metadata

Commencer par le dernier `run_id`, puis extraire les derniers rejets:

```bash
DB=/var/lib/solana-bot/bot_observer.sqlite3

sqlite3 "$DB" <<'SQL'
.headers on
.mode column
WITH latest_run AS (
  SELECT run_id
  FROM bot_runs
  ORDER BY started_at_unix_millis DESC
  LIMIT 1
)
SELECT
  event_seq,
  stage,
  route_id,
  reason_code,
  reason_detail,
  observed_slot,
  expected_profit_quote_atoms,
  datetime(occurred_at_unix_millis / 1000, 'unixepoch') AS ts_utc
FROM rejection_events
WHERE run_id = (SELECT run_id FROM latest_run)
ORDER BY event_seq DESC
LIMIT 500;
SQL
```

Utiliser `LIMIT 100`, `200` ou `500` selon le bruit du run.

### 2. Faire le tableau d'agregation par reject code

Commencer par separer les stages. Le mix `strategy` ne raconte pas la meme chose que `build` ou `submit`.

```bash
sqlite3 "$DB" <<'SQL'
.headers on
.mode column
WITH latest_run AS (
  SELECT run_id
  FROM bot_runs
  ORDER BY started_at_unix_millis DESC
  LIMIT 1
),
recent AS (
  SELECT *
  FROM rejection_events
  WHERE run_id = (SELECT run_id FROM latest_run)
  ORDER BY event_seq DESC
  LIMIT 500
),
agg AS (
  SELECT
    stage,
    reason_code,
    COUNT(*) AS rejects
  FROM recent
  GROUP BY stage, reason_code
)
SELECT
  stage,
  reason_code,
  rejects,
  ROUND(100.0 * rejects / SUM(rejects) OVER (), 2) AS pct_total,
  ROUND(100.0 * rejects / SUM(rejects) OVER (PARTITION BY stage), 2) AS pct_in_stage
FROM agg
ORDER BY rejects DESC, reason_code;
SQL
```

La lecture attendue est:

- cause dominante globale;
- cause dominante du stage `strategy`;
- cause secondaire;
- poids du tail `autre`.

### 3. Faire le tableau d'agregation par route_id

Il faut identifier si le probleme est concentre sur quelques routes.

```bash
sqlite3 "$DB" <<'SQL'
.headers on
.mode column
WITH latest_run AS (
  SELECT run_id
  FROM bot_runs
  ORDER BY started_at_unix_millis DESC
  LIMIT 1
),
recent AS (
  SELECT *
  FROM rejection_events
  WHERE run_id = (SELECT run_id FROM latest_run)
  ORDER BY event_seq DESC
  LIMIT 500
),
counts AS (
  SELECT
    route_id,
    reason_code,
    COUNT(*) AS rejects
  FROM recent
  GROUP BY route_id, reason_code
),
ranked AS (
  SELECT
    route_id,
    reason_code,
    rejects,
    SUM(rejects) OVER (PARTITION BY route_id) AS total_rejects,
    ROUND(
      100.0 * rejects / SUM(rejects) OVER (PARTITION BY route_id),
      2
    ) AS pct_route,
    ROW_NUMBER() OVER (
      PARTITION BY route_id
      ORDER BY rejects DESC, reason_code
    ) AS rn
  FROM counts
)
SELECT
  route_id,
  MAX(total_rejects) AS total_rejects,
  MAX(CASE WHEN rn = 1 THEN reason_code END) AS primary_reason,
  MAX(CASE WHEN rn = 1 THEN rejects END) AS primary_reason_count,
  MAX(CASE WHEN rn = 1 THEN pct_route END) AS primary_reason_pct,
  MAX(CASE WHEN rn = 2 THEN reason_code END) AS secondary_reason,
  MAX(CASE WHEN rn = 2 THEN rejects END) AS secondary_reason_count,
  MAX(CASE WHEN rn = 2 THEN pct_route END) AS secondary_reason_pct
FROM ranked
GROUP BY route_id
ORDER BY total_rejects DESC, route_id;
SQL
```

Ce tableau repond a la question utile: est-ce un probleme systemique, ou est-ce que 5 routes tres bruyantes polluent toute la lecture globale.

### 4. Comparer `gross pnl`, `cost`, `net pnl` sur les meilleurs candidats rejetes

Avec le schema actuel, cette analyse n'est propre que pour les rejets `build` et `submit`, ou pour les trades deja passes dans `trade_latest`.

Requete partielle sur les rejets `build` et `submit`:

```bash
sqlite3 "$DB" <<'SQL'
.headers on
.mode column
WITH latest_run AS (
  SELECT run_id
  FROM bot_runs
  ORDER BY started_at_unix_millis DESC
  LIMIT 1
),
candidate_rejects AS (
  SELECT
    event_seq,
    stage,
    route_id,
    reason_code,
    reason_detail,
    CAST(json_extract(payload_json, '$.candidate.trade_size') AS INTEGER) AS input_atoms,
    CAST(json_extract(payload_json, '$.candidate.expected_edge_quote_atoms') AS INTEGER) AS expected_gross_pnl,
    CAST(json_extract(payload_json, '$.candidate.estimated_execution_cost_quote_atoms') AS INTEGER) AS estimated_total_cost,
    CAST(json_extract(payload_json, '$.candidate.expected_pnl_quote_atoms') AS INTEGER) AS expected_net_pnl,
    CAST(json_extract(payload_json, '$.candidate.expected_shortfall_quote_atoms') AS INTEGER) AS expected_shortfall,
    CAST(json_extract(payload_json, '$.candidate.p_land_bps') AS INTEGER) AS p_land_bps
  FROM rejection_events
  WHERE run_id = (SELECT run_id FROM latest_run)
    AND stage IN ('build', 'submit')
    AND json_extract(payload_json, '$.candidate.route_id') IS NOT NULL
)
SELECT
  event_seq,
  stage,
  route_id,
  reason_code,
  input_atoms,
  expected_gross_pnl,
  estimated_total_cost,
  expected_net_pnl,
  expected_shortfall,
  p_land_bps
FROM candidate_rejects
ORDER BY expected_net_pnl DESC, event_seq DESC
LIMIT 20;
SQL
```

Limite importante:

- cette requete ne separe pas `network fee` et `jito tip`;
- elle ne donne pas `minimum profit floor`;
- elle ne donne pas `minimum required output`;
- elle n'est pas valable pour les rejets `strategy`.

### 5. Faire une vue par route sur les candidats effectivement soumis

Ce n'est pas le vrai taux de detection, mais c'est deja utile pour distinguer les routes qui capturent de l'edge de celles qui ne produisent jamais rien de bon.

```bash
sqlite3 "$DB" <<'SQL'
.headers on
.mode column
WITH latest_run AS (
  SELECT run_id
  FROM bot_runs
  ORDER BY started_at_unix_millis DESC
  LIMIT 1
),
selected AS (
  SELECT
    route_id,
    CAST(json_extract(payload_json, '$.trade.expected_edge_quote_atoms') AS INTEGER) AS gross_pnl,
    CAST(json_extract(payload_json, '$.trade.expected_pnl_quote_atoms') AS INTEGER) AS net_pnl
  FROM trade_latest
  WHERE run_id = (SELECT run_id FROM latest_run)
),
gross_ranked AS (
  SELECT
    route_id,
    gross_pnl,
    ROW_NUMBER() OVER (PARTITION BY route_id ORDER BY gross_pnl) AS rn,
    COUNT(*) OVER (PARTITION BY route_id) AS n
  FROM selected
  WHERE gross_pnl IS NOT NULL
),
net_ranked AS (
  SELECT
    route_id,
    net_pnl,
    ROW_NUMBER() OVER (PARTITION BY route_id ORDER BY net_pnl) AS rn,
    COUNT(*) OVER (PARTITION BY route_id) AS n
  FROM selected
  WHERE net_pnl IS NOT NULL
),
gross_stats AS (
  SELECT
    route_id,
    MAX(CASE WHEN rn = ((n + 1) / 2) THEN gross_pnl END) AS gross_p50,
    MAX(CASE WHEN rn = (((n * 95) + 99) / 100) THEN gross_pnl END) AS gross_p95
  FROM gross_ranked
  GROUP BY route_id
),
net_stats AS (
  SELECT
    route_id,
    MAX(CASE WHEN rn = ((n + 1) / 2) THEN net_pnl END) AS net_p50,
    MAX(CASE WHEN rn = (((n * 95) + 99) / 100) THEN net_pnl END) AS net_p95
  FROM net_ranked
  GROUP BY route_id
)
SELECT
  s.route_id,
  COUNT(*) AS selected_count,
  g.gross_p50,
  g.gross_p95,
  n.net_p50,
  n.net_p95,
  MAX(s.net_pnl) AS best_net_pnl
FROM selected s
LEFT JOIN gross_stats g
  ON g.route_id = s.route_id
LEFT JOIN net_stats n
  ON n.route_id = s.route_id
GROUP BY s.route_id, g.gross_p50, g.gross_p95, n.net_p50, n.net_p95
ORDER BY best_net_pnl DESC, selected_count DESC, s.route_id;
SQL
```

Attention: ceci decrit les routes qui ont atteint une soumission, pas toutes les opportunites detectees.
La mediane et le p95 calcules ici sont des quantiles discrets sur `trade_latest`; pour une analyse plus stricte par tentative de soumission, preferer `trade_events` filtre sur `audit_kind = 'submission'`.

## Analyse par route: ce qu'il faut vraiment viser

Pour chaque `route_id`, la cible d'analyse devrait etre:

- nombre de detections;
- taux de rejet;
- raison principale de rejet;
- mediane et p95 du profit brut;
- mediane et p95 du profit net;
- meilleur profit net observe.

Avec le schema actuel, seules les trois dernieres lignes sont partiellement accessibles sur les routes qui ont deja produit un `best_candidate` ou une soumission. Le vrai `nombre de detections` et le vrai `taux de rejet` demandent un journal de tous les candidats evalues.

## Ligne de verite economique a ajouter

Si l'objectif est de comprendre pourquoi tout est rejete, il faut une ligne de verite durable pour chaque opportunite evaluee, ou au minimum pour la meilleure opportunite rejetee du cycle.

Le minimum utile est:

- `route_id`
- `decision_stage`
- `reject_code`
- `reject_detail`
- `input_atoms`
- `gross_output_atoms`
- `net_output_atoms`
- `gross_edge_bps`
- `expected_gross_pnl_quote_atoms`
- `estimated_network_fee_quote_atoms`
- `jito_tip_quote_atoms`
- `estimated_total_cost_quote_atoms`
- `expected_shortfall_quote_atoms`
- `minimum_profit_floor_quote_atoms`
- `minimum_acceptable_output`
- `expected_net_profit_quote_atoms`
- `expected_value_quote_atoms`
- `source_input_balance`
- `oldest_snapshot_slot`
- `quote_state_age_slots`
- `blockhash_slot`
- `observed_slot`

La logique de lecture attendue doit devenir explicite:

- `gross positif, net negatif a cause du tip`
- `net positif mais sous floor`
- `net negatif avant meme le floor`
- `net positif, selectionne, mais bloque ensuite en build/submit`

## Telemetrie "best rejected candidate"

Quick win recommande: ajouter une telemetrie dediee `best_rejected_candidate`.

A chaque cycle sans submit, il faut logguer au moins:

- `route_id`
- `reason_code`
- `reason_detail`
- `gross_pnl`
- `net_pnl`
- `min_required_pnl`
- `input_amount`
- `tip_estimate`
- `source_balance`
- `snapshot_age_slots`
- `blockhash_slot`

Cette telemetrie doit vivre dans le meme plan d'audit que `rejection_events`, pas uniquement en `eprintln!`.

## Pistes d'implementation

Les points de code naturels pour ajouter cette visibilite sont:

- [`crates/strategy/src/selector.rs`](../../crates/strategy/src/selector.rs): c'est ici que `gross`, `cost`, `tip`, `shortfall`, `expected_value` et `minimum_acceptable_output` coexistent deja.
- [`crates/strategy/src/opportunity.rs`](../../crates/strategy/src/opportunity.rs): ajouter un objet d'audit pour les candidats rejetes ou non selectionnes.
- [`crates/bot/src/observer.rs`](../../crates/bot/src/observer.rs): exposer un event `best_rejected_candidate` ou etendre `RejectionEvent`.
- [`crates/bot/src/persistence.rs`](../../crates/bot/src/persistence.rs): creer une table append-only de type `opportunity_events` ou enrichir le schema de rejet.
- [`crates/strategy/src/guards.rs`](../../crates/strategy/src/guards.rs): remplacer le `eprintln!` actuel de `profit_below_threshold` par un event structure et durable.

Le `eprintln!` actuel dans `guards.rs` est utile comme indice, mais insuffisant:

- il ne couvre qu'un reject code;
- il n'est pas durable;
- il ne separe pas `network fee`, `tip`, `floor` et `shortfall`.

## Experiences controlees

Faire varier un seul parametre a la fois. Le but n'est pas de trouver une config de prod, mais d'isoler le filtre qui tue tout.

Le depot expose aujourd'hui les leviers suivants:

- `strategy.min_profit_quote_atoms`
- `strategy.min_profit_bps`
- `routes[].min_profit_quote_atoms`
- `builder.jito_tip_policy.min_lamports`
- `builder.jito_tip_policy.share_bps_of_expected_net_profit`
- `builder.jito_tip_lamports`
- `routes[].execution.default_jito_tip_lamports`
- `strategy.sizing.base_expected_shortfall_bps`
- `strategy.sizing.max_expected_shortfall_bps`
- `strategy.sizing.fixed_trade_size`
- `routes[].default_trade_size`
- `routes[].size_ladder`
- `routes[].enabled`

Note utile: il n'existe pas aujourd'hui de knob explicite `min_edge_bps`. Les leviers les plus proches sont le floor de profit, le tip et la penalite de shortfall.

Sequence recommandee:

1. Baisser temporairement `strategy.min_profit_quote_atoms`, puis `strategy.min_profit_bps`.
2. Si besoin, baisser aussi `routes[].min_profit_quote_atoms` sur quelques routes liquides.
3. Baisser le tip minimal via `builder.jito_tip_policy.min_lamports`, puis la part `share_bps_of_expected_net_profit`.
4. Reduire `strategy.sizing.base_expected_shortfall_bps` et `max_expected_shortfall_bps` pour tester si la penalite de risque tue tout.
5. Forcer un input plus petit avec `strategy.sizing.fixed_trade_size = true` et un ladder reduit.
6. Restreindre l'univers a 5 a 10 routes majeures via `routes[].enabled`.
7. Faire un run court avec seuils volontairement permissifs pour verifier si le pipeline atteint techniquement `submit`.

## Arbre de lecture des resultats

- Aucun candidat avec `expected_net_profit_quote_atoms > 0` avant floor:
  probleme economique. Les routes, les tailles ou les couts ne laissent pas de marge.

- Des candidats avec net positif, mais rejetes par `profit_below_threshold`:
  probleme de calibration des garde-fous. Commencer par floor, tip et shortfall.

- Des candidats positifs selectionnes, mais aucun submit:
  regarder ensuite `build` puis `submit`, pas avant.

- Des submits existent, mais le resultat final reste mauvais:
  le sujet n'est plus la detection, mais le landing, le chain execution ou la calibration d'execution protection.

## Checklist minimale pour un run de debug

1. Exporter les 100 a 500 derniers rejets.
2. Sortir le tableau `reason_code -> count -> %`.
3. Sortir le tableau `route_id -> primary_reason -> secondary_reason`.
4. Extraire les 20 meilleurs candidats rejetes avec les champs economiques disponibles.
5. Restreindre l'univers a quelques routes tres liquides.
6. Lancer un run avec seuils permissifs.
7. Repondre a la question centrale: existe-t-il au moins quelques candidats avec net positif avant floor.

## Outils operatoires rapides

Vue live rapide:

```bash
cargo run -p bot --bin botctl -- --endpoint http://127.0.0.1:8081 rejects --stage strategy --limit 200
cargo run -p bot --bin botctl -- --endpoint http://127.0.0.1:8081 trades --status all --limit 100
cargo run -p bot --bin botctl -- --endpoint http://127.0.0.1:8081 edge --sort expected --limit 50
```

Ces vues sont utiles pour le triage live, mais la conclusion doit rester fondee sur SQLite.
