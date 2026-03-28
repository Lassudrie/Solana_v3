# Analyse détaillée des rejets

- Base analysée: `/tmp/bot-persistence-1774706561550460892.sqlite3`
- Généré le: 2026-03-28 17:37:22 UTC)
- Total rejets: 2

## Distribution réelle des rejets (global)

| Cause | Nombre | Part du total |
|---|---:|---:|
| execution_path_congested | 1 | 50.00% |
| remote_rejected | 1 | 50.00% |

- Cause dominante: `execution_path_congested` (50.00%, 1 rejet(s))
- Cause secondaire: `remote_rejected` (50.00%, 1 rejet(s))

### Tri demandé: code -> fréquence -> route

| code | route_id | fréquence | share_du_code |
|---|---|---:|---:|

## Détail rejet par opportunité (entrée/sortie, brut/net, coûts, garde-fous)

| id | route_id | stage | reason_code | input_atoms | output_atoms | gross edge bps | expected gross pnl | estimated total cost | expected net pnl | network fee | jito tip | shortfall/slippage | minimum profit floor | verdict final |
|---:|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| 1 | route-a | build | execution_path_congested | 1000 | 1050 | 7500 | 30 | 5 | 25 | 5 | N/A | 10 | N/A | net positif |
| 2 | route-a | submit | remote_rejected | 1000 | 1050 | 7500 | 30 | 5 | 25 | 5 | N/A | 10 | N/A | net positif |

## Analyse par route_id

| route_id | détections (proxy route+signature) | rejets | taux_rejet | cause principale | gross p50 | gross p95 | net p50 | net p95 | meilleur net observé |
|---|---:|---:|---:|---|---:|---:|---:|---:|---:|
| route-a | 1 | 1 | 100.00% | execution_path_congested | 30 | 30 | 25 | 25 | 25 |

## Remarque de limitation des logs
- Le seuil plancher (floor minimum profit) n’est pas explicitement présent dans ces payloads (`minimum_profit_floor_*` absent) : impossible de distinguer précisément les rejets “net positif mais sous floor”.
- `network_fee` est assimilé à `estimated_execution_cost_quote_atoms` car le schéma ne sépare pas encore un coût réseau dédié.
- `jito_tip` est disponible uniquement dans le payload `trade` (soumission) et non dans la structure de rejet, donc non exploitable pour tous les rejets.
- La source est très petite (2 rejets / 1 exécution), donc les pourcentages ne doivent pas être extrapolés globalement.
