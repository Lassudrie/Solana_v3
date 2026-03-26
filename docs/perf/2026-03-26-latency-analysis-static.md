# Analyse de latence statique - 2026-03-26

## Perimetre

Cette analyse est une revue **statique** du code et des points de mesure exposes par le bot.

Elle ne repose pas sur une capture live de `/monitor/signals`, `/monitor/trades` ou `/metrics`.
Les conclusions ci-dessous decrivent donc:

- la structure reelle du pipeline;
- les postes de latence dominants probables;
- les bornes de tail latency imposees par la configuration et les timeouts;
- les angles morts de l'instrumentation actuelle.

## Verdict court

Le hot path CPU local (`state -> select -> build -> sign`) est globalement court et deterministe.
Le principal contributeur a la latence et au p99 est aujourd'hui `submit`, loin devant les autres etapes, parce qu'il execute un appel HTTP bloquant avec retry.

Le deuxieme risque n'est pas une etape CPU lente mais la **saturation de la boucle mono-consommateur**: `queue_wait` peut monter sous charge, tandis que l'ingestion live peut aussi **dropper** des evenements quand le buffer est plein.

La mesure actuelle est utile mais incomplete:

- `quote` n'est pas isole de `select`;
- `source_latency` n'est pas renseignee de bout en bout;
- Prometheus expose surtout des totaux, pas des percentiles par stage;
- une faible `queue_wait` ne prouve pas l'absence de saturation si la source droppe avant d'enqueuer.

## Pipeline mesure aujourd'hui

Le runtime trace ces champs dans [`runtime.rs`](../../crates/bot/src/runtime.rs):

- `ingest_duration`
- `queue_wait_duration`
- `state_apply_duration`
- `select_duration`
- `build_duration`
- `sign_duration`
- `submit_duration`
- `total_to_submit`

Voir [`PipelineTrace`](../../crates/bot/src/runtime.rs) et l'alimentation des samples monitor dans [`observer.rs`](../../crates/bot/src/observer.rs).

Les samples exposes par `/monitor/signals` contiennent:

- `source_latency_nanos`
- `ingest_nanos`
- `queue_wait_nanos`
- `state_apply_nanos`
- `select_nanos`
- `build_nanos`
- `sign_nanos`
- `submit_nanos`
- `total_to_submit_nanos`

Les trades exposes par `/monitor/trades` ajoutent:

- `source_to_submit_nanos`
- `submit_to_terminal_nanos`
- `source_to_terminal_nanos`

## Analyse par etape

### 1. Ingest

`ingest` correspond a `source_received_at -> normalized_at` dans [`runtime.rs`](../../crates/bot/src/runtime.rs).
Dans l'etat actuel, cette etape est legere: normalisation locale, enrichissement metadata, emission de `NormalizedEvent`.

Le point faible n'est pas le cout CPU mais l'absence de vraie `source_latency`: le champ reste optionnel et souvent nul cote sources actuelles. Voir [`normalized-event.md`](../architecture/normalized-event.md).

Conclusion:

- latence probablement basse;
- visibilite insuffisante sur la latence amont reelle.

### 2. Queue Wait

`queue_wait` mesure le temps entre `normalized_at` et le debut de `process_event` dans [`runtime.rs`](../../crates/bot/src/runtime.rs).

La boucle daemon consomme les evenements **en serie** et limite le travail par tick via `max_events_per_tick`; si aucun evenement n'est traite, elle dort `idle_sleep_millis` avant le tour suivant. Voir [`daemon.rs`](../../crates/bot/src/daemon.rs).

Les defaults importants sont dans [`config.rs`](../../crates/bot/src/config.rs):

- profil standard: `idle_sleep_millis = 1`, `max_events_per_tick = 128`
- profil `ultra_fast`: `idle_sleep_millis = 0`, `max_events_per_tick = 4096`

Conclusion:

- sous faible charge, `queue_wait` doit rester tres bas;
- sous burst, cette etape capte directement le backlog de la boucle mono-consommateur;
- en profil standard, il existe une quantification naturelle autour de 1 ms lorsque le worker dort entre deux polls.

### 3. State Apply

`state_apply` execute un `upsert` compte puis decode les dependances de ce compte vers les pools impactes dans [`state/src/lib.rs`](../../crates/state/src/lib.rs).

Le cout est principalement proportionnel a:

- `nombre de dependances account -> pool`;
- `cout du decodeur` associe;
- `nombre de snapshots impactes`.

L'implementation est locale en memoire et ne fait pas de RPC synchrone.

Conclusion:

- latence attendue faible a moderee;
- cout surtout data-dependent, pas reseau-dependent;
- les comptes avec beaucoup de dependances sont les candidats naturels au p95 de cette etape.

### 4. Select / Quote

Le runtime ne mesure pas `quote` separement: la quote est incluse dans `select`.
Le stage `Quote` existe dans `PipelineStage`, mais il n'est pas enregistre par `runtime.process_event`. Voir [`metrics.rs`](../../crates/telemetry/src/metrics.rs) et [`control.rs`](../../crates/bot/src/control.rs).

Dans [`selector.rs`](../../crates/strategy/src/selector.rs), le cout de `select` est proportionnel a:

- `nombre de routes impactees`;
- `nombre de tailles testees par route`;
- `cout de quote` pour chaque taille;
- evaluation des guards.

Le selector itere sur `trade_sizes(route)` et peut tester plusieurs tailles avant d'arreter apres deux declines consecutifs.

Conclusion:

- `select` est le principal cout CPU pur du hot path;
- son p95 depend directement de la cardinalite `routes impactees x tailles testees`;
- sans stage `quote` dedie, il est impossible de separer "cout alpha" et "cout selection/guards".

### 5. Build

Le builder est entierement local: resolution ALT deja rafraichies, compilation d'instructions, compilation du message Solana, serialisation finale. Voir [`transaction_builder.rs`](../../crates/builder/src/transaction_builder.rs).

Le cout varie avec:

- mode `Legacy` vs `V0`;
- nombre d'ALTs resolues;
- complexite des legs compilees.

Conclusion:

- latence typiquement faible a moderee;
- spikes possibles si les routes utilisent davantage d'ALTs ou de resolution de comptes.

### 6. Sign

Deux profils tres differents existent dans [`signer.rs`](../../crates/signing/src/signer.rs):

- `LocalWalletSigner`: signature locale, purement CPU;
- `SecureUnixWalletSigner`: socket Unix avec timeout de connexion et timeout IO.

Les defaults config sont:

- `connect_timeout_ms = 50`
- `read_timeout_ms = 50`

Conclusion:

- avec signer local: etape tres courte;
- avec signer Unix: tail latency et echec peuvent atteindre environ 100 ms ou plus selon les retries de connexion et l'etat du daemon signer.

### 7. Submit

`submit` est aujourd'hui le **goulot de latence principal**.

Le submitter Jito utilise `reqwest::blocking::Client` et execute un POST HTTP bloquant dans le hot path. Voir [`jito.rs`](../../crates/submit/src/jito.rs).

Defaults config:

- `connect_timeout_ms = 300`
- `request_timeout_ms = 1000`
- `retry_attempts = 3`
- `retry_backoff_ms = 50`

En cas de transport failure, le submitter peut faire jusqu'a 3 tentatives avec backoff exponentiel simple.
Sur la branche la plus defavorable, on obtient un ordre de grandeur theorique d'environ:

- `3 x 1000 ms` de timeout requete
- `50 ms + 100 ms` de backoff
- puis un `ws.probe()` supplementaire sans borne explicite visible dans cette couche

Conclusion:

- `submit` domine vraisemblablement le p95/p99 de `source_to_submit`;
- la tail latency peut facilement depasser 3 s avant echec franc;
- tant que `submit` reste synchrone dans la boucle hot path, il amplifie aussi `queue_wait` pour les evenements suivants.

### 8. Submit to Terminal

La latence terminale depend ensuite de la reconciliation asynchrone et des signaux on-chain.
Le poll interval par defaut est `100 ms`, reduit a `25 ms` en profil `ultra_fast`. Voir [`config.rs`](../../crates/bot/src/config.rs).

Le monitor reconstruit:

- `submit_to_terminal_nanos`
- `source_to_terminal_nanos`

dans [`observer.rs`](../../crates/bot/src/observer.rs).

Conclusion:

- meme avec un landing instantane, `submit_to_terminal` est quantifie par la cadence de reconciliation si le signal terminal n'arrive pas autrement;
- la lecture de cette metrique doit toujours etre rapprochee du profil runtime actif.

## Risques majeurs

### P0. `submit` bloque toute la boucle

Tant que `submit` reste dans `process_event`, un reseau lent ou des retries Jito ralentissent non seulement la route courante mais aussi l'ingestion chaude des evenements suivants.

Impact:

- hausse de `submit_nanos`
- hausse de `total_to_submit_nanos`
- hausse indirecte de `queue_wait_nanos`

### P0. Saturation silencieuse de la file live

La source live utilise un `sync_channel`, et l'emission vers cette file est faite via `try_send`.
Quand le buffer est plein, l'erreur est ignoree cote live source. Voir [`live.rs`](../../crates/bot/src/live.rs).

Impact:

- des evenements peuvent etre perdus sans backpressure explicite;
- `queue_wait` peut rester artificiellement contenu alors que le systeme perd deja des updates;
- il manque une metrique explicite de drops cote event source.

### P1. Angle mort sur `quote`

Le stage `Quote` existe semantiquement mais pas dans la mesure runtime actuelle.
Toute analyse de `select` melange:

- quote;
- guards;
- comparaison des candidats;
- iteration sur le size ladder.

Impact:

- impossible d'optimiser precisement la couche alpha;
- impossible de savoir si le cout vient du pricing, de la garde, ou du balayage des tailles.

### P1. `source_latency` encore inutilisable

Sans timestamp source fiable et transporte de bout en bout, la mesure d'avance informationnelle reste partielle.

Impact:

- impossible d'estimer correctement `wire -> bot`;
- comparaisons inter-sources peu fiables;
- `source_to_submit` reste surtout une mesure "depuis reception locale".

## Classement des contributeurs probables

1. `submit`
2. `queue_wait` sous charge
3. `select` sur routes/tailles nombreuses
4. `sign` si `secure_unix`
5. `build`
6. `state_apply`
7. `ingest`

## Recommandations

### Instrumentation

1. Enregistrer un vrai stage `quote`, distinct de `select`.
2. Compter explicitement les drops de la file live, comme c'est deja fait pour l'observer.
3. Exporter des percentiles par stage dans Prometheus, pas seulement des totaux.
4. Transporter une vraie `source_latency` amont quand la source peut la fournir.

### Architecture

1. Decoupler `submit` du coeur `process_event` si l'objectif prioritaire est de proteger `queue_wait`.
2. Garder `ultra_fast` comme profil de base pour les campagnes de mesure latence.
3. Verifier si `ws.probe()` doit avoir un timeout explicite pour ne pas allonger le p99 d'echec.

### Mesure live a faire ensuite

1. Capturer `/monitor/signals` sur une fenetre representative.
2. Capturer `/monitor/trades` sur la meme fenetre.
3. Calculer `p50/p95/p99` par route et par venue pour `select`, `submit`, `source_to_submit`.
4. Correlater `queue_wait` avec:
   - `bot_shredstream_events_per_second`
   - rejections
   - expired / transport failures

## References internes

- [`crates/bot/src/runtime.rs`](../../crates/bot/src/runtime.rs)
- [`crates/bot/src/daemon.rs`](../../crates/bot/src/daemon.rs)
- [`crates/bot/src/observer.rs`](../../crates/bot/src/observer.rs)
- [`crates/bot/src/control.rs`](../../crates/bot/src/control.rs)
- [`crates/bot/src/config.rs`](../../crates/bot/src/config.rs)
- [`crates/state/src/lib.rs`](../../crates/state/src/lib.rs)
- [`crates/strategy/src/selector.rs`](../../crates/strategy/src/selector.rs)
- [`crates/builder/src/transaction_builder.rs`](../../crates/builder/src/transaction_builder.rs)
- [`crates/signing/src/signer.rs`](../../crates/signing/src/signer.rs)
- [`crates/submit/src/jito.rs`](../../crates/submit/src/jito.rs)
