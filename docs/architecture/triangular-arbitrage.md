# Triangular Arbitrage Support

## Scope

Le bot supporte maintenant deux formes explicites de routes atomiques:

- `RouteKind::TwoLeg`
- `RouteKind::Triangular`

Le hot path reste piloté par configuration explicite. Aucune génération combinatoire globale de triangles n'est faite au runtime.

## Design

Le coeur du changement est la généralisation du modèle de route autour de `RouteLegSequence<T>`:

- `Two([T; 2])`
- `Three([T; 3])`

Ce choix garde des invariants forts sans introduire de `Vec` dynamique dans les structures chaudes déjà évaluées par la stratégie, le quote engine et le builder.

Les points d'intégration principaux sont:

- `strategy::route_registry`: type de route, invariants, validation
- `bot::config` et `bot::bootstrap`: parsing TOML, résolution des legs, erreurs explicites
- `strategy::quote` et `strategy::selector`: chaînage `exact_input` sur 2 ou 3 legs
- `strategy::guards` et `strategy::opportunity`: garde-fous et observabilité enrichie
- `builder`: planification et construction atomique 2 ou 3 legs

## Invariants

Une route enregistrée doit respecter:

- cardinalité cohérente avec `kind`
- route fermée: `input_mint == output_mint`
- `quote_mint`, si renseigné, égal au mint de profit de la route
- chaque leg a un `input_mint` et un `output_mint` explicites
- chaque leg chaîne sur le suivant
- le dernier leg revient sur l'asset initial
- pas de pool dupliqué sur une même route

Pour les routes triangulaires, le bootstrap rejette explicitement:

- legs non chaînés
- cycle non fermé
- legs sans mints explicites
- pool de conversion SOL/quote incohérent
- défaut de cardinalité

## Hot Path

Le quote engine exécute maintenant le cycle complet en mémoire:

1. quote leg 1
2. réinjection du output dans leg 2
3. réinjection du output dans leg 3
4. comparaison du output final avec l'input initial

Le sélecteur ne considère une route triangulaire tradable que si:

- les 3 snapshots sont présents
- les 3 legs sont exécutables
- le plus vieux snapshot respecte la fenêtre de fraîcheur
- les modèles concentrés requis sont chargés
- la conversion des coûts d'exécution reste possible si le mint de profit n'est pas SOL

## Risk And Builder Policy

Les routes triangulaires durcissent les seuils par défaut:

- `minimum_profit_multiplier_bps = 15000`
- `expected_shortfall_multiplier_bps = 15000`
- `minimum_compute_unit_limit = 420000`

Le builder refuse la construction si:

- la route ne matche pas son exécution venue par venue
- la borne de compute est sous le plancher de la route
- la transaction dépasse la taille packet/account-lock

Quand la transaction tient, les 3 swaps sont construits dans l'ordre du cycle dans une seule transaction versionnée.

## Known Limits

- Le nom `AtomicTwoLegTemplate` est conservé pour limiter le refactor, mais la logique interne supporte maintenant 2 et 3 legs.
- Les routes triangulaires restent strictement pilotées par manifest.
- Le budget compute et la taille de message restent les limites physiques dominantes pour certains triples legs lourds.

## Perf Impact

- coût CPU: une passe de quote et de guards supplémentaire au maximum sur un 3e leg
- coût mémoire: neutre à faible, grâce à `RouteLegSequence`
- coût builder: plus d'instructions et d'accounts, donc plus de risques de rejet taille/compute, mais sans RPC ajouté au hot path

## Next Optimizations

- renommer `AtomicTwoLegTemplate` vers un nom neutre
- ajouter une estimation plus fine du compute par combinaison de venues
- ajouter un scoring dédié 3-leg tenant compte d'une érosion de spread par hop
- dédupliquer encore mieux les accounts inter-legs pour les triangles multi-CLMM
