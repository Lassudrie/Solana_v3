# Crate `builder`

## Rôle

`builder` transforme un `OpportunityCandidate` en transaction Solana non signée prête à être
signée puis soumise. La crate porte:

- la config d'exécution venue par venue
- les templates de planification de legs
- la compilation des instructions Solana
- les refus explicites de build

## Point d'entrée

`AtomicArbTransactionBuilder` implémente `TransactionBuilder` et prend un `BuildRequest`:

- `candidate`: choix de la stratégie
- `dynamic`: blockhash, fee payer, ALTs et paramètres compute live

Il retourne un `BuildResult` soit construit, soit rejeté.

## Invariants

- Le build échoue si le route execution config est absent ou incompatible.
- Le builder rejette les quotes trop vieilles pour l'exécution.
- La limite de compute doit respecter le minimum imposé par le type de route.
- Le message final doit respecter les limites de taille paquet et d'account locks.
- Le plan d'exécution doit garantir un output minimal cohérent avec le candidat.

## Inventaire des fichiers

| Fichier | Rôle | Eléments clés |
| --- | --- | --- |
| `crates/builder/src/lib.rs` | Ré-export central + tests d'intégration crate | exports builder, config d'exécution et types |
| `crates/builder/src/execution.rs` | Config d'exécution des routes | `ExecutionRegistry`, `RouteExecutionConfig`, configs Orca/Raydium |
| `crates/builder/src/templates.rs` | Matérialisation du plan atomique | `AtomicTwoLegTemplate`, `AtomicLegPlan`, memos de legs |
| `crates/builder/src/transaction_builder.rs` | Compilation de transaction | `AtomicArbTransactionBuilder`, compilation d'instructions, ALTs, compute budget |
| `crates/builder/src/types.rs` | Contrats d'entrée/sortie | `BuildRequest`, `UnsignedTransactionEnvelope`, `BuildRejectionReason` |

## `execution.rs`

Le registre d'exécution décrit, pour chaque route:

- format de message (`Legacy` ou `V0`)
- lookup tables requises
- bornes compute/priorité tip
- tolérance de fraîcheur de quote et d'ALT
- description venue par venue des comptes et programmes nécessaires

## `templates.rs`

Le template:

- convertit les `LegQuote` de la stratégie en `AtomicLegPlan`
- choisit `ExactIn` ou `ExactOut` selon les capacités de la venue
- applique éventuellement un buffer d'input pour les legs d'achat base
- génère des mémos de debug par leg

Le nom `AtomicTwoLegTemplate` est historique; l'implémentation couvre aussi 3 legs.

## `transaction_builder.rs`

Le gros module:

- vérifie la cohérence route/config d'exécution
- résout les ALTs du runtime et filtre les ALTs trop vieux
- injecte compute budget et tip Jito
- prépare éventuellement les ATA idempotentes
- compile les instructions Orca simple, Orca Whirlpool, Raydium simple et Raydium CLMM
- construit le message `Legacy` ou `V0`
- sérialise le message et vérifie taille/locks
- décrit les instructions sous forme d'`InstructionTemplate` pour observabilité
