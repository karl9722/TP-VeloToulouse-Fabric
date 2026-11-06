# TP VéloToulouse — Plateforme de données quasi temps réel avec Microsoft Fabric

## Présentation

Ce projet répond au TP **« Building a unified data platform for real-time event analysis »**. Il vise à construire une chaîne de collecte, de traitement, de stockage et de visualisation de données fréquemment actualisées. Notre groupe utilise **Microsoft Fabric** et les données ouvertes de disponibilité des stations **VéloToulouse**.

**État du projet :** la **partie 1 (ingestion)** est fonctionnelle et validée sur plusieurs cycles. Le stockage historique et la visualisation sont les étapes suivantes, prises en charge par les autres membres du groupe.

## Architecture

```text
API Toulouse Métropole (VéloToulouse)
              |
              | Requêtes HTTP GET périodiques
              v
Python : ingestion_velotoulouse.py
  - Pagination de 100 enregistrements
  - Lecture du JSON embarqué
  - Déduplication par station_id
  - Extraction vélos mécaniques / électriques
  - Horodatage et journalisation
              |
              | Azure Event Hubs SDK (AMQP over WebSockets)
              v
Microsoft Fabric — Eventstream ES_VeloToulouse
  Source : SRC_VeloToulouse_API
  Flux   : ES_VeloToulouse-stream
              |
              v
Eventhouse / KQL Database  [À réaliser : partie 2]
              |
              v
Power BI / tableaux de bord [À réaliser : partie 3]
```

> L'API est interrogée toutes les **60 secondes** environ (intervalle entre les débuts de cycle, hors dépassement de durée). Il s'agit de **quasi-temps réel par polling**, et non d'un flux natif poussé par l'API. Le programme doit rester lancé pour continuer la collecte.

## Source de données

- **Portail :** [Toulouse Métropole Open Data](https://data.toulouse-metropole.fr/explore/dataset/velotoulouse-disponibilite-temps-reel/api/)
- **API Opendatasoft v2.1 :** `https://data.toulouse-metropole.fr/api/explore/v2.1/catalog/datasets/velotoulouse-disponibilite-temps-reel/records`
- **Méthode :** `GET` avec paramètres `limit=100` et `offset` pour la pagination.
- **Contenu :** nombre de vélos et de places disponibles, types de vélos, état de location et retour, horodatages et identifiants de stations.

Le nombre d'enregistrements API est supérieur au nombre de stations distinctes : plusieurs lignes peuvent correspondre à la même station. La transformation regroupe les lignes par `station_id` et conserve celle dont `last_reported` est le plus récent.

## Organisation du projet

```text
TP-VeloToulouse/
├── .env                       # Secret local, jamais versionné
├── .gitignore
├── README.md
├── requirements.txt
├── data/                      # Fichiers de travail éventuels
├── logs/
│   └── ingestion.log          # Journal généré à l'exécution
└── scripts/
    ├── ingestion_velotoulouse.py
    └── test_fabric.py
```

## Prérequis

- Windows / PowerShell (ou un terminal équivalent)
- Python 3.12 (version utilisée pendant les tests)
- Accès à Microsoft Fabric et à un workspace disposant d'une capacité compatible
- Eventstream Fabric avec une source **Custom endpoint** configurée
- Accès Internet à l'API Toulouse Métropole

Dépendances du fichier `requirements.txt` :

```text
requests
azure-eventhub
python-dotenv
```

## Installation

Depuis PowerShell :

```powershell
cd "D:\TP-VeloToulouse"
python -m pip install -r requirements.txt
```

### Configuration des secrets

Créer le fichier `.env` à la racine du projet :

```dotenv
FABRIC_EVENTHUB_CONNECTION_STRING="<CHAINE_DE_CONNEXION_FABRIC>"
```

Récupérer la chaîne dans **Fabric → ES_VeloToulouse → source Custom endpoint → Details → SAS Key Authentication → Connection string-primary key**. Ne jamais publier cette valeur dans un dépôt, un rapport ou une capture d'écran. Vérifier que `.gitignore` contient au minimum :

```gitignore
.env
.venv/
__pycache__/
*.pyc
logs/
```

Si une clé est divulguée, la régénérer dans Fabric.

## Exécution

### Vérifier la connexion Fabric

```powershell
python scripts\test_fabric.py
```

Ce test envoie un événement synthétique ; il ne représente pas une station réelle.

### Lancer la collecte périodique

```powershell
python scripts\ingestion_velotoulouse.py
```

Paramètres principaux dans le script :

| Paramètre | Valeur | Rôle |
|---|---:|---|
| `INTERVAL` | `60` | Période cible entre débuts de collecte, en secondes |
| `PAGE_SIZE` | `100` | Taille des pages API |
| `MAX_STATIONS` | `0` | `0` = toutes les stations distinctes ; valeur positive = limitation de test |

Arrêter avec **Ctrl+C**. Pour lire les derniers journaux :

```powershell
Get-Content logs\ingestion.log -Tail 20
```

Pour voir uniquement les envois réussis :

```powershell
Select-String -Path logs\ingestion.log -Pattern "SUCCES"
```

## Structure des événements envoyés

Chaque événement représente l'état d'une station au moment d'une collecte.

| Champ | Description |
|---|---|
| `source` | Valeur `VeloToulouse` |
| `station_id` | Identifiant de station (chaîne) |
| `num_bikes_available` | Vélos disponibles |
| `num_docks_available` | Places libres |
| `num_bikes_disabled` | Vélos indisponibles |
| `mechanical_bikes` | Vélos mécaniques disponibles |
| `electrical_bikes` | Vélos électriques disponibles |
| `is_renting` | Location possible (booléen ou null) |
| `is_returning` | Retour possible (booléen ou null) |
| `last_reported` | Horodatage de dernière déclaration fourni par la source (Unix) |
| `collected_at` | Horodatage UTC de la collecte par Python (ISO 8601) |

Exemple **illustratif** (non extrait d'une observation précise) :

```json
{
  "source": "VeloToulouse",
  "station_id": "13",
  "num_bikes_available": 10,
  "num_docks_available": 5,
  "num_bikes_disabled": 0,
  "mechanical_bikes": 5,
  "electrical_bikes": 5,
  "is_renting": true,
  "is_returning": true,
  "last_reported": 1793964574,
  "collected_at": "2026-11-06T10:33:37+00:00"
}
```

## Validation réalisée — 6 novembre 2026

Des tests ont confirmé la récupération, la transformation et l'envoi vers Fabric :

| Cycle observé | Enregistrements API | Stations distinctes | Envoi Fabric |
|---|---:|---:|---|
| 10:33 | 3 368 | 466 | 466 événements, succès |
| 10:34 | 3 348 | 466 | 466 événements, succès |
| 10:35 | 3 360 | 466 | 466 événements, succès |

Une nouvelle collecte était en cours à 10:36. Les captures de **Data preview** dans Eventstream ont également montré des stations réelles et leurs valeurs (`num_bikes_available`, `num_docks_available`, `mechanical_bikes`, `electrical_bikes`, etc.).

**Attention :** « événements envoyés » signifie que le client producteur a confirmé l'envoi au point d'entrée Fabric ; cela ne prouve pas encore la persistance dans une base historique.

## Vérification dans Fabric

1. Ouvrir le workspace du groupe `TP-VeloToulouse`.
2. Ouvrir l'Eventstream `ES_VeloToulouse`.
3. Sélectionner `ES_VeloToulouse-stream`.
4. Dans **Data preview**, cliquer sur **Refresh** pendant l'exécution du script.
5. Vérifier la présence d'identifiants réels de stations et des champs de disponibilité.

Des événements de test antérieurs (par exemple `TEST001` avec d'autres noms de champs) peuvent encore apparaître dans l'aperçu. Les filtrer ou les exclure lors de la mise en place du stockage et des analyses.

## Gestion des erreurs et limites

- Requêtes HTTP avec délai maximal et nouvelles tentatives pour certains codes d'erreur (`429`, `5xx`).
- Refus des réponses JSON sans liste `results`, sans `total_count` valide ou des pages anormalement vides.
- Traitement des doublons par station et conservation de l'enregistrement le plus récent selon `last_reported`.
- Envoi par lots compatibles avec la taille maximale autorisée par Event Hubs.
- Journalisation dans le terminal et `logs/ingestion.log`.
- En cas d'erreur pendant un cycle, le script la journalise puis retente au cycle suivant.

**Limites connues :**

- La pagination d'une API actualisée en continu n'offre pas nécessairement un instantané atomique ; les données peuvent évoluer entre les pages.
- Le script dépend de l'ordinateur sur lequel il s'exécute ; il n'est pas encore hébergé comme service permanent.
- L'intervalle de 60 secondes doit être adapté si les limites de l'API ou de la capacité Fabric l'exigent.
- Les événements envoyés à chaque cycle représentent des observations successives : il est normal de retrouver la même station à différents `collected_at` dans l'historique.
- Le stockage historique et la visualisation restent à mettre en place.

## Répartition du travail (3 personnes)

| Partie | Responsabilité | Livrable | État |
|---|---|---|---|
| 1 — Ingestion | API, Python, nettoyage, déduplication, Eventstream, logs | Flux alimenté automatiquement | **Validé** |
| 2 — Stockage / traitement | Destination Eventhouse, KQL Database, table historique, contrôle du schéma | Données historisées et requêtables | À réaliser |
| 3 — Visualisation | Indicateurs, graphiques, dashboard Power BI | Tableau de bord de suivi VéloToulouse | À réaliser |

### Passage de relais à la partie 2

- Utiliser le flux Fabric **`ES_VeloToulouse-stream`** comme source.
- Ajouter une destination **Eventhouse / KQL Database** et créer une table adaptée aux champs ci-dessus.
- Préserver `station_id` **et** `collected_at` pour analyser l'évolution dans le temps ; ne pas écraser toutes les observations d'une station.
- Exclure les événements de test (`TEST001`) ou les traiter séparément.
- Vérifier les types des colonnes, notamment `station_id`, booléens, entiers et dates.
- Vérifier le nombre de lignes ingérées et la latence entre `collected_at` et l'arrivée en base.

### Passage de relais à la partie 3

Idées de métriques Power BI : vélos disponibles par station, répartition mécaniques/électriques, stations vides ou saturées, évolution des disponibilités par heure et taux d'occupation approximatif. Les séries historiques nécessitent d'abord la mise en place de la partie 2.

## Référence du TP

[SUPINFO — 5CLOU Exam : 01-my-data-platform](https://github.com/SUPINFO-Organization/5CLOU-Exam/tree/main/01-my-data-platform)

Le sujet demande une plateforme d'ingestion, de traitement et de visualisation de données mises à jour fréquemment. Le groupe a choisi **Microsoft Fabric Eventstream + Python** pour l'ingestion, puis **Eventhouse** et **Power BI** pour les étapes suivantes. Cette architecture est l'implémentation choisie par le groupe ; elle n'est pas identique au parcours Azure Data Factory / Synapse présenté à titre d'exemple dans le sujet.
