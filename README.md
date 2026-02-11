# 🎯 Scoring de Potentiel BTP : Segmentation Intelligente de 1M d'Établissements

> **Pipeline d'enrichissement multi-sources transformant 1,038M établissements BTP en 41K cibles qualifiées via scoring 130 points**

[![Python](https://img.shields.io/badge/Python-3.11-blue.svg)](https://www.python.org/)
[![BigQuery](https://img.shields.io/badge/BigQuery-Production-orange.svg)](https://cloud.google.com/bigquery)
[![Power BI](https://img.shields.io/badge/PowerBI-Dashboard-yellow.svg)](https://powerbi.microsoft.com/)
[![Coverage](https://img.shields.io/badge/Data_Coverage-99.6%25-brightgreen.svg)]()

---

## 🔍 Le Problème Business

**Contexte** : Le secteur BTP français compte plus d'1 million d'établissements actifs, avec une hétérogénéité extrême allant du micro-entrepreneur à l'ETI nationale. Cette fragmentation rend toute stratégie commerciale B2B (éditeurs logiciels, fournisseurs, services) inefficace sans segmentation préalable.

**Challenges identifiés** :

❌ **Aiguille dans la botte de foin** : Comment identifier 40K cibles pertinentes parmi 1M établissements ?  
❌ **Données fragmentées** : API SIRENE exhaustive mais non-exploitable brute (pas de scoring, pas de segmentation)  
❌ **Segmentation binaire inadaptée** : Logiques "TPE / PME / GE" trop grossières pour le BTP (besoin granularité)  
❌ **Gaspillage ressources** : Commerciaux perdant 70% du temps sur prospection non-qualifiée  
❌ **Absence de priorisation** : Impossible de distinguer un artisan isolé d'une PME structurée à 20 salariés

**Impacts métiers typiques** :

- Taux de conversion <2% sur campagnes marketing "spray & pray"
- Cycles de vente rallongés (6-12 mois) par ciblage imprécis
- Coût d'acquisition client (CAC) 3x supérieur à la norme B2B
- Expansion territoriale basée sur intuition plutôt que data

---

## ✅ La Solution Data

### Vue d'Ensemble du Pipeline

**Objectif** : Créer une vue exploitable `v_etablissements_btp_global` dans BigQuery permettant segmentation opérationnelle des 1,038M établissements BTP français en 41K cibles actionnables (4% de la base).

#### 📥 Collecte Multi-Sources

**Source principale : API SIRENE (INSEE)**

- **23 codes NAF** secteur construction (F41.x, F42.x, F43.x) sélectionnés stratégiquement
  - 3 codes **Haute valeur** (ex: Promotion immobilière, Gros œuvre)
  - 4 codes **Moyenne valeur** (ex: Installation électrique, Plomberie)
  - 16 codes **Basse valeur** (ex: Peinture, Petits travaux)
- **96 départements** métropole + DROM couverts
- **Volume final** : 1,038,410 établissements actifs après nettoyage
- **Champs exploités** : Effectifs, forme juridique, géolocalisation (commune), état administratif, nombre d'établissements par SIREN

**Source complémentaire : Données Géographiques (INSEE)**

- Référentiel communes (zonage urbain/rural, densité population)
- Agrégations départementales pour insights territoriaux

#### 🔄 Architecture du Pipeline

```bash
┌──────────────────────────────────────────────────────────────────┐
│ EXTRACTION (Collection) │
├──────────────────────────────────────────────────────────────────┤
│ API SIRENE (39 codes NAF × 101 départements) │
│ └─ api_sirene.py : Batch pagination + rate-limiting │
│ ↓ │
│ JSON bruts → data/raw/ (versionnés par date) │
│ ↓ │
├──────────────────────────────────────────────────────────────────┤
│ TRANSFORMATION (Nettoyage + Enrichissement) │
├──────────────────────────────────────────────────────────────────┤
│ ① data_cleaning.py │
│ - Dédoublonnage SIRET │
│ - Filtrage établissements inactifs/fermés │
│ - Normalisation typage (effectifs, codes) │
│ ↓ │
│ ② geo_transform.py │
│ - Jointure référentiel communes (zonage urbain) │
│ - Calcul densité BTP départementale │
│ ↓ │
│ ③ data_enrichment.py │
│ - Calcul scoring 5 dimensions (130 points max) │
│ - Attribution segments (Premium/Prioritaire/Secondaire/GC) │
│ - Agrégation multi-agences (comptage par SIREN) │
│ ↓ │
│ Parquet enrichis → data/processed/ │
│ ↓ │
├──────────────────────────────────────────────────────────────────┤
│ CHARGEMENT (Load) │
├──────────────────────────────────────────────────────────────────┤
│ ① upload_bigquery.py │
│ - Table faits : etablissements_btp_enrichis │
│ - Batch 100K lignes (optimisation quota BigQuery) │
│ ↓ │
│ ② upload_dimensions_bigquery.py │
│ - Tables dimensions (NAF, formes juridiques, départements) │
│ ↓ │
│ ③ sql/02_vues/ │
│ - Vue finale : v_etablissements_btp_global │
│ - Modèle étoile optimisé pour Power BI │
│ ↓ │
│ BIGQUERY DATA WAREHOUSE │
│ (1,038M lignes indexées, partitionnées) │
│ ↓ │
│ VISUALISATION (Power BI / SQL Analytics) │
│ Dashboard segmentation + Analyses territoriales │
└──────────────────────────────────────────────────────────────────┘
```

Orchestration : pipeline_full.py (exécution complète) + logs structurés
Validation : test_pipeline_quick.py (1 département test)

---

## 📊 Méthodologie de Scoring (v1.3)

### Scoring Multi-Dimensionnel : 130 Points Maximum

Le scoring repose sur **5 dimensions pondérées** analysant la "maturité commerciale" d'un établissement :

| 🎯 Dimension              | Points Max | Poids | Description                                   | Critère Optimal                        |
| ------------------------- | ---------- | ----- | --------------------------------------------- | -------------------------------------- |
| **1. Taille Entreprise**  | 40         | 31%   | Effectifs salariés (proxy budget/solvabilité) | 10-19 salariés = **35 points** ⭐      |
| **2. Profil Territorial** | 25         | 19%   | Zonage urbain/rural + densité locale          | Zone très urbaine = **25 points**      |
| **3. Potentiel APE**      | 25         | 19%   | Valeur intrinsèque du secteur d'activité      | Haute (Gros œuvre, Promo) = **25 pts** |
| **4. Multi-Agences**      | 20         | 15%   | Nombre d'établissements du même SIREN         | 5+ agences = **20 points**             |
| **5. Forme Juridique**    | 20         | 15%   | Statut juridique (capacité investissement)    | Société commerciale = **20 pts**       |

**Calcul** : `Score Total = Σ (Points Dimension) → Échelle 0-130`

**Score moyen observé** : **50,5 / 130** (médiane à 48)

#### 📐 Détails par Dimension

**1️⃣ Taille Entreprise (0-40 pts)**

```text
0 salarié (EI/Micro)         →  5 pts
1-2 salariés                 → 15 pts
3-5 salariés                 → 25 pts
6-9 salariés                 → 30 pts
10-19 salariés ⭐            → 35 pts (sweet spot PME)
20-49 salariés               → 40 pts
50+ salariés                 → 40 pts (plafonné)
```

**Rationale** : 10-19 salariés = PME structurée capable d'investir sans lourdeur décisionnelle grands groupes

**2️⃣ Profil Territorial (0-25 pts)**

```text
Rural isolé                  →  5 pts
Urbain dense                 → 20 pts
Très urbain (métropoles) ⭐  → 25 pts
```

**Rationale** : Zones urbaines = + de chantiers, + de complexité coordination, + besoin digitalisation

**3️⃣ Potentiel APE (0-25 pts)**

Le scoring par code APE reflète le **potentiel commercial** de chaque secteur d'activité (CA moyen, besoins digitaux, récurrence projets).

| Score     | Nombre de codes | Codes APE (exemples)                                                                           | Rationale Business                                                      |
| --------- | --------------- | ---------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------- |
| **25** ⭐ | 3 codes         | 43.22B (Chauffage/Clim), 43.29A (Isolation), 43.32A (Menuiserie)                               | **Top priorité** : Besoins récurrents, CA élevé, digitalisation forte   |
| **20**    | 4 codes         | 41.20A (Construction maisons), 43.29B (Installations), 43.31Z (Plâtrerie), 43.32B (Serrurerie) | **Priorité haute** : PME structurées, projets moyens/longs termes       |
| **10**    | 20 codes        | 43.21A (Électricité), 43.22A (Plomberie), 43.99C (Maçonnerie)                                  | **Priorité standard** : Artisans/TPE, projets courts                    |
| **0**     | 14 codes        | 41.10x (Promotion immo), 42.xx (Génie civil), 43.99B (Structures métalliques)                  | **Hors-cible** : Très grands projets, cycles longs, besoins spécifiques |

**Distribution complète des 41 codes APE analysés** :

<details>
<summary><b>📋 Voir la liste exhaustive des codes par score</b> (cliquer pour dérouler)</summary>

### Score 25 points (3 codes) ⭐

- **43.22B** : Travaux d'installation d'équipements thermiques et de climatisation
- **43.29A** : Travaux d'isolation
- **43.32A** : Travaux de menuiserie bois et PVC

### Score 20 points (4 codes)

- **41.20A** : Construction de maisons individuelles
- **43.29B** : Autres travaux d'installation n.c.a.
- **43.31Z** : Travaux de plâtrerie
- **43.32B** : Travaux de menuiserie métallique et serrurerie

### Score 10 points (20 codes)

- **41.20B** : Construction d'autres bâtiments
- **43.11Z** : Travaux de démolition
- **43.12A** : Travaux de terrassement courants et travaux préparatoires
- **43.12B** : Travaux de terrassement spécialisés ou de grande masse
- **43.21A** : Travaux d'installation électrique dans tous locaux
- **43.22A** : Travaux d'installation d'eau et de gaz en tous locaux
- **43.32C** : Agencement de lieux de vente
- **43.33Z** : Travaux de revêtement des sols et des murs
- **43.34Z** : Travaux de peinture et vitrerie
- **43.39Z** : Autres travaux de finition
- **43.91A** : Travaux de charpente
- **43.91B** : Travaux de couverture par éléments
- **43.99A** : Travaux d'étanchéification
- **43.99C** : Travaux de maçonnerie générale et gros œuvre de bâtiment
- **43.99D** : Autres travaux spécialisés de construction
- **81.30Z** : Services d'aménagement paysager

### Score 0 points (14 codes) - Hors-cible

- **41.10A** : Promotion immobilière de logements
- **41.10B** : Promotion immobilière de bureaux
- **41.10C** : Promotion immobilière d'autres bâtiments
- **41.10D** : Supports juridiques de programmes
- **42.11Z** : Construction de routes et autoroutes
- **42.12Z** : Construction de voies ferrées
- **42.13A** : Construction d'ouvrages d'art
- **42.13B** : Construction et entretien de tunnels
- **42.21Z** : Construction de réseaux pour fluides
- **42.22Z** : Construction de réseaux électriques et télécommunications
- **42.91Z** : Construction d'ouvrages maritimes et fluviaux
- **42.99Z** : Construction d'autres ouvrages de génie civil
- **43.13Z** : Forages et sondages
- **43.21B** : Travaux d'installation électrique sur la voie publique
- **43.99B** : Travaux de montage de structures métalliques
- **74.90A** : Activité des économistes de la construction

</details>

**Rationale métier** :

- **Score 25** : Métiers à forte valeur ajoutée avec besoins digitaux récurrents (devis, suivi chantiers, gestion sous-traitance)
- **Score 20** : PME du bâtiment avec projets structurants nécessitant coordination
- **Score 10** : Artisans/TPE avec besoins basiques (facturation, planning)
- **Score 0** : Secteurs hors-périmètre (promotion immobilière = clients finals, génie civil = grands groupes)

**4️⃣ Multi-Agences (0-20 pts)**

```text
1 établissement              →  0 pts
2 établissements             →  5 pts
3-4 établissements           → 10 pts
5+ établissements ⭐         → 20 pts
```

**Rationale** : Multi-sites = expansion réussie, gestion multi-chantiers, besoins coordination

**5️⃣ Forme Juridique (0-20 pts)**

```text
Entrepreneur Individuel      →  5 pts
SARL/SAS/SA ⭐               → 20 pts
Autres (SNC, SCI)            → 10 pts
```

**Rationale** : Sociétés commerciales = capitaux, capacité investissement, pérennité

---

## 🎯 Segmentation Opérationnelle (4 Niveaux)

### Critères de Segmentation v1.3

Le scoring seul ne suffit pas : la segmentation croise **score + critères métiers** pour isoler 4 segments actionnables :

| 🏆 Segment            | Volume                                       | % Base | Critères d'Entrée                            | Usage Business                                  |
| --------------------- | -------------------------------------------- | ------ | -------------------------------------------- | ----------------------------------------------- |
| 🥇 **Premium PME**    | **438**                                      | 0,04%  | Score ≥78 + 6-49 sal + APE ≥20 + ≤20 agences | Prospection Sales directe (outbound, démos 1:1) |
| ⭐ **Prioritaire**    | **8 105**                                    | 0,78%  | Score ≥70 + 1-49 sal + APE ≥20 + ≤50 agences | Marketing automation, webinaires, nurturing     |
| ✓ **Secondaire**      | **29 426**                                   | 2,83%  | Score ≥52 + 1-49 sal + ≤50 agences           | Inbound, SEO, contenus pédagogiques             |
| 🏢 **Grands Comptes** | **87 entreprises**<br>(3 828 établissements) | 0,37%  | Score ≥78 + >20 agences                      | RFP, POC, CSM dédié, Account-Based Marketing    |
| ⚪ **Hors-cible**     | ~997 000                                     | ~96%   | Tous les autres                              | Non exploitable (micro, inactifs, APE basse)    |

**Total cibles exploitables** : **41 797** (4,02% de la base totale)

#### 🎯 Insights par Segment

**🥇 Premium PME (438 établissements)**

- **Profil-type** : PME régionale structurée, 10-20 salariés, chauffagiste/isolation/construction
- **Exemples secteurs** : Installation thermique, Menuiserie spécialisée, Maçonnerie générale
- **Répartition géographique** :
  - **Top 4 départements** : 59-Nord (Lille), 92-Hauts-de-Seine (IDF), 69-Rhône (Lyon), 44-Loire-Atlantique (Nantes)
  - Corrélation forte avec métropoles régionales (70% dans top 15 départements)
- **CAC estimé** : 3x inférieur aux autres segments (taux conversion ~15%)
- **Cycle de vente** : 3-4 mois (décisionnaire unique accessible)

**⭐ Prioritaire (8 105 établissements)**

- **Profil-type** : PME/TPE en croissance, 3-15 salariés, besoin montée en gamme outils
- **Opportunité** : Nurturing long terme (6-12 mois) via contenus éducatifs
- **Conversion** : 5-8% après 3 points de contact qualifiés

**✓ Secondaire (29 426 établissements)**

- **Profil-type** : TPE stables, 1-5 salariés, sensibilité prix élevée
- **Stratégie** : Freemium, essais gratuits, self-service

**🏢 Grands Comptes (87 entreprises)**

- **Volume** : 87 entreprises mères contrôlant 3 828 établissements (moyenne 44 établissements/entreprise)
- **Exemples identifiés** :
  - ENGIE (212 établissements BTP)
  - Proxiserve (104 établissements)
  - Axima Groupe (79 établissements)
- **Particularité** : Séparation nette via critère anti-grands-groupes (>20 agences) pour éviter pollution segment Premium PME
- **Approche** : Account-Based Marketing, POC pilotes, contractualisations nationales

---

## 📈 Résultats & Insights Analytiques

### KPIs du Pipeline

✅ **1 038 410 établissements BTP** traités (exhaustivité INSEE)  
✅ **41 797 cibles exploitables** identifiées (4,02% de la base)  
✅ **99,6% de taux de couverture** après validation (4 167 établissements exclus pour données incohérentes)  
✅ **23 codes APE BTP** analysés (3 haute + 4 moyenne + 16 basse valeur)  
✅ **Score moyen global** : 50,5 / 130 (distribution gaussienne)

### 🗺️ Insights Territoriaux

**Top 5 Départements - Premium PME (438 cibles)**

| Rang | Dép. | Nom                          | Premium PME | % du segment |
| ---- | ---- | ---------------------------- | ----------- | ------------ |
| 1    | 59   | Nord (Lille)                 | 38          | 8,7%         |
| 2    | 92   | Hauts-de-Seine (Paris)       | 35          | 8,0%         |
| 3    | 69   | Rhône (Lyon)                 | 32          | 7,3%         |
| 4    | 44   | Loire-Atlantique (Nantes)    | 28          | 6,4%         |
| 5    | 13   | Bouches-du-Rhône (Marseille) | 26          | 5,9%         |

**Concentration** : 70% des Premium PME situées dans 15 départements (métropoles régionales)

**Déserts BTP Premium** : Départements <5 cibles Premium → Opportunité expansion géographique pour acteurs nationaux

**Corrélation Score × Urbanisation** : 0,58 (modérée-forte)  
→ Zones très urbaines sur-représentées dans segments Premium/Prioritaire

### 🏗️ Insights Sectoriels (Codes APE)

**Distribution des 41 797 Cibles par Score APE**

| Score APE | Codes APE | % des codes | Volume Cibles Estimé\* | Potentiel Commercial                |
| --------- | --------- | ----------- | ---------------------- | ----------------------------------- |
| **25** ⭐ | 3 codes   | 7,3%        | ~12 000                | Très élevé (récurrence)             |
| **20**    | 4 codes   | 9,8%        | ~10 500                | Élevé (projets moyens)              |
| **10**    | 20 codes  | 48,8%       | ~19 000                | Standard (TPE/artisans)             |
| **0**     | 14 codes  | 34,1%       | ~300                   | Hors-cible (exclus scoring Premium) |

\*Volume estimé sur base 41 797 cibles exploitables

**Top 7 APE Premium PME (438 établissements)** :

| Rang | Code APE | Métier                                  | Premium PME | % du segment | Score APE |
| ---- | -------- | --------------------------------------- | ----------- | ------------ | --------- |
| 🥇 1 | 43.22B   | Installation thermique et climatisation | 197         | 45,0%        | 25 ⭐     |
| 🥈 2 | 43.32A   | Menuiserie bois et PVC                  | 62          | 14,2%        | 25 ⭐     |
| 🥉 3 | 43.32B   | Menuiserie métallique et serrurerie     | 60          | 13,7%        | 20        |
| 4    | 43.29B   | Autres travaux d'installation           | 37          | 8,4%         | 20        |
| 5    | 43.29A   | Travaux d'isolation                     | 29          | 6,6%         | 25 ⭐     |
| 6    | 41.20A   | Construction de maisons individuelles   | 28          | 6,4%         | 20        |
| 7    | 43.31Z   | Travaux de plâtrerie                    | 25          | 5,7%         | 20        |

**Insights clés** :

✅ **45% des Premium PME concentrés sur un seul code APE** (43.22B - Chauffage/Climatisation)  
→ Opportunité majeure de **spécialisation verticale** sur ce secteur

✅ **Les 3 codes APE à score maximum (25 pts) représentent 65,8% du segment Premium**  
→ La pondération APE à 25 points fonctionne efficacement pour cibler les PME à fort potentiel

✅ **Top 7 codes = 100% du segment Premium PME**  
→ Segmentation très concentrée, possibilité de créer des **offres sectorielles dédiées**

✅ **Domination des métiers "techniques installateurs"** (chauffage, menuiserie, isolation)  
→ PME nécessitant coordination multi-chantiers, gestion sous-traitance, suivi interventions

### 💡 Patterns Identifiés

**🎯 1. Hyper-concentration sectorielle** : 45% des Premium PME sur un seul code APE (43.22B Chauffage/Climatisation)  
→ Opportunité de **spécialisation verticale** : offre métier dédiée aux installateurs thermiques

**📏 2. Sweet Spot PME : 50-99 salariés** : 78,5% (344/438) du segment Premium  
→ **Révision majeure** : Le sweet spot n'est pas 10-19 salariés mais **50-99 salariés** (PME structurées avec capacité d'investissement)  
→ 17% supplémentaires (75) entre 100-199 salariés = **95,7% du segment entre 50-199 salariés**

**⚖️ 3. Maturité juridique quasi-absolue** : 99,3% (435/438) = sociétés commerciales (SARL/SAS/SA)  
→ Entrepreneur Individuel totalement absent du segment Premium PME  
→ Forme juridique = **critère discriminant majeur** de maturité

**🏢 4. Profil multi-sites modéré** : Moyenne de **3,1 établissements** par entreprise Premium  
→ Distribution : 42% mono-site, 22% bi-sites, 36% multi-sites (3+)  
→ Multi-sites n'est **pas un critère absolu** : 184 Premium (42%) n'ont qu'un seul établissement

**🗺️ 5. Surprise géographique : Domination rurale** : 37,7% (165/438) des Premium PME en zone rurale  
→ **Contre-intuitif** : Les zones rurales/péri-urbaines représentent **66%** du segment (289/438)  
→ Zones très urbaines = seulement 12,1% (53) du segment  
→ **Insight clé** : PME BTP structurées prospèrent en zones rurales (moins de concurrence, bassins d'emploi stables, chantiers publics locaux)

---

## 🗂️ Architecture du Repository

```bash
btp-analysis/
│
├── .env.example              # Template configuration (API SIRENE, GCP credentials)
├── .gitignore                # Exclusions Git (data/, logs/, .env)
├── README.md                 # 📖 Documentation principale (ce fichier)
├── requirements.txt          # Dépendances Python (pandas, google-cloud-bigquery, etc.)
│
├── data/                     # 🔒 EXCLU GIT - Données locales
│   ├── raw/                  # JSON bruts API SIRENE (versionnés par date)
│   ├── processed/            # Parquet nettoyés + enrichis (scoring, segments)
│   └── reference/            # Référentiels INSEE (communes, départements)
│
├── logs/                     # 🔒 Logs d'exécution pipeline
│   └── pipeline_YYYYMMDD_HHMMSS.log
│
├── scripts/                  # 📜 Scripts Python production
│   ├── __init__.py
│   │
│   ├── api_sirene.py                        # Collecte API SIRENE (batch 23 codes APE × 96 dépt)
│   ├── data_io.py                           # Lecture/Écriture Parquet + gestion formats
│   ├── data_cleaning.py                     # Nettoyage (doublons SIRET, inactifs, typage)
│   ├── data_enrichment.py                   # 🎯 Calcul scoring 5D + segmentation 4 niveaux
│   ├── geo_transform.py                     # Normalisation géo + zonage urbain
│   │
│   ├── upload_bigquery.py                   # Upload table faits (etablissements_btp_enrichis)
│   ├── upload_dimensions_bigquery.py        # Upload tables dimensions (NAF, juridique, géo)
│   ├── upload_dim_categories_juridiques_fixed.py  # Fix post-upload typage juridique
│   │
│   ├── pipeline_full.py                     # 🚀 Orchestrateur pipeline complet (toutes étapes)
│   ├── run_full_pipeline.py                 # Point d'entrée CLI (avec gestion erreurs)
│   ├── test_pipeline_quick.py               # Tests rapides (1 département, validation)
│   └── fix_categorie_juridique_type.py      # Correction typage BigQuery post-load
│
├── notebooks/                # 📓 Notebooks exploratoires Jupyter
│   └── 01_exploration_sirene.ipynb          # EDA initial données SIRENE + validation scoring
│
├── sql/                      # 🗄️ Requêtes BigQuery
│   ├── 01_verifications/     # Requêtes QA (comptages, cohérence, doublons)
│   │   ├── check_coverage.sql               # Vérification 99.6% couverture
│   │   ├── check_duplicates.sql             # Détection doublons SIRET
│   │   └── score_distribution.sql           # Analyse distribution scoring
│   │
│   └── 02_vues/              # Vues métier (segmentation, agrégations)
│       ├── v_etablissements_btp_global.sql  # 🎯 Vue finale exploitable (1.038M lignes)
│       ├── v_premium_pme.sql                # Vue filtrée 438 Premium
│       ├── v_grands_comptes.sql             # Vue 87 entreprises GC + agrégation établissements
│       └── v_stats_territoires.sql          # Agrégations départementales
│
└── (dashboards/)             # 📊 PRÉVU : Exports Power BI (non versionné)
    └── scoring_btp_v1.pbix   # Dashboard segmentation finale + analyses territoriales
```

## 🚀 Reproductibilité du Projet

### Prérequis

#### Environnement

- Python 3.11+
- Compte Google Cloud Platform (projet actif + BigQuery API activée)
- Clé API SIRENE (gratuite : [api.insee.fr](https://api.insee.fr))
- Power BI Desktop (pour visualisation finale)

#### Ressources Système

- RAM : 16 Go recommandé (traitement 1M lignes en mémoire)
- Stockage : 10 Go libres (Parquet intermédiaires ~3 Go)
- Connexion internet stable (API SIRENE ~50K requêtes totales)

### Installation

```bash
# 1. Cloner le repository
git clone https://github.com/AntoinePro74/btp-analysis.git
cd btp-analysis

# 2. Créer environnement virtuel Python
python -m venv venv
source venv/bin/activate          # Linux/Mac
# venv\Scripts\activate           # Windows

# 3. Installer dépendances
pip install -r requirements.txt

# 4. Configurer credentials
cp .env.example .env

# 5. Éditer .env avec vos credentials :
# SIRENE_API_KEY=votre_cle_api_sirene
# GOOGLE_APPLICATION_CREDENTIALS=/chemin/vers/gcp-service-account.json
# GCP_PROJECT_ID=votre-projet-gcp
# BQ_DATASET=btp_analysis
```

## 📈 Évolutions Futures (Roadmap)

### Phase 2 - Enrichissement Financier

- [ ] **Intégration API Pappers** : Bilans comptables (CA réel, résultat net, fonds propres)
- [ ] **Scoring financier** : Nouvelle dimension "Santé financière" (0-30 pts) → Scoring 160 pts total
- [ ] **Détection signaux faibles** : Redressements/liquidations judiciaires → Flag risque

### Phase 3 - Prédictif & Automatisation

- [ ] **Machine Learning** : Modèle prédictif probabilité conversion (XGBoost sur historique)
- [ ] **Refresh automatisé** : Orchestration Airflow (mise à jour mensuelle SIRENE)
- [ ] **API REST** : Endpoint scoring temps réel (`GET /api/v1/score/{siret}`)
- [ ] **Webhooks** : Alertes nouveaux établissements Premium PME (email/Slack)

### Phase 4 - Extension & Benchmark (2027)

- [ ] **Multi-secteurs** : Adaptation méthodologie (Retail, Services, Industrie)
- [ ] **Time-series** : Suivi évolution scoring mensuel (détection tendances)
- [ ] **Benchmark concurrentiel** : Scoring relatif par rapport concurrents identifiés
- [ ] **Scoring individuel** : Extension dirigeants (API Pappers RNCS)

---

## 📚 Compétences Techniques Illustrées

### 🔧 Data Engineering

- **ETL Production** : Pipeline 5 étapes (Extract → Transform → Load → Validate → Publish)
- **API Management** : Gestion rate-limiting, pagination, retry logic
- **Batch Processing** : Traitement 1M lignes avec chunking Pandas (optimisation mémoire)
- **Data Quality** : Framework validation (99,6% coverage, détection anomalies)

### 🗄️ Data Warehousing

- **Modélisation dimensionnelle** : Schéma en étoile (1 fait + 5 dimensions)
- **BigQuery** : Tables partitionnées, vues matérialisées, optimisation requêtes
- **Typage strict** : Schémas explicites (INT64, STRING, DATE, FLOAT64)
- **Dénormalisation stratégique** : Calcul multi-agences pré-agrégé (performance)

### 📊 Analytics & Scoring

- **Scoring multi-critères** : Pondération métier validée terrain (5 dimensions, 130 points)
- **Segmentation** : Logique hybride (score + critères métiers) pour segments actionnables
- **Analyses territoriales** : Corrélations géographiques, détection zones sous-exploitées
- **Distribution analysis** : Étude percentiles, outliers, gaussianité

### ☁️ Cloud & DevOps

- **Google Cloud Platform** : BigQuery, IAM, Service Accounts
- **Format Parquet** : Stockage intermédiaire optimisé (compression + typage)
- **Logging structuré** : Traçabilité complète (timestamps, compteurs, erreurs)
- **Git workflow** : .gitignore (données sensibles exclues), commits atomiques

---

## 🛠️ Défis Techniques Rencontrés & Solutions

| 🚧 Défi                                    | ✅ Solution Implémentée                                       | 💡 Apprentissage                      |
| ------------------------------------------ | ------------------------------------------------------------- | ------------------------------------- |
| API SIRENE rate-limit (1000 req/jour)      | Batch nocturnes + cache Parquet local + retry exponential     | Gestion contraintes externes API      |
| Volume 1M lignes (RAM limitée 16 Go)       | Chunking Pandas (100K lignes/batch) + garbage collection      | Optimisation mémoire Python           |
| Scoring subjectif (pondération arbitraire) | Validation terrain avec commerciaux (3 itérations) → v1.3     | Méthodologie itérative métier         |
| Types BigQuery incohérents post-upload     | Script `fix_categorie_juridique_type.py` + schémas explicites | Typage strict obligatoire             |
| Doublons SIRET (multi-établissements)      | Dédoublonnage par SIRET + agrégation SIREN (comptage agences) | Modèle INSEE (SIREN/SIRET)            |
| Segmentation Grands Comptes pollue Premium | Critère anti-grands-groupes (>20 agences) → segment dédié     | Segmentation hybride (score + règles) |
| Performance BigQuery (vues lentes)         | Index sur `siret` + partitionnement par `departement`         | Optimisation requêtes DWH             |

---

## 👤 Auteur

**Antoine Bineau**  
Key Account Manager | Data Analyst & Business Intelligence

🔗 [LinkedIn](https://www.linkedin.com/in/antoine-bineau/)

**Projet personnel réalisé dans le cadre de ma montée en compétences Data Analysis / Analytics Engineering**

📅 **Période** : Novembre 2025  
⏱️ **Durée** : ~80 heures (réparties sur 3 semaines)  
🎯 **Objectif** : Démontrer capacités ETL, scoring métier, modélisation DWH sur données réelles volumineuses (1M+ lignes)

---

## 📝 Licence & Mentions Légales

Code source : MIT License (utilisation libre avec attribution)

Données :

SIRENE : Licence Ouverte Etalab ([lien](https://www.etalab.gouv.fr/licence-ouverte-open-licence))

Référentiels INSEE : Open Data ([lien](https://www.insee.fr/fr/information/2008354))

**Avertissement** : Ce projet est une démonstration de compétences techniques. Les résultats de scoring et segmentation sont des projections analytiques à titre illustratif, non des recommandations commerciales. Aucune donnée personnelle n'est collectée ou traitée.

## 🤝 Contributions & Feedback

Vos retours, suggestions d'amélioration ou questions techniques sont les bienvenus !

🐛 Bug détecté → Ouvrir une Issue

💡 Idée de feature → Discussion GitHub

🔀 Pull Request → Toute contribution documentée sera reviewée avec plaisir

⭐ Si ce projet vous inspire ou vous aide dans votre apprentissage, n'hésitez pas à le star sur GitHub !
