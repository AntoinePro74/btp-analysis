# 🎯 Scoring de Potentiel BTP : Segmentation Intelligente de 1M d'Établissements

> **Pipeline d'enrichissement multi-sources transformant 984K entreprises BTP en 9 709 cibles ultra-qualifiées (0.99%) via segmentation entreprise 7 niveaux basée sur scoring hybride 130 points (score moyen SIREN)**

[![Python](https://img.shields.io/badge/Python-3.11-blue.svg)](https://www.python.org/)
[![BigQuery](https://img.shields.io/badge/BigQuery-Production-orange.svg)](https://cloud.google.com/bigquery)
[![Power BI](https://img.shields.io/badge/PowerBI-Dashboard-yellow.svg)](https://powerbi.microsoft.com/)
[![Coverage](https://img.shields.io/badge/Data_Coverage-99.6%25-brightgreen.svg)]()

---

## 🔍 Le Problème Business

**Contexte** : Le secteur BTP français compte plus d'1 million d'établissements actifs, avec une hétérogénéité extrême allant du micro-entrepreneur à l'ETI nationale. Cette fragmentation rend toute stratégie commerciale B2B (éditeurs logiciels, fournisseurs, services) inefficace sans segmentation préalable.

**Challenges identifiés** :

❌ **Aiguille dans la botte de foin** : Comment identifier 10K cibles pertinentes parmi 984K entreprises ?  
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

**Objectif** : Créer une vue exploitable `v_etablissements_btp_global` dans BigQuery permettant segmentation opérationnelle des 984K entreprises BTP françaises (1,038M établissements) en 9 709 cibles actives ultra-qualifiées (0.99% des entreprises) via 7 segments au niveau entreprise (SIREN).

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

## 📊 Méthodologie de Scoring (v1.6)

### Scoring Multi-Dimensionnel : 130 Points Maximum

Le scoring repose sur **5 dimensions pondérées** analysant la "maturité commerciale" d'un établissement :

| 🎯 Dimension              | Points Max | Poids | Description                                   | Critère Optimal                                            |
| ------------------------- | ---------- | ----- | --------------------------------------------- | ---------------------------------------------------------- |
| **1. Taille Entreprise**  | 40         | 31%   | Effectifs salariés (proxy budget/solvabilité) | 20-49 salariés = **40 points** ⭐                          |
| **2. Profil Territorial** | 25         | 19%   | Zonage urbain/rural + densité locale          | Zone très urbaine = **25 points**                          |
| **3. Potentiel APE**      | 25         | 19%   | Valeur intrinsèque du secteur d'activité      | Haute (Chauffage/Clim, Isolation, Menuiserie) = **25 pts** |
| **4. Multi-Agences**      | 20         | 15%   | Nombre d'établissements du même SIREN         | 5+ agences = **20 points**                                 |
| **5. Forme Juridique**    | 20         | 15%   | Statut juridique (capacité investissement)    | Société commerciale = **20 pts**                           |

**Calcul** : `Score Total = Σ (Points Dimension) → Échelle 0-130`

**Score moyen observé** : **53,4 / 130** (médiane à 50)

#### 📐 Détails par Dimension

**1️⃣ Taille Entreprise (0-40 pts)**

```text
0 salarié (EI/Micro)         →  5 pts
1-2 salariés                 → 15 pts
3-5 salariés                 → 20 pts
6-9 salariés                 → 30 pts
10-19 salariés               → 35 pts
20-49 salariés ⭐            → 40 pts (sweet spot Premium PME)
50+ salariés                 → 40 pts (plafonné)
```

**Rationale** : 20+ salariés = PME structurée avec capacité d'investissement significative, équipes spécialisées (commercial, technique, admin). Plateau à 40 pts dès 20 salariés pour éviter surpondération des grandes structures et favoriser les PME régionales.

**2️⃣ Profil Territorial (0-25 pts)**

```text
Rural                        →  5 pts
Péri-urbain                  → 15 pts
Urbain                       → 20 pts
Très urbain ⭐               → 25 pts
```

**Rationale** : Zones urbaines = + de chantiers, + de complexité coordination, + de coordination, + besoin digitalisation

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

## 🎯 Segmentation Entreprise (7 Segments)

### Architecture v1.6 : Segmentation au Niveau SIREN

**Révolution v1.6** : Tous les établissements d'une même entreprise (SIREN) appartiennent au **même segment**. La segmentation repose sur :
- **Score moyen SIREN** : Moyenne des scores de tous les établissements de l'entreprise
- **Nombre d'établissements** : Potentiel de déploiement multi-sites
- **Effectifs Unité Légale** : Proxy décision d'achat (DirCo/DirMarketing/Gérant)
- **Code APE** : Secteur d'activité prioritaire

**Cohérence garantie** : Un établissement = toujours le même segment que ses frères (même SIREN).

### Pyramide des 7 Segments (Volumes Réels Validés)

| 🏆 Segment                     | Entreprises | Établissements | Ratio | % Base | Score Moyen | Usage Business                                         |
| ------------------------------ | ----------- | -------------- | ----- | ------ | ----------- | ------------------------------------------------------ |
| 🏢 **1. Grands Comptes**       | **92**      | **5 170**      | 56.2  | 0.009% | ~94         | ABM, C-level, cycle 12-24 mois, contract value ×50+    |
| 🏗️ **2. Groupes Structurés**   | **358**     | **3 351**      | 9.4   | 0.036% | ~85         | Contact siège, POC groupe, cycle 6-12 mois, CV ×9      |
| 🚀 **3. Multi-Sites Qualifiés** | **601**     | **1 586**      | 2.6   | 0.061% | ~95         | POC 1 site → déploiement, cycle 3-6 mois, CV ×2-5      |
| 🥇 **4. Premium Mono-Site**    | **974**     | **974**        | 1.0   | 0.099% | ~105        | Vente directe ciblée, closing rapide, cycle 2-4 mois   |
| ⭐ **5. Prioritaire**          | **7 684**   | **10 587**     | 1.4   | 0.78%  | ~85         | Marketing automation, webinaires, nurturing 6-12 mois  |
| ✓ **6. Secondaire**            | **308 563** | **335 792**    | 1.09  | 31.4%  | ~68         | Nurturing passif, inbound, SEO, self-service           |
| ⚪ **7. Hors-Cible**           | **665 810** | **680 950**    | 1.02  | 67.7%  | ~49         | Exclus ciblage actif (micro, scores faibles)           |

**🎯 Total Cibles Actives (segments 1-5)** : **9 709 entreprises** (0.99%), **21 668 établissements** (potentiel contract value)

**💡 Insight Ratio** : Premium Mono-Site à 1.0 parfait valide la cohérence de segmentation (mono-sites purs). Grands Comptes à 56.2 valide le critère >20 agences.

---

### 📊 Détails par Segment

#### 🏢 **1. GRANDS COMPTES** (92 entreprises, 5 170 établissements)

**Critères d'entrée :**
- `nb_etablissements > 20`

**Profil-type :**
- Grands groupes nationaux BTP (20 à 515 établissements)
- Exemples identifiés : ENGIE (515), EDF (251), VINCI, BOUYGUES, EIFFAGE, Proxiserve (104)
- Score moyen : ~94 / 130

**Stratégie commerciale :**
- **Approche** : Account-Based Marketing (ABM), contact C-level (DirGén, DirOps)
- **Cycle de vente** : 12-24 mois
- **Contract value** : ×50 à ×500 (déploiement national)
- **Ressources** : CSM dédié groupe, POC pilotes 3-6 mois, contractualisation nationale

**Particularités :**
- Ratio 56.2 établissements/entreprise = validation critère >20 agences
- Priorité absolue dans la cascade de segmentation
- Volume réduit (92) = approche ultra-personnalisée possible

---

#### 🏗️ **2. GROUPES STRUCTURÉS** (358 entreprises, 3 351 établissements)

**Critères d'entrée :**
- `nb_etablissements BETWEEN 6 AND 20`
- `score_moyen_siren >= 75`
- `effectifs_unite_legale >= 20` (≥20 salariés totaux)

**Profil-type :**
- Groupes régionaux structurés (6-20 établissements, 100-999 salariés)
- Divisions régionales grands groupes (ex: EIFFAGE Energie régions)
- Exemples : CIEC (7 agences), BALAS (20), filiales régionales VINCI/BOUYGUES
- Score moyen : ~85 / 130

**Stratégie commerciale :**
- **Approche** : Contact siège ou direction régionale
- **Cycle de vente** : 6-12 mois
- **POC** : 1-2 sites pilotes → Déploiement groupe
- **Contract value** : ×6 à ×20

**Particularités :**
- Ratio 9.4 établissements/entreprise = validation critère 6-20 agences
- Potentiel déploiement groupe avec autonomie décisionnelle modérée
- Volume 358 = approche semi-personnalisée (ABM allégé)

---

#### 🚀 **3. MULTI-SITES QUALIFIÉS** (601 entreprises, 1 586 établissements)

**Critères d'entrée :**
- `nb_etablissements BETWEEN 2 AND 5`
- `score_moyen_siren >= 90`
- `effectifs_unite_legale BETWEEN 20 AND 199` (20-199 salariés totaux)
- `code_ape_score >= 20` (APE haute ou moyenne priorité)

**Profil-type :**
- PME en croissance (2-5 agences, 50-199 salariés)
- Secteurs porteurs : Chauffage/Clim, Isolation, Menuiserie, Plomberie
- Score moyen : ~95 / 130 (parmi les plus élevés !)

**Stratégie commerciale :**
- **Approche** : Contact DirGén ou DirOps
- **Cycle de vente** : 3-6 mois
- **POC** : 1 site → Déploiement 2-5 sites
- **Contract value** : ×2 à ×5

**Particularités :**
- Ratio 2.6 établissements/entreprise = majorité bi-sites (2) et tri-sites (3)
- **Différence vs Premium Mono-Site** : Potentiel déploiement multi-sites
- **Différence vs Prioritaire** : Score très élevé (≥90) = forte maturité
- Volume 601 = approche ciblée avec sales dédiés

---

#### 🥇 **4. PREMIUM MONO-SITE** (974 entreprises, 974 établissements)

**Critères d'entrée :**
- `nb_etablissements = 1` (mono-site uniquement)
- `score_total >= 100` (score établissement, pas score moyen)
- `effectifs_unite_legale BETWEEN 20 AND 199`
- `code_ape_score >= 20`

**Profil-type :**
- PME structurées mono-site (50-199 salariés)
- Forte maturité commerciale (score ≥100)
- Secteurs prioritaires : Chauffage, Isolation, Menuiserie
- Score moyen : ~105 / 130 ⭐ (le plus élevé de tous les segments !)

**Stratégie commerciale :**
- **Approche** : Vente directe ciblée, contact DirGén ou Responsable Ops
- **Cycle de vente** : 2-4 mois (le plus court !)
- **Contract value** : ×1 (mono-site, pas de déploiement)
- **Closing** : Rapide (décisionnaire unique, forte maturité)

**Particularités :**
- **Ratio 1.0 parfait** : Validation totale du critère mono-site ✅
- **Différence vs Multi-Sites Qualifiés** : Pas de potentiel déploiement, mais closing plus rapide
- **Maturité maximale** : Score ≥100 = PME avec fort besoin digitalisation
- Volume 974 = approche ciblée avec taux conversion élevé attendu

---

#### ⭐ **5. PRIORITAIRE** (7 684 entreprises, 10 587 établissements)

**Critères d'entrée :**
- `nb_etablissements <= 5`
- `score_moyen_siren >= 78`
- `code_ape_score >= 20`
- `effectifs_unite_legale >= 10` (≥10 salariés totaux)
  OU `nb_etablissements >= 2 AND effectifs_UL >= 6` (multi-sites avec 6-9 sal)

**Profil-type :**
- PME et multi-sites moyens (10-200 salariés)
- APE prioritaires (Chauffage, Isolation, Menuiserie, Plomberie, Plâtrerie)
- Mono ou multi-sites (≤5 établissements)
- Score moyen : ~85 / 130

**Stratégie commerciale :**
- **Approche** : Marketing automation + inbound marketing
- **Cycle de vente** : 2-6 mois
- **Tactiques** : Webinaires sectoriels, contenus ciblés, nurturing 6-12 mois
- **Sales** : Contact si signaux d'intérêt qualifiés (3+ interactions)

**Particularités :**
- Ratio 1.4 établissements/entreprise = majorité mono-sites + quelques bi-sites
- **Cœur de cible masse** : 7 684 entreprises = volume idéal pour marketing automation
- **Différence vs Premium** : Score 78-89 (vs ≥90+) = maturité légèrement inférieure
- Volume important = nécessite automatisation (pas de contact manuel systématique)

---

#### ✓ **6. SECONDAIRE** (308 563 entreprises, 335 792 établissements)

**Critères d'entrée :**
- `score_moyen_siren >= 60`
- Exclus des 5 segments supérieurs

**Profil-type :**
- TPE/PME 3-50 salariés
- Scores moyens (60-77)
- Tous secteurs BTP
- Score moyen : ~68 / 130

**Stratégie commerciale :**
- **Approche** : Nurturing long terme, inbound uniquement
- **Tactiques** : Contenus éducatifs (blog, guides), SEO, newsletter
- **Conversion** : Pas de prospection active (coût > ROI attendu)
- **Cycle** : Si inbound spontané uniquement

**Particularités :**
- Ratio 1.09 établissements/entreprise = quasi exclusivement mono-sites
- **Réservoir de croissance** : 308K entreprises pour scaling futur
- **Pas "secondaire" en qualité** : Score moyen 68 = bon fit, mais approche passive
- Volume important = nécessite stratégie inbound scalable (SEO, contenu)

---

#### ⚪ **7. HORS-CIBLE** (665 810 entreprises, 680 950 établissements)

**Critères d'entrée :**
- `score_moyen_siren < 60`

**Profil-type :**
- Micro-entreprises (1-5 salariés)
- Secteurs hors cible (APE score faible)
- Zones rurales isolées
- Entrepreneurs individuels
- Score moyen : ~49 / 130

**Stratégie commerciale :**
- **Approche** : Exclus du ciblage marketing/commercial actif
- **Autorisation** : Peuvent acheter via site web (self-service uniquement)
- **Coût** : Toute action marketing = ROI négatif attendu

**Particularités :**
- 67.7% de la base BTP = validation qualité segmentation (exclusion massive cohérente)
- Ratio 1.02 = quasi exclusivement mono-sites
- Score <60 = micro-entreprises, artisans isolés, faible capacité investissement

---

## 📈 Résultats & Insights Analytiques

### KPIs du Pipeline v1.6

✅ **984 082 entreprises (SIREN)** analysées (exhaustivité INSEE)  
✅ **1 038 410 établissements (SIRET)** traités avec scoring individuel  
✅ **9 709 cibles actives** ultra-qualifiées (0.99% de la base) - **7 segments exclusifs niveau SIREN**  
✅ **21 668 établissements** dans cibles actives (potentiel contract value)  
✅ **99,6% de taux de couverture** après validation  
✅ **23 codes APE BTP** analysés (3 haute + 4 moyenne + 16 basse valeur)  
✅ **Score moyen global** : 53,4 / 130 (médiane à 50)  
✅ **Cohérence SIREN parfaite** : 100% des établissements d'un même SIREN dans le même segment ✅  
✅ **Ratio Premium Mono-Site : 1.0** (validation mono-sites purs) ✅  
✅ **Score moyen cibles actives** : ~90 / 130 (vs ~68 Secondaire, ~49 Hors-Cible)


### 📈 Évolution de la Segmentation v1.5 → v1.6

**Révolution architecturale** : Passage de 5 à 7 segments avec segmentation niveau **entreprise (SIREN)** au lieu d'établissement (SIRET).

| Métrique                         | v1.5 (Fév 2026) | v1.6 (Fév 2026) | Évolution       | Impact                                   |
| -------------------------------- | --------------- | --------------- | --------------- | ---------------------------------------- |
| **Nombre de segments**           | 5               | **7**           | **+40%**        | Différenciation mono-sites               |
| **Cibles actives (entreprises)** | ~17 000         | **9 709**       | **-43%** 🎯     | Focus qualité maximale (top 1%)          |
| **Cibles actives (établ.)**      | ~63 500         | **21 668**      | **-66%** 🎯     | Élimination doublons multi-sites         |
| **Grands Comptes**               | 4 141 établ.    | 92 SIREN        | **Refonte**     | Comptage entreprises (vs établissements) |
| **Premium Mono-Site**            | 0               | **974** 🆕      | **Nouveau**     | Différenciation mono-sites qualifiés     |
| **Multi-Sites Qualifiés**        | 0               | **601** 🆕      | **Nouveau**     | Séparation 2-5 agences premium           |
| **Groupes Structurés**           | 0               | **358** 🆕      | **Nouveau**     | Groupes 6-20 agences isolés              |
| **Prioritaire**                  | 8 034 établ.    | 7 684 SIREN     | **-4%**         | Stabilité                                |
| **Secondaire**                   | 46 478 établ.   | 308 563 SIREN   | **+564%** 📈    | Capture mono-sites score moyen           |
| **Chevauchements SIREN**         | Non mesuré v1.5 | **0** ✅        | **Éliminé**     | Cohérence parfaite                       |
| **Ratio Mono-Site Premium**      | N/A             | **1.0** ✅      | **Validation**  | Mono-sites purs confirmés                |
| **Score moyen Cibles Actives**   | ~88.5           | **~90**         | **+1.7%** ⬆️    | Sélectivité accrue                       |

#### 🔑 Changements Clés v1.6

**1️⃣ Segmentation Niveau SIREN (Entreprise)**
- **v1.5** : Chaque établissement (SIRET) segmenté individuellement → Risque incohérence multi-sites
- **v1.6** : `score_moyen_siren` (AVG scores établissements) → **Tous établissements d'une entreprise = même segment**
- **Impact** : Cohérence décisionnelle parfaite (décision = niveau DirCo/Gérant, pas niveau établissement)

**2️⃣ Différenciation Mono-Sites (3 nouveaux segments)**
- **v1.5** : Mono-sites noyés dans Premium PME + Prioritaire + Secondaire
- **v1.6** : 
  - **Premium Mono-Site** (974) : Score ≥100, 20-199 sal, mono-site pur
  - **Groupes Structurés** (358) : 6-20 agences, score ≥75
  - **Multi-Sites Qualifiés** (601) : 2-5 agences, score ≥90
- **Impact** : Stratégies commerciales adaptées (closing rapide mono vs déploiement multi)

**3️⃣ Élimination Doublons Multi-Sites**
- **v1.5** : Lorillard (20 agences) = 20 lignes dans Premium PME → Surévaluation volume
- **v1.6** : Lorillard (20 agences) = 1 ligne "Multi-Sites Qualifiés" → Comptage entreprises réel
- **Impact** : Passage de 63 557 établissements cibles à **9 709 entreprises** (volume actionnable réaliste)

**4️⃣ Redéfinition Secondaire**
- **v1.5** : 46 478 établissements (score ≥52, effectifs établissement + SIREN)
- **v1.6** : 308 563 entreprises (score moyen SIREN ≥60)
- **Impact** : Capture massive mono-sites score moyen (60-77) = réservoir inbound/nurturing

**5️⃣ Qualité vs Quantité**
- **v1.5** : 17K entreprises cibles (1.7% base) = Risque dilution qualité
- **v1.6** : **9 709 entreprises cibles (0.99% base) = Top 1% ultra-qualifié** 🎯
- **Impact** : Focus maximal, ROI commercial optimisé, segments actionnables

---

#### 💡 Rationale Stratégique v1.6

**Pourquoi réduire de 63K à 21K établissements ?**

1. **Élimination doublons** : Lorillard = 1 entreprise (pas 20), ENGIE = 1 entreprise (pas 515)
2. **Cohérence commerciale** : On vend à une ENTREPRISE, pas à 20 établissements séparément
3. **Actionnable** : 9 709 entreprises = gérable par Sales/Marketing (vs 63K = irréaliste)
4. **Qualité maximale** : Top 1% (vs top 6%) = ROI commercial optimal

**Pourquoi 7 segments au lieu de 5 ?**

1. **Différenciation mono-sites** : Premium Mono (closing rapide) ≠ Multi-Sites (déploiement groupe)
2. **Stratégies distinctes** : Groupes 6-20 agences (approche siège) ≠ Multi-Sites 2-5 (PME croissance)
3. **Granularité opérationnelle** : Chaque segment = playbook commercial spécifique
4. **Scalabilité** : 308K Secondaire = réservoir pour croissance future (nurturing passif)

### 🗺️ Insights Territoriaux
> **Note v1.6** : Les données territoriales ci-dessous sont basées sur la v1.3 (438 Premium PME). Une mise à jour avec les 974 Premium Mono-Site + 601 Multi-Sites Qualifiés sera effectuée prochainement. Les tendances (concentration métropoles, domination zones urbaines) restent valables.

**Top 5 Départements - Premium PME (~3 000 cibles)**

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

### 🏗️ Insights Sectoriels (Codes APE) [EN COURS DE MISE A JOUR]

**Distribution des 41 797 Cibles par Score APE**

| Score APE | Codes APE | % des codes | Volume Cibles Estimé\* | Potentiel Commercial                |
| --------- | --------- | ----------- | ---------------------- | ----------------------------------- |
| **25** ⭐ | 3 codes   | 7,3%        | ~12 000                | Très élevé (récurrence)             |
| **20**    | 4 codes   | 9,8%        | ~10 500                | Élevé (projets moyens)              |
| **10**    | 20 codes  | 48,8%       | ~19 000                | Standard (TPE/artisans)             |
| **0**     | 14 codes  | 34,1%       | ~300                   | Hors-cible (exclus scoring Premium) |

\*Volume estimé sur base 41 797 cibles exploitables

**Top 7 APE Premium PME (données v1.3, distribution similaire attendue en v1.4)** :

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

**📏 2. Sweet Spot PME : 20-199 salariés UNITÉ LÉGALE (v1.5)** : Ciblage élargi aligné décision d'achat  
→ **Logique hybride v1.5** :

- **Score** basé sur effectifs ÉTABLISSEMENT (40 pts dès 20 sal/établissement = sweet spot)
- **Segmentation** basée sur effectifs UNITÉ LÉGALE (20-199 sal totaux = décision DirCo/DirMarketing)
  → **Impact** : +61% Premium PME (4 791 vs 2 975 en v1.4)  
  → **Exemples capturés** :
- Lorillard (20 agences × 4 sal = 80 sal totaux) ✅ Premium PME
- PME régionale mono-site 50 sal ✅ Premium PME
- Micro-établissement 3 sal de PME 10 sal totaux ✅ Prioritaire  
  → **Rationale** : Équilibre maturité commerciale (PME structurée) + alignement décision (niveau entreprise)

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

| 🚧 Défi                                                       | ✅ Solution Implémentée                                                                                         | 💡 Apprentissage                                |
| ------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------- | ----------------------------------------------- |
| API SIRENE rate-limit (1000 req/jour)                         | Batch nocturnes + cache Parquet local + retry exponential                                                       | Gestion contraintes externes API                |
| Volume 1M lignes (RAM limitée 16 Go)                          | Chunking Pandas (100K lignes/batch) + garbage collection                                                        | Optimisation mémoire Python                     |
| Scoring subjectif (pondération arbitraire)                    | Validation terrain avec commerciaux (3 itérations) → v1.3                                                       | Méthodologie itérative métier                   |
| Types BigQuery incohérents post-upload                        | Script `fix_categorie_juridique_type.py` + schémas explicites                                                   | Typage strict obligatoire                       |
| Doublons SIRET (multi-établissements)                         | Dédoublonnage par SIRET + agrégation SIREN (comptage agences)                                                   | Modèle INSEE (SIREN/SIRET)                      |
| Segmentation Grands Comptes pollue Premium                    | Critère anti-grands-groupes (>20 agences) → segment dédié                                                       | Segmentation hybride (score + règles)           |
| Performance BigQuery (vues lentes)                            | Index sur `siret` + partitionnement par `departement`                                                           | Optimisation requêtes DWH                       |
| Chevauchements segments (3 165 établissements multi-segments) | Refonte logique en **cascade exclusive** (v1.5) : if GC → stop, elif Filiales → stop, elif Premium → stop, etc. | Segmentation exclusive = meilleure exploitation |
| Volume Secondaire trop large (308K en v1.4)                   | Double critère effectifs : 3+ sal/établissement ET 6+ sal unité légale → Réduction à 46K avec score moyen 82.3  | Qualité > Quantité pour segments opérationnels  |

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
