-- ============================================================================
-- Fichier : sql/02_vues/create_view_btp_global.sql
-- Description : Vue consolidée tous codes APE BTP avec scoring optimisé
-- Version : 1.5 (SEGMENTATION HYBRIDE EXCLUSIVE - Effectifs Unité Légale)
-- Date : 2026-02-11
--
-- CHANGEMENTS MAJEURS v1.4 → v1.5 :
-- - 🔥 SEGMENTATION EXCLUSIVE EN CASCADE (correction chevauchements)
-- - 🔥 EFFECTIFS UNITÉ LÉGALE pour segmentation (décision niveau entreprise)
-- - 🔥 NOUVEAU SEGMENT : Moyennes Filiales (filiales régionales 200-999 sal)
-- - Conservation du scoring SIRET (granularité territoriale/APE intacte)
-- - Alignement décision d'achat : DirCo/DirMarketing/Gérant = niveau SIREN
--
-- Périmètre :
-- - ~1M établissements BTP actifs
-- - 23 codes APE retenus (UNION ALL de 23 tables full_*)
-- - Scoring sur 130 points (5 dimensions) - CALCULÉ AU NIVEAU SIRET
-- - Segmentation hybride : Score SIRET + Effectifs SIREN
--
-- Scoring optimisé (niveau SIRET) :
-- - Taille entreprise : 0-40 pts (31%) - Établissement individuel
-- - Profil territorial : 0-25 pts (19%) - Établissement individuel
-- - Potentiel APE : 0-25 pts (19%) - Établissement individuel
-- - Bonus multi-agences : 0-20 pts (15%) - Groupe (SIREN)
-- - Forme juridique : 0-20 pts (15%) - Groupe (SIREN)
--
-- Segmentation commerciale (5 segments EXCLUSIFS) :
-- 1. Grands Comptes : Score ≥78 + >20 agences (~3 600 établissements)
-- 2. Moyennes Filiales : Score ≥75 + 200-999 sal/étab. + ≤20 agences (~115)
-- 3. Premium PME : Score ≥78 + 20-199 sal UL + APE ≥20 + ≤20 agences (~4 500)
-- 4. Prioritaire : Score ≥70 + 10-199 sal UL + APE ≥20 + ≤50 agences (~5 500)
-- 5. Secondaire : Score ≥52 + 6-199 sal UL + ≤50 agences (~40 000)
-- ============================================================================

CREATE OR REPLACE VIEW `btp_analysis.v_etablissements_btp_global` AS

-- ============================================================================
-- CTE 1 : UNION ALL des 23 codes APE retenus
-- ============================================================================
WITH base_union AS (

  -- HAUTE PRIORITÉ (3 codes) - Score APE = 25/25
  SELECT *, '43.22B' AS code_ape FROM `btp_analysis.full_43_22B`  -- Chauffagiste
  UNION ALL
  SELECT *, '43.29A' AS code_ape FROM `btp_analysis.full_43_29A`  -- Isolation
  UNION ALL
  SELECT *, '43.32A' AS code_ape FROM `btp_analysis.full_43_32A`  -- Menuiserie bois

  -- MOYENNE PRIORITÉ (4 codes) - Score APE = 20/25
  UNION ALL
  SELECT *, '41.20A' AS code_ape FROM `btp_analysis.full_41_20A`  -- Construction maisons
  UNION ALL
  SELECT *, '43.29B' AS code_ape FROM `btp_analysis.full_43_29B`  -- Autres installations
  UNION ALL
  SELECT *, '43.31Z' AS code_ape FROM `btp_analysis.full_43_31Z`  -- Plâtrerie
  UNION ALL
  SELECT *, '43.32B' AS code_ape FROM `btp_analysis.full_43_32B`  -- Menuiserie métallique

  -- BASSE PRIORITÉ (16 codes) - Score APE = 10/25
  UNION ALL
  SELECT *, '41.20B' AS code_ape FROM `btp_analysis.full_41_20B`
  UNION ALL
  SELECT *, '43.11Z' AS code_ape FROM `btp_analysis.full_43_11Z`
  UNION ALL
  SELECT *, '43.12A' AS code_ape FROM `btp_analysis.full_43_12A`
  UNION ALL
  SELECT *, '43.12B' AS code_ape FROM `btp_analysis.full_43_12B`
  UNION ALL
  SELECT *, '43.21A' AS code_ape FROM `btp_analysis.full_43_21A`
  UNION ALL
  SELECT *, '43.22A' AS code_ape FROM `btp_analysis.full_43_22A`
  UNION ALL
  SELECT *, '43.32C' AS code_ape FROM `btp_analysis.full_43_32C`
  UNION ALL
  SELECT *, '43.33Z' AS code_ape FROM `btp_analysis.full_43_33Z`
  UNION ALL
  SELECT *, '43.34Z' AS code_ape FROM `btp_analysis.full_43_34Z`
  UNION ALL
  SELECT *, '43.39Z' AS code_ape FROM `btp_analysis.full_43_39Z`
  UNION ALL
  SELECT *, '43.91A' AS code_ape FROM `btp_analysis.full_43_91A`
  UNION ALL
  SELECT *, '43.91B' AS code_ape FROM `btp_analysis.full_43_91B`
  UNION ALL
  SELECT *, '43.99A' AS code_ape FROM `btp_analysis.full_43_99A`
  UNION ALL
  SELECT *, '43.99C' AS code_ape FROM `btp_analysis.full_43_99C`
  UNION ALL
  SELECT *, '43.99D' AS code_ape FROM `btp_analysis.full_43_99D`
  UNION ALL
  SELECT *, '81.30Z' AS code_ape FROM `btp_analysis.full_81_30Z`
),

-- ============================================================================
-- CTE 2 : Calcul du scoring par établissement (SIRET)
-- ============================================================================
scoring AS (
  SELECT
    -- ========================================================================
    -- Identifiants
    -- ========================================================================
    b.siret,
    b.siren,
    b.uniteLegale_denominationUniteLegale AS nom_entreprise,
    b.code_ape,
    b.departement AS code_departement,
    
    -- 🔥 NOUVEAU : Distinction effectifs ÉTABLISSEMENT vs UNITÉ LÉGALE
    b.trancheEffectifsEtablissement AS code_effectifs_etablissement,
    b.uniteLegale_trancheEffectifsUniteLegale AS code_effectifs_unite_legale,
    
    b.uniteLegale_categorieJuridiqueUniteLegale AS code_categorie_juridique,

    -- ========================================================================
    -- Informations complémentaires
    -- ========================================================================
    CONCAT(
      COALESCE(b.adresseEtablissement_numeroVoieEtablissement, ''), ' ',
      COALESCE(b.adresseEtablissement_typeVoieEtablissement, ''), ' ',
      COALESCE(b.adresseEtablissement_libelleVoieEtablissement, ''), ', ',
      COALESCE(b.adresseEtablissement_codePostalEtablissement, ''), ' ',
      COALESCE(b.adresseEtablissement_libelleCommuneEtablissement, '')
    ) AS adresse_complete,
    
    b.dateCreationEtablissement AS date_creation_etablissement,
    b.uniteLegale_dateCreationUniteLegale AS date_creation_entreprise,
    b.periode_etatAdministratifEtablissement AS etat_administratif,
    b.etablissementSiege AS est_siege,
    b.longitude,
    b.latitude,

    -- ========================================================================
    -- INFO : Multi-agences (niveau SIREN)
    -- ========================================================================
    b.nb_etablissements,
    CASE
      WHEN b.nb_etablissements >= 5 THEN 'Multi-agences (5+)'
      WHEN b.nb_etablissements >= 2 THEN 'Multi-agences (2-4)'
      ELSE 'Mono-établissement'
    END AS type_structure,

    -- ========================================================================
    -- DIMENSION 1 : Profil territorial (0-25 pts) - Niveau SIRET
    -- ========================================================================
    CASE
      WHEN d.profil_territorial = 'Très urbain' THEN 25
      WHEN d.profil_territorial = 'Urbain' THEN 20
      WHEN d.profil_territorial = 'Péri-urbain' THEN 15
      WHEN d.profil_territorial = 'Rural' THEN 10
      ELSE 5
    END AS score_profil_territorial,
    d.profil_territorial,

    -- ========================================================================
    -- DIMENSION 2 : Taille entreprise (0-40 pts) - Niveau SIRET ÉTABLISSEMENT
    -- Note : Score basé sur effectifs ÉTABLISSEMENT (granularité locale)
    --        Segmentation utilisera effectifs UNITÉ LÉGALE (décision groupe)
    -- ========================================================================
    CASE
      -- Sweet spot : 20-49 sal/établissement
      WHEN b.trancheEffectifsEtablissement = '12' THEN 40
      -- 50+ sal : plafonné à 40 pts
      WHEN b.trancheEffectifsEtablissement IN ('21','22','31','32','41','42','51','52','53') THEN 40
      -- 10-19 sal
      WHEN b.trancheEffectifsEtablissement = '11' THEN 35
      -- 6-9 sal
      WHEN b.trancheEffectifsEtablissement = '03' THEN 30
      -- 3-5 sal
      WHEN b.trancheEffectifsEtablissement = '02' THEN 20
      -- 1-2 sal
      WHEN b.trancheEffectifsEtablissement = '01' THEN 15
      -- 0 sal
      WHEN b.trancheEffectifsEtablissement IN ('00', 'NN') THEN 5
      ELSE 5
    END AS score_taille_entreprise,
    
    -- Libellés lisibles (ÉTABLISSEMENT et UNITÉ LÉGALE)
    eff_etab.Employes AS libelle_effectifs_etablissement,
    eff_ul.Employes AS libelle_effectifs_unite_legale,

    -- ========================================================================
    -- DIMENSION 3 : Forme juridique (0-20 pts) - Niveau SIREN
    -- ========================================================================
    CASE
      WHEN j.famille_juridique = 'Société commerciale' THEN 20
      WHEN j.famille_juridique = 'Société civile' THEN 15
      WHEN j.famille_juridique = 'Entrepreneur individuel' THEN 10
      ELSE 5
    END AS score_forme_juridique,
    j.famille_juridique,

    -- ========================================================================
    -- DIMENSION 4 : Potentiel APE (0-25 pts) - Niveau SIRET
    -- ========================================================================
    COALESCE(ape.score_priorite, 10) AS score_potentiel_ape,
    ape.libelle_ape,

    -- ========================================================================
    -- DIMENSION 5 : Bonus multi-agences (0-20 pts) - Niveau SIREN
    -- ========================================================================
    CASE
      WHEN b.nb_etablissements >= 5 THEN 20
      WHEN b.nb_etablissements >= 2 THEN 10
      ELSE 0
    END AS bonus_multi_agences

  FROM base_union b
  
  -- Jointure profil territorial
  LEFT JOIN `btp_analysis.dim_departements` d
    ON b.departement = d.dep
  
  -- Jointure effectifs ÉTABLISSEMENT (pour libellé)
  LEFT JOIN `btp_analysis.dim_categories_effectifs` eff_etab
    ON b.trancheEffectifsEtablissement = eff_etab.tranche_effectifs
  
  -- 🔥 NOUVEAU : Jointure effectifs UNITÉ LÉGALE (pour segmentation)
  LEFT JOIN `btp_analysis.dim_categories_effectifs` eff_ul
    ON b.uniteLegale_trancheEffectifsUniteLegale = eff_ul.tranche_effectifs
  
  -- Jointure forme juridique
  LEFT JOIN `btp_analysis.dim_categories_juridiques` j
    ON b.uniteLegale_categorieJuridiqueUniteLegaleNiv2 = j.categorie_juridique_ul_niv2
  
  -- Jointure scoring APE
  LEFT JOIN `btp_analysis.dim_codes_ape` ape
    ON b.code_ape = ape.code_ape

  -- Filtre : Établissements actifs uniquement
  WHERE b.periode_etatAdministratifEtablissement = 'A'
),

-- ============================================================================
-- CTE 3 : Calcul score total + Segmentation PHASE 1 (Grands Comptes)
-- ============================================================================
score_et_grands_comptes AS (
  SELECT
    *,
    
    -- Score total (0-130 points)
    (score_profil_territorial + score_taille_entreprise + 
     score_forme_juridique + score_potentiel_ape + bonus_multi_agences) AS score_total,
    
    -- Catégorie potentiel (4 niveaux)
    CASE
      WHEN (score_profil_territorial + score_taille_entreprise +
            score_forme_juridique + score_potentiel_ape + bonus_multi_agences) >= 104
        THEN 'Très fort potentiel'
      WHEN (score_profil_territorial + score_taille_entreprise +
            score_forme_juridique + score_potentiel_ape + bonus_multi_agences) >= 78
        THEN 'Fort potentiel'
      WHEN (score_profil_territorial + score_taille_entreprise +
            score_forme_juridique + score_potentiel_ape + bonus_multi_agences) >= 52
        THEN 'Potentiel moyen'
      ELSE 'Potentiel faible'
    END AS categorie_potentiel,
    
    -- ========================================================================
    -- 🥇 SEGMENT 1 : GRANDS COMPTES (Priorité absolue)
    -- ========================================================================
    -- Critères : Score ≥78 + >20 agences
    -- Volume : ~3 600 établissements (~89 entreprises SIREN)
    -- Usage : ABM, POC 3-6 mois, CSM dédié niveau groupe
    -- Exemples : ENGIE (212), Proxiserve (104), Hervé (90), Axima (79)
    -- ========================================================================
    CASE
      WHEN (score_profil_territorial + score_taille_entreprise +
            score_forme_juridique + score_potentiel_ape + bonus_multi_agences) >= 78
           AND nb_etablissements > 20
           AND COALESCE(score_potentiel_ape, 0) >= 10  -- Exclure APE score 0
        THEN TRUE
      ELSE FALSE
    END AS est_grand_compte
    
  FROM scoring
),

-- ============================================================================
-- CTE 4 : Segmentation PHASE 2 (Moyennes Filiales)
-- ============================================================================
avec_moyennes_filiales AS (
  SELECT
    *,
    
    -- ========================================================================
    -- 🏢 SEGMENT 2 : MOYENNES FILIALES (Nouveau en v1.5)
    -- ========================================================================
    -- Critères : Score ≥75 + 200-999 sal/ÉTABLISSEMENT + ≤20 agences
    -- Volume : ~115 établissements (filiales régionales grands groupes)
    -- Usage : POC régionaux, approche groupe avec validation siège
    -- Exemples : EIFFAGE Energie (16 agences), VINCI Construction (13), CIEC (7)
    -- Rationale : Filiales régionales avec autonomie limitée, pas des PME indépendantes
    -- ========================================================================
    CASE
      WHEN est_grand_compte = FALSE  -- 🔥 EXCLUSION GRANDS COMPTES
           AND (score_profil_territorial + score_taille_entreprise +
                score_forme_juridique + score_potentiel_ape + bonus_multi_agences) >= 75
           AND code_effectifs_etablissement IN ('31', '32', '41')  -- 200-999 sal PAR ÉTABLISSEMENT
           AND nb_etablissements <= 20
           AND COALESCE(score_potentiel_ape, 0) >= 10
        THEN TRUE
      ELSE FALSE
    END AS est_moyenne_filiale
    
  FROM score_et_grands_comptes
),

-- ============================================================================
-- CTE 5 : Segmentation PHASE 3 (Premium PME)
-- ============================================================================
avec_premium AS (
  SELECT
    *,
    
    -- ========================================================================
    -- 🥇 SEGMENT 3 : PREMIUM PME (Priorité commerciale max)
    -- ========================================================================
    -- Critères : Score ≥78 + 20-199 sal UNITÉ LÉGALE + APE ≥20 + ≤20 agences
    -- Volume : ~4 500 établissements (inclut Lorillard et autres PME multi-sites)
    -- Score moyen : ~102
    -- Usage : Prospection Sales directe, démo 1:1, CSM dédié
    -- Profil : PME régionales indépendantes (chauffage, isolation, menuiserie)
    -- 🔥 HYBRIDE : Effectifs SIREN (décision) + Score SIRET (qualité)
    -- ========================================================================
    CASE
      WHEN est_grand_compte = FALSE  -- 🔥 EXCLUSION GC
           AND est_moyenne_filiale = FALSE  -- 🔥 EXCLUSION MOYENNES FILIALES
           AND (score_profil_territorial + score_taille_entreprise +
                score_forme_juridique + score_potentiel_ape + bonus_multi_agences) >= 78
           AND code_effectifs_unite_legale IN ('12', '21', '22')  -- 🔥 20-199 sal UNITÉ LÉGALE
           AND COALESCE(score_potentiel_ape, 0) >= 20  -- APE haute ou moyenne
           AND nb_etablissements <= 20
        THEN TRUE
      ELSE FALSE
    END AS est_cible_premium
    
  FROM avec_moyennes_filiales
),

-- ============================================================================
-- CTE 6 : Segmentation PHASE 4 (Prioritaire)
-- ============================================================================
avec_prioritaire AS (
  SELECT
    *,
    
    -- ========================================================================
    -- ⭐ SEGMENT 4 : PRIORITAIRE
    -- ========================================================================
    -- Critères : Score ≥70 + 10-199 sal UNITÉ LÉGALE + APE ≥20 + ≤50 agences
    -- Volume : ~5 500 établissements
    -- Usage : Marketing automation, webinaires, nurturing 6-12 mois
    -- Profil : Petites PME 10-19 sal + PME 20-199 sal (métiers prioritaires, score <78)
    -- ========================================================================
    CASE
      WHEN est_grand_compte = FALSE  -- 🔥 EXCLUSION GC
           AND est_moyenne_filiale = FALSE  -- 🔥 EXCLUSION MOYENNES FILIALES
           AND est_cible_premium = FALSE  -- 🔥 EXCLUSION PREMIUM
           AND (score_profil_territorial + score_taille_entreprise +
                score_forme_juridique + score_potentiel_ape + bonus_multi_agences) >= 70
           AND code_effectifs_unite_legale IN ('11', '12', '21', '22')  -- 🔥 10-199 sal UL
           AND COALESCE(score_potentiel_ape, 0) >= 20
           AND nb_etablissements <= 50
        THEN TRUE
      ELSE FALSE
    END AS est_cible_prioritaire
    
  FROM avec_premium
)

-- ============================================================================
-- SÉLECTION FINALE : Segmentation PHASE 5 (Secondaire)
-- ============================================================================
SELECT
  *,
  
  -- ========================================================================
  -- ✓ SEGMENT 5 : SECONDAIRE
  -- ========================================================================
  -- Critères : Score ≥52 + 6-199 sal UNITÉ LÉGALE + ≤50 agences
  -- Volume : ~40 000 établissements
  -- Usage : Inbound, freemium, self-service, contenus SEO
  -- Profil : Micro-structurées (6-9 sal UL) + PME (tous métiers, score moyen)
  -- ========================================================================
  CASE
    WHEN est_grand_compte = FALSE  -- 🔥 EXCLUSION GC
         AND est_moyenne_filiale = FALSE  -- 🔥 EXCLUSION MOYENNES FILIALES
         AND est_cible_premium = FALSE  -- 🔥 EXCLUSION PREMIUM
         AND est_cible_prioritaire = FALSE  -- 🔥 EXCLUSION PRIORITAIRE
         AND (score_profil_territorial + score_taille_entreprise +
              score_forme_juridique + score_potentiel_ape + bonus_multi_agences) >= 52
         AND code_effectifs_etablissement IN ('02', '03', '11', '12', '21', '22', '31', '32', '41', '42', '51', '52', '53')  -- 🔥 3+ sal PAR ÉTABLISSEMENT
         AND code_effectifs_unite_legale IN ('03', '11', '12', '21', '22', '31', '32', '41', '42', '51', '52', '53')  -- 🔥 6+ sal UNITÉ LÉGALE (code '03' = 6-9)
         AND nb_etablissements <= 50
      THEN TRUE
    ELSE FALSE
  END AS est_cible_secondaire

FROM avec_prioritaire;

-- ============================================================================
-- FIN DU SCRIPT v1.5
-- ============================================================================
