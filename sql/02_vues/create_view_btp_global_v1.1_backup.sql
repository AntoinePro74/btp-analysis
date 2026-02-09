-- ============================================================================
-- Fichier : sql/02_vues/create_view_btp_global.sql
-- Description : Vue consolidée tous codes APE BTP avec scoring optimisé
-- Version : 1.1 (Adapté structure btp_analysis)
-- Date : 2026-02-07
-- 
-- Périmètre :
-- - ~1,2M établissements BTP actifs
-- - 23 codes APE retenus (UNION ALL de 23 tables full_*)
-- - Scoring sur 130 points (5 dimensions)
-- - Catégorisation automatique (4 niveaux)
-- 
-- Scoring optimisé :
-- - Taille entreprise : 0-40 pts (31%) - Critère dominant
-- - Profil territorial : 0-25 pts (19%)
-- - Potentiel APE : 0-25 pts (19%) - Basse = 10 pts
-- - Bonus multi-agences : 0-20 pts (15%)
-- - Forme juridique : 0-20 pts (15%)
-- ============================================================================

CREATE OR REPLACE VIEW `btp_analysis.v_etablissements_btp_global` AS

-- ============================================================================
-- CTE 1 : UNION ALL des 29 codes APE retenus
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
  SELECT *, '41.20B' AS code_ape FROM `btp_analysis.full_41_20B`  -- Construction autres bâtiments
  UNION ALL
  SELECT *, '43.11Z' AS code_ape FROM `btp_analysis.full_43_11Z`  -- Démolition
  UNION ALL
  SELECT *, '43.12A' AS code_ape FROM `btp_analysis.full_43_12A`  -- Terrassement courants
  UNION ALL
  SELECT *, '43.12B' AS code_ape FROM `btp_analysis.full_43_12B`  -- Terrassement spécialisés
  UNION ALL
  SELECT *, '43.21A' AS code_ape FROM `btp_analysis.full_43_21A`  -- Électricien
  UNION ALL
  SELECT *, '43.22A' AS code_ape FROM `btp_analysis.full_43_22A`  -- Plombier
  UNION ALL
  SELECT *, '43.32C' AS code_ape FROM `btp_analysis.full_43_32C`  -- Agencement
  UNION ALL
  SELECT *, '43.33Z' AS code_ape FROM `btp_analysis.full_43_33Z`  -- Carreleur
  UNION ALL
  SELECT *, '43.34Z' AS code_ape FROM `btp_analysis.full_43_34Z`  -- Peintre
  UNION ALL
  SELECT *, '43.39Z' AS code_ape FROM `btp_analysis.full_43_39Z`  -- Autres finitions
  UNION ALL
  SELECT *, '43.91A' AS code_ape FROM `btp_analysis.full_43_91A`  -- Charpentier
  UNION ALL
  SELECT *, '43.91B' AS code_ape FROM `btp_analysis.full_43_91B`  -- Couvreur
  UNION ALL
  SELECT *, '43.99A' AS code_ape FROM `btp_analysis.full_43_99A`  -- Étanchéité
  UNION ALL
  SELECT *, '43.99C' AS code_ape FROM `btp_analysis.full_43_99C`  -- Maçon
  UNION ALL
  SELECT *, '43.99D' AS code_ape FROM `btp_analysis.full_43_99D`  -- Autres travaux spécialisés
  UNION ALL
  SELECT *, '81.30Z' AS code_ape FROM `btp_analysis.full_81_30Z`  -- Paysagiste
),

-- ============================================================================
-- CTE 2 : Calcul du scoring par établissement
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
    b.trancheEffectifsEtablissement AS code_effectifs,
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
    -- INFO : Multi-agences (déjà calculé dans la table !)
    -- ========================================================================
    b.nb_etablissements,
    CASE 
      WHEN b.nb_etablissements >= 5 THEN 'Multi-agences (5+)'
      WHEN b.nb_etablissements >= 2 THEN 'Multi-agences (2-4)'
      ELSE 'Mono-établissement'
    END AS type_structure,
    
    -- ========================================================================
    -- DIMENSION 1 : Profil territorial (0-25 pts) - 19%
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
    -- DIMENSION 2 : Taille entreprise (0-40 pts) - 31% CRITÈRE DOMINANT
    -- ========================================================================
    CASE 
      -- Grandes structures (200-999 salariés)
      WHEN b.trancheEffectifsEtablissement IN ('53', '52', '51') THEN 40
      
      -- Structures développées (50-199 salariés)
      WHEN b.trancheEffectifsEtablissement IN ('42', '41', '32', '31') THEN 38
      
      -- 🎯 CIBLE SWEET SPOT : 10-19 salariés
      WHEN b.trancheEffectifsEtablissement = '22' THEN 35
      
      -- ✅ CIBLE PRIORITAIRE : 6-9 salariés
      WHEN b.trancheEffectifsEtablissement = '21' THEN 30
      
      -- Micro-entreprises (1-5 salariés)
      WHEN b.trancheEffectifsEtablissement IN ('12', '11') THEN 15
      
      -- Non employeur ou non renseigné
      WHEN b.trancheEffectifsEtablissement IN ('03', '02', '01', '00', 'NN') THEN 5
      
      ELSE 5
    END AS score_taille_entreprise,
    eff.Employes,
    
    -- ========================================================================
    -- DIMENSION 3 : Forme juridique (0-20 pts) - 15%
    -- ========================================================================
    CASE 
      WHEN j.famille_juridique = 'Société commerciale' THEN 20
      WHEN j.famille_juridique = 'Société civile' THEN 15
      WHEN j.famille_juridique = 'Entrepreneur individuel' THEN 10
      ELSE 5
    END AS score_forme_juridique,
    j.famille_juridique,
    
    -- ========================================================================
    -- DIMENSION 4 : Potentiel APE (0-25 pts) - 19%
    -- ========================================================================
    COALESCE(ape.score_priorite, 10) AS score_potentiel_ape,
    ape.libelle_ape,
    
    -- ========================================================================
    -- DIMENSION 5 : Bonus multi-agences (0-20 pts) - 15%
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
  
  -- Jointure catégories effectifs
  LEFT JOIN `btp_analysis.dim_categories_effectifs` eff
    ON b.trancheEffectifsEtablissement = eff.tranche_effectifs
  
  -- Jointure forme juridique
  LEFT JOIN `btp_analysis.dim_categories_juridiques` j 
    ON b.uniteLegale_categorieJuridiqueUniteLegaleNiv2 = j.categorie_juridique_ul_niv2
  
  -- Jointure scoring APE (table enrichie)
  LEFT JOIN `btp_analysis.dim_codes_ape` ape 
    ON b.code_ape = ape.code_ape
  
  -- ========================================================================
  -- FILTRE : Établissements actifs uniquement
  -- ========================================================================
  WHERE b.periode_etatAdministratifEtablissement = 'A'  -- Actif
)

-- ============================================================================
-- SÉLECTION FINALE : Score total et catégorisation
-- ============================================================================
SELECT 
  -- Toutes les colonnes du scoring
  *,
  
  -- ========================================================================
  -- SCORE TOTAL (0-130 points)
  -- ========================================================================
  (score_profil_territorial + 
   score_taille_entreprise + 
   score_forme_juridique + 
   score_potentiel_ape +
   bonus_multi_agences) AS score_total,
  
  -- ========================================================================
  -- CATÉGORIE POTENTIEL (4 niveaux)
  -- ========================================================================
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
  -- FLAG CIBLE PRIORITAIRE (pour CRM/Sales)
  -- ========================================================================
  CASE 
    WHEN (score_profil_territorial + score_taille_entreprise + 
          score_forme_juridique + score_potentiel_ape + bonus_multi_agences) >= 78 
         AND code_effectifs IN ('22', '21', '31', '32')  -- 6-49 salariés
         AND score_potentiel_ape >= 25  -- Fit produit confirmé
    THEN TRUE
    ELSE FALSE
  END AS est_cible_prioritaire

FROM scoring;

-- ============================================================================
-- FIN DU SCRIPT
-- ============================================================================
