-- ============================================================================
-- Fichier : sql/02_vues/create_view_btp_global.sql
-- Description : Vue consolidée tous codes APE BTP avec scoring optimisé
-- Version : 1.4 (CORRECTION codes effectifs INSEE - cible PME 20-199 sal)
-- Date : 2026-02-11
-- 
-- CHANGEMENTS MAJEURS v1.3 → v1.4 :
-- - Correction mapping codes effectifs INSEE (erreur détectée)
-- - Nouvelle cible Premium : 20-199 sal/établissement (vs 50-499 en v1.3)
-- - Scoring taille simplifié : plateau à 40 pts dès 20+ salariés
-- - Volume Premium optimisé : ~3,000 cibles (vs 438 en v1.3)
-- - Les 10-19 sal basculent en Prioritaire (marketing automation)
-- 
-- Périmètre :
-- - ~1M établissements BTP actifs
-- - 23 codes APE retenus (UNION ALL de 23 tables full_*)
-- - Scoring sur 130 points (5 dimensions)
-- - Catégorisation automatique (4 niveaux)
-- - Segmentation commerciale (4 flags : 3 PME + 1 Grands Comptes)
-- 
-- Scoring optimisé :
-- - Taille entreprise : 0-40 pts (31%) - Critère dominant, plateau à 20+ sal
-- - Profil territorial : 0-25 pts (19%)
-- - Potentiel APE : 0-25 pts (19%) - Basse = 10 pts
-- - Bonus multi-agences : 0-20 pts (15%)
-- - Forme juridique : 0-20 pts (15%)
-- 
-- Segmentation commerciale :
-- - Cible Premium PME : Score ≥78 + 20-199 sal + APE ≥20 + ≤20 agences (~3,000)
-- - Cible Prioritaire : Score ≥70 + 10-199 sal + APE ≥20 + ≤50 agences (~12,000)
-- - Cible Secondaire : Score ≥52 + 6-199 sal + ≤50 agences (~15,000)
-- - Grand Compte : Score ≥78 + >20 agences (~89 entreprises)
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
    -- CODES EFFECTIFS INSEE (CORRIGÉS v1.4) :
    -- NN/00 = 0 salarié
    -- 01 = 1-2 salariés
    -- 02 = 3-5 salariés
    -- 03 = 6-9 salariés
    -- 11 = 10-19 salariés
    -- 12 = 20-49 salariés
    -- 21 = 50-99 salariés
    -- 22 = 100-199 salariés
    -- 31 = 200-249 salariés
    -- 32 = 250-499 salariés
    -- 41 = 500-999 salariés
    -- 42 = 1000-1999 salariés
    -- 51-53 = 2000+ salariés
    -- 
    -- LOGIQUE SIMPLIFIÉE :
    -- - Progression jusqu'à 20-49 sal (40 pts = maximum)
    -- - Plateau à 40 pts pour 50+ sal (pas de bonus supplémentaire)
    -- - Sweet spot cible : 20-199 salariés pour Premium (score 40 pts)
    -- ========================================================================
    CASE 
      -- 🎯 SWEET SPOT : 20-49 salariés (optimal PME) - MAXIMUM DE POINTS
      WHEN b.trancheEffectifsEtablissement = '12' THEN 40
      
      -- 50+ salariés : PLAFONNÉ à 40 pts (pas de surpondération grosse structure)
      WHEN b.trancheEffectifsEtablissement IN ('21', '22', '31', '32', '41', '42', '51', '52', '53') THEN 40
      
      -- 10-19 salariés (petite PME structurée) - Cible Prioritaire
      WHEN b.trancheEffectifsEtablissement = '11' THEN 35
      
      -- 6-9 salariés (micro structurée)
      WHEN b.trancheEffectifsEtablissement = '03' THEN 30
      
      -- 3-5 salariés (micro-entreprise)
      WHEN b.trancheEffectifsEtablissement = '02' THEN 20
      
      -- 1-2 salariés (très petite structure)
      WHEN b.trancheEffectifsEtablissement = '01' THEN 15
      
      -- 0 salarié ou non renseigné (EI sans salarié, hors cible)
      WHEN b.trancheEffectifsEtablissement IN ('00', 'NN') THEN 5
      
      ELSE 5
    END AS score_taille_entreprise,
    eff.Employes AS libelle_effectifs,
    
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
-- SÉLECTION FINALE : Score total, catégorisation et segmentation
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
  -- FLAG 1 : CIBLE PREMIUM PME (20-199 sal, max 20 agences)
  -- ========================================================================
  -- Critères : Score ≥78 + 20-199 salariés/établissement + APE ≥20 + ≤20 agences
  -- Volume : ~3,000 établissements (2,556 + 344 + 75)
  -- Score moyen : 102.5
  -- Usage : Prospection Sales directe PME, démo personnalisée, CSM dédié
  -- Profil type : PME régionales 20-199 sal (chauffagiste, isolation, menuiserie)
  -- Charge commerciale : 3-5 Sales à temps plein
  CASE 
    WHEN (score_profil_territorial + score_taille_entreprise + 
          score_forme_juridique + score_potentiel_ape + bonus_multi_agences) >= 78 
         AND code_effectifs IN ('12', '21', '22')  -- 20-199 salariés/établissement
         AND score_potentiel_ape >= 20  -- APE haute (25) ou moyenne (20)
         AND nb_etablissements <= 20  -- Max 20 agences (PME, pas groupe national)
    THEN TRUE
    ELSE FALSE
  END AS est_cible_premium,
  
  -- ========================================================================
  -- FLAG 2 : CIBLE PRIORITAIRE (10-199 sal, max 50 agences)
  -- ========================================================================
  -- Critères : Score ≥70 + 10-199 salariés/établissement + APE ≥20 + ≤50 agences
  -- Volume : ~12,000 établissements (inclut les 10-19 sal exclus de Premium)
  -- Score moyen : 95-100
  -- Usage : Marketing automation, webinaires, essais gratuits, nurturing
  -- Profil : Petites PME 10-19 sal + PME moyennes 20-199 sal (métiers prioritaires)
  CASE 
    WHEN (score_profil_territorial + score_taille_entreprise + 
          score_forme_juridique + score_potentiel_ape + bonus_multi_agences) >= 70 
         AND code_effectifs IN ('11', '12', '21', '22')  -- 10-199 salariés/établissement
         AND score_potentiel_ape >= 20  -- APE haute ou moyenne
         AND nb_etablissements <= 50  -- Max 50 agences
    THEN TRUE
    ELSE FALSE
  END AS est_cible_prioritaire,
  
  -- ========================================================================
  -- FLAG 3 : CIBLE SECONDAIRE (6-199 sal, max 50 agences)
  -- ========================================================================
  -- Critères : Score ≥52 + 6-199 salariés/établissement + ≤50 agences
  -- Volume : ~15,000 établissements
  -- Usage : Inbound marketing, contenus, SEO, self-service
  -- Profil : Tous métiers BTP, micro-entreprises structurées (6-9 sal) + PME
  CASE 
    WHEN (score_profil_territorial + score_taille_entreprise + 
          score_forme_juridique + score_potentiel_ape + bonus_multi_agences) >= 52 
         AND code_effectifs IN ('03', '11', '12', '21', '22')  -- 6-199 salariés/établissement
         AND nb_etablissements <= 50  -- Max 50 agences
    THEN TRUE
    ELSE FALSE
  END AS est_cible_secondaire,
  
  -- ========================================================================
  -- FLAG 4 : GRAND COMPTE (>20 agences uniquement)
  -- ========================================================================
  -- Critères : Score ≥78 + >20 agences
  -- Volume : ~89 entreprises (SIREN) / ~4,100 établissements (SIRET)
  -- Usage : Approche Grands Comptes (RFP, POC 3-6 mois, CSM dédié niveau groupe)
  -- Exemples : ENGIE Home Services (212), Proxiserve (104), Axima (79), Hervé (90)
  CASE 
    WHEN (score_profil_territorial + score_taille_entreprise + 
          score_forme_juridique + score_potentiel_ape + bonus_multi_agences) >= 78 
         AND nb_etablissements > 20  -- Plus de 20 agences (structure groupe)
    THEN TRUE
    ELSE FALSE
  END AS est_grand_compte

FROM scoring;


-- ============================================================================
-- FIN DU SCRIPT v1.4
-- ============================================================================
