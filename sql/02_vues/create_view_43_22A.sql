-- =============================================================================
-- VUE CONSOLIDÉE : Établissements BTP - Code APE 43.22A (Plomberie/Chauffage)
-- =============================================================================
-- Objectif : Consolidation de toutes les dimensions + scoring pour Power BI
-- Date création : 2026-02-04
-- Tables sources : full_43_22A + 5 dimensions
-- Établissements couverts : ~72,484 actifs
-- =============================================================================

CREATE OR REPLACE VIEW `projet-sirene-480919.btp_analysis.v_etablissements_43_22A` AS

SELECT 
  -- =============================================================================
  -- IDENTIFIANTS
  -- =============================================================================
  f.siret,
  f.siren,
  
  -- =============================================================================
  -- INFORMATIONS ENTREPRISE
  -- =============================================================================
  f.uniteLegale_denominationUniteLegale as nom_entreprise,
  f.uniteLegale_sigleUniteLegale as sigle_entreprise,
  f.uniteLegale_denominationUsuelle1UniteLegale as denomination_usuelle,
  
  -- =============================================================================
  -- STATUT DIFFUSION
  -- =============================================================================
  f.statutDiffusionEtablissement as statut_diffusion_etablissement,
  f.uniteLegale_statutDiffusionUniteLegale as statut_diffusion_unite_legale,
  
  -- =============================================================================
  -- ACTIVITÉ (ENRICHIE AVEC dim_codes_ape)
  -- =============================================================================
  f.periode_activitePrincipaleEtablissement as code_ape,
  ape.libelle_ape as ape_nom_complet,
  ape.nom_commun_metier as ape_metier,
  ape.famille_metier as ape_famille,
  
  -- Division APE (déjà calculée dans la table)
  f.division_ape_etab as division_ape,
  f.type_activite_etab as type_activite,
  
  -- Activité unité légale
  f.uniteLegale_activitePrincipaleUniteLegale as code_ape_unite_legale,
  
  -- Activité registre métiers
  f.activitePrincipaleRegistreMetiersEtablissement as code_activite_registre_metiers,
  
  -- =============================================================================
  -- GÉOGRAPHIE (ENRICHIE AVEC dim_departements)
  -- =============================================================================
  f.adresseEtablissement_numeroVoieEtablissement as numero_voie,
  f.adresseEtablissement_typeVoieEtablissement as type_voie,
  f.adresseEtablissement_libelleVoieEtablissement as libelle_voie,
  f.adresseEtablissement_codePostalEtablissement as code_postal,
  f.adresseEtablissement_libelleCommuneEtablissement as commune,
  f.adresseEtablissement_codeCommuneEtablissement as code_commune,
  
  -- Coordonnées géographiques
  f.longitude,
  f.latitude,
  f.adresseEtablissement_coordonneeLambertAbscisseEtablissement as lambert_x,
  f.adresseEtablissement_coordonneeLambertOrdonneeEtablissement as lambert_y,
  
  -- Pays étranger (si applicable)
  f.adresseEtablissement_libellePaysEtrangerEtablissement as pays_etranger,
  
  -- Département enrichi
  f.departement as code_departement,
  f.dept_anomaly as departement_anomalie,
  d.dep_name as departement_nom,
  d.prefecture as departement_prefecture,
  d.region,
  d.km2 as departement_superficie_km2,
  d.population_2022 as departement_population,
  
  -- Profil territorial (✨ ENRICHISSEMENT CLÉ)
  d.profil_territorial,
  d.profil_territorial_ordre,
  
  -- =============================================================================
  -- EFFECTIFS ÉTABLISSEMENT (ENRICHI AVEC dim_categories_effectifs)
  -- =============================================================================
  f.trancheEffectifsEtablissement as code_tranche_effectifs_etab,
  f.anneeEffectifsEtablissement as annee_effectifs_etab,
  eff.Employes as effectifs_libelle_etab,
  
  -- Catégorie effectifs (déjà calculée dans la table)
  f.categorie_effectifs_etab,
  
  -- =============================================================================
  -- EFFECTIFS UNITÉ LÉGALE
  -- =============================================================================
  f.uniteLegale_trancheEffectifsUniteLegale as code_tranche_effectifs_ul,
  f.uniteLegale_anneeEffectifsUniteLegale as annee_effectifs_ul,
  f.categorie_effectifs_ul,
  
  -- Nombre d'établissements de l'unité légale
  f.nb_etablissements,
  
  -- =============================================================================
  -- FORME JURIDIQUE (ENRICHIE AVEC dim_categories_juridiques)
  -- =============================================================================
  f.uniteLegale_categorieJuridiqueUniteLegale as code_categorie_juridique_niv1,
  f.uniteLegale_categorieJuridiqueUniteLegaleNiv2 as code_categorie_juridique_niv2,
  jur.Libelle as categorie_juridique_libelle,
  jur.famille_juridique,
  
  -- Catégorie entreprise
  f.uniteLegale_categorieEntreprise as categorie_entreprise,
  
  -- =============================================================================
  -- DATES ET ÉTATS
  -- =============================================================================
  
  -- Établissement
  f.dateCreationEtablissement as date_creation_etablissement,
  f.periode_etatAdministratifEtablissement as etat_administratif_etablissement,
  f.etablissementSiege as est_siege,
  
  -- Unité légale
  f.uniteLegale_dateCreationUniteLegale as date_creation_unite_legale,
  f.uniteLegale_etatAdministratifUniteLegale as etat_administratif_unite_legale,
  
  -- =============================================================================
  -- ANCIENNETÉ (DÉJÀ CALCULÉE DANS LA TABLE)
  -- =============================================================================
  
  -- Ancienneté établissement
  f.anciennete_etab_annees,
  f.tranche_anciennete_etab,
  
  -- Ancienneté unité légale
  f.anciennete_ul_annees,
  f.tranche_anciennete_ul,
  
  -- =============================================================================
  -- SCORING MULTI-CRITÈRES (✨ CALCUL AUTOMATIQUE)
  -- =============================================================================
  
  -- Score territoire (0-40 points)
  CASE d.profil_territorial
    WHEN 'Très urbain' THEN 40
    WHEN 'Urbain' THEN 30
    WHEN 'Péri-urbain' THEN 20
    WHEN 'Rural' THEN 10
    ELSE 0
  END as score_territoire,
  
  -- Score taille établissement (0-30 points)
  CASE f.trancheEffectifsEtablissement
    -- Grandes entreprises (50+)
    WHEN '21' THEN 30  -- 50-99
    WHEN '22' THEN 30  -- 100-199
    WHEN '31' THEN 30  -- 200-249
    WHEN '32' THEN 30  -- 250-499
    WHEN '41' THEN 30  -- 500-999
    WHEN '42' THEN 30  -- 1000-1999
    WHEN '51' THEN 30  -- 2000-4999
    WHEN '52' THEN 30  -- 5000-9999
    WHEN '53' THEN 30  -- >10000
    
    -- Moyennes entreprises (10-49)
    WHEN '11' THEN 20  -- 10-19
    WHEN '12' THEN 20  -- 20-49
    
    -- Petites entreprises (6-9)
    WHEN '03' THEN 10  -- 6-9
    
    -- TPE (1-5)
    WHEN '01' THEN 5   -- 1-2
    WHEN '02' THEN 5   -- 3-5
    
    -- 0 salarié ou inconnu
    WHEN '00' THEN 2   -- 0
    WHEN 'NN' THEN 0   -- Inconnu
    
    ELSE 0
  END as score_taille,
  
  -- Score forme juridique (0-30 points)
  CASE jur.famille_juridique
    WHEN 'Société commerciale' THEN 30
    WHEN 'Entrepreneur individuel' THEN 15
    ELSE 10
  END as score_juridique,
  
  -- SCORE TOTAL (0-100)
  CASE d.profil_territorial
    WHEN 'Très urbain' THEN 40 WHEN 'Urbain' THEN 30
    WHEN 'Péri-urbain' THEN 20 WHEN 'Rural' THEN 10 ELSE 0
  END +
  CASE f.trancheEffectifsEtablissement
    WHEN '21' THEN 30 WHEN '22' THEN 30 WHEN '31' THEN 30 WHEN '32' THEN 30
    WHEN '41' THEN 30 WHEN '42' THEN 30 WHEN '51' THEN 30 WHEN '52' THEN 30 WHEN '53' THEN 30
    WHEN '11' THEN 20 WHEN '12' THEN 20
    WHEN '03' THEN 10
    WHEN '01' THEN 5 WHEN '02' THEN 5
    WHEN '00' THEN 2 WHEN 'NN' THEN 0
    ELSE 0
  END +
  CASE jur.famille_juridique
    WHEN 'Société commerciale' THEN 30
    WHEN 'Entrepreneur individuel' THEN 15
    ELSE 10
  END as score_total,
  
  -- Catégorie de potentiel
  CASE 
    WHEN (
      CASE d.profil_territorial
        WHEN 'Très urbain' THEN 40 WHEN 'Urbain' THEN 30
        WHEN 'Péri-urbain' THEN 20 WHEN 'Rural' THEN 10 ELSE 0
      END +
      CASE f.trancheEffectifsEtablissement
        WHEN '21' THEN 30 WHEN '22' THEN 30 WHEN '31' THEN 30 WHEN '32' THEN 30
        WHEN '41' THEN 30 WHEN '42' THEN 30 WHEN '51' THEN 30 WHEN '52' THEN 30 WHEN '53' THEN 30
        WHEN '11' THEN 20 WHEN '12' THEN 20
        WHEN '03' THEN 10
        WHEN '01' THEN 5 WHEN '02' THEN 5
        WHEN '00' THEN 2 WHEN 'NN' THEN 0
        ELSE 0
      END +
      CASE jur.famille_juridique
        WHEN 'Société commerciale' THEN 30
        WHEN 'Entrepreneur individique' THEN 15
        ELSE 10
      END
    ) >= 80 THEN 'Très fort potentiel'
    WHEN (
      CASE d.profil_territorial
        WHEN 'Très urbain' THEN 40 WHEN 'Urbain' THEN 30
        WHEN 'Péri-urbain' THEN 20 WHEN 'Rural' THEN 10 ELSE 0
      END +
      CASE f.trancheEffectifsEtablissement
        WHEN '21' THEN 30 WHEN '22' THEN 30 WHEN '31' THEN 30 WHEN '32' THEN 30
        WHEN '41' THEN 30 WHEN '42' THEN 30 WHEN '51' THEN 30 WHEN '52' THEN 30 WHEN '53' THEN 30
        WHEN '11' THEN 20 WHEN '12' THEN 20
        WHEN '03' THEN 10
        WHEN '01' THEN 5 WHEN '02' THEN 5
        WHEN '00' THEN 2 WHEN 'NN' THEN 0
        ELSE 0
      END +
      CASE jur.famille_juridique
        WHEN 'Société commerciale' THEN 30
        WHEN 'Entrepreneur individuel' THEN 15
        ELSE 10
      END
    ) >= 60 THEN 'Fort potentiel'
    WHEN (
      CASE d.profil_territorial
        WHEN 'Très urbain' THEN 40 WHEN 'Urbain' THEN 30
        WHEN 'Péri-urbain' THEN 20 WHEN 'Rural' THEN 10 ELSE 0
      END +
      CASE f.trancheEffectifsEtablissement
        WHEN '21' THEN 30 WHEN '22' THEN 30 WHEN '31' THEN 30 WHEN '32' THEN 30
        WHEN '41' THEN 30 WHEN '42' THEN 30 WHEN '51' THEN 30 WHEN '52' THEN 30 WHEN '53' THEN 30
        WHEN '11' THEN 20 WHEN '12' THEN 20
        WHEN '03' THEN 10
        WHEN '01' THEN 5 WHEN '02' THEN 5
        WHEN '00' THEN 2 WHEN 'NN' THEN 0
        ELSE 0
      END +
      CASE jur.famille_juridique
        WHEN 'Société commerciale' THEN 30
        WHEN 'Entrepreneur individuel' THEN 15
        ELSE 10
      END
    ) >= 40 THEN 'Potentiel moyen'
    ELSE 'Potentiel faible'
  END as categorie_potentiel

FROM `projet-sirene-480919.btp_analysis.full_43_22A` f

-- Jointure codes APE
LEFT JOIN `projet-sirene-480919.btp_analysis.dim_codes_ape` ape 
  ON f.periode_activitePrincipaleEtablissement = ape.code_ape

-- Jointure départements (avec profil territorial)
LEFT JOIN `projet-sirene-480919.btp_analysis.dim_departements` d 
  ON f.departement = d.dep

-- Jointure effectifs
LEFT JOIN `projet-sirene-480919.btp_analysis.dim_categories_effectifs` eff 
  ON f.trancheEffectifsEtablissement = eff.tranche_effectifs

-- Jointure catégories juridiques
LEFT JOIN `projet-sirene-480919.btp_analysis.dim_categories_juridiques` jur 
  ON f.uniteLegale_categorieJuridiqueUniteLegaleNiv2 = jur.categorie_juridique_ul_niv2

-- Filtrer uniquement les établissements actifs
WHERE f.periode_etatAdministratifEtablissement = 'A';

-- =============================================================================
-- FIN DE LA VUE
-- =============================================================================
