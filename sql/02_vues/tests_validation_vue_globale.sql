-- ============================================================================
-- Fichier : sql/02_vues/tests_validation_vue_globale.sql
-- Description : Tests de validation adaptés à la structure btp_analysis
-- Version : 1.1
-- Date : 2026-02-07
-- ============================================================================


-- ============================================================================
-- TEST 2.1 : Volume total et couverture codes APE
-- ============================================================================
SELECT 
  'TEST 2.1 - Volume et codes APE' AS test_id,
  COUNT(*) AS nb_total_etablissements,
  COUNT(DISTINCT code_ape) AS nb_codes_ape,
  COUNT(DISTINCT siren) AS nb_entreprises_uniques,
  COUNT(DISTINCT code_departement) AS nb_departements_couverts,
  COUNT(DISTINCT CASE WHEN etat_administratif = 'A' THEN siret END) AS nb_actifs
FROM `btp_analysis.v_etablissements_btp_global`;

-- Résultat obtenu :
Ligne	test_id	nb_total_etablissements	nb_codes_ape	nb_entreprises_uniques	nb_departements_couverts	nb_actifs
1	TEST 2.1 - Volume et codes APE	1038410	23	984082	107	1038410

-- commentaire : RAS tout est ok
--✅ Validation TEST 2.1 : SUCCÈS !
--Métrique	Résultat	Validation
--Volume	1 038 410 établissements	✅ Excellent
--Codes APE	23 codes	✅ Cohérent avec le script
--Entreprises	984 082 SIREN	✅ ~95% mono-établissement
--Départements	107	✅ France entière
--Actifs	100%	✅ Filtre OK

-- TEST 2.2 : Répartition par score APE
SELECT 
  'TEST 2.2 - Répartition scores APE' AS test_id,
  score_potentiel_ape,
  COUNT(*) AS nb_etablissements,
  ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS pourcentage,
  ROUND(AVG(score_total), 1) AS score_total_moyen
FROM `btp_analysis.v_etablissements_btp_global`
GROUP BY score_potentiel_ape
ORDER BY score_potentiel_ape DESC;

-- Résultat obtenu :
Ligne	test_id	score_potentiel_ape	nb_etablissements	pourcentage	score_total_moyen
1	TEST 2.2 - Répartition scores APE	25	138424	13.33	62.9
2	TEST 2.2 - Répartition scores APE	20	133789	12.88	59.0
3	TEST 2.2 - Répartition scores APE	10	766197	73.79	46.8

-- Commentaire : RAS tout est ok

-- TEST 2.3 : Top 20 cibles prioritaires
SELECT 
  'TEST 2.3 - Top 20 cibles' AS test_id,
  siret,
  nom_entreprise,
  code_ape,
  libelle_ape,
  code_departement,
  nb_etablissements,
  type_structure,
  code_effectifs,
  Employes,
  famille_juridique,
  
  score_profil_territorial,
  score_taille_entreprise,
  score_forme_juridique,
  score_potentiel_ape,
  bonus_multi_agences,
  score_total,
  
  categorie_potentiel,
  est_cible_prioritaire
FROM `btp_analysis.v_etablissements_btp_global`
WHERE est_cible_prioritaire = TRUE
ORDER BY score_total DESC, nb_etablissements DESC
LIMIT 20;

-- Résultat obtenu :
Ligne	test_id	siret	nom_entreprise	code_ape	libelle_ape	code_departement	nb_etablissements	type_structure	code_effectifs	Employes	famille_juridique	score_profil_territorial	score_taille_entreprise	score_forme_juridique	score_potentiel_ape	bonus_multi_agences	score_total	categorie_potentiel	est_cible_prioritaire
1	TEST 2.3 - Top 20 cibles	85480074500994	AXIMA CONCEPT	43.22B	Travaux d'installation d'équipements thermiques et de climatisation	94	79	Multi-agences (5+)	32	250-499	Société commerciale	25	38	20	25	20	128	Très fort potentiel	true
2	TEST 2.3 - Top 20 cibles	85480074500838	AXIMA CONCEPT	43.22B	Travaux d'installation d'équipements thermiques et de climatisation	92	79	Multi-agences (5+)	32	250-499	Société commerciale	25	38	20	25	20	128	Très fort potentiel	true
3	TEST 2.3 - Top 20 cibles	43305622300152	CIEC	43.22B	Travaux d'installation d'équipements thermiques et de climatisation	75	7	Multi-agences (5+)	32	250-499	Société commerciale	25	38	20	25	20	128	Très fort potentiel	true
4	TEST 2.3 - Top 20 cibles	30134058406761	ENGIE HOME SERVICES	43.22B	Travaux d'installation d'équipements thermiques et de climatisation	92	212	Multi-agences (5+)	22	100-199	Société commerciale	25	35	20	25	20	125	Très fort potentiel	true
5	TEST 2.3 - Top 20 cibles	33487372602490	PROXISERVE	43.22B	Travaux d'installation d'équipements thermiques et de climatisation	93	104	Multi-agences (5+)	22	100-199	Société commerciale	25	35	20	25	20	125	Très fort potentiel	true
6	TEST 2.3 - Top 20 cibles	85480074501778	AXIMA CONCEPT	43.22B	Travaux d'installation d'équipements thermiques et de climatisation	69	79	Multi-agences (5+)	32	250-499	Société commerciale	20	38	20	25	20	123	Très fort potentiel	true
7	TEST 2.3 - Top 20 cibles	81295097000143	NC 2008 ENVIRONNEMENT	43.29A	Travaux d'isolation	69	23	Multi-agences (5+)	31	200-249	Société commerciale	20	38	20	25	20	123	Très fort potentiel	true
8	TEST 2.3 - Top 20 cibles	51813786400105	EIFFAGE ENERGIE SYSTEMES - CLEVIA CENTRE EST	43.22B	Travaux d'installation d'équipements thermiques et de climatisation	69	13	Multi-agences (5+)	32	250-499	Société commerciale	20	38	20	25	20	123	Très fort potentiel	true
9	TEST 2.3 - Top 20 cibles	47973926000012	CIE EUROPEENNE DE SERVICE CIAL	43.22B	Travaux d'installation d'équipements thermiques et de climatisation	69	11	Multi-agences (5+)	31	200-249	Société commerciale	20	38	20	25	20	123	Très fort potentiel	true
10	TEST 2.3 - Top 20 cibles	33487372601815	PROXISERVE	43.22B	Travaux d'installation d'équipements thermiques et de climatisation	94	104	Multi-agences (5+)	21	50-99	Société commerciale	25	30	20	25	20	120	Très fort potentiel	true
11	TEST 2.3 - Top 20 cibles	33487372602441	PROXISERVE	43.22B	Travaux d'installation d'équipements thermiques et de climatisation	93	104	Multi-agences (5+)	21	50-99	Société commerciale	25	30	20	25	20	120	Très fort potentiel	true
12	TEST 2.3 - Top 20 cibles	33487372601930	PROXISERVE	43.22B	Travaux d'installation d'équipements thermiques et de climatisation	93	104	Multi-agences (5+)	21	50-99	Société commerciale	25	30	20	25	20	120	Très fort potentiel	true
13	TEST 2.3 - Top 20 cibles	33487372602318	PROXISERVE	43.22B	Travaux d'installation d'équipements thermiques et de climatisation	92	104	Multi-agences (5+)	21	50-99	Société commerciale	25	30	20	25	20	120	Très fort potentiel	true
14	TEST 2.3 - Top 20 cibles	62722004900951	HERVE THERMIQUE	43.22B	Travaux d'installation d'équipements thermiques et de climatisation	69	90	Multi-agences (5+)	22	100-199	Société commerciale	20	35	20	25	20	120	Très fort potentiel	true
15	TEST 2.3 - Top 20 cibles	62722004901074	HERVE THERMIQUE	43.22B	Travaux d'installation d'équipements thermiques et de climatisation	95	90	Multi-agences (5+)	22	100-199	Société commerciale	20	35	20	25	20	120	Très fort potentiel	true
16	TEST 2.3 - Top 20 cibles	85480074500440	AXIMA CONCEPT	43.22B	Travaux d'installation d'équipements thermiques et de climatisation	67	79	Multi-agences (5+)	22	100-199	Société commerciale	20	35	20	25	20	120	Très fort potentiel	true
17	TEST 2.3 - Top 20 cibles	85480074501489	AXIMA CONCEPT	43.22B	Travaux d'installation d'équipements thermiques et de climatisation	13	79	Multi-agences (5+)	22	100-199	Société commerciale	20	35	20	25	20	120	Très fort potentiel	true
18	TEST 2.3 - Top 20 cibles	85480074501737	AXIMA CONCEPT	43.22B	Travaux d'installation d'équipements thermiques et de climatisation	92	79	Multi-agences (5+)	21	50-99	Société commerciale	25	30	20	25	20	120	Très fort potentiel	true
19	TEST 2.3 - Top 20 cibles	85480074501711	AXIMA CONCEPT	43.22B	Travaux d'installation d'équipements thermiques et de climatisation	06	79	Multi-agences (5+)	21	50-99	Société commerciale	25	30	20	25	20	120	Très fort potentiel	true
20	TEST 2.3 - Top 20 cibles	44476855000402	IZI CONFORT	43.22B	Travaux d'installation d'équipements thermiques et de climatisation	91	72	Multi-agences (5+)	22	100-199	Société commerciale	20	35	20	25	20	120	Très fort potentiel	true

-- commentaire : réfléchir au poids des grandes entreprises > 100 salariés ou plus de 10 ou 20 agences car pas sur que ce soit la cible

-- TEST 2.4 : Distribution scoring et catégories
SELECT 
  'TEST 2.4 - Distribution scoring' AS test_id,
  
  MIN(score_total) AS score_min,
  MAX(score_total) AS score_max,
  ROUND(AVG(score_total), 1) AS score_moyen,
  ROUND(STDDEV(score_total), 1) AS score_ecart_type,
  
  SUM(CASE WHEN categorie_potentiel = 'Très fort potentiel' THEN 1 ELSE 0 END) AS nb_tres_fort,
  SUM(CASE WHEN categorie_potentiel = 'Fort potentiel' THEN 1 ELSE 0 END) AS nb_fort,
  SUM(CASE WHEN categorie_potentiel = 'Potentiel moyen' THEN 1 ELSE 0 END) AS nb_moyen,
  SUM(CASE WHEN categorie_potentiel = 'Potentiel faible' THEN 1 ELSE 0 END) AS nb_faible,
  
  ROUND(SUM(CASE WHEN categorie_potentiel = 'Très fort potentiel' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS pct_tres_fort,
  ROUND(SUM(CASE WHEN categorie_potentiel = 'Fort potentiel' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS pct_fort,
  ROUND(SUM(CASE WHEN categorie_potentiel = 'Potentiel moyen' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS pct_moyen,
  ROUND(SUM(CASE WHEN categorie_potentiel = 'Potentiel faible' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS pct_faible,
  
  SUM(CASE WHEN score_total IS NULL THEN 1 ELSE 0 END) AS nb_null,
  SUM(CASE WHEN score_total < 0 OR score_total > 130 THEN 1 ELSE 0 END) AS nb_aberrants,
  
  -- Bonus : Nb cibles prioritaires
  SUM(CASE WHEN est_cible_prioritaire = TRUE THEN 1 ELSE 0 END) AS nb_cibles_prioritaires,
  ROUND(SUM(CASE WHEN est_cible_prioritaire = TRUE THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS pct_cibles_prioritaires
  
FROM `btp_analysis.v_etablissements_btp_global`;

-- Résultat obtenu :
Ligne	test_id	score_min	score_max	score_moyen	score_ecart_type	nb_tres_fort	nb_fort	nb_moyen	nb_faible	pct_tres_fort	pct_fort	pct_moyen	pct_faible	nb_null	nb_aberrants	nb_cibles_prioritaires	pct_cibles_prioritaires
1	TEST 2.4 - Distribution scoring	25	128	50.5	11.2	487	15585	390998	631340	0.05	1.5	37.65	60.8	0	0	394	0.04

-- cible très fort et fort plus bas qu'attendu. A creuser avec 
-- Analyse des 16K établissements Fort/Très Fort
SELECT 
  'Analyse Fort/Très Fort' AS analyse,
  
  -- Total
  COUNT(*) AS total_fort_tres_fort,
  
  -- Par effectif
  SUM(CASE WHEN code_effectifs IN ('22', '21', '31', '32') THEN 1 ELSE 0 END) AS nb_effectifs_6_49_sal,
  ROUND(SUM(CASE WHEN code_effectifs IN ('22', '21', '31', '32') THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS pct_effectifs_ok,
  
  -- Par APE
  SUM(CASE WHEN score_potentiel_ape >= 20 THEN 1 ELSE 0 END) AS nb_ape_haute_moyenne,
  ROUND(SUM(CASE WHEN score_potentiel_ape >= 20 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS pct_ape_ok,
  
  -- Les 2 critères
  SUM(CASE WHEN code_effectifs IN ('22', '21', '31', '32') AND score_potentiel_ape >= 20 THEN 1 ELSE 0 END) AS nb_avec_2_criteres,
  
  -- Distribution effectifs complets
  SUM(CASE WHEN code_effectifs IN ('53', '52', '51') THEN 1 ELSE 0 END) AS nb_200_999_sal,
  SUM(CASE WHEN code_effectifs IN ('42', '41', '32', '31') THEN 1 ELSE 0 END) AS nb_50_199_sal,
  SUM(CASE WHEN code_effectifs = '22' THEN 1 ELSE 0 END) AS nb_10_19_sal,
  SUM(CASE WHEN code_effectifs = '21' THEN 1 ELSE 0 END) AS nb_6_9_sal,
  SUM(CASE WHEN code_effectifs IN ('12', '11') THEN 1 ELSE 0 END) AS nb_1_5_sal,
  SUM(CASE WHEN code_effectifs IN ('03', '02', '01', '00', 'NN') THEN 1 ELSE 0 END) AS nb_0_sal

FROM `btp_analysis.v_etablissements_btp_global`
WHERE score_total >= 78;

-- résultat obtenu :
Ligne	analyse	total_fort_tres_fort	nb_effectifs_6_49_sal	pct_effectifs_ok	nb_ape_haute_moyenne	pct_ape_ok	nb_avec_2_criteres	nb_200_999_sal	nb_50_199_sal	nb_10_19_sal	nb_6_9_sal	nb_1_5_sal	nb_0_sal
1	Analyse Fort/Très Fort	16072	2041	12.7	12813	79.7	604	0	157	521	1390	5021	8983

-- Modification du script créant la vue pour faire 3 flags

--TEST 2.5 : Taux de couverture dimensions
SELECT 
  'TEST 2.5 - Couverture dimensions' AS test_id,
  COUNT(*) AS nb_total,
  
  SUM(CASE WHEN profil_territorial IS NOT NULL THEN 1 ELSE 0 END) AS nb_avec_profil_territorial,
  ROUND(SUM(CASE WHEN profil_territorial IS NOT NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS taux_profil_territorial,
  
  SUM(CASE WHEN famille_juridique IS NOT NULL THEN 1 ELSE 0 END) AS nb_avec_juridique,
  ROUND(SUM(CASE WHEN famille_juridique IS NOT NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS taux_juridique,
  
  SUM(CASE WHEN libelle_ape IS NOT NULL THEN 1 ELSE 0 END) AS nb_avec_ape,
  ROUND(SUM(CASE WHEN libelle_ape IS NOT NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS taux_ape

FROM `btp_analysis.v_etablissements_btp_global`;

-- Résultat obtenu :
Ligne	test_id	nb_total	nb_avec_profil_territorial	taux_profil_territorial	nb_avec_juridique	taux_juridique	nb_avec_ape	taux_ape
1	TEST 2.5 - Couverture dimensions	1038410	1025528	98.76	1038410	100.0	1038410	100.0

-- ok RAS

-- TEST 2.6 : Top 10 départements cibles premium
SELECT 
  'TEST 2.6 - Top 10 depts Premium' AS test_id,
  code_departement,
  COUNT(*) AS nb_cibles_premium,
  ROUND(AVG(score_total), 1) AS score_moyen,
  
  SUM(CASE WHEN score_potentiel_ape = 25 THEN 1 ELSE 0 END) AS nb_ape_haute,
  SUM(CASE WHEN score_potentiel_ape = 20 THEN 1 ELSE 0 END) AS nb_ape_moyenne,
  
  ROUND(SUM(CASE WHEN type_structure != 'Mono-établissement' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS pct_multi_agences
  
FROM `btp_analysis.v_etablissements_btp_global`
WHERE est_cible_premium = TRUE
GROUP BY code_departement
ORDER BY nb_cibles_premium DESC
LIMIT 10;

-- résultat obtenu :
Ligne	test_id	code_departement	nb_cibles_premium	score_moyen	nb_ape_haute	nb_ape_moyenne	pct_multi_agences
1	TEST 2.6 - Top 10 depts Premium	59	34	97.7	25	9	76.5
2	TEST 2.6 - Top 10 depts Premium	92	32	112.7	15	17	71.9
3	TEST 2.6 - Top 10 depts Premium	69	29	109.4	17	12	89.7
4	TEST 2.6 - Top 10 depts Premium	44	28	97.2	20	8	71.4
5	TEST 2.6 - Top 10 depts Premium	93	21	107.3	14	7	47.6
6	TEST 2.6 - Top 10 depts Premium	31	21	100.5	12	9	76.2
7	TEST 2.6 - Top 10 depts Premium	91	19	104.5	10	9	68.4
8	TEST 2.6 - Top 10 depts Premium	33	18	99.2	10	8	55.6
9	TEST 2.6 - Top 10 depts Premium	13	17	107.4	10	7	82.4
10	TEST 2.6 - Top 10 depts Premium	62	17	92.4	11	6	52.9


-- 🎯 Insights stratégiques pour le Go-to-Market
-- Priorisation géographique Sales (Top 5)
-- 🥇 Nord (59) - Lille : 34 cibles, mix APE équilibré, 76% multi-agences
--🥈 Hauts-de-Seine (92) : 32 cibles, score max (112.7), zone très urbaine 
--🥉 Rhône (69) - Lyon : 29 cibles, record multi-agences (90%), score élevé
--4️⃣ Loire-Atlantique (44) - Nantes : 28 cibles, 71% multi-agences
--5️⃣ Seine-Saint-Denis (93) : 21 cibles, score 107, zone dynamique
--→ Ces 5 départements = 164 cibles premium (27% du total) ! 🎯


-- TEST de la version 1.4
-- Test de validation post création vue

-- Volume par segment
SELECT 
  'Volumes v1.4' AS version,
  SUM(CASE WHEN est_cible_premium THEN 1 ELSE 0 END) AS nb_premium,
  SUM(CASE WHEN est_cible_prioritaire THEN 1 ELSE 0 END) AS nb_prioritaire,
  SUM(CASE WHEN est_cible_secondaire THEN 1 ELSE 0 END) AS nb_secondaire,
  SUM(CASE WHEN est_grand_compte THEN 1 ELSE 0 END) AS nb_gc_siret,
  COUNT(DISTINCT CASE WHEN est_grand_compte THEN siren END) AS nb_gc_siren
FROM `btp_analysis.v_etablissements_btp_global`;

-- Vérifier la répartition 20-49 / 50-99 / 100-199
SELECT 
  code_effectifs,
  libelle_effectifs,
  COUNT(*) AS nb_premium,
  ROUND(COUNT(*) * 100.0 / 2975, 1) AS pct,
  ROUND(AVG(score_total), 1) AS score_moyen
FROM `btp_analysis.v_etablissements_btp_global`
WHERE est_cible_premium = TRUE
GROUP BY code_effectifs, libelle_effectifs
ORDER BY code_effectifs;

-- Identifier les départements les plus riches en cibles Premium
SELECT 
  code_departement,
  COUNT(*) AS nb_premium,
  ROUND(AVG(score_total), 1) AS score_moyen,
  STRING_AGG(DISTINCT libelle_ape LIMIT 3) AS top_ape
FROM `btp_analysis.v_etablissements_btp_global`
WHERE est_cible_premium = TRUE
GROUP BY code_departement
ORDER BY nb_premium DESC
LIMIT 20;

-- Vérifier la répartition Haute/Moyenne priorité APE
SELECT 
  CASE 
    WHEN score_potentiel_ape = 25 THEN 'Haute priorité (25 pts)'
    WHEN score_potentiel_ape = 20 THEN 'Moyenne priorité (20 pts)'
  END AS niveau_ape,
  COUNT(*) AS nb_premium,
  ROUND(COUNT(*) * 100.0 / 2975, 1) AS pct,
  STRING_AGG(DISTINCT libelle_ape LIMIT 5) AS codes_ape
FROM `btp_analysis.v_etablissements_btp_global`
WHERE est_cible_premium = TRUE
GROUP BY niveau_ape
ORDER BY niveau_ape DESC;

-- Résultat obtenu :
1.
Ligne code_effectifs libelle_effectifs nb_premium score_moyen
1 12 20-49 2556 102.0
2 21 50-99 344 105.0
3 22 100-199 75 109.0

2.
Ligne code_departement nb_premium score_moyen top_ape
1 59 108 97.1 Travaux de menuiserie métallique et serrurerie,Autres travaux d'installation n.c.a.,Travaux d'installation d'équipements thermiques et de climatisation
2 93 105 109.5 Travaux de menuiserie métallique et serrurerie,Travaux d'isolation,Autres travaux d'installation n.c.a.
3 69 97 108.6 Travaux de menuiserie métallique et serrurerie,Travaux d'isolation,Travaux de plâtrerie
4 77 92 100.8 Travaux d'isolation,Autres travaux d'installation n.c.a.,Travaux de plâtrerie
5 62 86 97.0 Travaux de menuiserie métallique et serrurerie,Travaux d'isolation,Autres travaux d'installation n.c.a.
6 67 79 106.3 Travaux d'isolation,Travaux d'installation d'équipements thermiques et de climatisation,Travaux de plâtrerie
7 13 76 109.7 Travaux de menuiserie métallique et serrurerie,Autres travaux d'installation n.c.a.,Travaux d'installation d'équipements thermiques et de climatisation
8 94 75 110.5 Travaux de menuiserie métallique et serrurerie,Autres travaux d'installation n.c.a.,Travaux d'installation d'équipements thermiques et de climatisation
9 33 72 103.1 Autres travaux d'installation n.c.a.,Travaux d'installation d'équipements thermiques et de climatisation,Travaux de plâtrerie
10 44 71 96.4 Travaux de plâtrerie,Construction de maisons individuelles,Travaux de menuiserie métallique et serrurerie
11 78 64 105.5 Travaux d'isolation,Autres travaux d'installation n.c.a.,Construction de maisons individuelles
12 76 63 101.8 Travaux de menuiserie métallique et serrurerie,Travaux d'isolation,Travaux d'installation d'équipements thermiques et de climatisation
13 95 60 106.7 Travaux d'isolation,Travaux d'installation d'équipements thermiques et de climatisation,Travaux de menuiserie bois et PVC
14 31 59 103.1 Travaux de menuiserie métallique et serrurerie,Autres travaux d'installation n.c.a.,Construction de maisons individuelles
15 29 59 99.2 Travaux de menuiserie métallique et serrurerie,Travaux d'isolation,Travaux de plâtrerie
16 35 59 103.8 Autres travaux d'installation n.c.a.,Travaux de plâtrerie,Construction de maisons individuelles
17 91 57 106.3 Travaux d'isolation,Travaux d'installation d'équipements thermiques et de climatisation,Travaux de plâtrerie
18 49 56 95.5 Travaux de menuiserie métallique et serrurerie,Travaux d'isolation,Travaux d'installation d'équipements thermiques et de climatisation
19 42 54 101.4 Autres travaux d'installation n.c.a.,Travaux d'installation d'équipements thermiques et de climatisation,Travaux de plâtrerie
20 85 52 98.0 Autres travaux d'installation n.c.a.,Travaux d'installation d'équipements thermiques et de climatisation,Travaux de plâtrerie

3.
Ligne niveau_ape nb_premium pct codes_ape
1 Moyenne priorité (20 pts) 1175 39.5 Travaux de menuiserie métallique et serrurerie,Construction de maisons individuelles,Autres travaux d'installation n.c.a.,Travaux de plâtrerie
2 Haute priorité (25 pts) 1800 60.5 Travaux d'isolation,Travaux de menuiserie bois et PVC,Travaux d'installation d'équipements thermiques et de climatisation

-- Tout est ok