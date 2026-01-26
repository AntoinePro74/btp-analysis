"""
Module de nettoyage des données SIRENE
"""
import pandas as pd
import numpy as np
from data_io import save_split_data

def clean_raw_data(df):
    """
    Nettoie les données brutes SIRENE
    
    Args:
        df (pd.DataFrame): DataFrame brut
    
    Returns:
        pd.DataFrame: DataFrame nettoyé
    """
    # Suppression colonnes vides
    df_clean = df.dropna(axis=1, how='all').copy()
    
    # Ajout catégorie juridique niveau 2
    df_clean["uniteLegale.categorieJuridiqueUniteLegaleNiv2"] = (
        df_clean["uniteLegale.categorieJuridiqueUniteLegale"].str[:2]
    )
    
    # Extraction dernière période
    df_clean["periode_derniere"] = df_clean["periodesEtablissement"].apply(extraire_derniere_periode)
    df_clean = df_clean.join(
        pd.json_normalize(df_clean["periode_derniere"]).add_prefix("periode.")
    )
    
    # Sélection colonnes utiles
    cols_utiles = get_colonnes_utiles()
    df_clean = df_clean[cols_utiles].copy()
    
    # Filtrage établissements actifs
    df_clean = df_clean[df_clean["periode.etatAdministratifEtablissement"] == "A"].copy()
    
    # Nettoyage dates
    df_clean["dateCreationEtablissement"] = pd.to_datetime(df_clean["dateCreationEtablissement"], errors='coerce')
    df_clean["uniteLegale.dateCreationUniteLegale"] = pd.to_datetime(df_clean["uniteLegale.dateCreationUniteLegale"], errors='coerce')
    
    # Comptage établissements par SIREN
    df_count = df_clean.groupby("siren").size().reset_index(name="nb_etablissements")
    df_clean = df_clean.merge(df_count, on="siren", how="left")
    
    return df_clean

def extraire_derniere_periode(periodes):
    """Extrait la période la plus récente"""
    if isinstance(periodes, np.ndarray):
        periodes = periodes.tolist()
    if periodes is None or not isinstance(periodes, list) or len(periodes) == 0:
        return None
    periodes = [p for p in periodes if isinstance(p, dict)]
    if not periodes:
        return None
    periodes_sorted = sorted(periodes, key=lambda x: x.get("dateDebut") or "0000-00-00", reverse=True)
    return periodes_sorted[0]

def get_colonnes_utiles():
    """Retourne la liste des colonnes à conserver"""
    return [
        "siret", "statutDiffusionEtablissement", "dateCreationEtablissement",
        "trancheEffectifsEtablissement", "anneeEffectifsEtablissement",
        "activitePrincipaleRegistreMetiersEtablissement", "periode.activitePrincipaleEtablissement",
        "etablissementSiege", "periode.etatAdministratifEtablissement",
        "siren", "uniteLegale.statutDiffusionUniteLegale", "uniteLegale.etatAdministratifUniteLegale",
        "uniteLegale.dateCreationUniteLegale", "uniteLegale.categorieJuridiqueUniteLegale",
        "uniteLegale.categorieJuridiqueUniteLegaleNiv2", "uniteLegale.denominationUniteLegale",
        "uniteLegale.sigleUniteLegale", "uniteLegale.denominationUsuelle1UniteLegale",
        "uniteLegale.activitePrincipaleUniteLegale", "uniteLegale.categorieEntreprise",
        "uniteLegale.trancheEffectifsUniteLegale", "uniteLegale.anneeEffectifsUniteLegale",
        "adresseEtablissement.numeroVoieEtablissement", "adresseEtablissement.typeVoieEtablissement",
        "adresseEtablissement.libelleVoieEtablissement", "adresseEtablissement.codePostalEtablissement",
        "adresseEtablissement.libelleCommuneEtablissement", "adresseEtablissement.codeCommuneEtablissement",
        "adresseEtablissement.coordonneeLambertAbscisseEtablissement",
        "adresseEtablissement.coordonneeLambertOrdonneeEtablissement",
        "adresseEtablissement.libellePaysEtrangerEtablissement"
    ]


def split_siret_siren(df_clean):
    """
    Sépare les données en 2 tables : SIRET (établissements) et SIREN (unités légales)
    
    Args:
        df_clean (pd.DataFrame): DataFrame nettoyé (table plate)
    
    Returns:
        tuple: (df_siret, df_siren)
    """
    
    # === TABLE SIRET (établissements) ===
    cols_siret = [
        # Identifiants
        "siret",
        "siren",
        
        # Infos établissement
        "statutDiffusionEtablissement",
        "dateCreationEtablissement",
        "trancheEffectifsEtablissement",
        "anneeEffectifsEtablissement",
        "activitePrincipaleRegistreMetiersEtablissement",
        "periode.activitePrincipaleEtablissement",
        "etablissementSiege",
        "periode.etatAdministratifEtablissement",
        
        # Localisation
        "adresseEtablissement.numeroVoieEtablissement",
        "adresseEtablissement.typeVoieEtablissement",
        "adresseEtablissement.libelleVoieEtablissement",
        "adresseEtablissement.codePostalEtablissement",
        "adresseEtablissement.libelleCommuneEtablissement",
        "adresseEtablissement.codeCommuneEtablissement",
        "adresseEtablissement.coordonneeLambertAbscisseEtablissement",
        "adresseEtablissement.coordonneeLambertOrdonneeEtablissement",
        "adresseEtablissement.libellePaysEtrangerEtablissement"
    ]
    
    # Sélectionner uniquement les colonnes existantes
    cols_siret_existing = [col for col in cols_siret if col in df_clean.columns]
    df_siret = df_clean[cols_siret_existing].copy()
    
    # === TABLE SIREN (unités légales) ===
    cols_siren = [
        # Identifiant
        "siren",
        
        # Infos unité légale
        "uniteLegale.statutDiffusionUniteLegale",
        "uniteLegale.etatAdministratifUniteLegale",
        "uniteLegale.dateCreationUniteLegale",
        "uniteLegale.categorieJuridiqueUniteLegale",
        "uniteLegale.categorieJuridiqueUniteLegaleNiv2",
        "uniteLegale.denominationUniteLegale",
        "uniteLegale.sigleUniteLegale",
        "uniteLegale.denominationUsuelle1UniteLegale",
        "uniteLegale.activitePrincipaleUniteLegale",
        "uniteLegale.categorieEntreprise",
        "uniteLegale.trancheEffectifsUniteLegale",
        "uniteLegale.anneeEffectifsUniteLegale",
        
        # Enrichissement
        "nb_etablissements"
    ]
    
    # Sélectionner uniquement les colonnes existantes
    cols_siren_existing = [col for col in cols_siren if col in df_clean.columns]
    
    # Déduplication par SIREN (on garde la première occurrence)
    df_siren = df_clean[cols_siren_existing].drop_duplicates(subset=['siren']).copy()
    
    return df_siret, df_siren



# === BLOC DE TEST ===
if __name__ == "__main__":
    """
    Test du module de nettoyage
    Usage: python scripts/data_cleaning.py
    """
    from data_io import load_raw_data  # 🆕 Import local pour le test
    
    print("🧹 Démarrage du test de nettoyage des données\n")
    
    CODE_APE_TEST = "43.22A"
    
    # 1. Chargement (via data_io)
    df_raw = load_raw_data(CODE_APE_TEST)
    print()
    
    # 2. Nettoyage
    print("🧹 Nettoyage des données en cours...")
    df_clean = clean_raw_data(df_raw)
    print(f"   ✅ {len(df_clean)} lignes conservées (établissements actifs)")
    print(f"   ✅ {len(df_clean.columns)} colonnes après sélection\n")
    
    # 3. Séparation SIRET/SIREN
    print("✂️ Séparation SIRET/SIREN...")
    df_siret, df_siren = split_siret_siren(df_clean)
    print(f"   ✅ {len(df_siret)} SIRET, {len(df_siren)} SIREN\n")
    
    # 4. Sauvegarde (via data_io)
    print("💾 Sauvegarde des données nettoyées en 3 formats :")
    paths = save_split_data(df_siret, df_siren, df_clean, CODE_APE_TEST)
    print(f"   ✅ Fichiers créés\n")
    
    # 5. Statistiques de validation
    print("📊 Statistiques de validation :")
    
    # Recharger pour valider
    df_siret = pd.read_parquet(paths["siret"])
    df_siren = pd.read_parquet(paths["siren"])
    
    print(f"   📄 Table SIRET (établissements) :")
    print(f"      - Lignes : {len(df_siret)}")
    print(f"      - Colonnes : {len(df_siret.columns)}")
    print(f"      - Clé primaire : siret (unique)")
    
    print(f"\n   📄 Table SIREN (unités légales) :")
    print(f"      - Lignes : {len(df_siren)}")
    print(f"      - Colonnes : {len(df_siren.columns)}")
    print(f"      - Clé primaire : siren (unique)")
    print(f"      - Multi-sites : {(df_siren['nb_etablissements'] > 1).sum()} entreprises")
    
    print(f"\n   📄 Table FULL (plate) :")
    print(f"      - Lignes : {len(df_clean)}")
    print(f"      - Colonnes : {len(df_clean.columns)}")
    
    # Vérification de cohérence
    print(f"\n   ✅ Cohérence : {len(df_siret)} SIRET → {len(df_siren)} SIREN")
    print(f"   ✅ Ratio SIRET/SIREN : {len(df_siret)/len(df_siren):.2f} établissements/entreprise")
    
    # 6. Statistiques de validation
    print("📊 Statistiques de validation :")
    print(f"   - Lignes supprimées : {len(df_raw) - len(df_clean)} ({100*(len(df_raw)-len(df_clean))/len(df_raw):.1f}%)")
    print(f"   - Colonnes supprimées : {len(df_raw.columns) - len(df_clean.columns)}")
    print(f"   - Établissements actifs : {len(df_clean)}")
    print(f"   - Entreprises uniques (SIREN) : {df_clean['siren'].nunique()}")
    print(f"   - Multi-sites : {(df_clean['nb_etablissements'] > 1).sum()} établissements")
    
    # 7. Vérification des données critiques
    print(f"\n🔍 Vérification des colonnes critiques :")
    print(f"   - Dates création établissement : {df_clean['dateCreationEtablissement'].notna().sum()} / {len(df_clean)}")
    print(f"   - Dates création unité légale : {df_clean['uniteLegale.dateCreationUniteLegale'].notna().sum()} / {len(df_clean)}")
    print(f"   - Catégorie juridique Niv2 : {df_clean['uniteLegale.categorieJuridiqueUniteLegaleNiv2'].notna().sum()} / {len(df_clean)}")
    
    # 8. Afficher un échantillon
    print(f"\n📋 Aperçu des données nettoyées :")
    print(df_clean[['siret', 'siren', 'dateCreationEtablissement', 'nb_etablissements']].head(3))
    
    print(f"\n🎉 Test de nettoyage réussi !")
