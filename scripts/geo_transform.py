"""
Module de transformation géographique des données SIRENE
"""
import pandas as pd
import numpy as np
from pyproj import Transformer


def transform_lambert_to_wgs84(df_siret):
    """
    Convertit les coordonnées Lambert 93 (EPSG:2154) en WGS84 (EPSG:4326)
    Compatible BigQuery et Power BI
    
    Args:
        df_siret (pd.DataFrame): DataFrame des établissements avec coordonnées Lambert
    
    Returns:
        pd.DataFrame: DataFrame enrichi avec longitude/latitude
    """
    df = df_siret.copy()
    
    # Colonnes Lambert
    col_x = "adresseEtablissement.coordonneeLambertAbscisseEtablissement"
    col_y = "adresseEtablissement.coordonneeLambertOrdonneeEtablissement"
    
    # Vérifier que les colonnes existent
    if col_x not in df.columns or col_y not in df.columns:
        print(f"⚠️ Colonnes Lambert absentes, skip transformation coordonnées")
        df["longitude"] = None
        df["latitude"] = None
        return df
    
    # Transformer les "ND" en NaN et convertir en float
    for col in [col_x, col_y]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    
    # Créer le transformer (Lambert 93 → WGS84)
    transformer = Transformer.from_crs(2154, 4326, always_xy=True)
    
    # Masque des coordonnées valides
    mask = df[col_x].notna() & df[col_y].notna()
    nb_valid = mask.sum()
    
    if nb_valid == 0:
        print("⚠️ Aucune coordonnée Lambert valide trouvée")
        df["longitude"] = None
        df["latitude"] = None
        return df
    
    print(f"🌍 Transformation de {nb_valid} coordonnées Lambert → WGS84...")
    
    # Extraire les valeurs valides
    x_values = df.loc[mask, col_x].astype(float).values
    y_values = df.loc[mask, col_y].astype(float).values
    
    # Transformer
    lon, lat = transformer.transform(x_values, y_values)
    
    # Assigner les résultats
    df.loc[mask, "longitude"] = lon
    df.loc[mask, "latitude"] = lat
    
    # Pour les lignes sans coordonnées Lambert
    df.loc[~mask, "longitude"] = None
    df.loc[~mask, "latitude"] = None
    
    # Validation : vérifier les valeurs aberrantes
    nb_aberrant = (
        (df["latitude"].abs() > 90) | 
        (df["longitude"].abs() > 180)
    ).sum()
    
    if nb_aberrant > 0:
        print(f"⚠️ {nb_aberrant} coordonnées aberrantes détectées (seront mises à None)")
        mask_aberrant = (df["latitude"].abs() > 90) | (df["longitude"].abs() > 180)
        df.loc[mask_aberrant, ["longitude", "latitude"]] = None
    
    nb_final = df["longitude"].notna().sum()
    taux = 100 * nb_final / len(df)
    
    print(f"✅ {nb_final} coordonnées converties ({taux:.1f}%)")
    
    return df


def extract_departement(code_commune):
    """
    Extrait le département depuis le code commune INSEE
    
    Args:
        code_commune (str): Code commune INSEE (5 caractères)
    
    Returns:
        str: Code département (2 ou 3 caractères)
    """
    if code_commune is None or pd.isna(code_commune):
        return None
    
    code = str(code_commune).strip()
    
    if code == "" or code == "[ND]":
        return None
    
    # Corse : 2A ou 2B
    if code.upper().startswith("2A"):
        return "2A"
    if code.upper().startswith("2B"):
        return "2B"
    
    # DROM-COM : 97x ou 98x (3 premiers caractères)
    if code.startswith("97") or code.startswith("98"):
        return code[:3]
    
    # Métropole standard : 2 premiers caractères
    return code[:2]


def add_departement(df_siret):
    """
    Ajoute la colonne département et un flag d'anomalie
    
    Args:
        df_siret (pd.DataFrame): DataFrame des établissements
    
    Returns:
        pd.DataFrame: DataFrame enrichi avec département
    """
    df = df_siret.copy()
    
    col_commune = "adresseEtablissement.codeCommuneEtablissement"
    
    if col_commune not in df.columns:
        print(f"⚠️ Colonne {col_commune} absente, skip extraction département")
        df["departement"] = None
        df["dept_anomaly"] = True
        return df
    
    print(f"🏛️ Extraction des départements depuis code commune...")
    
    # Extraire département
    df["departement"] = df[col_commune].apply(extract_departement)
    
    # Flag anomalie
    df["dept_anomaly"] = df["departement"].isna()
    
    nb_valid = df["departement"].notna().sum()
    nb_anomaly = df["dept_anomaly"].sum()
    taux = 100 * nb_valid / len(df)
    
    print(f"✅ {nb_valid} départements extraits ({taux:.1f}%)")
    if nb_anomaly > 0:
        print(f"⚠️ {nb_anomaly} anomalies détectées (dept_anomaly=True)")
    
    return df


def enrich_geo_data(df_siret):
    """
    Applique toutes les transformations géographiques
    
    Args:
        df_siret (pd.DataFrame): DataFrame des établissements
    
    Returns:
        pd.DataFrame: DataFrame enrichi avec longitude, latitude, département
    """
    print("🌍 Début enrichissement géographique\n")
    
    # 1. Transformation coordonnées
    df = transform_lambert_to_wgs84(df_siret)
    print()
    
    # 2. Extraction département
    df = add_departement(df)
    print()
    
    print("✅ Enrichissement géographique terminé")
    return df


# === BLOC DE TEST ===
if __name__ == "__main__":
    """
    Test du module de transformation géographique
    Usage: python scripts/geo_transform.py
    """
    from data_io import load_processed_data, save_split_data
    
    print("🌍 Démarrage du test de transformation géographique\n")
    
    CODE_APE_TEST = "43.22A"
    
    # 1. Chargement table SIRET
    print("📂 Chargement de la table SIRET...")
    df_siret = load_processed_data(CODE_APE_TEST, table_type="siret")
    print()
    
    # 2. Enrichissement géographique
    df_siret_geo = enrich_geo_data(df_siret)
    print()
    
    # 3. Statistiques
    print("📊 Statistiques géographiques :")
    print(f"   - Longitude/Latitude : {df_siret_geo['longitude'].notna().sum()} / {len(df_siret_geo)} ({100*df_siret_geo['longitude'].notna().sum()/len(df_siret_geo):.1f}%)")
    print(f"   - Départements : {df_siret_geo['departement'].notna().sum()} / {len(df_siret_geo)} ({100*df_siret_geo['departement'].notna().sum()/len(df_siret_geo):.1f}%)")
    print(f"   - Anomalies département : {df_siret_geo['dept_anomaly'].sum()}")
    
    # 4. Top départements
    print(f"\n📍 Top 10 départements :")
    print(df_siret_geo['departement'].value_counts().head(10))
    
    # 5. Échantillon
    print(f"\n📋 Échantillon avec coordonnées :")
    cols_display = ['siret', 'adresseEtablissement.libelleCommuneEtablissement', 
                    'longitude', 'latitude', 'departement', 'dept_anomaly']
    cols_available = [c for c in cols_display if c in df_siret_geo.columns]
    print(df_siret_geo[df_siret_geo['longitude'].notna()][cols_available].head(3))
    
    # 6. Sauvegarde (mise à jour table SIRET)
    print(f"\n💾 Sauvegarde de la table SIRET enrichie...")
    
    # Charger SIREN et FULL pour reconstruire les 3 tables
    df_siren = load_processed_data(CODE_APE_TEST, table_type="siren")
    
    # Reconstruire FULL (fusion SIRET enrichi + SIREN)
    df_full = df_siret_geo.merge(df_siren, on="siren", how="left", suffixes=("", "_ul"))
    
    # Sauvegarder
    paths = save_split_data(df_siret_geo, df_siren, df_full, CODE_APE_TEST)
    print()
    
    print("🎉 Test de transformation géographique réussi !")
