"""
Module d'enrichissement métier des données SIRENE
"""
import pandas as pd
import numpy as np


def calculate_anciennete(df, date_column, output_column):
    """
    Calcule l'ancienneté en années depuis une date de création
    
    Args:
        df (pd.DataFrame): DataFrame
        date_column (str): Nom de la colonne date de création
        output_column (str): Nom de la colonne de sortie
    
    Returns:
        pd.DataFrame: DataFrame enrichi avec ancienneté
    """
    df = df.copy()
    
    if date_column not in df.columns:
        print(f"⚠️ Colonne {date_column} absente, skip calcul ancienneté")
        df[output_column] = None
        return df
    
    # Date de référence : aujourd'hui
    date_ref = pd.Timestamp.today().normalize()
    
    # Calcul ancienneté en années
    df[output_column] = (
        (date_ref - pd.to_datetime(df[date_column], errors='coerce')).dt.days // 365
    )
    
    nb_valid = df[output_column].notna().sum()
    print(f"📅 Ancienneté calculée : {nb_valid} / {len(df)} ({100*nb_valid/len(df):.1f}%)")
    
    return df


def categorize_anciennete(anciennete_annees):
    """
    Catégorise l'ancienneté en tranches
    
    Args:
        anciennete_annees (int): Ancienneté en années
    
    Returns:
        str: Catégorie d'ancienneté
    """
    if anciennete_annees is None or pd.isna(anciennete_annees):
        return None
    elif anciennete_annees <= 5:
        return '0-5 ans'
    elif anciennete_annees <= 10:
        return '6-10 ans'
    elif anciennete_annees <= 20:
        return '11-20 ans'
    else:
        return '20+ ans'


def add_tranche_anciennete(df, anciennete_column, output_column):
    """
    Ajoute la catégorie d'ancienneté
    
    Args:
        df (pd.DataFrame): DataFrame
        anciennete_column (str): Nom de la colonne ancienneté en années
        output_column (str): Nom de la colonne de sortie
    
    Returns:
        pd.DataFrame: DataFrame enrichi
    """
    df = df.copy()
    
    if anciennete_column not in df.columns:
        print(f"⚠️ Colonne {anciennete_column} absente, skip catégorisation ancienneté")
        df[output_column] = None
        return df
    
    df[output_column] = df[anciennete_column].apply(categorize_anciennete)
    
    nb_valid = df[output_column].notna().sum()
    print(f"📊 Tranche ancienneté : {nb_valid} / {len(df)} ({100*nb_valid/len(df):.1f}%)")
    
    return df


def categorize_effectifs(tranche_code):
    """
    Catégorise la tranche d'effectifs en catégories lisibles
    
    Args:
        tranche_code (str): Code tranche effectifs INSEE
    
    Returns:
        str: Catégorie d'effectifs
    """
    if pd.isna(tranche_code) or tranche_code is None:
        return None
    
    tranche = str(tranche_code).strip()
    
    if tranche in ['NN', '00']:
        return 'Non employeur'
    elif tranche in ['01', '02', '03']:
        return 'TPE (1-9)'
    elif tranche in ['11', '12', '21', '22', '31']:
        return 'PME (10-249)'
    elif tranche in ['32', '41', '42', '51', '52', '53']:
        return 'Grande structure (250+)'
    else:
        return None


def add_categorie_effectifs(df, tranche_column, output_column):
    """
    Ajoute la catégorie d'effectifs
    
    Args:
        df (pd.DataFrame): DataFrame
        tranche_column (str): Nom de la colonne tranche effectifs
        output_column (str): Nom de la colonne de sortie
    
    Returns:
        pd.DataFrame: DataFrame enrichi
    """
    df = df.copy()
    
    if tranche_column not in df.columns:
        print(f"⚠️ Colonne {tranche_column} absente, skip catégorisation effectifs")
        df[output_column] = None
        return df
    
    df[output_column] = df[tranche_column].apply(categorize_effectifs)
    
    nb_valid = df[output_column].notna().sum()
    print(f"👥 Catégorie effectifs : {nb_valid} / {len(df)} ({100*nb_valid/len(df):.1f}%)")
    
    return df


def add_division_ape(df, ape_column, output_column):
    """
    Extrait la division APE (2 premiers caractères)
    
    Args:
        df (pd.DataFrame): DataFrame
        ape_column (str): Nom de la colonne code APE
        output_column (str): Nom de la colonne de sortie
    
    Returns:
        pd.DataFrame: DataFrame enrichi
    """
    df = df.copy()
    
    if ape_column not in df.columns:
        print(f"⚠️ Colonne {ape_column} absente, skip extraction division APE")
        df[output_column] = None
        return df
    
    df[output_column] = df[ape_column].str[:2]
    
    nb_valid = df[output_column].notna().sum()
    print(f"🏗️ Division APE : {nb_valid} / {len(df)} ({100*nb_valid/len(df):.1f}%)")
    
    return df


def add_type_activite(df, ape_column, output_column, codes_btp):
    """
    Identifie si l'activité est BTP ou autre
    
    Args:
        df (pd.DataFrame): DataFrame
        ape_column (str): Nom de la colonne code APE
        output_column (str): Nom de la colonne de sortie
        codes_btp (list): Liste des codes APE BTP
    
    Returns:
        pd.DataFrame: DataFrame enrichi
    """
    df = df.copy()
    
    if ape_column not in df.columns:
        print(f"⚠️ Colonne {ape_column} absente, skip type activité")
        df[output_column] = None
        return df
    
    df[output_column] = df[ape_column].apply(
        lambda x: 'BTP' if x in codes_btp else 'Autres'
    )
    
    nb_btp = (df[output_column] == 'BTP').sum()
    print(f"🏗️ Type activité : {nb_btp} BTP, {len(df)-nb_btp} Autres")
    
    return df


def enrich_siret(df_siret, codes_btp):
    """
    Enrichit la table SIRET (établissements)
    
    Args:
        df_siret (pd.DataFrame): DataFrame des établissements
        codes_btp (list): Liste des codes APE BTP
    
    Returns:
        pd.DataFrame: DataFrame enrichi
    """
    print("🏢 Enrichissement table SIRET (établissements)\n")
    
    df = df_siret.copy()
    
    # 1. Ancienneté établissement
    df = calculate_anciennete(
        df, 
        'dateCreationEtablissement', 
        'anciennete_etab_annees'
    )
    
    # 2. Tranche ancienneté établissement
    df = add_tranche_anciennete(
        df, 
        'anciennete_etab_annees', 
        'tranche_anciennete_etab'
    )
    
    # 3. Catégorie effectifs établissement
    df = add_categorie_effectifs(
        df, 
        'trancheEffectifsEtablissement', 
        'categorie_effectifs_etab'
    )
    
    # 4. Division APE établissement
    df = add_division_ape(
        df, 
        'periode.activitePrincipaleEtablissement', 
        'division_ape_etab'
    )
    
    # 5. Type activité (BTP vs Autres)
    df = add_type_activite(
        df, 
        'periode.activitePrincipaleEtablissement', 
        'type_activite_etab',
        codes_btp
    )
    
    print()
    return df


def enrich_siren(df_siren):
    """
    Enrichit la table SIREN (unités légales)
    
    Args:
        df_siren (pd.DataFrame): DataFrame des unités légales
    
    Returns:
        pd.DataFrame: DataFrame enrichi
    """
    print("🏛️ Enrichissement table SIREN (unités légales)\n")
    
    df = df_siren.copy()
    
    # 1. Ancienneté unité légale
    df = calculate_anciennete(
        df, 
        'uniteLegale.dateCreationUniteLegale', 
        'anciennete_ul_annees'
    )
    
    # 2. Tranche ancienneté unité légale
    df = add_tranche_anciennete(
        df, 
        'anciennete_ul_annees', 
        'tranche_anciennete_ul'
    )
    
    # 3. Catégorie effectifs unité légale
    df = add_categorie_effectifs(
        df, 
        'uniteLegale.trancheEffectifsUniteLegale', 
        'categorie_effectifs_ul'
    )
    
    print()
    return df


def get_codes_btp():
    """
    Retourne la liste des codes APE du secteur BTP
    
    Returns:
        list: Liste des codes APE BTP
    """
    return [
        '43.99C', '43.99D', '41.20A', '41.20B', '42.11Z', '42.12Z', '42.13A', '42.13B',
        '42.21Z', '42.22Z', '42.91Z', '42.99Z', '43.11Z', '43.12A', '43.12B', '43.13Z',
        '43.21A', '43.21B', '43.22A', '43.22B', '43.29A', '43.29B', '43.31Z', '43.32A',
        '43.32B', '43.32C', '43.33Z', '43.34Z', '43.39Z', '43.91A', '43.91B', '43.99A',
        '43.99B', '41.10A', '41.10B', '41.10C', '41.10D', '81.30Z', '74.90A'
    ]


# === BLOC DE TEST ===
if __name__ == "__main__":
    """
    Test du module d'enrichissement métier
    Usage: python scripts/data_enrichment.py
    """
    from data_io import load_processed_data, save_split_data
    
    print("📊 Démarrage du test d'enrichissement métier\n")
    
    CODE_APE_TEST = "43.22A"
    codes_btp = get_codes_btp()
    
    # 1. Chargement tables
    print("📂 Chargement des tables...")
    df_siret = load_processed_data(CODE_APE_TEST, table_type="siret")
    df_siren = load_processed_data(CODE_APE_TEST, table_type="siren")
    print()
    
    # 2. Enrichissement SIRET
    df_siret_enriched = enrich_siret(df_siret, codes_btp)
    
    # 3. Enrichissement SIREN
    df_siren_enriched = enrich_siren(df_siren)
    
    # 4. Statistiques
    print("📊 Statistiques d'enrichissement :\n")
    
    print("   🏢 Table SIRET :")
    print(f"      - Ancienneté établissement : {df_siret_enriched['anciennete_etab_annees'].notna().sum()} / {len(df_siret_enriched)}")
    print(f"      - Tranche ancienneté : {df_siret_enriched['tranche_anciennete_etab'].notna().sum()} / {len(df_siret_enriched)}")
    print(f"      - Catégorie effectifs : {df_siret_enriched['categorie_effectifs_etab'].notna().sum()} / {len(df_siret_enriched)}")
    print(f"      - Type activité BTP : {(df_siret_enriched['type_activite_etab'] == 'BTP').sum()} / {len(df_siret_enriched)}")
    
    print(f"\n   🏛️ Table SIREN :")
    print(f"      - Ancienneté unité légale : {df_siren_enriched['anciennete_ul_annees'].notna().sum()} / {len(df_siren_enriched)}")
    print(f"      - Tranche ancienneté : {df_siren_enriched['tranche_anciennete_ul'].notna().sum()} / {len(df_siren_enriched)}")
    print(f"      - Catégorie effectifs : {df_siren_enriched['categorie_effectifs_ul'].notna().sum()} / {len(df_siren_enriched)}")
    
    # 5. Distributions
    print(f"\n📊 Distribution tranche ancienneté établissement :")
    print(df_siret_enriched['tranche_anciennete_etab'].value_counts().sort_index())
    
    print(f"\n👥 Distribution catégorie effectifs établissement :")
    print(df_siret_enriched['categorie_effectifs_etab'].value_counts())
    
    # 6. Échantillon
    print(f"\n📋 Échantillon enrichi :")
    cols_display = ['siret', 'anciennete_etab_annees', 'tranche_anciennete_etab', 
                    'categorie_effectifs_etab', 'type_activite_etab']
    print(df_siret_enriched[cols_display].head(3))
    
    # 7. Reconstruire FULL
    print(f"\n🔗 Reconstruction table FULL (fusion SIRET + SIREN)...")
    df_full = df_siret_enriched.merge(
        df_siren_enriched, 
        on="siren", 
        how="left",
        suffixes=("", "_siren_dup")
    )
    
    # Supprimer les colonnes dupliquées du merge
    cols_to_drop = [c for c in df_full.columns if c.endswith('_siren_dup')]
    df_full = df_full.drop(columns=cols_to_drop)
    
    print(f"   ✅ Table FULL : {len(df_full)} lignes, {len(df_full.columns)} colonnes")
    
    # 8. Sauvegarde
    print(f"\n💾 Sauvegarde des tables enrichies...")
    paths = save_split_data(df_siret_enriched, df_siren_enriched, df_full, CODE_APE_TEST)
    print()
    
    print("🎉 Test d'enrichissement métier réussi !")
