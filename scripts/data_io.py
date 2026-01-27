"""
Module de gestion des entrées/sorties de données
"""
import pandas as pd
import os
import json
from datetime import datetime


# ========================================
# FONCTIONS DONNÉES BRUTES (raw)
# ========================================

def load_raw_data(code_ape, data_dir="data/raw"):
    """
    Charge les données brutes depuis un fichier Parquet
    
    Args:
        code_ape (str): Code APE
        data_dir (str): Répertoire des données brutes
    
    Returns:
        pd.DataFrame: DataFrame des données brutes
    """
    filepath = f"{data_dir}/raw_entreprises_{code_ape}.parquet"
    
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"❌ Fichier introuvable : {filepath}")
    
    df = pd.read_parquet(filepath)
    print(f"📂 Chargé : {filepath} ({len(df)} lignes, {len(df.columns)} colonnes)")
    return df


def save_raw_data(entreprises, code_ape, output_dir="data/raw"):
    """
    Sauvegarde les données brutes finales + métadonnées
    
    Args:
        entreprises (list): Liste des établissements (dict)
        code_ape (str): Code APE
        output_dir (str): Répertoire de sortie
    
    Returns:
        str: Chemin du fichier créé
    """
    os.makedirs(output_dir, exist_ok=True)
    df = pd.json_normalize(entreprises)
    filepath = f"{output_dir}/raw_entreprises_{code_ape}.parquet"
    df.to_parquet(filepath, engine="pyarrow", index=False)
    
    # Sauvegarder métadonnées comme "completed"
    save_metadata(code_ape, "completed", len(entreprises), output_dir)
    
    # Supprimer le checkpoint si existe
    checkpoint_path = f"{output_dir}/checkpoint_{code_ape}.parquet"
    if os.path.exists(checkpoint_path):
        os.remove(checkpoint_path)
    
    print(f"💾 {code_ape} : Données sauvegardées - {filepath}")
    return filepath


def save_checkpoint(entreprises, code_ape, output_dir="data/raw"):
    """
    Sauvegarde un checkpoint (extraction partielle)
    
    Args:
        entreprises (list): Liste des établissements (dict)
        code_ape (str): Code APE
        output_dir (str): Répertoire de sortie
    """
    os.makedirs(output_dir, exist_ok=True)
    df = pd.json_normalize(entreprises)
    filepath = f"{output_dir}/checkpoint_{code_ape}.parquet"
    df.to_parquet(filepath, engine="pyarrow", index=False)
    print(f"💾 Checkpoint : {len(entreprises)} établissements sauvegardés")


# ========================================
# FONCTIONS MÉTADONNÉES
# ========================================

def get_metadata_path(code_ape, output_dir="data/raw"):
    """Retourne le chemin du fichier de métadonnées"""
    return f"{output_dir}/.metadata_{code_ape}.json"


def save_metadata(code_ape, status, nb_etablissements, output_dir="data/raw"):
    """
    Sauvegarde les métadonnées d'extraction
    
    Args:
        code_ape (str): Code APE
        status (str): 'completed' ou 'partial'
        nb_etablissements (int): Nombre d'établissements extraits
        output_dir (str): Répertoire de sortie
    """
    metadata = {
        "code_ape": code_ape,
        "extraction_date": datetime.now().isoformat(),
        "status": status,
        "nb_etablissements": nb_etablissements
    }
    
    filepath = get_metadata_path(code_ape, output_dir)
    with open(filepath, 'w') as f:
        json.dump(metadata, f, indent=2)


def load_metadata(code_ape, output_dir="data/raw"):
    """
    Charge les métadonnées d'extraction
    
    Args:
        code_ape (str): Code APE
        output_dir (str): Répertoire de sortie
    
    Returns:
        dict: Métadonnées ou None si absent
    """
    filepath = get_metadata_path(code_ape, output_dir)
    
    if not os.path.exists(filepath):
        return None
    
    try:
        with open(filepath, 'r') as f:
            return json.load(f)
    except:
        return None


# ========================================
# FONCTIONS DONNÉES TRAITÉES (processed/final)
# ========================================

def save_split_data(df_siret, df_siren, df_full, code_ape, output_dir="data/processed"):
    """
    Sauvegarde les données en 3 formats : SIRET, SIREN, et FULL
    
    Args:
        df_siret (pd.DataFrame): DataFrame des établissements (grain SIRET)
        df_siren (pd.DataFrame): DataFrame des unités légales (grain SIREN)
        df_full (pd.DataFrame): DataFrame complet (table plate)
        code_ape (str): Code APE
        output_dir (str): Répertoire de sortie
    
    Returns:
        dict: Chemins des fichiers créés
    """
    os.makedirs(output_dir, exist_ok=True)
    
    # 1. Sauvegarde SIRET
    path_siret = f"{output_dir}/siret_{code_ape}.parquet"
    df_siret.to_parquet(path_siret, engine="pyarrow", index=False)
    print(f"   📄 SIRET : {len(df_siret)} lignes → {path_siret}")
    
    # 2. Sauvegarde SIREN
    path_siren = f"{output_dir}/siren_{code_ape}.parquet"
    df_siren.to_parquet(path_siren, engine="pyarrow", index=False)
    print(f"   📄 SIREN : {len(df_siren)} lignes → {path_siren}")
    
    # 3. Sauvegarde FULL
    path_full = f"{output_dir}/full_{code_ape}.parquet"
    df_full.to_parquet(path_full, engine="pyarrow", index=False)
    print(f"   📄 FULL  : {len(df_full)} lignes → {path_full}")
    
    return {
        "siret": path_siret,
        "siren": path_siren,
        "full": path_full
    }


def load_processed_data(code_ape, table_type="full", data_dir="data/processed"):
    """
    Charge les données traitées depuis un fichier Parquet
    
    Args:
        code_ape (str): Code APE
        table_type (str): Type de table ('siret', 'siren', ou 'full')
        data_dir (str): Répertoire des données traitées
    
    Returns:
        pd.DataFrame: DataFrame des données traitées
    """
    if table_type not in ["siret", "siren", "full"]:
        raise ValueError(f"❌ table_type doit être 'siret', 'siren', ou 'full', pas '{table_type}'")
    
    filepath = f"{data_dir}/{table_type}_{code_ape}.parquet"
    
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"❌ Fichier introuvable : {filepath}")
    
    df = pd.read_parquet(filepath)
    print(f"📂 Chargé : {filepath} ({len(df)} lignes, {len(df.columns)} colonnes)")
    return df


# === BLOC DE TEST ===
if __name__ == "__main__":
    """
    Test du module I/O
    Usage: python scripts/data_io.py
    """
    print("🧪 Test du module data_io\n")
    
    CODE_APE_TEST = "43.22A"
    
    # Test 1: Chargement données brutes
    print("📂 Test chargement données brutes...")
    df_raw = load_raw_data(CODE_APE_TEST)
    print(f"   ✅ {len(df_raw)} lignes chargées\n")
    
    # Test 2: Métadonnées
    print("📄 Test métadonnées...")
    metadata = load_metadata(CODE_APE_TEST)
    if metadata:
        print(f"   ✅ Métadonnées chargées : {metadata['status']}, {metadata['nb_etablissements']} étab., {metadata['extraction_date']}\n")
    else:
        print(f"   ⚠️ Pas de métadonnées trouvées\n")
    
    # Test 3: Chargement données traitées
    print("📂 Test chargement données traitées...")
    try:
        df_siret = load_processed_data(CODE_APE_TEST, "siret")
        df_siren = load_processed_data(CODE_APE_TEST, "siren")
        df_full = load_processed_data(CODE_APE_TEST, "full")
        print(f"   ✅ 3 tables chargées avec succès\n")
    except FileNotFoundError as e:
        print(f"   ⚠️ {e}\n")
    
    print("🎉 Tests I/O réussis !")
