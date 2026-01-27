"""
Module d'upload des données vers BigQuery
"""
import sys
import os
from datetime import datetime
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
import pandas as pd


def get_bigquery_client(project_id=None):
    """
    Initialise le client BigQuery
    
    Args:
        project_id (str): ID du projet GCP (optionnel, détecté auto si ADC configuré)
    
    Returns:
        bigquery.Client: Client BigQuery
    """
    if project_id:
        client = bigquery.Client(project=project_id)
    else:
        # Utilise Application Default Credentials (gcloud auth)
        client = bigquery.Client()
    
    print(f"✅ Client BigQuery initialisé : projet '{client.project}'")
    return client


def create_dataset_if_not_exists(client, dataset_id, location="EU"):
    """
    Crée le dataset BigQuery s'il n'existe pas
    
    Args:
        client (bigquery.Client): Client BigQuery
        dataset_id (str): ID du dataset
        location (str): Localisation (défaut: EU)
    
    Returns:
        bigquery.Dataset: Dataset créé ou existant
    """
    dataset_ref = f"{client.project}.{dataset_id}"
    
    try:
        dataset = client.get_dataset(dataset_ref)
        print(f"⏭️ Dataset '{dataset_id}' existe déjà")
        return dataset
    except NotFound:
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = location
        dataset = client.create_dataset(dataset, timeout=30)
        print(f"✅ Dataset '{dataset_id}' créé avec succès (location: {location})")
        return dataset


def prepare_dataframe_for_bigquery(df, table_type):
    """
    Prépare un DataFrame pour l'upload BigQuery (conversion types + nettoyage colonnes)
    
    Args:
        df (pd.DataFrame): DataFrame à préparer
        table_type (str): Type de table ('siret', 'siren', 'full')
    
    Returns:
        pd.DataFrame: DataFrame préparé
    """
    df = df.copy()
    
    # ========================================
    # 1. NETTOYER LES NOMS DE COLONNES
    # ========================================
    # BigQuery n'accepte pas les points (.) dans les noms de colonnes
    # Remplacer . par _
    df.columns = df.columns.str.replace('.', '_', regex=False)
    
    # Supprimer caractères invalides (garder lettres, chiffres, _)
    df.columns = df.columns.str.replace(r'[^a-zA-Z0-9_]', '_', regex=True)
    
    # ========================================
    # 2. CONVERSIONS DE TYPES
    # ========================================
    
    # Conversions dates
    date_columns = [col for col in df.columns if 'date' in col.lower() and 'datetime' not in col.lower() and 'traitement' not in col.lower()]
    for col in date_columns:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce').dt.date
    
    # Conversions timestamps
    timestamp_columns = [col for col in df.columns if 'traitement' in col.lower()]
    for col in timestamp_columns:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')
    
    # Convertir boolean
    bool_columns = [col for col in df.columns if df[col].dtype == 'bool']
    for col in bool_columns:
        df[col] = df[col].astype('boolean')  # pandas nullable boolean
    
    # Convertir integers (nullable)
    int_columns = ['anneeEffectifsEtablissement', 'nombrePeriodesEtablissement', 
                   'anciennete_etab_annees', 'anneeEffectifsUniteLegale', 'anciennete_ul_annees']
    for col in int_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')
    
    # Convertir floats
    float_columns = ['longitude', 'latitude']
    for col in float_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    print(f"📊 DataFrame préparé : {len(df)} lignes, {len(df.columns)} colonnes")
    return df


def upload_table_to_bigquery(client, df, dataset_id, table_id, 
                             write_disposition="WRITE_TRUNCATE",
                             clustering_fields=None,
                             partition_field=None):
    """
    Upload un DataFrame vers BigQuery (schéma auto-détecté)
    
    Args:
        client (bigquery.Client): Client BigQuery
        df (pd.DataFrame): DataFrame à uploader
        dataset_id (str): ID du dataset
        table_id (str): ID de la table
        write_disposition (str): Mode d'écriture (WRITE_TRUNCATE ou WRITE_APPEND)
        clustering_fields (list): Colonnes pour clustering
        partition_field (str): Colonne pour partitioning
    
    Returns:
        str: Référence de la table créée
    """
    table_ref = f"{client.project}.{dataset_id}.{table_id}"
    
    print(f"\n📤 Upload vers BigQuery : {table_ref}")
    print(f"   - Lignes : {len(df)}")
    print(f"   - Colonnes : {len(df.columns)}")
    print(f"   - Mode : {write_disposition}")
    
    # Configuration du job (SANS schéma explicite = auto-détection)
    job_config = bigquery.LoadJobConfig(
        write_disposition=write_disposition,
        autodetect=True,  # Auto-détection du schéma
    )
    
    # Clustering (optionnel) - vérifier que les colonnes existent
    if clustering_fields:
        valid_clustering = [col for col in clustering_fields if col in df.columns]
        if valid_clustering:
            job_config.clustering_fields = valid_clustering
            print(f"   - Clustering : {valid_clustering}")
        else:
            print(f"   ⚠️ Clustering ignoré : colonnes {clustering_fields} non trouvées")
    
    # Partitioning (optionnel)
    if partition_field and partition_field in df.columns:
        job_config.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field=partition_field,
        )
        print(f"   - Partitioning : {partition_field}")
    
    # Upload
    job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result()  # Attendre la fin
    
    # Vérification
    table = client.get_table(table_ref)
    print(f"✅ Upload terminé : {table.num_rows} lignes dans {table_ref}")
    
    return table_ref


def upload_code_ape_to_bigquery(code_ape, client, dataset_id, 
                                write_disposition="WRITE_TRUNCATE",
                                data_dir="data/final"):
    """
    Upload les 3 tables d'un code APE vers BigQuery
    
    Args:
        code_ape (str): Code APE
        client (bigquery.Client): Client BigQuery
        dataset_id (str): ID du dataset BigQuery
        write_disposition (str): Mode d'écriture
        data_dir (str): Répertoire des données finales
    
    Returns:
        dict: Résumé de l'upload
    """
    print(f"\n{'='*60}")
    print(f"📤 Upload {code_ape} vers BigQuery")
    print(f"{'='*60}\n")
    
    result = {
        "code_ape": code_ape,
        "success": False,
        "tables_uploaded": [],
        "error": None
    }
    
    try:
        # 1. Charger les 3 tables
        df_siret = pd.read_parquet(f"{data_dir}/siret_{code_ape}.parquet")
        df_siren = pd.read_parquet(f"{data_dir}/siren_{code_ape}.parquet")
        df_full = pd.read_parquet(f"{data_dir}/full_{code_ape}.parquet")
        
        print(f"📂 Tables chargées :")
        print(f"   - SIRET : {len(df_siret)} lignes")
        print(f"   - SIREN : {len(df_siren)} lignes")
        print(f"   - FULL  : {len(df_full)} lignes")
        
        # 2. Préparer les DataFrames
        df_siret = prepare_dataframe_for_bigquery(df_siret, "siret")
        df_siren = prepare_dataframe_for_bigquery(df_siren, "siren")
        df_full = prepare_dataframe_for_bigquery(df_full, "full")
        
        # 3. Upload SIRET (avec clustering si colonnes existent)
        table_siret = upload_table_to_bigquery(
            client, 
            df_siret, 
            dataset_id, 
            f"siret_{code_ape.replace('.', '_')}", 
            write_disposition=write_disposition,
            clustering_fields=["departement", "type_activite_etab"]
        )
        result["tables_uploaded"].append(table_siret)
        
        # 4. Upload SIREN
        table_siren = upload_table_to_bigquery(
            client, 
            df_siren, 
            dataset_id, 
            f"siren_{code_ape.replace('.', '_')}", 
            write_disposition=write_disposition
        )
        result["tables_uploaded"].append(table_siren)
        
        # 5. Upload FULL (table principale avec clustering)
        table_full = upload_table_to_bigquery(
            client, 
            df_full, 
            dataset_id, 
            f"full_{code_ape.replace('.', '_')}", 
            write_disposition=write_disposition,
            clustering_fields=["departement", "type_activite_etab", "categorie_effectifs_etab"]
        )
        result["tables_uploaded"].append(table_full)
        
        result["success"] = True
        print(f"\n🎉 {code_ape} : Upload BigQuery réussi")
        
    except Exception as e:
        result["error"] = str(e)
        print(f"\n❌ {code_ape} : Erreur upload - {str(e)}")
    
    return result


def upload_all_codes_ape(client, dataset_id, codes_ape_list=None, 
                        write_disposition="WRITE_TRUNCATE",
                        data_dir="data/final"):
    """
    Upload tous les codes APE vers BigQuery
    
    Args:
        client (bigquery.Client): Client BigQuery
        dataset_id (str): ID du dataset BigQuery
        codes_ape_list (list): Liste des codes APE (défaut: tous)
        write_disposition (str): Mode d'écriture
        data_dir (str): Répertoire des données
    
    Returns:
        dict: Rapport d'upload
    """
    from data_enrichment import get_codes_btp
    
    if codes_ape_list is None:
        codes_ape_list = get_codes_btp()
    
    print(f"\n{'#'*60}")
    print(f"# UPLOAD BIGQUERY - {len(codes_ape_list)} CODES APE")
    print(f"{'#'*60}\n")
    print(f"Dataset : {dataset_id}")
    print(f"Mode : {write_disposition}\n")
    
    results = []
    
    for i, code_ape in enumerate(codes_ape_list, 1):
        print(f"\n[{i}/{len(codes_ape_list)}] {code_ape}")
        result = upload_code_ape_to_bigquery(
            code_ape, client, dataset_id, write_disposition, data_dir
        )
        results.append(result)
    
    # Rapport final
    success_count = sum(1 for r in results if r["success"])
    error_count = sum(1 for r in results if not r["success"])
    
    print(f"\n{'#'*60}")
    print(f"# RAPPORT UPLOAD BIGQUERY")
    print(f"{'#'*60}\n")
    print(f"✅ Succès : {success_count}/{len(codes_ape_list)} codes APE")
    print(f"❌ Erreurs : {error_count}/{len(codes_ape_list)} codes APE")
    
    if error_count > 0:
        print(f"\n⚠️ Codes APE en erreur :")
        for r in results:
            if not r["success"]:
                print(f"   - {r['code_ape']} : {r['error']}")
    
    return {
        "results": results,
        "success_count": success_count,
        "error_count": error_count
    }


# === BLOC DE TEST / EXÉCUTION ===
if __name__ == "__main__":
    """
    Usage:
    - Test 1 code APE : python scripts/upload_bigquery.py
    - Tous les codes APE : python scripts/upload_bigquery.py --all
    - Spécifier projet : python scripts/upload_bigquery.py --project my-project-id
    - Dataset custom : python scripts/upload_bigquery.py --dataset my_dataset
    """
    
    # Parse arguments
    run_all = "--all" in sys.argv
    
    # Projet GCP (optionnel)
    project_id = None
    if "--project" in sys.argv:
        idx = sys.argv.index("--project")
        project_id = sys.argv[idx + 1] if idx + 1 < len(sys.argv) else None
    
    # Dataset BigQuery
    dataset_id = "btp_analysis"
    if "--dataset" in sys.argv:
        idx = sys.argv.index("--dataset")
        dataset_id = sys.argv[idx + 1] if idx + 1 < len(sys.argv) else "btp_analysis"
    
    # Initialiser client
    print("🚀 Initialisation BigQuery\n")
    client = get_bigquery_client(project_id)
    
    # Créer dataset
    create_dataset_if_not_exists(client, dataset_id, location="EU")
    
    if run_all:
        # Upload tous les codes APE
        print("\n🚀 Mode : Upload tous les codes APE\n")
        report = upload_all_codes_ape(client, dataset_id)
    else:
        # Test sur 1 code APE
        print("\n🚀 Mode : Test sur 1 code APE (43.22A)\n")
        result = upload_code_ape_to_bigquery("43.22A", client, dataset_id)
        
        if result["success"]:
            print(f"\n🎉 Upload réussi !")
            print(f"   Tables créées : {len(result['tables_uploaded'])}")
            for table_ref in result["tables_uploaded"]:
                print(f"   - {table_ref}")
        else:
            print(f"\n❌ Upload échoué : {result['error']}")
