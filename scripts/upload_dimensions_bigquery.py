"""
Upload des tables de dimensions depuis CSV vers BigQuery
"""
import pandas as pd
from google.cloud import bigquery
import os

# Import direct (même dossier scripts/)
from upload_bigquery import get_bigquery_client


def upload_dimension_from_csv(
    client, 
    dataset_id, 
    table_name, 
    csv_path, 
    clustering_fields=None
):
    """
    Upload une dimension depuis un CSV vers BigQuery
    
    Args:
        client: Client BigQuery
        dataset_id: ID du dataset (ex: 'btp_analysis')
        table_name: Nom de la table (ex: 'dim_departements')
        csv_path: Chemin vers le CSV
        clustering_fields: Liste des champs de clustering (optionnel)
    
    Returns:
        bool: True si succès, False sinon
    """
    print(f"\n{'='*60}")
    print(f"📤 Upload {table_name}")
    print(f"{'='*60}\n")
    
    # Vérifier que le fichier existe
    if not os.path.exists(csv_path):
        print(f"❌ Fichier introuvable : {csv_path}")
        return False
    
    try:
        # Charger le CSV
        print(f"📂 Lecture du fichier : {csv_path}")
        df = pd.read_csv(csv_path)
        print(f"   📊 {len(df)} lignes, {len(df.columns)} colonnes")
        print(f"   📋 Colonnes : {', '.join(df.columns.tolist())}")
        
        # Configuration upload
        table_id = f"{client.project}.{dataset_id}.{table_name}"
        
        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE",  # Écrase la table si existe
            autodetect=True,  # Détection automatique du schéma
        )
        
        # Ajouter clustering si spécifié
        if clustering_fields:
            job_config.clustering_fields = clustering_fields
            print(f"   🔗 Clustering : {clustering_fields}")
        
        # Upload vers BigQuery
        print(f"\n🚀 Upload vers BigQuery : {table_id}")
        job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
        job.result()  # Attendre la fin
        
        # Vérification
        table = client.get_table(table_id)
        print(f"✅ Upload réussi : {table.num_rows:,} lignes dans {table_id}\n")
        
        return True
        
    except Exception as e:
        print(f"❌ Erreur lors de l'upload : {e}\n")
        return False


def upload_all_dimensions():
    """
    Upload toutes les dimensions depuis les CSV
    """
    print("\n" + "="*60)
    print("📦 UPLOAD DIMENSIONS VERS BIGQUERY")
    print("="*60)
    
    # Initialiser client BigQuery
    try:
        client = get_bigquery_client()
        print(f"\n✅ Client BigQuery initialisé : {client.project}")
    except Exception as e:
        print(f"\n❌ Erreur initialisation BigQuery : {e}")
        print("\n⚠️ Vérifiez que :")
        print("   1. GOOGLE_APPLICATION_CREDENTIALS est défini")
        print("   2. Le fichier credentials.json existe")
        print("   3. Vous avez les permissions BigQuery")
        return
    
    dataset_id = 'btp_analysis'
    
    # Définir les dimensions à uploader
    dimensions = [
        {
            'table_name': 'dim_departements',
            'csv_path': 'data/dimensions/departements.csv',
            'clustering': ['region']
        },
        {
            'table_name': 'dim_codes_ape',
            'csv_path': 'data/dimensions/codes_ape.csv',
            'clustering': ['division_ape']
        },
        {
            'table_name': 'dim_categories_effectifs',
            'csv_path': 'data/dimensions/categories_effectifs.csv',
            'clustering': None
        },
        {
            'table_name': 'dim_categories_juridiques',
            'csv_path': 'data/dimensions/categories_juridiques.csv',
            'clustering': ['famille_juridique']
        },
        {
            'table_name': 'dim_anciennete',
            'csv_path': 'data/dimensions/anciennete.csv',
            'clustering': None
        },
    ]
    
    # Upload chaque dimension
    success_count = 0
    error_count = 0
    
    for dim in dimensions:
        result = upload_dimension_from_csv(
            client=client,
            dataset_id=dataset_id,
            table_name=dim['table_name'],
            csv_path=dim['csv_path'],
            clustering_fields=dim['clustering']
        )
        if result:
            success_count += 1
        else:
            error_count += 1
    
    # Rapport final
    print("\n" + "="*60)
    print("📊 RAPPORT FINAL")
    print("="*60 + "\n")
    print(f"✅ Succès : {success_count}/{len(dimensions)} dimensions")
    print(f"❌ Erreurs : {error_count}/{len(dimensions)} dimensions")
    
    if success_count == len(dimensions):
        print("\n🎉 Toutes les dimensions ont été uploadées avec succès !")
        print("\n📍 Prochaines étapes :")
        print("   1. Vérifier dans BigQuery Console")
        print("   2. Créer les vues consolidées (v_all_etablissements)")
        print("   3. Connecter Power BI")
    else:
        print("\n⚠️ Certaines dimensions n'ont pas été uploadées.")
        print("   Vérifiez les erreurs ci-dessus.")
    
    # Lister les tables créées
    print("\n" + "="*60)
    print("📋 VÉRIFICATION BIGQUERY")
    print("="*60 + "\n")
    
    try:
        query = f"""
        SELECT 
            table_id,
            row_count,
            ROUND(size_bytes / 1024, 2) as size_kb
        FROM `{client.project}.{dataset_id}.__TABLES__`
        WHERE table_id LIKE 'dim_%'
        ORDER BY table_id
        """
        
        print("Tables de dimensions dans BigQuery :\n")
        df_tables = client.query(query).to_dataframe()
        print(df_tables.to_string(index=False))
        
    except Exception as e:
        print(f"⚠️ Impossible de lister les tables : {e}")


if __name__ == "__main__":
    upload_all_dimensions()
