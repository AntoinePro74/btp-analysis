"""
scripts/upload_dim_categories_juridiques_fixed.py
Upload dim_categories_juridiques avec schéma STRING forcé
"""
import pandas as pd
from google.cloud import bigquery

def upload_categories_juridiques_fixed():
    """
    Upload dim_categories_juridiques avec schéma STRING explicite
    """
    print("\n" + "="*60)
    print("📤 UPLOAD dim_categories_juridiques (SCHÉMA FORCÉ)")
    print("="*60 + "\n")
    
    # Initialiser client
    client = bigquery.Client(project='projet-sirene-480919')
    dataset_id = 'btp_analysis'
    table_name = 'dim_categories_juridiques'
    table_id = f"{client.project}.{dataset_id}.{table_name}"
    
    # Charger le CSV
    df = pd.read_csv('data/dimensions/categories_juridiques.csv')
    
    # FORCER le type STRING
    df['categorie_juridique_ul_niv2'] = df['categorie_juridique_ul_niv2'].astype(str)
    df['famille_juridique'] = df['famille_juridique'].astype(str)
    df['Libelle'] = df['Libelle'].astype(str)
    
    print(f"📂 CSV chargé : {len(df)} lignes")
    print(f"   Types Pandas :")
    print(df.dtypes.to_string())
    print(f"\n   Aperçu :")
    print(df.head(5).to_string(index=False))
    
    # Définir le schéma BigQuery EXPLICITEMENT
    schema = [
        bigquery.SchemaField("categorie_juridique_ul_niv2", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("famille_juridique", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("Libelle", "STRING", mode="NULLABLE"),
    ]
    
    # Configuration de la job
    job_config = bigquery.LoadJobConfig(
        schema=schema,  # ✨ SCHÉMA FORCÉ
        write_disposition="WRITE_TRUNCATE",  # Remplacer la table existante
        clustering_fields=["famille_juridique"]
    )
    
    print(f"\n🚀 Upload vers BigQuery : {table_id}")
    print(f"   🔧 Schéma forcé :")
    for field in schema:
        print(f"      - {field.name} : {field.field_type}")
    
    # Upload
    job = client.load_table_from_dataframe(
        df,
        table_id,
        job_config=job_config
    )
    
    job.result()  # Attendre la fin
    
    print(f"\n✅ Upload réussi : {len(df)} lignes")
    
    # Vérifier le schéma dans BigQuery
    table = client.get_table(table_id)
    print(f"\n📋 Schéma BigQuery après upload :")
    for field in table.schema:
        print(f"   - {field.name} : {field.field_type}")
    
    return table

if __name__ == "__main__":
    table = upload_categories_juridiques_fixed()
    
    print("\n" + "="*60)
    print("🎉 TERMINÉ")
    print("="*60)
    print("\n🔍 Vérifie dans BigQuery Console que le type est STRING")
    print("   https://console.cloud.google.com/bigquery?project=projet-sirene-480919")
