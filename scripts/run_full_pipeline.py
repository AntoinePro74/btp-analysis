"""
Pipeline complet : extraction + traitement + upload BigQuery
Pour les 40 codes APE du BTP
"""
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__)))

from pipeline_full import run_full_pipeline
from upload_bigquery import get_bigquery_client, create_dataset_if_not_exists, upload_all_codes_ape
from datetime import datetime


if __name__ == "__main__":
    print("🚀 PIPELINE COMPLET BTP (40 codes APE)\n")
    print(f"Démarrage : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    
    # ===== PHASE 1 : PIPELINE COMPLET =====
    print("="*60)
    print("PHASE 1 : EXTRACTION + TRAITEMENT")
    print("="*60 + "\n")
    
    report_pipeline = run_full_pipeline()  # Utilise tous les codes BTP
    
    # ===== PHASE 2 : UPLOAD BIGQUERY =====
    print("\n" + "="*60)
    print("PHASE 2 : UPLOAD BIGQUERY")
    print("="*60 + "\n")
    
    client = get_bigquery_client()
    create_dataset_if_not_exists(client, 'btp_analysis')
    report_upload = upload_all_codes_ape(client, 'btp_analysis')
    
    # ===== RAPPORT FINAL =====
    print("\n" + "="*60)
    print("RAPPORT FINAL PIPELINE COMPLET")
    print("="*60 + "\n")
    print(f"Fin : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"\nPipeline :")
    print(f"  ✅ Succès : {report_pipeline['success_count']}/40")
    print(f"  ❌ Erreurs : {report_pipeline['error_count']}/40")
    print(f"  📊 Total SIRET : {report_pipeline['total_siret']:,}")
    print(f"  📊 Total SIREN : {report_pipeline['total_siren']:,}")
    print(f"  ⏱️ Durée : {report_pipeline['duration_sec']/3600:.1f}h")
    print(f"\nUpload BigQuery :")
    print(f"  ✅ Succès : {report_upload['success_count']}/40")
    print(f"  ❌ Erreurs : {report_upload['error_count']}/40")
