"""
Test rapide du pipeline complet sur 2 codes APE
"""
import sys
import os

# Ajouter le répertoire scripts au path
sys.path.insert(0, os.path.join(os.path.dirname(__file__)))

from pipeline_full import run_full_pipeline
from upload_bigquery import get_bigquery_client, create_dataset_if_not_exists, upload_all_codes_ape


if __name__ == "__main__":
    print("🧪 Test rapide du pipeline complet (2 codes APE)\n")
    
    # Test sur 2 codes APE
    codes_test = ['43.11Z', '41.10A']
    
    print(f"Codes APE testés : {codes_test}\n")
    
    # ===== PHASE 1 : PIPELINE COMPLET =====
    print("="*60)
    print("PHASE 1 : PIPELINE COMPLET")
    print("="*60 + "\n")
    
    report_pipeline = run_full_pipeline(codes_ape_list=codes_test)
    
    # ===== PHASE 2 : UPLOAD BIGQUERY =====
    print("\n" + "="*60)
    print("PHASE 2 : UPLOAD BIGQUERY")
    print("="*60 + "\n")
    
    client = get_bigquery_client()
    create_dataset_if_not_exists(client, 'btp_analysis')
    report_upload = upload_all_codes_ape(client, 'btp_analysis', codes_ape_list=codes_test)
    
    # ===== RAPPORT FINAL =====
    print("\n" + "="*60)
    print("RAPPORT FINAL TEST RAPIDE")
    print("="*60 + "\n")
    print(f"Pipeline :")
    print(f"  ✅ Succès : {report_pipeline['success_count']}/{len(codes_test)}")
    print(f"  📊 Total SIRET : {report_pipeline['total_siret']:,}")
    print(f"  📊 Total SIREN : {report_pipeline['total_siren']:,}")
    print(f"\nUpload BigQuery :")
    print(f"  ✅ Succès : {report_upload['success_count']}/{len(codes_test)}")
    
    if report_pipeline['success_count'] == len(codes_test) and report_upload['success_count'] == len(codes_test):
        print(f"\n🎉 Test réussi ! Le pipeline complet fonctionne.")
        print(f"   Vous pouvez lancer le pipeline sur les 40 codes APE.")
    else:
        print(f"\n⚠️ Des erreurs sont survenues. Vérifiez les logs ci-dessus.")
