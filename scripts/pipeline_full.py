"""
Pipeline complet d'extraction et traitement des données SIRENE BTP
Orchestration de bout en bout pour tous les codes APE
"""
import time
import sys
import os
from datetime import datetime
import pandas as pd

# Import des modules
from api_sirene import extract_multiple_ape, get_all_entreprises_btp
from data_io import load_raw_data, save_split_data, save_raw_data
from data_cleaning import clean_raw_data, split_siret_siren
from geo_transform import enrich_geo_data
from data_enrichment import enrich_siret, enrich_siren, get_codes_btp


def process_single_ape(code_ape, codes_btp, skip_extraction=False):
    """
    Traite un code APE complet : extraction → nettoyage → geo → enrichissement
    
    Args:
        code_ape (str): Code APE à traiter
        codes_btp (list): Liste des codes APE BTP
        skip_extraction (bool): Skip l'extraction API (utilise fichier existant)
    
    Returns:
        dict: Résumé du traitement
    """
    print(f"\n{'='*60}")
    print(f"🚀 Traitement {code_ape}")
    print(f"{'='*60}\n")
    
    start_time = time.time()
    result = {
        "code_ape": code_ape,
        "success": False,
        "error": None,
        "nb_siret": 0,
        "nb_siren": 0,
        "duration_sec": 0
    }
    
    try:
        # === ÉTAPE 1 : EXTRACTION API ===
        if not skip_extraction:
            print("📡 ÉTAPE 1/5 : Extraction API SIRENE")
            
            entreprises = get_all_entreprises_btp(
                code_ape, 
                sleep_time=0.3, 
                max_age_days=30
            )
            
            if not entreprises:
                result["error"] = "Aucune donnée extraite"
                return result
            
            save_raw_data(entreprises, code_ape)
            print(f"✅ Extraction terminée : {len(entreprises)} établissements\n")
        else:
            print("⏭️ ÉTAPE 1/5 : Extraction API skippée (utilise fichier existant)\n")
        
        # === ÉTAPE 2 : NETTOYAGE + SPLIT ===
        print("🧹 ÉTAPE 2/5 : Nettoyage et split SIRET/SIREN")
        df_raw = load_raw_data(code_ape)
        df_clean = clean_raw_data(df_raw)
        df_siret, df_siren = split_siret_siren(df_clean)
        print(f"✅ Nettoyage terminé : {len(df_siret)} SIRET, {len(df_siren)} SIREN\n")
        
        # === ÉTAPE 3 : TRANSFORMATION GÉOGRAPHIQUE ===
        print("🌍 ÉTAPE 3/5 : Transformation géographique")
        df_siret = enrich_geo_data(df_siret)
        print(f"✅ Transformation géo terminée\n")
        
        # === ÉTAPE 4 : ENRICHISSEMENT MÉTIER ===
        print("📊 ÉTAPE 4/5 : Enrichissement métier")
        df_siret = enrich_siret(df_siret, codes_btp)
        df_siren = enrich_siren(df_siren)
        print(f"✅ Enrichissement terminé\n")
        
        # === ÉTAPE 5 : RECONSTRUCTION FULL + SAUVEGARDE ===
        print("💾 ÉTAPE 5/5 : Reconstruction table FULL et sauvegarde")
        df_full = df_siret.merge(
            df_siren, 
            on="siren", 
            how="left",
            suffixes=("", "_siren_dup")
        )
        
        # Supprimer colonnes dupliquées
        cols_to_drop = [c for c in df_full.columns if c.endswith('_siren_dup')]
        df_full = df_full.drop(columns=cols_to_drop)
        
        # Sauvegarde finale
        paths = save_split_data(df_siret, df_siren, df_full, code_ape, output_dir="data/final")
        
        print(f"✅ Sauvegarde terminée : {len(df_siret)} SIRET, {len(df_siren)} SIREN, {len(df_full)} FULL\n")
        
        # Résultat succès
        elapsed = time.time() - start_time
        result["success"] = True
        result["nb_siret"] = len(df_siret)
        result["nb_siren"] = len(df_siren)
        result["duration_sec"] = elapsed
        
        print(f"🎉 {code_ape} : Traitement réussi en {elapsed/60:.1f} minutes")
        
    except Exception as e:
        elapsed = time.time() - start_time
        result["error"] = str(e)
        result["duration_sec"] = elapsed
        print(f"❌ {code_ape} : Erreur - {str(e)}")
    
    return result


def run_full_pipeline(codes_ape_list=None, force_reextract=False, skip_extraction=False):
    """
    Lance le pipeline complet pour tous les codes APE
    
    Args:
        codes_ape_list (list): Liste des codes APE (défaut: tous les codes BTP)
        force_reextract (bool): Forcer la ré-extraction API
        skip_extraction (bool): Skip complètement l'extraction (utilise fichiers existants)
    
    Returns:
        dict: Rapport complet
    """
    print(f"\n{'#'*60}")
    print(f"# PIPELINE COMPLET BTP ANALYSIS")
    print(f"{'#'*60}\n")
    print(f"Démarrage : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Liste des codes APE
    if codes_ape_list is None:
        codes_ape_list = get_codes_btp()
    
    codes_btp = get_codes_btp()  # Pour l'enrichissement
    
    print(f"Codes APE à traiter : {len(codes_ape_list)}")
    print(f"Force re-extraction : {force_reextract}")
    print(f"Skip extraction : {skip_extraction}\n")
    
    # === PHASE 1 : EXTRACTION API (si nécessaire) ===
    if not skip_extraction:
        print(f"\n{'='*60}")
        print(f"PHASE 1 : EXTRACTION API")
        print(f"{'='*60}\n")
        
        extraction_results = extract_multiple_ape(
            codes_ape_list=codes_ape_list,
            sleep_time=0.3,
            force_reextract=force_reextract,
            max_age_days=30
        )
    else:
        print(f"\n⏭️ PHASE 1 : EXTRACTION API skippée\n")
        extraction_results = {"skipped": codes_ape_list}
    
    # === PHASE 2 : TRAITEMENT COMPLET ===
    print(f"\n{'='*60}")
    print(f"PHASE 2 : TRAITEMENT COMPLET")
    print(f"{'='*60}\n")
    
    results = []
    start_global = time.time()
    
    for i, code_ape in enumerate(codes_ape_list, 1):
        print(f"\n[{i}/{len(codes_ape_list)}] {code_ape}")
        
        result = process_single_ape(code_ape, codes_btp, skip_extraction=True)
        results.append(result)
    
    # === RAPPORT FINAL ===
    elapsed_global = time.time() - start_global
    
    success_count = sum(1 for r in results if r["success"])
    error_count = sum(1 for r in results if not r["success"])
    total_siret = sum(r["nb_siret"] for r in results if r["success"])
    total_siren = sum(r["nb_siren"] for r in results if r["success"])
    
    print(f"\n{'#'*60}")
    print(f"# RAPPORT FINAL")
    print(f"{'#'*60}\n")
    print(f"Fin : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Durée totale : {elapsed_global/60:.1f} minutes ({elapsed_global/3600:.1f}h)")
    print(f"\n📊 Résultats :")
    print(f"   ✅ Succès : {success_count}/{len(codes_ape_list)} codes APE")
    print(f"   ❌ Erreurs : {error_count}/{len(codes_ape_list)} codes APE")
    print(f"\n📈 Données finales :")
    print(f"   🏢 Total SIRET : {total_siret:,} établissements")
    print(f"   🏛️ Total SIREN : {total_siren:,} unités légales")
    
    if error_count > 0:
        print(f"\n⚠️ Codes APE en erreur :")
        for r in results:
            if not r["success"]:
                print(f"   - {r['code_ape']} : {r['error']}")
    
    # Créer répertoire pour rapports
    os.makedirs("data/final", exist_ok=True)
    
    # Sauvegarder rapport
    report_path = f"data/final/pipeline_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
    with open(report_path, 'w', encoding='utf-8') as f:
        f.write(f"RAPPORT PIPELINE BTP ANALYSIS\n")
        f.write(f"Date : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"Durée : {elapsed_global/60:.1f} min\n\n")
        f.write(f"Succès : {success_count}/{len(codes_ape_list)}\n")
        f.write(f"Total SIRET : {total_siret:,}\n")
        f.write(f"Total SIREN : {total_siren:,}\n\n")
        
        for r in results:
            status = "✅" if r["success"] else "❌"
            f.write(f"{status} {r['code_ape']} : {r['nb_siret']} SIRET, {r['nb_siren']} SIREN ({r['duration_sec']:.1f}s)\n")
            if r["error"]:
                f.write(f"   Erreur : {r['error']}\n")
    
    print(f"\n📄 Rapport sauvegardé : {report_path}")
    
    return {
        "results": results,
        "success_count": success_count,
        "error_count": error_count,
        "total_siret": total_siret,
        "total_siren": total_siren,
        "duration_sec": elapsed_global
    }


# === BLOC DE TEST / EXÉCUTION ===
if __name__ == "__main__":
    """
    Usage:
    - Test sur 1 code APE : python scripts/pipeline_full.py
    - Tous les codes APE : python scripts/pipeline_full.py --all
    - Skip extraction : python scripts/pipeline_full.py --all --skip-extraction
    - Force re-extraction : python scripts/pipeline_full.py --all --force
    """
    
    # Parse arguments
    run_all = "--all" in sys.argv
    force = "--force" in sys.argv
    skip_extraction = "--skip-extraction" in sys.argv
    
    if run_all:
        # Pipeline complet (40 codes APE)
        print("🚀 Mode : Pipeline complet (40 codes APE)\n")
        report = run_full_pipeline(
            force_reextract=force,
            skip_extraction=skip_extraction
        )
    else:
        # Test sur 1 code APE
        print("🚀 Mode : Test sur 1 code APE (43.22A)\n")
        codes_btp = get_codes_btp()
        result = process_single_ape("43.22A", codes_btp, skip_extraction=False)
        
        if result["success"]:
            print(f"\n🎉 Test réussi !")
            print(f"   - {result['nb_siret']} SIRET")
            print(f"   - {result['nb_siren']} SIREN")
            print(f"   - Durée : {result['duration_sec']/60:.1f} min")
        else:
            print(f"\n❌ Test échoué : {result['error']}")
