"""
Module d'extraction des données SIRENE via API INSEE (version production)
"""
import requests
import time
import os
import json
from dotenv import load_dotenv
import pandas as pd
from datetime import datetime, timedelta

# Import fonctions I/O
from data_io import (
    save_raw_data, 
    save_checkpoint, 
    save_metadata, 
    load_metadata,
    get_metadata_path
)

load_dotenv()
API_KEY = os.getenv("SIRENE_API_KEY")


def get_headers():
    """Retourne les headers pour l'API SIRENE"""
    return {
        "X-INSEE-Api-Key-Integration": API_KEY,
        "Accept": "application/json"
    }


def should_reextract(code_ape, max_age_days=30, output_dir="data/raw"):
    """
    Détermine si une re-extraction est nécessaire
    
    Args:
        code_ape (str): Code APE
        max_age_days (int): Age max des données avant re-extraction (défaut: 30j)
        output_dir (str): Répertoire de sortie
    
    Returns:
        tuple: (should_extract: bool, reason: str)
    """
    # 1. Vérifier si le fichier final existe
    filepath = f"{output_dir}/raw_entreprises_{code_ape}.parquet"
    if not os.path.exists(filepath):
        return (True, "Fichier absent")
    
    # 2. Charger les métadonnées
    metadata = load_metadata(code_ape, output_dir)
    
    if metadata is None:
        # Pas de métadonnées = ancien système ou fichier corrompu
        return (True, "Métadonnées absentes")
    
    # 3. Vérifier le statut
    if metadata.get("status") == "partial":
        return (True, "Extraction incomplète (checkpoint)")
    
    # 4. Vérifier l'âge des données
    try:
        extraction_date = datetime.fromisoformat(metadata["extraction_date"])
        age_days = (datetime.now() - extraction_date).days
        
        if age_days > max_age_days:
            return (True, f"Données anciennes ({age_days}j > {max_age_days}j)")
        else:
            return (False, f"Données récentes ({age_days}j)")
    
    except:
        return (True, "Date d'extraction invalide")


def get_all_entreprises_btp(code_ape, headers=None, sleep_time=0.3, 
                            force_reextract=False, max_age_days=30, output_dir="data/raw"):
    """
    Récupère tous les établissements pour un code APE donné (version production)
    
    Args:
        code_ape (str): Code APE (ex: "43.22A")
        headers (dict): Headers API (générés automatiquement si None)
        sleep_time (float): Temps d'attente entre requêtes (défaut: 0.3s)
        force_reextract (bool): Forcer la ré-extraction même si récent
        max_age_days (int): Age max des données avant auto-refresh (défaut: 30j)
        output_dir (str): Répertoire de sortie
    
    Returns:
        list: Liste des établissements (format JSON)
    """
    if headers is None:
        headers = get_headers()
    
    # Déterminer si extraction nécessaire
    should_extract, reason = should_reextract(code_ape, max_age_days, output_dir)
    
    if not force_reextract and not should_extract:
        print(f"⏭️ {code_ape} : Skip extraction ({reason})")
        # Charger depuis le fichier existant
        filepath = f"{output_dir}/raw_entreprises_{code_ape}.parquet"
        df = pd.read_parquet(filepath)
        return df.to_dict('records')
    
    # Afficher la raison de l'extraction
    if force_reextract:
        print(f"🔄 {code_ape} : Extraction forcée")
    else:
        print(f"🔄 {code_ape} : Extraction nécessaire ({reason})")
    
    all_entreprises = []
    curseur = "*"
    nombre = 1000
    batch_count = 0
    
    print(f"📡 {code_ape} : Début extraction...")
    start_time = time.time()
    
    while True:
        url = f"https://api.insee.fr/api-sirene/3.11/siret?q=activitePrincipaleUniteLegale:{code_ape}&nombre={nombre}&curseur={curseur}"
        
        try:
            response = requests.get(url, headers=headers, timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                entreprises_batch = data.get("etablissements", [])
                all_entreprises.extend(entreprises_batch)
                batch_count += 1
                
                # Afficher progression tous les 10 batchs
                if batch_count % 10 == 0 or len(entreprises_batch) < nombre:
                    elapsed = time.time() - start_time
                    print(f"   📊 {code_ape} : {len(all_entreprises)} établissements ({elapsed:.1f}s)")
                
                # Checkpoint tous les 50 batchs (50k établissements)
                if batch_count % 50 == 0:
                    save_checkpoint(all_entreprises, code_ape, output_dir)
                    # Sauvegarder métadonnées comme "partial"
                    save_metadata(code_ape, "partial", len(all_entreprises), output_dir)
                
                header = data.get("header", {})
                curseur_suivant = header.get("curseurSuivant")
                
                # Condition d'arrêt
                if curseur_suivant == header.get("curseur") or not entreprises_batch:
                    elapsed = time.time() - start_time
                    print(f"✅ {code_ape} : Extraction terminée - {len(all_entreprises)} établissements ({elapsed:.1f}s)")
                    break
                
                curseur = curseur_suivant
                time.sleep(sleep_time)
            
            elif response.status_code == 429:
                print(f"⏸️ {code_ape} : Limite atteinte, attente 10s...")
                time.sleep(10)
            
            else:
                print(f"❌ {code_ape} : Erreur {response.status_code}")
                # Sauvegarder checkpoint en cas d'erreur
                if all_entreprises:
                    save_checkpoint(all_entreprises, code_ape, output_dir)
                    save_metadata(code_ape, "partial", len(all_entreprises), output_dir)
                break
        
        except requests.exceptions.Timeout:
            print(f"⏱️ {code_ape} : Timeout, retry dans 5s...")
            time.sleep(5)
        
        except Exception as e:
            print(f"❌ {code_ape} : Erreur inattendue - {str(e)}")
            # Sauvegarder checkpoint
            if all_entreprises:
                save_checkpoint(all_entreprises, code_ape, output_dir)
                save_metadata(code_ape, "partial", len(all_entreprises), output_dir)
            break
    
    return all_entreprises


def extract_multiple_ape(codes_ape_list, sleep_time=0.3, force_reextract=False, 
                        max_age_days=30, output_dir="data/raw"):
    """
    Extrait les données pour plusieurs codes APE (mode batch production)
    
    Args:
        codes_ape_list (list): Liste des codes APE à extraire
        sleep_time (float): Temps d'attente entre requêtes
        force_reextract (bool): Forcer la ré-extraction de tout
        max_age_days (int): Age max des données avant auto-refresh (défaut: 30j)
        output_dir (str): Répertoire de sortie
    
    Returns:
        dict: Résumé des extractions
    """
    print(f"🚀 Extraction de {len(codes_ape_list)} codes APE")
    print(f"   - Age max accepté : {max_age_days} jours")
    print(f"   - Force re-extraction : {force_reextract}\n")
    
    results = {
        "success": [],
        "skipped": [],
        "errors": [],
        "updated": []
    }
    
    start_global = time.time()
    
    for i, code_ape in enumerate(codes_ape_list, 1):
        print(f"\n[{i}/{len(codes_ape_list)}] Traitement {code_ape}...")
        
        try:
            # Vérifier si extraction nécessaire
            should_extract, reason = should_reextract(code_ape, max_age_days, output_dir)
            
            if not force_reextract and not should_extract:
                results["skipped"].append(code_ape)
                continue
            
            # Extraction
            entreprises = get_all_entreprises_btp(
                code_ape, 
                sleep_time=sleep_time, 
                force_reextract=force_reextract,
                max_age_days=max_age_days,
                output_dir=output_dir
            )
            
            if entreprises:
                # Sauvegarde finale
                save_raw_data(entreprises, code_ape, output_dir)
                
                if reason.startswith("Données anciennes"):
                    results["updated"].append(code_ape)
                else:
                    results["success"].append(code_ape)
            else:
                print(f"⚠️ {code_ape} : Aucune donnée extraite")
                results["errors"].append(code_ape)
        
        except Exception as e:
            print(f"❌ {code_ape} : Erreur - {str(e)}")
            results["errors"].append(code_ape)
    
    # Rapport final
    elapsed_global = time.time() - start_global
    print(f"\n{'='*60}")
    print(f"📊 RAPPORT D'EXTRACTION")
    print(f"{'='*60}")
    print(f"✅ Nouvelles extractions : {len(results['success'])} codes APE")
    print(f"🔄 Mises à jour : {len(results['updated'])} codes APE")
    print(f"⏭️ Skipped (récents) : {len(results['skipped'])} codes APE")
    print(f"❌ Erreurs : {len(results['errors'])} codes APE")
    print(f"⏱️ Durée totale : {elapsed_global/60:.1f} minutes")
    
    if results['errors']:
        print(f"\n⚠️ Codes APE en erreur : {results['errors']}")
    
    if results['updated']:
        print(f"\n🔄 Codes APE mis à jour : {results['updated']}")
    
    return results


# === BLOC DE TEST ===
if __name__ == "__main__":
    """
    Test du module d'extraction (version production)
    Usage: python scripts/api_sirene.py
    """
    print("🚀 Test d'extraction API SIRENE (version production)\n")
    
    CODE_APE_TEST = "43.22A"
    
    # Test 1 : Première extraction
    print("📡 Test 1 : Extraction...")
    entreprises = get_all_entreprises_btp(CODE_APE_TEST, sleep_time=0.3, max_age_days=30)
    
    if entreprises:
        filepath = save_raw_data(entreprises, CODE_APE_TEST)
        print(f"\n✅ Extraction réussie : {len(entreprises)} établissements")
    
    # Test 2 : Re-lancement immédiat (devrait skip car récent)
    print(f"\n📡 Test 2 : Re-lancement immédiat (devrait skip)...")
    entreprises2 = get_all_entreprises_btp(CODE_APE_TEST, sleep_time=0.3, max_age_days=30)
    
    # Test 3 : Force re-extraction
    print(f"\n📡 Test 3 : Force re-extraction...")
    entreprises3 = get_all_entreprises_btp(CODE_APE_TEST, sleep_time=0.3, force_reextract=True)
    
    print(f"\n🎉 Tous les tests réussis !")
