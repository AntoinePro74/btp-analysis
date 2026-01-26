"""
Module d'extraction des données SIRENE via API INSEE
"""
import requests
import time
import os
from dotenv import load_dotenv
import pandas as pd

load_dotenv()
API_KEY = os.getenv("SIRENE_API_KEY")

def get_headers():
    """Retourne les headers pour l'API SIRENE"""
    return {
        "X-INSEE-Api-Key-Integration": API_KEY,
        "Accept": "application/json"
    }

def get_all_entreprises_btp(code_ape, headers=None):
    """
    Récupère tous les établissements pour un code APE donné
    
    Args:
        code_ape (str): Code APE (ex: "43.22A")
        headers (dict): Headers API (générés automatiquement si None)
    
    Returns:
        list: Liste des établissements (format JSON)
    """
    if headers is None:
        headers = get_headers()
    
    all_entreprises = []
    curseur = "*"
    nombre = 1000
    
    while True:
        url = f"https://api.insee.fr/api-sirene/3.11/siret?q=activitePrincipaleUniteLegale:{code_ape}&nombre={nombre}&curseur={curseur}"
        response = requests.get(url, headers=headers)
        
        if response.status_code == 200:
            data = response.json()
            entreprises_batch = data.get("etablissements", [])
            all_entreprises.extend(entreprises_batch)
            print(f"✅ {code_ape} : Récupéré {len(entreprises_batch)} entreprises. Total : {len(all_entreprises)}")
            
            header = data.get("header", {})
            curseur_suivant = header.get("curseurSuivant")
            
            if curseur_suivant == header.get("curseur"):
                print(f"✅ {code_ape} : Fin des résultats")
                break
            
            curseur = curseur_suivant
            time.sleep(1)
        
        else:
            print(f"❌ Erreur {response.status_code} pour {code_ape}")
            if response.status_code == 429:
                print("⏸️ Limite atteinte, attente 5s...")
                time.sleep(5)
            else:
                break
    
    return all_entreprises

def save_raw_data(entreprises, code_ape, output_dir="data/raw"):
    """Sauvegarde les données brutes en Parquet"""
    os.makedirs(output_dir, exist_ok=True)
    df = pd.json_normalize(entreprises)
    filepath = f"{output_dir}/raw_entreprises_{code_ape}.parquet"
    df.to_parquet(filepath, engine="pyarrow")
    print(f"💾 Données brutes sauvegardées : {filepath}")
    return filepath


# === BLOC DE TEST ===
if __name__ == "__main__":
    """
    Test du module sur un code APE
    Usage: python scripts/api_sirene.py
    """
    print("🚀 Démarrage du test d'extraction API SIRENE\n")
    
    # Code APE à tester
    CODE_APE_TEST = "43.22A"
    
    # 1. Test extraction
    print(f"📡 Extraction des données pour {CODE_APE_TEST}...")
    entreprises = get_all_entreprises_btp(CODE_APE_TEST)
    
    print(f"\n✅ Extraction terminée : {len(entreprises)} établissements récupérés\n")
    
    # 2. Test sauvegarde
    print("💾 Sauvegarde des données...")
    filepath = save_raw_data(entreprises, CODE_APE_TEST)
    
    print(f"\n🎉 Test réussi ! Fichier créé : {filepath}")
    
    # 3. Vérification rapide
    df = pd.read_parquet(filepath)
    print(f"\n📊 Vérification du fichier :")
    print(f"   - Nombre de lignes : {len(df)}")
    print(f"   - Nombre de colonnes : {len(df.columns)}")
    print(f"\n   Colonnes principales : {list(df.columns[:5])}")