"""
scripts/fix_categorie_juridique_type.py
Corriger le type de categorie_juridique_ul_niv2 : INTEGER → STRING
"""
import pandas as pd

def fix_categorie_juridique_type():
    """
    Convertir categorie_juridique_ul_niv2 en STRING
    pour correspondre au type dans les tables de faits
    """
    print("\n" + "="*60)
    print("🔧 CORRECTION TYPE CATÉGORIE JURIDIQUE")
    print("="*60 + "\n")
    
    # Charger le CSV
    df = pd.read_csv('data/dimensions/categories_juridiques.csv')
    
    print(f"📂 Fichier chargé : {len(df)} lignes")
    print(f"\n   Types actuels :")
    print(df.dtypes.to_string())
    
    print(f"\n   Aperçu valeurs actuelles :")
    print(df[['categorie_juridique_ul_niv2', 'famille_juridique']].head(10).to_string(index=False))
    
    # Forcer le type STRING
    df['categorie_juridique_ul_niv2'] = df['categorie_juridique_ul_niv2'].astype(str)
    
    # Nettoyer les décimales si présentes (ex: "54.0" → "54")
    df['categorie_juridique_ul_niv2'] = df['categorie_juridique_ul_niv2'].str.replace('.0', '', regex=False)
    
    # Vérifier qu'il n'y a pas de NaN
    if df['categorie_juridique_ul_niv2'].isna().any():
        print("\n⚠️  ATTENTION : Valeurs NaN détectées !")
        print(df[df['categorie_juridique_ul_niv2'].isna()])
    
    print("\n" + "="*60)
    print("✅ CORRECTION APPLIQUÉE")
    print("="*60)
    
    print(f"\n   Types après correction :")
    print(df.dtypes.to_string())
    
    print(f"\n   Aperçu valeurs corrigées :")
    print(df[['categorie_juridique_ul_niv2', 'famille_juridique']].head(10).to_string(index=False))
    
    # Sauvegarder
    df.to_csv('data/dimensions/categories_juridiques.csv', index=False)
    print(f"\n💾 Fichier sauvegardé : data/dimensions/categories_juridiques.csv")
    
    print("\n🚀 Prochaine étape : Re-upload vers BigQuery")
    print("   python scripts/upload_dimensions_bigquery.py")
    
    return df

if __name__ == "__main__":
    df = fix_categorie_juridique_type()
