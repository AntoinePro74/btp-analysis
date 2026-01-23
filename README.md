# 🏗️ Analyse Data du Secteur BTP

## 🎯 Objectif du projet

Construire une analyse data complète du secteur BTP français (source SIRENE) avec :
- Pipeline automatisé Python → Parquet → BigQuery
- Dashboard Power BI de segmentation des entreprises
- Enrichissement DVF pour analyses territoriales

## 📊 Stack technique

- **Langage** : Python 3.x
- **Données** : API SIRENE, DVF 2024
- **Storage** : Parquet, BigQuery
- **Visualisation** : Power BI
- **Orchestration** : Scripts Python automatisés

## 🗂️ Structure du projet
├── data/ # Données (exclu Git)
├── notebooks/ # Notebooks exploratoires
├── scripts/ # Scripts production
├── sql/ # Requêtes BigQuery
├── config/ # Configurations
└── docs/ # Documentation


## 🚀 Installation

```bash
# Cloner le repo
git clone https://github.com/AntoinePro74/btp-analysis.git

# Installer les dépendances
pip install -r requirements.txt

# Configurer les credentials
cp .env.example .env
# Éditer .env avec vos clés API

