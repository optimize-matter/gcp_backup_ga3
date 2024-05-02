# global.py
Permet de faire des backups sur les dimensions et metrics de son choix. Se référer au [dev tools](https://ga-dev-tools.web.app/dimensions-metrics-explorer/) pour connaître la compatibilité des metrics et dimensions entre eux.

## Setup 
Copier le script et le requirement dans une cloud function ayant 1G de mémoire. Ajouter le credential sur le GA avec le rôle viewer et activer les APIs (Analytics et Analytics reporting) dans le projet GCP. Faire une requête POST avec un body, voir la section "Params".

## Features

✅ Vérification de l'existence du dataset et de la table
✅ Création automatique du dataset et de la table
✅ Création automatique de schémas de données
✅ Sélection des dims et métriques désirés
✅ Possibilité de reprendre une backup arrêté à un pageToken ou à une date (manuellement)
✅ Prise en compte des customs Dimensions 

## 📄 Doc  

[Lien du mode d'emploi de la backup](https://docs.google.com/document/d/1UHjJxYq0UqEhx2LhYrk-z_lwluGACdBD0sies1K67fU/edit?usp=drive_link)
