# BDA-LD
Projet Big Data Analytics : Analyse de la Clientèle d'un Concessionnaire Automobile pour la Recommandation de Modèles de Véhicules

# Info
Ce repo corresponds au dossier script de la vm vagrant du projet (/vagrant/TPA/vagrant-projects/OracleDatabase/21.3.0/TPA/script).
On y retrouve les scripts pour :
- Ajouter les csv dans les bases de données Mongo et MySQL
- Ainsi que les access drivers/extracteur de ces derniers pour les ajouters à Hive
- Il y a aussi les scripts pour la visualization 

# IMPORTANT
- A noter : Tout ce fait en local
- Attention : Pour lancer la page html, il faut le lancer au travers d'un server - nous l'avons fait en local - pour contourner les problèmes de cors. De plus je recommande de d'avord tester la visualisation sur des fichiers pas trop volumineux.
- Il se peut que certains scripts specifient d'autre informations qui pourrait être utile pour les lancer avec succès. Une fois dans vagrant, assurez-vous de lancer en ligne de commande :
    - start-dfs.sh
    - start-yarn.sh
    - nohup hive --service metastore > /dev/null &
    - nohup hiveserver2 > /dev/null &


# Authors

- [Benjamin BERNAUD](https://github.com/benj-b)
- [Maxime BELLET](https://github.com/PandaShad)
- [Paul ZANAGLIA](https://github.com/PaulZANAGLIA)
- [Ayoub ADMESSIEV](https://github.com/AyoubAdm)
- [Yahya AARJI](https://github.com/aarjinho)
