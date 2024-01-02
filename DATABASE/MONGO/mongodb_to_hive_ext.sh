#!/bin/bash

# Important, run this in shell :
# start-dfs.sh
# start-yarn.sh
# nohup hive --service metastore > /dev/null &
# nohup hiveserver2 > /dev/null &

# Beeline connection parameters
BEELINE="beeline"
HIVE_HOST="localhost"
HIVE_PORT="10000"

# Define SQL statements for creating external tables
sql_statements=(
"CREATE EXTERNAL TABLE IF NOT EXISTS Catalogue ( id STRING, marque STRING, nom STRING, puissance INT, longueur STRING, nbPlaces INT, nbPortes INT, couleur STRING, occasion STRING, prix INT) STORED BY 'com.mongodb.hadoop.hive.MongoStorageHandler' WITH SERDEPROPERTIES('mongo.columns.mapping' = '{\"id\":\"_id\", \"marque\":\"marque\", \"nom\":\"nom\", \"puissance\":\"puissance\", \"longueur\":\"longueur\", \"nbPlaces\":\"nbPlaces\", \"nbPortes\":\"nbPortes\", \"couleur\":\"couleur\", \"occasion\":\"occasion\", \"prix\":\"prix\"}') TBLPROPERTIES('mongo.uri'='mongodb://127.0.0.1:27017/TPA.Catalogue');"

"CREATE EXTERNAL TABLE IF NOT EXISTS Clients_14 ( id STRING, age INT, sexe STRING, taux INT, situationFamiliale STRING, nbEnfantsAcharge INT, deuxiemeVoiture STRING, immatriculation STRING) STORED BY 'com.mongodb.hadoop.hive.MongoStorageHandler' WITH SERDEPROPERTIES('mongo.columns.mapping' = '{\"id\":\"_id\", \"age\":\"age\", \"sexe\":\"sexe\", \"taux\":\"taux\", \"situationFamiliale\":\"situationFamiliale\", \"nbEnfantsAcharge\":\"nbEnfantsAcharge\", \"deuxiemeVoiture\":\"deuxiemeVoiture\", \"immatriculation\":\"immatriculation\"}') TBLPROPERTIES('mongo.uri'='mongodb://127.0.0.1:27017/TPA.Clients_14');"

"CREATE EXTERNAL TABLE IF NOT EXISTS Clients_19 ( id STRING, age INT, sexe STRING, taux INT, situationFamiliale STRING, nbEnfantsAcharge INT, deuxiemeVoiture STRING, immatriculation STRING) STORED BY 'com.mongodb.hadoop.hive.MongoStorageHandler' WITH SERDEPROPERTIES('mongo.columns.mapping' = '{\"id\":\"_id\", \"age\":\"age\", \"sexe\":\"sexe\", \"taux\":\"taux\", \"situationFamiliale\":\"situationFamiliale\", \"nbEnfantsAcharge\":\"nbEnfantsAcharge\", \"deuxiemeVoiture\":\"deuxiemeVoiture\", \"immatriculation\":\"immatriculation\"}') TBLPROPERTIES('mongo.uri'='mongodb://127.0.0.1:27017/TPA.Clients_19');"

"CREATE EXTERNAL TABLE IF NOT EXISTS Immatriculations ( id STRING, immatriculation STRING, marque STRING, nom STRING, puissance INT, longueur STRING, nbPlaces INT, nbPortes INT, couleur STRING, occasion STRING, prix INT) STORED BY 'com.mongodb.hadoop.hive.MongoStorageHandler' WITH SERDEPROPERTIES('mongo.columns.mapping' = '{\"id\":\"_id\", \"immatriculation\":\"immatriculation\", \"marque\":\"marque\", \"nom\":\"nom\", \"puissance\":\"puissance\", \"longueur\":\"longueur\", \"nbPlaces\":\"nbPlaces\", \"nbPortes\":\"nbPortes\", \"couleur\":\"couleur\", \"occasion\":\"occasion\", \"prix\":\"prix\"}') TBLPROPERTIES('mongo.uri'='mongodb://127.0.0.1:27017/TPA.Immatriculations');"

"CREATE EXTERNAL TABLE IF NOT EXISTS Marketing ( id STRING, age INT, sexe STRING, taux INT, situationFamiliale STRING, nbEnfantsAcharge INT, deuxiemeVoiture STRING) STORED BY 'com.mongodb.hadoop.hive.MongoStorageHandler' WITH SERDEPROPERTIES('mongo.columns.mapping' = '{\"id\":\"_id\", \"age\":\"age\", \"sexe\":\"sexe\", \"taux\":\"taux\", \"situationFamiliale\":\"situationFamiliale\", \"nbEnfantsAcharge\":\"nbEnfantsAcharge\", \"deuxiemeVoiture\":\"deuxiemeVoiture\"}') TBLPROPERTIES('mongo.uri'='mongodb://127.0.0.1:27017/TPA.Marketing');"
)

# Loop through SQL statements and execute them
for statement in "${sql_statements[@]}"; do
    $BEELINE -u "jdbc:hive2://$HIVE_HOST:$HIVE_PORT" -e "$statement"
done


echo "JOB DONE"