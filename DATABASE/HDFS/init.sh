# Init et chargement des fichiers local sur HDFS

# Lancement des services nécessaires
echo "Vérification et lancement des services nécessaires"
processes=("NodeManager" "DataNode" "SecondaryNameNode" "ResourceManager" "NameNode")

for process in "${processes[@]}"; do
    if ! jps | grep -q "$process"; then
        echo "$process n'est pas en cours d'exécution. Lancement du script approprié..."
        case "$process" in
            "NodeManager" | "DataNode" | "SecondaryNameNode")
                /usr/local/hadoop/sbin/start-dfs.sh
                ;;
            "ResourceManager" | "NameNode")
                /usr/local/hadoop/sbin/start-yarn.sh
                ;;
        esac
    else
        echo "$process est en cours d'exécution."
    fi
done

# Les fichiers doivent être placés dans un dossier DataCSV
echo "Ajout des fichiers locaux dans HDFS"
hadoop fs -put -f /root/tpa/tpa/DATABASE/data/Catalogue.csv /user/data
hadoop fs -put -f /root/tpa/tpa/DATABASE/data/Clients_14.csv /user/data
hadoop fs -put -f /root/tpa/tpa/DATABASE/data/Clients_19.csv /user/data
hadoop fs -put -f /root/tpa/tpa/DATABASE/data/CO2.csv /user/data
hadoop fs -put -f /root/tpa/tpa/DATABASE/data/Immatriculations.csv /user/data
hadoop fs -put -f /root/tpa/tpa/DATABASE/data/Marketing.csv /user/data

# Lancement du script hive
# chmod u+x *.hql

# echo "Suppression des bases Hive"
# hive -f delete_bases.hql

# echo "Création des bases Hive"
# hive -f create_bases.hql

# echo "Ajout des fichiers dans les bases Hive"
# hive -f populate_bases.hql

# echo "ALL DONE"
