#!/bin/bash

# Important, run this in shell :
# start-dfs.sh
# start-yarn.sh
# nohup hive --service metastore > /dev/null &
# nohup hiveserver2 > /dev/null &

DB_FILE="TPA" # Source db name from mySQL

echo "Clean and remove table from hive"

# Drop existing Hive tables if they exist
hive -e "DROP TABLE IF EXISTS Catalogue;"
hive -e "DROP TABLE IF EXISTS Clients_14;"
hive -e "DROP TABLE IF EXISTS Clients_19;"
hive -e "DROP TABLE IF EXISTS Immatriculations;"
hive -e "DROP TABLE IF EXISTS Marketing;"

echo "Starting data import from mySQL to Hive"

# Perform Sqoop import into Hive internal tables
sqoop import --connect "jdbc:mysql://localhost/$DB_FILE" \
    --username root \
    --table Catalogue \
    --hive-import \
    --hive-table Catalogue \
    --create-hive-table \
    -m 1

sqoop import --connect "jdbc:mysql://localhost/$DB_FILE" \
    --username root \
    --table Clients_14 \
    --hive-import \
    --hive-table Clients_14 \
    --create-hive-table \
    -m 1

sqoop import --connect "jdbc:mysql://localhost/$DB_FILE" \
    --username root \
    --table Clients_19 \
    --hive-import \
    --hive-table Clients_19 \
    --create-hive-table \
    -m 1

sqoop import --connect "jdbc:mysql://localhost/$DB_FILE" \
    --username root \
    --table Immatriculations \
    --hive-import \
    --hive-table Immatriculations \
    --create-hive-table \
    -m 1

sqoop import --connect "jdbc:mysql://localhost/$DB_FILE" \
    --username root \
    --table Marketing \
    --hive-import \
    --hive-table Marketing \
    --create-hive-table \
    -m 1

echo "JOB DONE"
