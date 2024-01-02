#!/bin/bash

# Database connection details
MYSQL_HOST="localhost"
MYSQL_DB="TPA"
DATA_PATH="./../data"
AUTHORIZED_PATH="/var/lib/mysql-files" #Only path authorised to load content
echo "Start MySQL setup and data insertion..."

# Function to alter table character set and collation
function alter_table_charset() {
    mysql --local-infile=1 -h$MYSQL_HOST $MYSQL_DB -e "ALTER TABLE $1 CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;"
}


# Convert CSV files from ANSI to UTF-8
iconv -f WINDOWS-1252 -t UTF-8 < "$DATA_PATH/Catalogue.csv" > "$AUTHORIZED_PATH/Catalogue.csv"
iconv -f WINDOWS-1252 -t UTF-8 < "$DATA_PATH/Clients_14.csv" > "$AUTHORIZED_PATH/Clients_14.csv"
iconv -f WINDOWS-1252 -t UTF-8 < "$DATA_PATH/Clients_19.csv" > "$AUTHORIZED_PATH/Clients_19.csv"
iconv -f WINDOWS-1252 -t UTF-8 < "$DATA_PATH/Immatriculations.csv" > "$AUTHORIZED_PATH/Immatriculations.csv"
iconv -f WINDOWS-1252 -t UTF-8 < "$DATA_PATH/Marketing.csv" > "$AUTHORIZED_PATH/Marketing.csv"

echo "new encodage done"

# Drop the database if it exists
mysql -h$MYSQL_HOST -e "DROP DATABASE IF EXISTS $MYSQL_DB;"

# Create database if it doesn't exist and clear tables
mysql -h$MYSQL_HOST -e "CREATE DATABASE IF NOT EXISTS $MYSQL_DB;"

mysql --local-infile=1 -h$MYSQL_HOST $MYSQL_DB <<EOF
DROP TABLE Catalogue;
DROP TABLE Clients_14;
DROP TABLE Clients_19;
DROP TABLE Immatriculations;
DROP TABLE Marketing;

CREATE TABLE IF NOT EXISTS Catalogue (
    marque VARCHAR(255),
    nom VARCHAR(255),
    puissance INT,
    longueur VARCHAR(255),
    nbPlaces INT,
    nbPortes INT,
    couleur TEXT,
    occasion TEXT,
    prix INT
);

CREATE TABLE IF NOT EXISTS Clients_14 (
    age INT,
    sexe VARCHAR(255),
    taux INT DEFAULT NULL,
    situationFamiliale VARCHAR(255),
    nbEnfantsAcharge INT,
    deuxiemeVoiture VARCHAR(255),
    immatriculation VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS Clients_19 (
    age INT,
    sexe VARCHAR(255),
    taux INT,
    situationFamiliale VARCHAR(255),
    nbEnfantsAcharge INT,
    deuxiemeVoiture VARCHAR(255),
    immatriculation VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS Immatriculations (
    immatriculation VARCHAR(255),
    marque VARCHAR(255),
    nom VARCHAR(255),
    puissance INT,
    longueur VARCHAR(255),
    nbPlaces INT,
    nbPortes INT,
    couleur TEXT,
    occasion TEXT,
    prix INT
);

CREATE TABLE IF NOT EXISTS Marketing (
    age INT,
    sexe VARCHAR(255),
    taux INT,
    situationFamiliale VARCHAR(255),
    nbEnfantsAcharge INT,
    deuxiemeVoiture VARCHAR(255)
);
EOF

echo "Tables created in $MYSQL_DB"

# Import CSV data into MySQL tables
mysql --local-infile=1 -h$MYSQL_HOST $MYSQL_DB <<EOF
LOAD DATA INFILE '$AUTHORIZED_PATH/Catalogue.csv'
INTO TABLE Catalogue
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(marque, nom, @puissance, longueur, @nbPlaces, @nbPortes, couleur, occasion, @prix)
SET puissance = IF(@puissance REGEXP '^[0-9]+$' = 0, NULL, @puissance),
    nbPlaces = IF(@nbPlaces REGEXP '^[0-9]+$' = 0, NULL, @nbPlaces),
    nbPortes = IF(@nbPortes REGEXP '^[0-9]+$' = 0, NULL, @nbPortes),
    prix = IF(@prix REGEXP '^[0-9]+$' = 0, NULL, @prix);

LOAD DATA INFILE '$AUTHORIZED_PATH/Clients_14.csv'
INTO TABLE Clients_14
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(@age, sexe, @taux, situationFamiliale, @nbEnfantsAcharge, deuxiemeVoiture, immatriculation)
SET age = IF(@age REGEXP '^[0-9]+$' > 0, @age, NULL), 
    taux = IF(@taux REGEXP '^[0-9]+$' > 0, @taux, NULL), 
    nbEnfantsAcharge = IF(@nbEnfantsAcharge REGEXP '^[0-9]+$' > 0, @nbEnfantsAcharge, NULL);

LOAD DATA INFILE '$AUTHORIZED_PATH/Clients_19.csv'
INTO TABLE Clients_19
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(@age, sexe, @taux, situationFamiliale, @nbEnfantsAcharge, deuxiemeVoiture, immatriculation)
SET age = IF(@age REGEXP '^[0-9]+$' > 0, @age, NULL), 
    taux = IF(@taux REGEXP '^[0-9]+$' = 0, NULL, @taux),
    nbEnfantsAcharge = IF(@nbEnfantsAcharge REGEXP '^[0-9]+$' = 0, NULL, @nbEnfantsAcharge);

LOAD DATA INFILE '$AUTHORIZED_PATH/Immatriculations.csv'
INTO TABLE Immatriculations
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(immatriculation, marque, nom, @puissance, longueur, @nbPlaces, @nbPortes, couleur, occasion, @prix)
SET puissance = IF(@puissance REGEXP '^[0-9]+$' = 0, NULL, @puissance),
    nbPlaces = IF(@nbPlaces REGEXP '^[0-9]+$' = 0, NULL, @nbPlaces),
    nbPortes = IF(@nbPortes REGEXP '^[0-9]+$' = 0, NULL, @nbPortes),
    prix = IF(@prix REGEXP '^[0-9]+$' = 0, NULL, @prix);

LOAD DATA INFILE '$AUTHORIZED_PATH/Marketing.csv'
INTO TABLE Marketing
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(age, sexe, @taux, situationFamiliale, @nbEnfantsAcharge, deuxiemeVoiture)
SET taux = IF(@taux REGEXP '^[0-9]+$' = 0, NULL, @taux),
    nbEnfantsAcharge = IF(@nbEnfantsAcharge REGEXP '^[0-9]+$' = 0, NULL, @nbEnfantsAcharge);

EOF

# Alter table character set and collation
alter_table_charset "Catalogue"
alter_table_charset "Clients_14"
alter_table_charset "Clients_19"
alter_table_charset "Immatriculations"
alter_table_charset "Marketing"

# Remove files from authorised folder
rm $AUTHORIZED_PATH/*

echo "JOB DONE"
