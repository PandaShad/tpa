#!/bin/bash

# MongoDB connection information
HOST="localhost"    # Change this to your MongoDB host
PORT="27017"        # Change this to your MongoDB port
DB="TPA"  # Change this to your target database name


echo "Start mongodb clean-up..."
# Drop all collections in the TPA database
mongo --host $HOST:$PORT $DB --eval "db.getCollectionNames().forEach(function(collName) { db[collName].drop() })"


echo "Start mongodb insertion..."
# Loop through all CSV files in the folder
for file in ./../data/*.csv; do
  filename=$(basename -- "$file")  # Get the file name without the path
  collection_name="${filename%.csv}"  # Remove the .csv extension to use as the collection name

  # Use mongoimport to import the CSV file into MongoDB
  mongoimport --host $HOST --port $PORT --db $DB --collection $collection_name --type csv --headerline --file "$file"

  # Print a message to indicate successful import
  echo "Imported $filename into $DB.$collection_name"
done

echo "JOB DONE"
