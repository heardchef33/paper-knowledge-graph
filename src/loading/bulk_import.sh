#!bin/bash

echo "starting bulk csv load into neo4j"
DATA_DIR = /Users/thananpornsethjinda/Desktop/rkg/data/neo4j_import 
NEO4J_DATA_DIR = $HOME/neo4j/data
NEO4J_IMPORT_DIR = $HOME/neo4j/import 

echo "stop database"
docker compose down 
echo "database stopped"

echo "deleting old database files"
rm -rf $NEO4J_DATA_DIR/databases/neo4j
echo "old database files deleted"

echo "copying all files to neo4j import directory"
cp $DATA_DIR/papers/part-*.csv $NEO4J_IMPORT_DIR/papers.csv
cp $DATA_DIR/authors/part-*.csv $NEO4J_IMPORT_DIR/authors.csv
cp $DATA_DIR/categories/part-*.csv $NEO4J_IMPORT_DIR/categories.csv
cp $DATA_DIR/concepts/part-*.csv $NEO4J_IMPORT_DIR/concepts.csv
cp $DATA_DIR/wrote/part-*.csv $NEO4J_IMPORT_DIR/wrote.csv
cp $DATA_DIR/contains/part-*.csv $NEO4J_IMPORT_DIR/contains.csv
cp $DATA_DIR/has_concept/part-*.csv $NEO4J_IMPORT_DIR/has_concept.csv
echo "done copying files"

echo "run docker command" 
docker run --rm \
  -v "$NEO4J_DATA_DIR:/data" \
  -v "$NEO4J_IMPORT_DIR:/import" \
  neo4j:latest \
  neo4j-admin database import full \
    --nodes=/import/papers.csv \
    --nodes=/import/authors.csv \
    --nodes=/import/categories.csv \
    --nodes=/import/concepts.csv \
    --relationships=/import/wrote.csv \
    --relationships=/import/contains.csv \
    --relationships=/import/has_concept.csv \
    --overwrite-destination neo4j
echo "import complete" 

echo "starting neo4j"
docker compose up -d 

sleep 10

echo "neo4j is now ready"

echo "neo4j is now running at:"
echo "  browser: http://localhost:7474"
echo "  bolt:    bolt://localhost:7687"
echo ""
echo "credentials:"
echo "  username: neo4j"
echo "  password: your_password"




