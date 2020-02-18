#!/bin/bash

export PATH=/home/spitalhygiene/anaconda3/bin:$PATH
declare -a arr=("TACS_DATEN" "deleted_screenings" "V_DH_REF_CHOP" "LA_ISH_NGPA" "LA_ISH_NFPZ" "V_LA_ISH_NRSF_NORM" "V_DH_DIM_PATIENT_CUR" "LA_ISH_NBEW" "V_LA_IPD_DRUG_NORM" "V_DH_DIM_TERMIN_CUR" "V_DH_FACT_TERMINMITARBEITER" "V_DH_FACT_TERMINRAUM" "V_DH_FACT_TERMINGERAET" "V_LA_ISH_NFAL_NORM" "V_DH_FACT_TERMINPATIENT" "V_DH_DIM_GERAET_CUR" "V_DH_DIM_RAUM_CUR")

cd /home/spitalhygiene

# backup HDFS files
echo "backup HDFS files"
hdfs dfs -rm -r /data/spitalhygiene/old
hdfs dfs -mkdir /data/spitalhygiene/old
hdfs dfs -mv /data/spitalhygiene/*.csv /data/spitalhygiene/old/

# load new files from sql server into hdfs
echo "load new files from sql server into hdfs"
for i in "${arr[@]}"
do
   echo "$i"
   java -jar spitalhygiene-2.0-SNAPSHOT-jar-with-dependencies.jar sql/${i}.sql /tmp/spitalhygiene/${i}.csv
   hdfs dfs -put /tmp/spitalhygiene/${i}.csv /data/spitalhygiene/
   rm /tmp/spitalhygiene/${i}.csv
done

# bakup old neo4j csv files
echo "backup old neo4j csv files"
mv /tmp/spitalhygiene/csv/*.csv /tmp/spitalhygiene/old/

# create new neo4j csv files
echo "create new neo4j csv files"
python -m CreateModel

# back up old neo4j database
echo "back up old neo4j database"
rm -rf /tmp/spitalhygiene/graph.db.old
mv neo4j/data/databases/graph.db /tmp/spitalhygiene/graph.db.old
rm -rf /tmp/spitalhygiene/graph.db

# create new neo4j database
echo "create new neo4j database"
neo4j-import --into /tmp/spitalhygiene/graph.db --nodes patient.csv --nodes drugs.csv --nodes zimmer.csv --nodes geraet.csv --nodes employees.csv --nodes referrers.csv --relationships referrer_patient.csv --relationships patient_employee.csv --relationships patient_medication.csv --relationships patient_geraed.csv --relationships patient_patient.csv --relationships patient_zimmer.csv

mv /tmp/spitalhygiene/graph.db neo4j/data/databases/
mv *.csv /tmp/spitalhygiene/csv/
# restart neo4j
echo "restart neo4j"
neo4j restart

echo "done."
