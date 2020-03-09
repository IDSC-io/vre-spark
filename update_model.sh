#!/bin/bash

# Start time measurement
START_TIME=$SECONDS

# Activate virtual environment
conda activate vre-spark
# source ./vre_venv/bin/activate

###########################################################################################################################
# VARIABLE DECLARATION
###########################################################################################################################

MODEL_DIR=./data/raw
NEO4J_DIR=./data/processed/neo4j
FEATVEC_DIR=./data/processed/feature_vector
LOG_PATH=./log

###########################################################################################################################
# MODEL CALCULATION
###########################################################################################################################

# Run the feature_extractor.py script, which performs
# --> data load from the CSV files produced by the SQL query
# --> creation and export of the feature vector into FEATVEC_DIR
# --> Export of data in Neo4J-compatible format into NEO4J_DIR
# --> Calculation of the statistical model (random forests???) and its desired output (NOT DESIGNED YET - WHAT SHOULD WE PRODUCE?)
echo "Running data_compiler.py..."
python ./src/features/data_compiler.py
echo "...Done!"

###########################################################################################################################
# TRANSFER TO HDFS
###########################################################################################################################

# Add extracted CSV files to HDFS
#echo "Putting files to HDFS:"
#printf ">> SQL Query files...   "
# sudo -u hdfs hdfs dfs -put ${MODEL_DIR}/*.csv /data1/sqooba/vre/model_data/
#echo "Done !"

#printf ">> Feature vector...   "
# sudo -u hdfs hdfs dfs -put ${FEATVEC_DIR}/*.csv /data1/sqooba/vre/data_export/feature_vector
#echo "Done !"

#printf ">> Neo4J files...   "
# sudo -u hdfs hdfs dfs -put ${NEO4J_DIR}/*.csv /data1/sqooba/vre/data_export/neo4j
#echo "Done !"

###########################################################################################################################
# PROCESS MODEL OUTPUT (NOT YET USED)
###########################################################################################################################
# This part includes any additional processing steps (e.g. sending of a name list by email for screening candidates, etc)
#
# This part (i.e. the model PURPOSE) has not yet been determined.

###########################################################################################################################
# PRINT SUCCESS AND EXECUTION TIME TO LOGFILE
###########################################################################################################################
ELAPSED_TIME=$(($SECONDS - $START_TIME))
HOURS=$(($ELAPSED_TIME/3600))
MINUTES=$((($ELAPSED_TIME-$HOURS*3600)/60))
SECONDS=$((($ELAPSED_TIME-$HOURS*3600)%60))

echo "\nModel updated successfully!"
echo "Update duration: $HOURS h, $MINUTES minutes, $SECONDS seconds"


