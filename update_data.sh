#!/bin/bash -e

# Start time measurement
START_TIME=$SECONDS

# Activate virtual environment
conda activate vre-spark
# source ./vre_venv/bin/activate

###########################################################################################################################
# VARIABLE DECLARATION
###########################################################################################################################

MODEL_DIR=./data/raw
LOG_PATH=./log

###########################################################################################################################
# FILE BACKUP (ONLY PERFORMED IN HDFS)
###########################################################################################################################

# Backup HDFS files from the SQL export (stored in MODEL_DIR)
#echo "backing up HDFS files for model data"
# sudo -u hdfs hdfs dfs -rm -r ${MODEL_DIR}/old
# sudo -u hdfs hdfs dfs -mkdir ${MODEL_DIR}/old
# sudo -u hdfs hdfs dfs -mv ${MODEL_DIR}/*.csv ${MODEL_DIR}/old
#echo "Done !"

# Backup HDFS files from the Neo4J export
#echo "backing up HDFS files for Neo4J data"
# sudo -u hdfs hdfs dfs -rm -r ${NEO4J_DIR}/old
# sudo -u hdfs hdfs dfs -mkdir ${NEO4J_DIR}/old
# sudo -u hdfs hdfs dfs -mv ${NEO4J_DIR}/*.csv ${NEO4J_DIR}/old
#echo "Done !"

# Backup the feature vector
#echo "backing up the feature vector"
# sudo -u hdfs hdfs dfs -rm -r ${FEATVEC_DIR}/old
# sudo -u hdfs hdfs dfs -mkdir ${FEATVEC_DIR}/old
# sudo -u hdfs hdfs dfs -mv ${FEATVEC_DIR}/*.csv ${FEATVEC_DIR}/old
#echo "Done !"

###########################################################################################################################
# MODEL DATA EXTRACTION
###########################################################################################################################

# Extract data from the SQL server into CSV (must be called by the virtual environment interpreter to load pyodbc module)
echo "Extracting data from  DB into CSV..."
python ./src/data/pull_raw_dataset.py    # pyodbc module installed
echo "...Done!"

###########################################################################################################################
# PRINT SUCCESS AND EXECUTION TIME TO LOGFILE
###########################################################################################################################
ELAPSED_TIME=$(($SECONDS - $START_TIME))
HOURS=$(($ELAPSED_TIME/3600))
MINUTES=$((($ELAPSED_TIME-$HOURS*3600)/60))
SECONDS=$((($ELAPSED_TIME-$HOURS*3600)%60))

echo "\n Data updated successfully!"
echo "Update duration: $HOURS h, $MINUTES minutes, $SECONDS seconds"


