#!/bin/bash -e

### Start time measurement
START_TIME=$SECONDS

### Activate virtual environment
source /home/i0308559/vre_venv/bin/activate

###########################################################################################################################
### VARIABLE DECLARATION
###########################################################################################################################

MODEL_DIR=/home/i0308559/vre_input
NEO4J_DIR=/home/i0308559/vre_output/neo4j
FEATVEC_DIR=/home/i0308559/vre_output/feature_vector
LOG_PATH=/home/i0308559/vre_log

###########################################################################################################################
### FILE BACKUP (ONLY PERFORMED IN HDFS)
###########################################################################################################################

### Backup HDFS files from the SQL export (stored in MODEL_DIR)
echo "backing up HDFS files for model data"
# sudo -u hdfs hdfs dfs -rm -r ${MODEL_DIR}/old
# sudo -u hdfs hdfs dfs -mkdir ${MODEL_DIR}/old
# sudo -u hdfs hdfs dfs -mv ${MODEL_DIR}/*.csv ${MODEL_DIR}/old
echo "Done !"

### Backup HDFS files from the Neo4J export
echo "backing up HDFS files for Neo4J data"
# sudo -u hdfs hdfs dfs -rm -r ${NEO4J_DIR}/old
# sudo -u hdfs hdfs dfs -mkdir ${NEO4J_DIR}/old
# sudo -u hdfs hdfs dfs -mv ${NEO4J_DIR}/*.csv ${NEO4J_DIR}/old
echo "Done !"

### Backup the feature vector
echo "backing up the feature vector"
# sudo -u hdfs hdfs dfs -rm -r ${FEATVEC_DIR}/old
# sudo -u hdfs hdfs dfs -mkdir ${FEATVEC_DIR}/old
# sudo -u hdfs hdfs dfs -mv ${FEATVEC_DIR}/*.csv ${FEATVEC_DIR}/old
echo "Done !"

###########################################################################################################################
### DATA PRE-PROCESSING
###########################################################################################################################
echo "Pre-processing data into the Atelier_DataScience database..."
python3.6 /home/i0308559/spitalhygiene/resources/preprocessor.py > ${LOG_PATH}/Preprocessor.log 2>&1
echo "Done !"

###########################################################################################################################
### MODEL DATA EXTRACTION
###########################################################################################################################

### Extract data from the SQL server into CSV (must be called by the virtual environment interpreter to load pyodbc module)
echo "Extracting new data from the Atelier_DataScience into CSV..."
python3.6 /home/i0308559/spitalhygiene/resources/Query_Atelier_Data.py > ${LOG_PATH}/SQL_Data_Load.log 2>&1  # pyodbc module installed
# python3.6 /home/i0308559/spitalhygiene/resources/update_db_jar.py > ${LOG_PATH}/SQL_Data_Load.log 2>&1     # pyodbc module NOT installed
echo "Done !"

###########################################################################################################################
### MODEL CALCULATION
###########################################################################################################################

### Run the feature_extractor.py script, which performs
# --> data load from the CSV files produced by the SQL query
# --> creation and export of the feature vector into FEATVEC_DIR
# --> Export of data in Neo4J-compatible format into NEO4J_DIR
# --> Calculation of the statistical model (random forests???) and its desired output (NOT DESIGNED YET - WHAT SHOULD WE PRODUCE?)
echo "Running feature_extractor.py..."
python3.6 /home/i0308559/spitalhygiene/vre/src/main/python/vre/data_compiler.py > ${LOG_PATH}/Compiler.log 2>&1
echo "Done !"

###########################################################################################################################
### TRANSFER TO HDFS
###########################################################################################################################

### Add extracted CSV files to HDFS
echo "Putting files to HDFS:"
printf ">> SQL Query files...   "
# sudo -u hdfs hdfs dfs -put ${MODEL_DIR}/*.csv /data1/sqooba/vre/model_data/
echo "Done !"

printf ">> Feature vector...   "
# sudo -u hdfs hdfs dfs -put ${FEATVEC_DIR}/*.csv /data1/sqooba/vre/data_export/feature_vector
echo "Done !"

printf ">> Neo4J files...   "
# sudo -u hdfs hdfs dfs -put ${NEO4J_DIR}/*.csv /data1/sqooba/vre/data_export/neo4j
echo "Done !"

###########################################################################################################################
### PROCESS MODEL OUTPUT (NOT YET USED)
###########################################################################################################################
# This part includes any additional processing steps (e.g. sending of a name list by email for screening candidates, etc)
#
# This part (i.e. the model PURPOSE) has not yet been determined.

###########################################################################################################################
### PRINT SUCCESS AND EXECUTION TIME TO LOGFILE
###########################################################################################################################
ELAPSED_TIME=$(($SECONDS - $START_TIME))
HOURS=$(($ELAPSED_TIME/3600))
MINUTES=$((($ELAPSED_TIME-$HOURS*3600)/60))
SECONDS=$((($ELAPSED_TIME-$HOURS*3600)%60))

echo ""
echo "Model updated successfully !"
echo "Update duration: $HOURS h, $MINUTES minutes, $SECONDS seconds"

