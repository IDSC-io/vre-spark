#!/bin/bash

# Start time measurement
START_TIME=$SECONDS

echo "Extracting data from DB into CSV and preprocessing..."
python ./src/data/make_dataset.py    # pyodbc module installed
echo "...Done!"

echo "Updating hotspot lists..."
python ./src/models/compose_model.py
echo "...Done!"

echo "Exporting data into sql table..."
python ./src/postprocessing/export_metrics_to_db.py
echo "...Done!"

###########################################################################################################################
# PRINT SUCCESS AND EXECUTION TIME TO LOGFILE
###########################################################################################################################
ELAPSED_TIME=$(($SECONDS - $START_TIME))
HOURS=$(($ELAPSED_TIME/3600))
MINUTES=$((($ELAPSED_TIME-$HOURS*3600)/60))
SECONDS=$((($ELAPSED_TIME-$HOURS*3600)%60))

echo "Update duration: $HOURS h, $MINUTES minutes, $SECONDS seconds"