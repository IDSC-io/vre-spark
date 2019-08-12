# Project Spitalhygiene - Details on Model Creation

This file contains additional information on the procedures involved in the Spitalhygiene project, including 

1. Querying of data in the Data_Science_Atelier and its export to CSV
2. Import of CSV data into the model
3. Creation of the feature vectors for each patient
4. Export of feature vectors into CSV (serves as basis for Andrew Atkinson's univariate and multivariate analysis)
5. Export of data in Neo4J-compatible format
6. Import into the Neo4J database

All the above steps are controlled and logged by the file `~/resources/Update_Model.sh`.

Note: `~` in this script refers to the `spitalhygiene` folder in the master branch of the insel mono-repo on Bitbucket.

### 1. Querying of data in the Data_Science_Atelier and its export to CSV

This step is controlled by the `~/resources/Query_Aterlier_Data.py`. This file controls the querying of data from the Aterlier_DataScience via the `pyodbc` module, and
requires that ODBC Driver 17 for SQL Server is installed. In addition, a file containing connection information is required and must be formatted as follows:

```
DRIVER={ODBC Driver 17 for SQL Server}
SERVER=PATH/TO/SERVER
DATABASE=YOUR_DATABASE
UID=YOUR_USERNAME
PWD=PASSWORD
```

This file is **not** part of this branch and must be available separately, so as not to hard-code SQL connection information in the repo. 

The `Query_Atelier_Data.py` script essentially performs an easy task: it connects to the Data_Science_Atelier, executes a series of SQL queries, 
and saves the retrieved data in CSV format to a specified folder. All executed queries are found in the `~/sql` folder. Created CSV files are named identically to the SQL query
scripts, i.e. `LA_ISH_NICP.sql` &rarr; `LA_ISH_NICP.csv`.

### 2. Import of CSV data into VRE model

The CSV import and model creation is controlled by the `~/vre/src/main/python/vre/feature_extractor.py` script. Data are loaded in multiple steps: 

1. An HDFS_data_loader() instance is created, responsible for the extraction of data from the loaded CSV files
2. Patient data are extracted and loaded via a call to the HDFS_data_loader.patient_data() function
3. Features and labels are processed using an instance of the features_extractor() class
4. Features and labels are prepared via a call to features_extractor().prepare_features_and_labels()

### 3. Creation of the feature vectors for each patient

This step is also controlled by the `~/vre/src/main/python/vre/feature_extractor.py` script.

1. Feature vectors are created based on the HDFS_data_loader.patient_data() object from step 2. 
2. Features and labels are processed using an instance of the features_extractor() class and
3. prepared via a call to features_extractor().prepare_features_and_labels()

### 4. Export of feature vectors into CSV 

Created feature vectors from step 3 are exported as CSV using a call to the features_extractor().export_csv() method. This file is formatted as follows:

|Patient_ID|Label|age|antibiotic=J01AA02|chop=Z95|employee=0081218|device=Oxy 929992|…|
|---|---|---|---|---|---|---|---|
|ID01|32|68|0|0|0|1|
|ID02|42|50|0|1|0|0|
|ID03|32|65|0|0|30|0|
|ID04|142|61|1|0|0|1|

Patient_IDs and labels are actually supplied as separate vectors to the model. Important to note is that all contacts are converted into a one-of-k kind of table, meaning that there is a 
boolean feature for each "entity" in the data indicating whether or not a patient came into contact with it *at any point during the relevant case*. The only exception are continuous 
variables (such as age) and contacts with employees, for which the table lists the *exposure time in minutes* between a particular patient and an employee *in the relevant case*. 
Contacts are not restricted to patients, but can be much more diverse:

- Patient-Bed
- Patient-Device
- Patient-Employee
- Patient-Antibiotic (i.e. if the patient received a particular medicine)
- Patient-CHOP Code (i.e. if a patient underwent a particular surgery)
- etc.

This "spectrum" of contact possibilities is what makes this approach much more sophisticated than the current approach in the clinics, in which the VRE screening method is merely based
on Patient-Patient interactions over the last 7 days.

This feature vector file, which is exported as a simple CSV file, also serves as the basis for Andrew Atkinson's univariate and multivariate analysis.

### 5. Export of data in Neo4J-compatible format

This part is not yet implemented.

### 6. Import into the Neo4J database

This part is not yet implemented.

-----

The file `BasicConfig.ini` is used by all modules and intended to simplify the adjustment of paths, settings, etc. when working on different file systems.

