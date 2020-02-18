# VRE report import script

Python script to filter out patient reports based on CSV file containing PIDs and FIDs

## Usage:

    ./filter_reports.py source_folder destination_folder csv_to_include

where:
  - source_folder: dump from hdfs containing the report files
  - destination_folder: where to copy the files of interest
  - csv_to_include: csv file containing the list of PIDs and FIDs to retain

The format of the CSV is the following:

    nr,pid,fid,...
    1,34245,547436,...
    2,76755,655665,...
    ...

If a fid number (column nº3) is missing all the reports
for that PID will be included


To get the hdfs dump:

    hdfs dfs -get /data/searchbox/staging/ident source_folder