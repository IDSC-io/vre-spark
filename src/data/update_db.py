import datetime
import os
import pyodbc
import csv

"""
This script controls the data update for all "raw" data in the VRE Model. It will execute all SQL
queries (i.e. all files with a '.sql' extension') found in SQL_DIR) and save extracted data to CSV files in CSV_DIR, where
CSV and SQL files are named identically. The data science atelier is queried via subprocess.call(...),
using Sqooba's JAR file, which must be found in JAR_FILEPATH.
The JAR file controls correct authentication with the SQL server and takes 2 arguments:
arg_1 >> name of SQL file containing query to be executed
arg_2 >> name of CSV file where data are to be written to

Note that this script also requires JDK to be installed and java to be callable on the command line (add to path on Windows systems).
Any "logs" are written directly to stdout via print()
"""


def write_sql_to_csv(path_to_sql, path_to_csv, csv_sep, connection_file, trusted_connection=True):
    """
    Executes an SQL query and writes the results to path_to_csv.

    :param path_to_sql:         Path to .sql file containing the query to be executed
    :param path_to_csv:         Path to .csv file destination, to which data will be written to in "csv_sep"-delimited fashion.
    :param csv_sep:             Delimiter used in the csv file.
    :param connection_file:     path to file containing information used for server connection and authentication as well as database selection (read and passed to the pyodbc.connect() )
                                This information is read from an external file so as to avoid hard-coding usernames and passwords in the code
    :param trusted_connection:  additional argument passed to pyodbc.connect(), converted to 'yes' if True and 'no' otherwise
    """
    connection_string = ';'.join([line.replace('\n', '') for line in open(connection_file, 'r')])
    conn = pyodbc.connect(connection_string, trusted_connection='yes' if trusted_connection else 'no')
    cursor = conn.cursor()

    # Read the SQL file
    query = ' '.join([line.replace('\n', '') for line in open(path_to_sql, 'r')])

    # execute query
    cursor.execute(query)

    # Then write results to SQL
    csv.register_dialect('sql_special', delimiter=csv_sep, lineterminator='\n')  # register special dialect to control csv delimiter and proper newline formatting
    with open(path_to_csv, 'w') as writefile:
        csv_writer = csv.writer(writefile, dialect='sql_special')
        csv_writer.writerow([i[0] for i in cursor.description])  # write headers
        csv_writer.writerows(cursor)

    # close connection
    conn.close()


SQL_DIR = "../sql"  # absolute or relative path to directory containing SQL files
CSV_DIR = "T:/IDSC Projects/VRE Model/Data/Model Data"  # absolute or relative path to directory where data should be stored

CSV_DELIM = ','  # delimiter for CSV files written from SQL results

#JAR_FILEPATH = "T:/IDSC Projects/VRE Model/JAR Files/spitalhygiene-2.0-SNAPSHOT-jar-with-dependencies.jar"

# Execute all queries in SQL_DIR
print('Loading data from SQL server:\n')

# sql_files = [each_file for each_file in os.listdir(SQL_DIR) if each_file.endswith('.sql')]
sql_files = [each_file for each_file in os.listdir(SQL_DIR) if each_file in ['deleted_screenings.sql']]  # Use this line instead for loading only specific files

for each_file in sql_files:
    print('--> Loading file: ' + each_file + '... ', end='')
    start_dt = datetime.datetime.now()

    # Execute query and write results
    write_sql_to_csv(path_to_sql=os.path.join(SQL_DIR, each_file), path_to_csv=os.path.join(CSV_DIR, each_file.replace('.sql', '.csv')), csv_sep=CSV_DELIM,
                     connection_file="T:/IDSC Projects/VRE Model/Connection File/Server_Connection.txt", trusted_connection=True)

    print(f'\tDone !\t Total processing time: {str(datetime.datetime.now()-start_dt).split(".")[0]}')  # print timedelta without fractional seconds (original string would be printed as 0:00:13.4567)

print('\nAll files loaded successfully !')
