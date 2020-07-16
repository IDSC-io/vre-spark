# -*- coding: utf-8 -*-
"""This script contains various functions for pre-processing data required in the VRE project.
This includes:

- Regenerating the ward screening overview data stored in the ``Atelier_DataScience.dbo.WARD_SCREENINGS`` table
- ...

Please refer to the script code for details on the various functions.

-----
"""

import datetime
import os

import pyodbc

from configuration.basic_configuration import configuration


def execute_sql(sql_command, connection_file, trusted_connection=True):
    """
    Executes an arbitrary SQL command, but does **not** return any results.

    Args:
        sql_command (str):          SQL command to be executed
        connection_file (str):      path to file containing information used for server connection and authentication,
                                    as well as database selection (read and passed to ``pyodbc.connect()`` )

        trusted_connection (bool):  additional argument passed to pyodbc.connect(), converted to "yes" if ``True`` and
                                    "no" otherwise (defaults to ``True``)
    """
    connection_string = ';'.join([line.replace('\n', '') for line in open(connection_file, 'r')])

    conn = pyodbc.connect(connection_string, trusted_connection='yes' if trusted_connection else 'no')
    cursor = conn.cursor()

    cursor.execute(sql_command)

    cursor.commit()  # this is required for INSERT, DELETE and UPDATE statements to take effect in the DB

    cursor.close()
    conn.close()


def recreate_ward_overview_data(csv_sep=';'):
    """
    Recreates dates at which specific screening types were active in various clinics.

    This information is found in the ``[...]/vre_input/screening_data/screening_overview.csv`` file. Its contents
    are used to create a query for updating the ``Atelier_DataScience.dbo.WARD_SCREENINGS`` table. This query is written
    to the ``[...]/vre_output/manual_sql_queries`` folder and named ``update_ward_screenings.sql``,
    since the Atelier_Datascience_Reader does not have permission to execute TRUNCATE, DELETE, INSERT or UPDATE
    statements.

    To Do:
        Find a solution to automate this part.

    Args:
        csv_sep (str):              separator used in the read file (defaults to ``;``)
    """
    # Prepare INSERT statement for table
    sql_statement = 'TRUNCATE TABLE [Atelier_DataScience].[dbo].[WARD_SCREENINGS];\n'

    # Load data from file
    all_screening_data = [each_line.replace('\n', '') for each_line in
                          open(os.path.join(configuration['PATHS']['input_dir'], 'screening_data',
                                            'screening_overview.csv'), 'r')]
    #   --> Note: the data file contains the date of active screening in column 1, day of week in column 2 (not used),
    #               and active screenings in all other "pflegerischen" wards in subsequent columns

    for index, line in enumerate(all_screening_data):
        if index == 0:  # first line contains headers
            all_wards = line.split(csv_sep)
        else:
            line_data = line.split(csv_sep)
            for line_index, each_entry in enumerate(line_data):
                if line_index > 1 and each_entry != '':
                    sql_statement += f"INSERT INTO [Atelier_DataScience].[dbo].[WARD_SCREENINGS] VALUES" \
                                     f"( '{datetime.datetime.strptime(line_data[0], '%d.%m.%Y').strftime('%Y-%m-%d')}'," \
                                     f"'{all_wards[line_index]}', '{each_entry}' )\n"
    # Write statement to file
    with open(os.path.join(configuration['PATHS']['output_dir'], 'manual_sql_queries',
                           'update_ward_screenings.sql'), 'w') as writequery:
        writequery.write(sql_statement)


def recreate_hospital_map(csv_sep=';'):
    """
    Recreates the *Hospital Map* in the ``Atelier_DataScience``.

    This map links the following important entities in the model:

    - fachliche OE
    - pflegerische OE
    - official abbreviation of pflegerische OE
    - building in which pflegerische OE is located
    - floor of building in which pflegerische OE is located

    All information required is found in the ``[...]/vre_input/maps/insel_map.csv`` file. Its contents
    are used to create a query for updating the ``Atelier_DataScience.dbo.INSEL_MAP`` table. This query is written
    to the ``[...]/vre_output/manual_sql_queries`` folder and named ``update_insel_map.sql``, since the
    Atelier_Datascience_Reader does not have permission to execute TRUNCATE, DELETE, INSERT or UPDATE statements.

    Note:
        Floors are very important, since rooms are exported "floor-wise" from Waveware.

    Args:
        csv_sep (str):              separator used in the read file (defaults to ``;``)
    """
    # Prepare INSERT statement for table
    sql_statement = 'TRUNCATE TABLE [Atelier_DataScience].[dbo].[INSEL_MAP];\n'

    # Load data from file
    all_map_data = [each_line.replace('\n', '') for each_line in
                    open(os.path.join(configuration['PATHS']['input_dir'], 'maps', 'insel_map.csv'), 'r')]

    for index, each_line in enumerate(all_map_data):
        if index > 0:  # skip header row
            line_data = each_line.split(csv_sep)
            sql_statement += f'INSERT INTO [Atelier_DataScience].[dbo].[INSEL_MAP] VALUES ('
            sql_statement += f"'{line_data[0]}', "
            sql_statement += f"'{line_data[1]}', " if line_data[1] != '' else 'NULL, '
            sql_statement += f"'{line_data[2]}', " if line_data[2] != '' else 'NULL, '
            sql_statement += f"'{line_data[3]}', "
            sql_statement += f"'{line_data[4]}', "
            sql_statement += f"'{line_data[5]}', "
            sql_statement += f"'{line_data[6]}', " if line_data[6] != '' else 'NULL, '
            sql_statement += f"'{line_data[7]}', "
            sql_statement += f"'{line_data[8]}', " if line_data[8] != '' else 'NULL, '
            sql_statement += f"'{line_data[9]}' )\n"

    # Write statement to file
    with open(os.path.join(configuration['PATHS']['output_dir'], 'manual_sql_queries',
                           'update_insel_map.sql'), 'w') as writequery:
        writequery.write(sql_statement)


def recreate_care_oe_map(csv_sep=';'):
    """
    Recreates the map for pflegerische OEs in the ``Atelier_DataScience``.

    This map links "free-text" pflegerische OE names to the *official* names in the ``OE_pflege_abk`` column of the
     ``Atelier_DataScience.dbo.INSEL_MAP`` table.

    All information required is found in the ``[...]/vre_input/maps/oe_pflege_map.csv`` file. Its contents
    are used to create a query for updating the ``Atelier_DataScience.dbo.OE_PFLEGE_MAP`` table. This query is written
    to the ``[...]/vre_output/manual_sql_queries`` folder and named ``update_oe_pflege_map.sql``, since the
    Atelier_Datascience_Reader does not have permission to execute TRUNCATE, DELETE, INSERT or UPDATE statements.

    Args:
        csv_sep (str):              separator used in the read file (defaults to ``;``)
    """

    # Prepare INSERT statement for table
    sql_statement = 'TRUNCATE TABLE [Atelier_DataScience].[dbo].[OE_PFLEGE_MAP];\n'

    # Load data from file
    all_map_data = [each_line.replace('\n', '') for each_line in
                    open(os.path.join(configuration['PATHS']['input_dir'], 'maps', 'oe_pflege_map.csv'), 'r')]

    for index, each_line in enumerate(all_map_data):
        if index > 0:  # skip header row
            line_data = each_line.split(csv_sep)
            sql_statement += f'INSERT INTO [Atelier_DataScience].[dbo].[OE_PFLEGE_MAP] VALUES ('
            sql_statement += f"'{line_data[0]}', '{line_data[1]}' )\n"

    # Write statement to file
    with open(os.path.join(configuration['PATHS']['output_dir'], 'manual_sql_queries',
                           'update_oe_pflege_map.sql'), 'w') as writequery:
        writequery.write(sql_statement)


def recreate_screening_data(csv_sep=';'):
    """
    Recreates all screening data in the ``Atelier_DataScience``.

    All information required is found in the ``[...]/vre_input/screening_data/vre_screenings.csv`` file. Its contents
    are used to create a query for updating the ``Atelier_DataScience.dbo.VRE_SCREENING_DATA`` table. This query is
    written to the ``[...]/vre_output/manual_sql_queries`` folder and named ``update_VRE_SCREENING_DATA.sql``, since the
    Atelier_Datascience_Reader does not have permission to execute TRUNCATE, DELETE, INSERT or UPDATE statements.

    Args:
        csv_sep (str):              separator used in the read file (defaults to ``;``)
    """
    # Prepare INSERT statement for table
    sql_statement = 'TRUNCATE TABLE [Atelier_DataScience].[dbo].[VRE_SCREENING_DATA];\n'

    # Load data from file
    all_screening_data = [each_line.replace('\n', '') for each_line in
                          open(os.path.join(configuration['PATHS']['input_dir'], 'screening_data',
                                            'vre_screenings.csv'), 'r')]

    for index, each_line in enumerate(all_screening_data):
        if index > 0:  # skip header row
            line_data = each_line.split(csv_sep)
            sql_statement += f'INSERT INTO [Atelier_DataScience].[dbo].[VRE_SCREENING_DATA] VALUES ('
            sql_statement += f"'{line_data[0]}', "
            sql_statement += f"'{line_data[1]}', "
            sql_statement += f"'{line_data[2]}', " if line_data[2] != '' else 'NULL, '
            sql_statement += f"'{line_data[3]}', "
            sql_statement += f"'{line_data[4]}', " if line_data[4] != '' else 'NULL, '
            sql_statement += f"'{line_data[5]}', " if line_data[5] != '' else 'NULL, '
            sql_statement += f"'{line_data[6]}', " if line_data[6] != '' else 'NULL, '
            sql_statement += f"'{line_data[7]}', " if line_data[7] != '' else 'NULL, '
            sql_statement += f"'{line_data[8]}', "
            sql_statement += f"'{line_data[9]}', " if line_data[9] != '' else 'NULL, '
            sql_statement += f"'{line_data[10]}', "
            sql_statement += f"'{line_data[11]}', " if line_data[11] != '' else 'NULL, '
            sql_statement += f"'{line_data[12]}', "
            sql_statement += f"'{line_data[13]}' )\n" if line_data[13] != '' else 'NULL) \n'

    # Write statement to file
    with open(os.path.join(configuration['PATHS']['output_dir'], 'manual_sql_queries', 'update_VRE_SCREENING_DATA.sql'), 'w') as writequery:
        writequery.write(sql_statement)


if __name__ == '__main__':
    # Extract correct filepath
    this_filepath = os.path.dirname(os.path.realpath(__file__))
    # contains the directory in which this script is located, irrespective of the current working directory
    # #>> Preprocess data:
    print('Pre-processing data:')
    # 1) Ward Overview Data
    print('--> Ward Overview Data... ', end='')
    recreate_ward_overview_data()
    print('Done !\n')

    # 2) Hospital Map
    print('--> Hospital Map... ', end='')
    recreate_hospital_map()
    print('Done !\n')

    # 3) Pflegerische OE Map
    print('--> Pflegerische OE Map... ', end='')
    recreate_care_oe_map()
    print('Done !\n')

    # 4) Screening Data
    print('--> Screening Data... ', end='')
    recreate_screening_data()
    print('Done !\n')
