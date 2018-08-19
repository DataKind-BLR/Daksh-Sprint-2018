# python 3.6
import pandas as pd
import sqlite3
import sqlalchemy
import os
import re

def combine_data(filenames, outfilename):
    '''
    Combine the data of cases and hearing into a single file for analysis

    Args:
        filenames(list(str)): The list of filenames to be combined
        outfilename(str): The filename where to store the combined data.
    '''
    skip_header = False
    with open(outfilename, 'w') as outputfile:
        for fn in filenames:
            with open(fn, 'r') as datafile:
                if skip_header:
                    datafile.readline()
                for line in datafile.readlines():
                    outputfile.write(line)
            skip_header = True
    return True

def generate_filename(folder_path, ext):
    '''
    Generate a filename to store the combined data

    Args:
        folder_path(str): The folder path from where data is being processed
        ext(str): This can be cases or hearings based on what we are processing
    '''
    name_from_folder_path = folder_path.strip('/').split('/')[-1]
    name = '_'.join(name_from_folder_path.lower().split())
    return '{}_{}.csv'.format(name, ext)

def filter_out_columns(data):
    '''
    Filter out unwanted columns by checking the number of empty entries in the
    column
    '''
    non_udef_columns = [col for col in data.columns if not col.startswith('udef')]
    cols_without_nans = []
    for col in non_udef_columns:
        nan_ratio = pd.isnull(data[col]).sum()/data.shape[0]
        if nan_ratio < .5:
            cols_without_nans.append(col)
    return data[cols_without_nans]

def process_and_store_data(filename, cases=True):
    '''
    Process and store the data in sqlite db for further use.

    Args:
        filename(str): filename of the combined data generated
        cases(boolean): A boolean to represent what we are processing
    '''
    engine = sqlalchemy.create_engine('sqlite:///daksh_db.sqlite')
    data = pd.read_csv(filename)
    processed_data = filter_out_columns(data)
    table_name = 'cases' if cases else 'hearings'
    processed_data.to_sql(name=table_name, if_exists='replace', con=engine, chunksize=10000)
    processed_data_filename = filename.split('.')[0] + '_processed.csv'
    processed_case_files.to_csv(processed_data_filename)
    return True

def main(folder_path):
    '''
    The main function that takes the folder path of the district and generates
    cases and hearing data files and sqlite db version.
    '''
    cases_files = [os.path.join(folder_path, fn) for fn in os.listdir(folder_path) if 'CaseInfo' in fn]
    hearing_files = [os.path.join(folder_path, fn) for fn in os.listdir(folder_path) if 'HistoryTableRows' in fn]
    case_file_name = generate_filename(folder_path, 'cases')
    combine_data(cases_files, case_file_name)
    # hearing data might not be present in all folders.
    if len(hearing_files) > 0:
        hearing_file_name = generate_filename(folder_path, 'hearings')
        combine_data(cases_files, hearing_file_name)
    process_and_store_data(case_file_name)
    process_and_store_data(hearing_file_name, False)
    return True
