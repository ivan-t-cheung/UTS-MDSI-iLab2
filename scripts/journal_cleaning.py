## Libraries
import glob
from pathlib import Path
import argparse
import os
import json
import pandas as pd
import configparser
from src.author_info import extract_author_info

## load config.ini
config_file = '../config.ini'
settings = configparser.ConfigParser(inline_comment_prefixes="#")
settings.read(config_file)

# Define the folder path
src_folder_path = settings['DEFAULT']['raw_data_folder'] + settings['LENS_API.JOURNALS']['subfolder']
dest_folder = settings['DEFAULT']['processed_data_folder'] + settings['LENS_API.JOURNALS']['subfolder']


def append_processed_files_to_log(files):
    df = pd.read_csv('../data/meta/process_log/processed_journals.csv')
    new_df = pd.DataFrame(pd.concat([df['processed files'], files]), columns=['processed files'])
    new_df.to_csv('../data/meta/process_log/processed_journals.csv')
    return

def clean_journal(files):
    # Initialize an empty list to store DataFrames from each file
    dfs = []

    # Loop through each JSON file and read it into a DataFrame
    for json_file in files:
        # Read the JSON data from the file
        with open(json_file, 'r') as json_data:
            data = json.load(json_data)
            content = data['data']

            # Convert the JSON content into a DataFrame
            df = pd.DataFrame.from_dict(content)
            # Append the DataFrame to the list
            dfs.append(df)
        
        ## after reading, add file to processed log
        append_processed_files_to_log(json_file)

    # Concatenate all DataFrames in the list into a single DataFrame
    df = pd.concat(dfs, ignore_index=True)
    df['date_published'] = pd.to_datetime(df['date_published']).dt.strftime('%y-%m-%d')
    # Apply the function to create the "author", "institution", and "country" columns
    df[['author', 'institution', 'country']] = df.apply(extract_author_info, axis=1).apply(pd.Series)
    # drop authors column
    df.drop(['authors'], axis=1, inplace=True)
    # Convert the list of strings into a single string, separated by commas
    df['fields_of_study'] = df['fields_of_study'].apply(lambda x: ', '.join(x) if isinstance(x, list) else '')
    df['keywords'] = df['keywords'].apply(lambda x: ', '.join(x) if isinstance(x, list) else '')


    ## save to dest folder
    first_dt = df['date_published'].min()
    last_dt = df['date_published'].max()
    filename = dest_folder + f'journals_{first_dt}_to_{last_dt}.csv'
    df.to_csv(filename, index = False)

    return filename


def save_data_gdrive(file):
    from src.google_drive import create_gdrive_client, upload_file
    from journal_cleaning import clean_journal

    #get google drive info
    gdrive_cred_file = settings['GDRIVE']['credentials']
    gdrive_folder_id = settings['GDRIVE.FOLDER_IDS']['journal_data']

    # authenticate and create Google Drive client
    gdrive = create_gdrive_client(gdrive_cred_file)
    # upload file to Google Drive
    upload_file(gdrive, gdrive_folder_id, file)
    print('Data saved in Google Drive')
    return

def save_data_azure(file):
    print('Save to Azure has not been configured. Action skipped')
    return

## read the processed log and compare against files in folder.
## return the files that have not yet been processed
def identify_new_files():
    ## read the raw files
    path = src_folder_path + '*'
    files = glob.glob(path)
    s1 = pd.Series(files)

    ## read the processed log
    df = pd.read_csv('../data/meta/process_log/processed_journals.csv')
    s2 = df['processed files']

    ## compare and return pd Series
    s3 = s1[~ s1.isin(s2)]
    return s3


def main(save_to = None):
    files = identify_new_files()
    filename = clean_journal(files)

    if save_to is not None:
            if save_to == 'gdrive':
                save_data_gdrive(filename)
            if save_to == 'azure':
                save_data_azure(filename)


    return
    

## Execute main
if __name__ == "__main__":
    # Define the command-line argument parser
    parser = argparse.ArgumentParser(description='Parse journal data from Lens.org.')
    parser.add_argument('--save', dest='save_to', type=str, help = "value determines how the data will be saved. See config.ini for default and valid options")
    args = parser.parse_args()

    main(parser.save_to)