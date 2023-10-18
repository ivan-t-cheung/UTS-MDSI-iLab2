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

def clean_journal():
    # Initialize an empty list to store DataFrames from each file
    dfs = []
    # List all the JSON files in the folder
    json_files = [f for f in os.listdir(src_folder_path) if f.endswith('.json')]

    # Loop through each JSON file and read it into a DataFrame
    for json_file in json_files:
        # Construct the full file path
        file_path = os.path.join(src_folder_path, json_file)

        # Read the JSON data from the file
        with open(file_path, 'r') as json_data:
            data = json.load(json_data)
            content = data['data']

            # Convert the JSON content into a DataFrame
            df = pd.DataFrame.from_dict(content)
            # Append the DataFrame to the list
            dfs.append(df)

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

    filename = dest_folder + "journals.csv"
    # save csv file in processed folder
    df.to_csv(filename, index = False)

    return filename