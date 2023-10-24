### MAIN PROGRAM ###
def main(source, input_filename, output_filename, save_option):
    ### Initialise ###
    # import libraries
    import os, ast
    import configparser
    import pandas as pd
    from src.google_drive import create_gdrive_client, upload_file
    from src.regex import define_tech_terms, add_regex_pattern
    config_file = '../config.ini'
    # read settings from config file
    settings = configparser.ConfigParser(inline_comment_prefixes="#")
    settings.read(config_file)
    ##load gdrive data
    gdrive_cred_file = settings['GDRIVE']['credentials']
    

    # initialise regex patterns
    tech_terms = define_tech_terms()
    add_regex_pattern(tech_terms)
    ### Text processing and filtering ###
    # create input filepath
    input_filepath = os.path.join(settings['DEFAULT']['processed_data_folder'], settings[source]['subfolder'], input_filename)
    if(source == 'GDELT'):
        input_filepath = os.path.join(settings['DEFAULT']['raw_data_folder'], settings[source]['subfolder'], input_filename)
    print(f'Reading file {input_filepath}')
    # read CSV or parquet based on file extension
    file_extension = input_filename.split('.')[1].lower()
    if file_extension=='csv':
        df = pd.read_csv(input_filepath)
    elif file_extension=='parquet':
        df = pd.read_parquet(input_filepath)
    else:
        raise ValueError('Input file must be a CSV or parquet')
    # combine text columns
    input_cols = ast.literal_eval(settings[source]['filter_text_fields'])
    df['combined_text'] = ''
    for col in input_cols:
        df['combined_text'] = df['combined_text'] + ' ' + df[col].astype(str)
    # regex match
    for tech in tech_terms:
        print(f'Regex matching for {tech["tech"]}')
        df[tech['tech']] = df['combined_text'].str.contains(tech['regex'], na=False)
    # get list of output columns
    output_cols = [tech['tech'] for tech in tech_terms]
    # filter dataframe
    df.drop(columns='combined_text', inplace=True)
    filtered_df = df[df[output_cols].any(axis='columns')]

    ### Save data as CSV in local drive ###
    # define output filepath
    if output_filename is None:
        output_filename = input_filename.split('.')[0] + '_filtered.csv'
    output_filepath = os.path.join(settings['DEFAULT']['filtered_data_folder'], settings[source]['subfolder'], output_filename)
    print(f'Saving filtered data as {output_filepath}')
    # save as CSV
    filtered_df.to_csv(output_filepath)

    ### Save data as CSV in Google Drive ###
    if (save_option == 'gdrive'):
        gdrive_folder_id = settings['GDRIVE.FILTERED.FOLDER_IDS'][source]
        # authenticate and create Google Drive client
        gdrive = create_gdrive_client(gdrive_cred_file)
        # upload file to Google Drive
        upload_file(gdrive, gdrive_folder_id, output_filepath)
        print('Data saved in Google Drive')
    if (save_option == 'azure'):
        print('Save to Azure has not been configured. Action skipped')

    return

### SCRIPT TO RUN WHEN CALLED STANDALONE ###
if __name__=='__main__':
    # input arguments
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--source', help='choose from GDELT, LENS_API.PATENTS, LENS_API.JOURNALS')
    parser.add_argument('--input_filename', help='name of input CSV file')
    parser.add_argument('--output_filename', default=None, help='name for output CSV file')
    parser.add_argument('--save', default=None, type=str, help = "value determines how the data will be saved. See config.ini for default and valid options")
    args = parser.parse_args()

    # run main
    main(args.source, args.input_filename, args.output_filename, args.save)