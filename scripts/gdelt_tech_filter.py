### MAIN PROGRAM ###
def main(input_filename, output_filename, columns, gdrive_cred_file , gdrive_folder_id, save_option):
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
    # initialise regex patterns
    tech_terms = define_tech_terms()
    add_regex_pattern(tech_terms)
    ### Text processing and filtering ###
    # read CSV
    input_filepath = os.path.join(settings['DEFAULT']['raw_data_folder'], settings['GDELT']['subfolder'], input_filename)
    df = pd.read_csv(input_filepath)
    # combine text columns
    input_cols = ast.literal_eval(columns)
    df['combined_text'] = ''
    for col in input_cols:
        df['combined_text'] = df['combined_text'] + ' ' + df[col].astype(str)
    # regex match
    for tech in tech_terms:
        df[tech['tech']] = df['combined_text'].str.contains(tech['regex'], na=False)
    # get list of output columns
    output_cols = [tech['tech'] for tech in tech_terms]
    # filter dataframe
    df.dropna(how='all', subset=output_cols, inplace=True)
    df.drop(columns='combined_text', inplace=True)

    ### Save data as CSV in local drive ###
    # define filename
    output_filepath = os.path.join(settings['DEFAULT']['processed_data_folder'], settings['GDELT']['subfolder'], output_filename)
    print(f'Saving filtered data as {output_filepath}')
    # save as CSV
    df.to_csv(output_filepath)

    ### Save data as CSV in Google Drive ###
    if (save_option == 'gdrive'):
        # authenticate and create Google Drive client
        gdrive = create_gdrive_client(gdrive_cred_file)
        # upload file to Google Drive
        upload_file(gdrive, gdrive_folder_id, output_filename)
        print('Data saved in Google Drive')
    return

### SCRIPT TO RUN WHEN CALLED STANDALONE ###
if __name__=='__main__':
    # input arguments
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--input_filename', help='name of input CSV file')
    parser.add_argument('--output_filename', help='name for output CSV file')
    parser.add_argument('--columns', default=['Extras'], help='')
    parser.add_argument('--gdrive_cred_file', default=r'../auth/gdrive_credentials.txt', help='path to Google Drive credentials file')
    parser.add_argument('--gdrive_folder_id', default='17Jd7UpDaN230tO_U3MTFuE4GDbfYSIv6', help='Google Drive folder ID')
    parser.add_argument('--save', type=str, help = "value determines how the data will be saved. See config.ini for default and valid options")
    args = parser.parse_args()

    # run main
    main(args.input_filename, args.output_filename, args.columns, args.gdrive_cred_file , args.gdrive_folder_id, args.save)