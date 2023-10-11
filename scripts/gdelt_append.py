### SUB FUNCTIONS ###
def define_dimension_cols():
    return [{'dimension': 'locations',
             'input': 'LocationsV2',
             'outputs': ['location_type','location_name','country_code','adm1_code','adm2_code','','latitude','longitude','text_position']},
            {'dimension': 'organisations',
             'input': 'OrganizationsV2',
             'outputs': ['org_name','text_position']},
            {'dimension': 'persons',
             'input': 'PersonsV2',
             'outputs': ['person_name','text_position']},
            {'dimension': 'names',
             'input': 'NamesV2',
             'outputs': ['name','text_position']}]

def create_dimension_df(df, input_col, output_cols):
    import pandas as pd
    cols = df.loc[:,input_col].str.split(';').explode().str.split('#', expand=True)
    return pd.concat(df.loc[:,'id'], cols).rename(columns=(['id'] + output_cols))

### MAIN PROGRAM ###
def main(gdrive_cred_file , gdrive_folder_id, save_option):
    ### Initialise ###
    # import libraries
    import os, glob
    import configparser
    import pandas as pd
    from src.google_drive import create_gdrive_client, upload_file
    # read settings from config file
    config_file = '../config.ini'
    settings = configparser.ConfigParser(inline_comment_prefixes="#")
    settings.read(config_file)
    input_path = os.path.normpath(settings['DEFAULT']['processed_data_folder'], settings['GDELT']['sub_folder'])
    output_path = os.path.normpath(settings['DEFAULT']['dashboard_data_folder'], settings['GDELT']['sub_folder'])

    ### Identify and read new files ###
    # read the log of ingested files
    ingested_files = pd.read_csv(os.path.join(input_path, 'ingested_files.csv'))['filenames'].to_list()
    # get a list of files in input folder
    all_files = glob.glob(os.path.join(input_path , "/*_filtered.csv"))
    new_files = [filename for filename in all_files if filename not in ingested_files]
    # read all unread files
    new_df_list = []
    for filename in new_files:
        df = pd.read_csv(filename)
        new_df_list.append(df)
    # combine
    record_df = pd.concat(new_df_list, axis=0)

    ### Parse dimension features ###
    dims = define_dimension_cols()
    for dim in dims:
        dim['df'] = create_dimension_df(df, dim['input'], dim['outputs'])
    # select columns for main records table
    select_cols = {'':'record_id', '':'date', '':'domain', '':'url', '':'technology'}
    record_df = record_df.rename(columns=select_cols)[select_cols.values()]

    ### Append new data to files ###  
    output_df_list = [record_df] + [dim['df'] for dim in dims]
    for df in output_df_list:
        output_filepath = os.path.join(output_path, 'gdelt_records.csv')
        df.to_csv(output_filepath, mode='a', index=False, header=False)
        
        ### Save data as CSV in Google Drive ###
        if (save_option == 'gdrive'):
            # authenticate and create Google Drive client
            gdrive = create_gdrive_client(gdrive_cred_file)
            # upload file to Google Drive
            upload_file(gdrive, gdrive_folder_id, output_filepath)
            print('Data saved in Google Drive')
        return

    ### Append files to ingested log ###
    new_files_df = pd.DataFrame(new_files, columns=['filenames'])
    new_files_df.to_csv(input_path, mode='a', index=False, header=False)

### SCRIPT TO RUN WHEN CALLED STANDALONE ###
if __name__=='__main__':
    # input arguments
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--gdrive_cred_file', default=r'../auth/gdrive_credentials.txt', help='path to Google Drive credentials file')
    parser.add_argument('--gdrive_folder_id', default='1zsKuXBfbf9rowN32mOpkpVbZJFGAgPQA', help='Google Drive folder ID')
    parser.add_argument('--save', default=None, type=str, help = "value determines how the data will be saved. See config.ini for default and valid options")
    args = parser.parse_args()

    # run main
    main(args.gdrive_cred_file , args.gdrive_folder_id, args.save)