### SUB FUNCTIONS ###
def define_dimension_cols():
    return [{'dimension': 'locations',
             'input': 'V2Locations',
             'duplicate_index': 2,
             'delim': '#',
             'outputs': ['location_type','location_name','country_code','adm1_code','adm2_code','latitude','longitude','feature_id','text_position','extra']},
            {'dimension': 'organisations',
             'input': 'V2Organizations',
             'duplicate_index': 1,
             'delim': ',',
             'outputs': ['org_name','text_position']},
            {'dimension': 'persons',
             'input': 'V2Persons',
             'duplicate_index': 1,
             'delim': ',',
             'outputs': ['person_name','text_position']},
            {'dimension': 'names',
             'input': 'AllNames',
             'duplicate_index': 1,
             'delim': ',',
             'outputs': ['name','text_position']}]

def create_dimension_df(df, input_col, output_cols, delim, duplicate_index):
    import pandas as pd
    dim_cols = pd.concat([df.loc[:,'GKGRECORDID'], df.loc[:,input_col].str.split(';')], axis=1, ignore_index=True)
    dim_cols = dim_cols.explode(1)
    out_df = pd.concat([dim_cols.iloc[:,0], dim_cols.iloc[:,1].str.split(delim, expand=True)], axis=1, ignore_index=True)
    out_df.drop_duplicates(subset=[out_df.columns[0],out_df.columns[duplicate_index]], inplace=True)
    output_cols = ['record_id'] + output_cols
    return out_df.set_axis(output_cols, axis=1)

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
    input_path = os.path.join(settings['DEFAULT']['filtered_data_folder'], settings['GDELT']['subfolder'])
    output_path = os.path.join(settings['DEFAULT']['dashboard_data_folder'])

    ### Identify and read new files ###
    # get a list of files in input folder
    all_files = glob.glob(input_path + "*_filtered.csv")
    try:
        # read the log of ingested files
        ingested_files = pd.read_csv(os.path.join(input_path, 'ingested_files.csv'))['filenames'].to_list()
    except pd.errors.EmptyDataError:
        new_files = all_files
    else:    
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
        dim['df'] = create_dimension_df(record_df, dim['input'], dim['outputs'], dim['delim'], dim['duplicate_index'])
    # catagorise technologies
    record_df['tech'] = record_df[['quantum', 'semiconductors', 'cell-based meats', 'hydrogen power', 'personalised medicine']].idxmax(1)
    # select columns for main records table
    select_cols = {'GKGRECORDID':'record_id', 'DATE':'date', 'SourceCommonName':'domain', 'DocumentIdentifier':'url', 'tech':'technology'}
    record_df = record_df.rename(columns=select_cols)[select_cols.values()]

    ### Append new data to files ###  
    dims.append({'dimension': 'record', 'df': record_df})
    print(len(dims))
    for dim in dims:
        output_filepath = os.path.join(output_path, f'gdelt_{dim["dimension"]}.csv')
        dim['df'].to_csv(output_filepath, mode='a', index=False)
        
        ### Save data as CSV in Google Drive ###
        if (save_option == 'gdrive'):
            # authenticate and create Google Drive client
            gdrive = create_gdrive_client(gdrive_cred_file)
            # upload file to Google Drive
            upload_file(gdrive, gdrive_folder_id, output_filepath)
            print('Data saved in Google Drive')

    ### Append files to ingested log ###
    new_files_df = pd.DataFrame(new_files, columns=['filenames'])
    new_files_df.to_csv(os.path.join(input_path, 'ingested_files.csv'), mode='a', index=False, header=False)
    return

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