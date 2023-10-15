### SUB-FUNCTIONS ###
def extract_filename(url_str, ext_index=0):
    # extracts filename, splits by '.' and returns item specified by ext_index 
    import os
    try:
        return os.path.split(str(url_str))[-1].split('.')[ext_index]
    except:
        return None

def update_master_file(csv_filepath):
    import pandas as pd
    # read GDELT 2.0 Global Knowledge Graph master list
    master_list_url = r'http://data.gdeltproject.org/gdeltv2/masterfilelist.txt'
    # download text file, read into dataframe
    master_df = pd.read_csv(master_list_url, delimiter=' ', names=['size','hash','url'])
    # remove rows with null values
    master_df = master_df.dropna()
    # extra file types from URLs
    master_df['type'] = master_df['url'].apply(extract_filename, args=(1,))
    # keep only Global Knowledge Graph (gkg) files
    master_df = master_df.loc[master_df['type']=='gkg']
    # extract datetimes from URLs
    master_df['datetime_str'] = master_df['url'].apply(extract_filename)
    master_df['datetime']= pd.to_datetime(master_df['datetime_str'])
    # save as CSV
    master_df.to_csv(csv_filepath, index=None)
    return master_df

def define_gkg_header(mode='all'):
    if mode=='all':
        return ['GKGRECORDID','DATE','SourceCollectionIdentifier','SourceCommonName',\
                'DocumentIdentifier', 'Counts', 'V2Counts', 'Themes', 'V2Themes', 'Locations', \
                'V2Locations','Persons','V2Persons','Organizations','V2Organizations','V2Tone',\
                'Dates','GCAM','SharingImage','RelatedImages','SocialImageEmbeds',\
                'SocialVideoEmbeds','Quotations','AllNames','Amounts','TranslationInfo','Extras']
    elif mode=='usecols':
        return ['GKGRECORDID','DATE','SourceCollectionIdentifier','SourceCommonName',\
                'DocumentIdentifier','Counts','V2Counts','V2Locations','V2Persons',\
                'V2Organizations','Dates','SharingImage','RelatedImages','SocialImageEmbeds',\
                'SocialVideoEmbeds','Quotations','AllNames','Amounts','TranslationInfo','Extras']
    else:
        return
    
### MAIN PROGRAM ###
def main(before, after, update_master=True, save_option='local'):
    ### Initialise ###
    # import libraries
    import os
    import configparser
    import pandas as pd
    from tqdm import tqdm
    import urllib
    from src.google_drive import create_gdrive_client, upload_file
    config_file = '../config.ini'
    settings = configparser.ConfigParser(inline_comment_prefixes="#")
    settings.read(config_file)

    ### Master file list ###
    # define filepath for master list
    master_csv_filepath = os.path.normpath(settings['GDELT']['master_filepath'])
    # either download master file or use local copy
    if (not os.path.isfile(master_csv_filepath)) or (update_master==True):
        print('Getting the latest master file list from data.gdeltproject.org')
        master_df = update_master_file(master_csv_filepath)
    else:
        print(f'Using the local master file list in {master_csv_filepath}')
        master_df = pd.read_csv(master_csv_filepath)

    ### Get URLs within date range ###
    # TODO: type check the datetime input arguments
    # apply datetime range filter to master list dataframe
    datetime_mask = (master_df['datetime'] > after) & (master_df['datetime'] <= before)   # assumes datetime is end of period
    print(f'Getting files between the start of {after} and the start of {before}')
    filtered_master_df = master_df.loc[datetime_mask]

    ### Download CSVs and append into a dataframe ###
    # initialise dataframe and header names for GKG data
    gkg_df = pd.DataFrame()
    gkg_header = define_gkg_header('all')
    http_err_count = 0
    # for each URL in master list range
    for url in tqdm(filtered_master_df['url'].to_list(), desc="Downloading files"):
        # read zipped CSV file, select only required columns
        try:
            file_df = pd.read_csv(url, compression='zip', encoding='utf-8', encoding_errors='replace', \
                                sep='\t', names=gkg_header, usecols=define_gkg_header('usecols'))
        # skip if http error
        except urllib.error.HTTPError as err:
            http_err_count += 1
        # append into dataframe
        gkg_df = pd.concat([gkg_df, file_df])
    if http_err_count > 0:
        print(f'{http_err_count} files skipped due to HTTP errors')


    

    ### Save data as CSV in LOCAL Drive ###
    # define filename
    gkg_csv_filename = f'gdelt_gkg_{after}_{before}.csv.gz'
    print(f'Saving GKG data as {gkg_csv_filename}')
    filepath = settings['DEFAULT']['raw_data_folder'] + settings['GDELT']['subfolder']
    # save in compressed format
    gkg_df.to_csv(filepath + gkg_csv_filename, index=None, compression='infer')

    ### Save data as CSV in Google Drive ###
    if (save_option is not None):
        if (save_option == 'gdrive'):
            # authenticate and create Google Drive client
            gdrive = create_gdrive_client(settings['GDRIVE']['credentials'])
            gdrive_folder_id = settings['GDRIVE.RAWDATA.FOLDER_IDS']['gdelt_data']
            # upload file to Google Drive
            upload_file(gdrive, gdrive_folder_id, gkg_csv_filename)
            print('Data saved in Google Drive')
    

def get_month():
    from datetime import date
    from datetime import timedelta
    d = date.today()
    end = d.replace(day=1) - timedelta(days = 1)
    start = end.replace(day=1)
    return str(start), str(end)

### SCRIPT TO RUN WHEN CALLED STANDALONE ###
if __name__=='__main__':
    # input arguments
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--after', help='date input in the format YYYY-MM-DD')
    parser.add_argument('--before', help='date input in the format YYYY-MM-DD')
    parser.add_argument('--month', action='store_true', help = 'set search range to last month (default value)')
    parser.add_argument('--update_master', action=argparse.BooleanOptionalAction, default=True, help='download and save the master file list from GDELT')
    parser.add_argument('--save', type=str, help = "value determines how the data will be saved. See config.ini for default and valid options")
    args = parser.parse_args()

    ## check number of date options used are valid.
    d = 0
    if (args.month):
        d = d + 1
    if (args.after != None or args.before != None):
        d = d + 1

    ## If both month and before/after are used together, return error message
    if (d > 1):
        print("cannot use --month (last month) together with --before & --after. Refer to documentation for guidance.")
    
    else:
        # set before and after
        # trigger: --month used, or no selection was picked.
        if (args.month or d == 0):
            after, before = get_month()
        else:
            before = args.before
            after = args.after

        # run main
        main(before, after, args.update_master, args.save)