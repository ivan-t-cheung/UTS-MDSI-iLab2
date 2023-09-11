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
def main(before, after, data_path=r'../data', update_master=True):
    ### Initialise ###
    # import libraries
    import os
    import pandas as pd
    from tqdm import tqdm

    ### Master file list ###
    # define filepath for master list
    master_csv_filepath = os.path.join(data_path, 'meta', 'gdelt_gkg_masterfilelist.csv')
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
    print(f'Getting files between start of {after} and end of {before}')
    filtered_master_df = master_df.loc[datetime_mask]

    ### Download CSVs and append into a dataframe ###
    # initialise dataframe and header names for GKG data
    gkg_df = pd.DataFrame()
    gkg_header = define_gkg_header('all')
    # for each URL in master list range
    for url in tqdm(filtered_master_df['url'].to_list(), desc="Downloading files"):
        # read zipped CSV file, select only required columns
        file_df = pd.read_csv(url, compression='zip', encoding='utf-8', encoding_errors='replace', \
                              sep='\t', names=gkg_header, usecols=define_gkg_header('usecols'))
        # append into dataframe
        gkg_df = pd.concat([gkg_df, file_df])

    ### Save data as CSV ###
    gkg_csv_filepath = os.path.join(data_path, 'raw', f'gdelt_gkg_{after}_{before}.csv.zip')
    print(f'Saving GKG data as {gkg_csv_filepath}')
    gkg_df.to_csv(gkg_csv_filepath, index=None, compression='infer')

### SCRIPT TO RUN WHEN CALLED STANDALONE ###
if __name__=='__main__':
    # input arguments
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--after', help='date input in the format YYYY-MM-DD')
    parser.add_argument('--before', help='date input in the format YYYY-MM-DD')
    parser.add_argument('--data_path', default=r'../data', help='path to data folder, must contain "raw" and "meta" subfolders')
    parser.add_argument('--update_master', action=argparse.BooleanOptionalAction, default=True, help='download and save the master file list from GDELT')
    args = parser.parse_args()

    # run main
    main(args.before, args.after, args.data_path, args.update_master)