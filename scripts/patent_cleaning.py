## Libraries
import glob
from pathlib import Path
import configparser
import argparse

# - to extract gz files
import gzip
import json

# - to convert json to dataframe
import pandas as pd

## load config.ini
config_file = '../config.ini'
settings = configparser.ConfigParser(inline_comment_prefixes="#")
settings.read(config_file)


## read the processed log and compare against files in folder.
## return the files that have not yet been processed
def identify_new_files():
    ## read the raw files
    path = settings['DEFAULT']['raw_data_folder'] + settings['LENS_API.PATENTS']['subfolder'] + '*'
    files = glob.glob(path)
    s1 = pd.Series(files)

    ## read the processed log
    df = pd.read_csv('../data/meta/process_log/processed_patents.csv')
    s2 = df['processed files']

    ## compare and return pd Series

    s3 = s1[~ s1.isin(s2)]
    return s3

def append_processed_files_to_log(files):
    df = pd.read_csv('../data/meta/process_log/processed_patents.csv')
    new_df = pd.DataFrame(pd.concat([df['processed files'], files]), columns=['processed files'])
    new_df.to_csv('../data/meta/process_log/processed_patents.csv')
    return


def save_data_gdrive(patents_data, patents_classifications, patents_applicants, patents_inventors):
    from src.google_drive import create_gdrive_client, upload_file
    from journal_cleaning import clean_journal

    #get google drive info
    gdrive_cred_file = settings['GDRIVE']['credentials']
    gdrive_folder_id = settings['GDRIVE.FOLDER_IDS']['patent_data']

    # authenticate and create Google Drive client
    gdrive = create_gdrive_client(gdrive_cred_file)
    # upload file to Google Drive
    upload_file(gdrive, gdrive_folder_id, patents_data)
    upload_file(gdrive, gdrive_folder_id, patents_classifications)
    upload_file(gdrive, gdrive_folder_id, patents_applicants)
    upload_file(gdrive, gdrive_folder_id, patents_inventors)
    print('Data saved in Google Drive')

    return

def save_data_azure(patents_data, patents_classifications, patents_applicants, patents_inventors):
    print('Save to Azure has not been configured. Action skipped')
    return

def main(save_to = None):
    files = identify_new_files()

    for file in files:
        patents_data = []
        patents_classifications = []
        patents_applicants = []
        patents_inventors = []
        file_ext = Path(file).suffix

        if (file_ext =='.gz'):
            f = gzip.open(file, 'rt', encoding="ascii", errors="ignore")
        if (file_ext =='.json'):
            file_reader = open(file)
            data = json.load(file_reader)
            f = data['data']
            file_reader.close()
        
        for line in f:
            patent = ''
            if (file_ext =='.gz'):
                patent = json.loads(line)
            if (file_ext =='.json'):
                patent = line

            if patent['biblio'].get('invention_title') is None:         ## if invention title is None, then do not capture, skip to next record.
                continue                                                

            abstract = patent.get('abstract', 'na')
            if abstract != 'na':
                abstract = abstract[0]['text']
            data = {
                'lens_id': patent['lens_id'],
                'jurisdiction': patent['jurisdiction'],
                'patent_id': patent['doc_key'],
                'date_published': patent['date_published'],
                'title': patent['biblio']['invention_title'][0]['text'],
                'abstract': abstract
            }
            patents_data.append(data)

            for applicant in patent['biblio']['parties']['applicants']:
                app_data = {
                    'lens_id': patent['lens_id'],
                    'patent_id': patent['doc_key'],
                    'residence': applicant.get('residence', 'NA'),
                    'name': applicant['extracted_name']['value']
                }
                patents_applicants.append(app_data)
            
            if patent['biblio']['parties'].get('inventors') is not None:
                for inventor in patent['biblio']['parties']['inventors']:
                    inv_data = {
                        'lens_id': patent['lens_id'],
                        'patent_id': patent['doc_key'],
                        'residence': inventor.get('residence', 'NA'),
                        'name': inventor['extracted_name']['value']
                    }
                    patents_inventors.append(inv_data)

            #print('process classifications')
            classifications_cpc = patent['biblio'].get('classifications_cpc')
            if classifications_cpc is not None:
                for classification in classifications_cpc['classifications']:
                    #print('process classification')
                    class_data = {
                        'lens_id': patent['lens_id'],
                        'patent_id': patent['doc_key'],
                        'classification': classification['symbol']
                    }
                    patents_classifications.append(class_data)

            del(patent)    ## clear variable from memory
            del(line)

        ## save data to parquet
        filename = Path(Path(file).stem).stem
        path = settings['DEFAULT']['processed_data_folder'] + settings['LENS_API.PATENTS']['subfolder']

        pd.DataFrame(patents_data).to_parquet(path + filename + "_data.parquet", index=False)
        pd.DataFrame(patents_classifications).to_parquet(path + filename + "_classifications.parquet", index=False)
        pd.DataFrame(patents_applicants).to_parquet(path + filename + "_applicants.parquet", index=False)
        pd.DataFrame(patents_inventors).to_parquet(path + filename + "_inventors.parquet", index=False)

        if save_to is not None:
            if save_to == 'gdrive':
                save_data_gdrive(path + filename + "_data.parquet", 
                                 path + filename + "_classifications.parquet", 
                                 path + filename + "_applicants.parquet", 
                                 path + filename + "_inventors.parquet")
            if save_to == 'azure':
                save_data_azure(path + filename + "_data.parquet", 
                                 path + filename + "_classifications.parquet", 
                                 path + filename + "_applicants.parquet", 
                                 path + filename + "_inventors.parquet")

        del(patents_data)
        del(patents_classifications)
        del(patents_applicants)
        del(patents_inventors)

    ## append files to processed log
    append_processed_files_to_log(files)





## Execute main
if __name__ == "__main__":
    # Define the command-line argument parser
    parser = argparse.ArgumentParser(description='Parse patent data from Lens.org.')
    parser.add_argument('--save', dest='save_to', type=str, help = "value determines how the data will be saved. See config.ini for default and valid options")
    args = parser.parse_args()



    main(parser.save_to)
