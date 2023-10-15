import json
import sys
import configparser
import requests
import argparse

search_url = 'https://api.lens.org/scholarly/search'
auth_json = '../auth/api_auth.json' 
q_type = 'Journal'                               ## set the publication types to retrieve, see https://docs.api.lens.org/response-scholar.html                                     ## set empty year
q_size = 50000                                   ## set the number of journals to return each query. For paid licences change this number to 1,000 - 10,000
max_limit = 999999                               ## set the limit on the number of results to query for. This will override the max results if lower.
 

# Define the filters for match
filters_dict = {
    'source.type': q_type,
    'language': "en",                 
    'is_open_access': True,
    'has_abstract': True
}


###
# Get API authorisation code from file.
###
def get_auth():
    global authkey

    api_auth = open(auth_json, "r")
    authkey = json.load(api_auth)['lens']
    api_auth.close()

    return authkey

def build_query(filters_dict, start_from, start_d, end_d):
    # Initialize the query conditions list
    query_conditions = []

    # Iterate through the dictionary and build query conditions
    for key, value in filters_dict.items():
        if isinstance(value, list):
            # For list values (e.g., 'source.country'), use 'terms' query
            query_conditions.append({
                'terms': {key: value}
            })
        else:
            # For single values (e.g., 'source.type'), use 'match' query
            query_conditions.append({
                'match': {key: value}
            })
    
    date_range = {
        "range": {
            "date_published": {
                "gte": start_d,
                "lte": end_d
            }
        }
    }

    query_conditions.append(date_range)


    # Build the 'must' clause of the query
    query_must = {
        "bool": {
            "must": query_conditions
        }
    }

    # Build the final query
    query = {
        "query": query_must,
        "sort": [{"date_published": "asc"}], # sort with date published
        "include": ["lens_id", 
                "title", 
                "abstract", 
                "date_published", 
                "authors",
                "fields_of_study",
                "keywords"
                ],
        "from": start_from, 
        "size": q_size  # Number of results per page (adjust as needed)
    }
    
    return query

def get_response(start_d, end_d, start_from = 0):
    
    query = build_query(filters_dict, start_from, start_d, end_d)
    headers = {'Authorization': get_auth(), 'Content-Type': 'application/json'}
    response = requests.post(search_url, data=json.dumps(query), headers=headers)

    return response

def ingest_journals(start_d, end_d):
    import configparser
    config_file = '../config.ini'
    settings = configparser.ConfigParser(inline_comment_prefixes="#")
    settings.read(config_file)

    start_from = 0
    max_results = None
    ## check if there are more results to query || or if this is the first query
    ## Condition 1: results is None - make a request
    ## Condition 2: keep querying if the results is lower than max_results, or max_limit
    while (max_results is None) or (start_from < max_results):
        response = get_response(start_d, end_d, start_from)
        print(response)

        if response.status_code != requests.codes.ok:
            print("Error: " + response.status_code)
            print(response.text)
            return
        else:
            ## save results
            filepath = settings['DEFAULT']['raw_data_folder'] + settings['LENS_API.JOURNALS']['subfolder']
            response_json = response.json()
            filename = filepath + f"journals_{start_d}_to_{end_d}_from_{start_from}.json"
            f = open(filename, "w", encoding='utf-8')
            f.write(response.text)
            f.close()

            print("saved results to: " + filename)
            
            ## get results info
            max_results = response_json['total']
            start_from = start_from + response_json['results']

            ## if max_results exists limit, set limit
            if (max_results > max_limit):
                max_results = max_limit

    return

def save_journal_data():
    from src.google_drive import create_gdrive_client, upload_file
    from journal_cleaning import clean_journal
    # calling function to save csv to processed folder
    csv_filename = clean_journal()
    print("saved CSV file to local processed folder")
    
    #get google drive info
    gdrive_cred_file = ""
    gdrive_folder_id = "1zsKuXBfbf9rowN32mOpkpVbZJFGAgPQA"
    
    # authenticate and create Google Drive client
    gdrive = create_gdrive_client(gdrive_cred_file)
    # upload file to Google Drive
    upload_file(gdrive, gdrive_folder_id, csv_filename)
    print('Data saved in Google Drive')

    return

def main():
    import configparser
    config_file = '../config.ini'
    settings = configparser.ConfigParser(inline_comment_prefixes="#")
    settings.read(config_file)

    # Define the command-line argument parser
    parser = argparse.ArgumentParser(description='Extract journal data from Lens.org.')
    parser.add_argument('--after', type=str, required=True, help='Start date of the date range (format: YYYY-MM-DD)')
    parser.add_argument('--before', type=str, required=True, help='End date of the date range (format: YYYY-MM-DD)')
    parser.add_argument('--save', dest='save_to', type=str, help = "value determines how the data will be saved. See config.ini for default and valid options")
    args = parser.parse_args()
    start_d = args.after
    end_d = args.before
    
    print("== Starting ingestion from Lens ==")
    print("from: " + start_d)
    print("to: " + end_d)
    ingest_journals(start_d, end_d)

    print("== Data ingestion completed ==")

    if parser.save_to is not None:
        if parser.save_to == 'gdrive':
            save_journal_data()
        if parser.save_to == 'azure':
            print('Save to Azure has not been configured. Action skipped')

    return 

## Execute main
if __name__ == "__main__":
    main()
