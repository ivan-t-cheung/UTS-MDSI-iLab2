import json
import requests
import argparse

search_url = 'https://api.lens.org/scholarly/search'
auth_json = '../../api_auth.json' 
q_countries = ['United States', 'Australia']     ## set the countries to retrieve, see https://docs.api.lens.org/request-scholar.html
q_type = 'Journal'                               ## set the publication types to retrieve, see https://docs.api.lens.org/response-scholar.html
q_date = ''                                      ## set empty year
q_size = 100                                     ## set the number of journals to return each query. For paid licences change this number to 1,000 - 10,000
max_limit = 300                                  ## set the limit on the number of results to query for. This will override the max results if lower.
 

# Define the filters for match
filters_dict = {
    'source.type': q_type,
    'source.country':  q_countries,                 
    'is_open_access': True,
    'has_abstract': True,
    'year_published': q_date
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


def build_query(filters_dict, start_from):
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
        "from": start_from, 
        "size": q_size  # Number of results per page (adjust as needed)
    }
    
    return query

def get_response(start_from = 0):
    
    query = build_query(filters_dict, start_from)
    print(query)
    headers = {'Authorization': get_auth(), 'Content-Type': 'application/json'}
    response = requests.post(search_url, data=json.dumps(query), headers=headers)

    return response

def ingest_journals():
    start_from = 0
    max_results = None
    ## check if there are more results to query || or if this is the first query
    ## Condition 1: results is None - make a request
    ## Condition 2: keep querying if the results is lower than max_results, or max_limit
    while (max_results is None) or (start_from < max_results):
        response = get_response(start_from)
        print(response)

        if response.status_code != requests.codes.ok:
            print("Error: " + response.status_code)
            print(response.text)
            return
        else:
            ## save results
            response_json = response.json()
            filename = "../data/raw/journals/" + f"journals_{filters_dict['year_published']}_from_{start_from}.json"
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


def main():

    # Define the command-line argument parser
    parser = argparse.ArgumentParser(description='Extract journal data from Lens.org.')
    parser.add_argument('--year', type=int, required=True, help='Year of publication (e.g., 2020)')
    args = parser.parse_args()
    q_date = str(args.year)                           ## update year
    filters_dict['year_published'] = q_date           ## add year to filter dictionary

    print("== Starting ingestion from Lens ==")
    print("publication year: " + q_date)
    ingest_journals()
    
    print("== Data ingestion completed ==")
    return 

## Execute main
if __name__ == "__main__":
    main()
