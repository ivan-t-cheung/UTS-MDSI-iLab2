## Library imports
import json
import requests
import argparse
from datetime import datetime
from datetime import date
from datetime import timedelta
import sys

##set global variables
search_url = 'https://api.lens.org/patent/search'
authkey = ''

auth_json = '../auth/api_auth.json'                                 ## change folder path if required
q_juridictions = ['jurisdiction', 'US', 'AU']                       ## set the countries to retrieve, see https://docs.api.lens.org/request-patent.html
q_types = ['publication_type', 'GRANTED_PATENT', 'AMENDED_PATENT']  ## set the publication types to retrieve, see https://docs.api.lens.org/response-patent.html
q_size = 100                                                        ## set the number of patents to return each query. For paid licences change this number to 1,000 - 10,000
max_limit = 300                                                     ## set the limit on the number of results to query for. This will override the max results if lower.

###
# Get API authorisation code from file.
###
def get_auth():
    global authkey

    api_auth = open(auth_json, "r")
    authkey = json.load(api_auth)['lens']
    api_auth.close()

    return authkey

###
#   This function builds the query search parameters for each termininology to filter against.
#   Parameters:
#       term: this is the search terminology field, see https://docs.api.lens.org/request-patent.html for valid fields
#       term_list: this is the values to apply to the search, input as List, see https://docs.api.lens.org/response-patent.html for valid values
###
def term_builder(term, term_list):
    query_start = '''
                {
                    "bool": {
                        "should": [ '''

    query_end = '''
                        ]
                    }
                }, '''

    ## iterate through list of countries and builk query for each country it
    query_builder = []
    for i in term_list:
        term_string =   '''
                            {
                                "term" : {
                                    "%s": "%s"
                                }
                            }
                            ''' % (term, i)
        
        query_builder.append(term_string)

    ## join country list into one string and append to query for return
    terms = (",".join(query_builder))
    query = query_start + terms + query_end

    return query

###
#   This function defines the search range for the query.
#   accepted date format is a string, using YYYY-MM-DD
###
def query_range(start, end):
    query = '''
                {
                    "range" : {
                        "date_published": {
                            "gte": "%s",
                            "lte": "%s"
                        }
                    }
                
                }
            ''' % (start, end)
    return query

###
#   This function defines which fields to include in the data response
#   See https://docs.api.lens.org/response-patent.html for valid options
###
def response_include():
    include =   '''
    "include": ["lens_id",  
                "abstract.text", 
                "date_published", 
                "publication_type",
                "biblio.application_reference.doc_number",
                "biblio.invention_title.text", 
                "biblio.classifications_cpc.classifications.symbol", 
                "biblio.parties.inventors",
                "biblio.parties.applicants"
                ],
    '''
    return include

def build_data(start_d, end_d):
    query_1 =  '''{
    "query": {
        "bool": {
            "must": [ '''
    
    query_2 =   '''
            ]
        }          
    },
    "sort": [{"date_published": "asc"}],
    '''
    
    data = query_1 + term_builder(q_juridictions[0], q_juridictions[1:]) + term_builder(q_types[0], q_types[1:]) + query_range(start_d, end_d) + query_2 + response_include()
    return data


def get_response(data, start_from = 0):
    size = q_size      

    data_suffix = '''
    "from" : %d,
    "size" : %d
}
    ''' % (start_from, size)

    data = data + data_suffix
    headers = {'Authorization': get_auth(), 'Content-Type': 'application/json'}
    response = requests.post(search_url, data=data, headers=headers)

    return response

###
#   1) package query and data
#   2) get response
#   3) check response code
#   4) save data as json
#   5) check if there is more results
#   6) iterate until no more results
#   
###
def ingest_patents(start_d, end_d):
    start_from = 0
    data = build_data(start_d, end_d)
    max_results = None
    ## check if there are more results to query || or if this is the first query
    ## Condition 1: results is None - make a request
    ## Condition 2: keep querying if the results is lower than max_results, or max_limit
    while (max_results is None) or (start_from < max_results):
        response = get_response(data, start_from)

        if response.status_code != requests.codes.ok:
            print("Error: " + response.status_code)
            print(response.text)
            return
        else:
            ## save results
            response_json = response.json()
            filename = "../data/raw/patents/" + f"patents_{start_d}_to_{end_d}_from_{start_from}.json"
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

########### HELPER FUNCTIONS

###
#   Print error statments for invalid argument combinations
###
def invalid_args(error):
    match error:
        case 1:
            print("Not enough arguments found, call 'lens_api_reader.py --help' for guidance.")
        case 2:
            print("cannot use --month (last month) together with --before & --after. Refer to documentation for guidance.")
        case 3:
            print("--before and --after must be used together.")
    return

##
#   Based on the input date. get the start date of last month to the start of this month (for date building)
#   if no date provided, use today's date
#   Return the dates as strings in a len 2 array
##
def get_prev_month(date = date.today()):
    end = date.replace(day=1) - timedelta(days = 1)
    start = end.replace(day=1)

    return str(start), str(end)


######### MAIN FUNCTION

def main():
    #arg definitions:
    #   --month     search range is last month                  || acts as default if no date range set
    #   -csv        save results to csv file                    || acts as default if no save option set (TODO: apply db save as default)
    #   --before/--after                                        || manually set the date range to query.

    args = sys.argv[1:]                                     ## replace sys.argv with argparse
    d_range = False

     ## collect arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('--month', action='store_true', help = 'set search range to last month (default value)')
    parser.add_argument('--before', dest='before', type=lambda d: datetime.strptime(d, '%Y-%m-%d'), help = 'date input must use the format YYYY-MM-DD')
    parser.add_argument('--after', dest='after', type=lambda d: datetime.strptime(d, '%Y-%m-%d'), help = 'date input must use the format YYYY-MM-DD')

    pa = parser.parse_args()


    ##
    #   Part 1:
    #   Validate arguments and return error messages as required
    ## 

    ###### ERROR 1
    if len(args) == 0:
        invalid_args(1)
        return
    
    ## check number of date options used are valid.
    d = 0
    if (pa.month):
        d = d + 1
    if (pa.after != None or pa.before != None):
        d = d + 1

    ###### ERROR 2
    ## if d > 1 then both month and before/after were used together
    if d > 1:
        invalid_args(2)
        return
    
    ###### ERROR 3
    ## check both after and before were added
    if (pa.before != None and pa.after == None) or (pa.before == None and pa.after != None):
        invalid_args(3)
        return
    elif pa.before != None and pa.after != None:
        d_range = True      ## both before and after dates were provided

    

    ##
    #   Part 2:
    #   after checking arguments are valid, 
    #   Execute the request and save all data to json.
    #  
    ## 

    ## assign query start and end date
    start_d = ''
    end_d = ''

    if d_range:
        start_d = pa.after.date()
        end_d = pa.before.date()
    elif d_range == False:
        start_d, end_d = get_prev_month()
    
    print("== Starting ingestion from Lens ==")
    print("from: " + start_d)
    print("to: " + end_d)
    ingest_patents(start_d, end_d)
    
    print("== Data ingestion completed ==")

    ##
    #
    #   Part 3:
    #   after saving all responses to csv, process the data into a dataframe and save to csv
    #
    ##


    return


## Execute main
if __name__ == "__main__":
    main()