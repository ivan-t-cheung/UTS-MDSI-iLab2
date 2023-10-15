## Library imports
import configparser
import ast
import json
import requests
import argparse
from datetime import datetime
from datetime import date
from datetime import timedelta
import sys

##set global variables
config_file = '../config.ini'

search_url = ''
authkey = ''
q_juridictions = []
q_types = []  
q_size = 0 
max_limit = 0 
save_to = ''
patent_data_folder = ''

##
# set global variables from config file
#
##
def set_config():
    ## open config file
    settings = configparser.ConfigParser(inline_comment_prefixes="#")
    settings.read(config_file)

    ## define global variables
    global search_url
    global authkey
    global q_juridictions
    global q_types
    global q_size
    global max_limit
    global save_to
    global patent_data_folder

    ## load config settings
    authkey = settings['LENS_API']['api_key']
    search_url = settings['LENS_API.PATENTS']['patent_search']
    q_juridictions = ast.literal_eval(settings["LENS_API.PATENTS"]["juridictions"])
    q_types = ast.literal_eval(settings['LENS_API.PATENTS']['types'])
    q_size = settings['LENS_API.PATENTS']['size'] 
    max_limit = settings['LENS_API.PATENTS']['max_limit'] 
    patent_data_folder = settings['DEFAULT']['raw_data_folder'] + settings['LENS_API.PATENTS']['patent_subfolder'] 

    save_to = settings['DEFAULT']['save_data']

    return

def confirm_valid_save(option):
    settings = configparser.ConfigParser(inline_comment_prefixes="#")
    settings.read(config_file)

    valid_options = ast.literal_eval(settings['DEFAULT']['valid_save_options'])

    if option in valid_options:
        return True
    else:
        return False

##
#   Override the default save option with argument
##
def set_save_option(option):
    global save_to

    save_to = option
    return

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
    headers = {'Authorization': authkey, 'Content-Type': 'application/json'}
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
            # 1) create filename
            # 2) call save 
            filename = f"patents_{start_d}_to_{end_d}_from_{start_from}.json"
            save_patent_data(response.text, filename)
            
            ## get results info
            response_json = response.json()
            max_results = response_json['total']
            start_from = start_from + response_json['results']

            ## if max_results exists limit, set limit
            if (max_results > max_limit):
                max_results = max_limit

    return


def save_patent_data(response_text, filename):
    
    file_destination = patent_data_folder + filename
    ## save to local (this option always happens regardless of save_to settting)
    f = open(file_destination, "w", encoding='utf-8')
    f.write(response_text)
    f.close()
    print("saved results to local folder: " + patent_data_folder + filename)

    ## if save option is gdrive
    if (save_to == 'gdrive'):
        save_patent_gdrive(file_destination)

    ## if save option is azure
    if (save_to == 'azure'):
        save_patent_azure()

    return

def save_patent_gdrive(file_destination):
    from src.google_drive import create_gdrive_client, upload_file
    settings = configparser.ConfigParser(inline_comment_prefixes="#")
    settings.read(config_file)
    
    #get google drive info
    gdrive_cred_file = settings['GDRIVE']['credentials']
    gdrive_folder_id = settings['GDRIVE.RAWDATA.FOLDER_IDS']['patent_data']
    
    # authenticate and create Google Drive client
    gdrive = create_gdrive_client(gdrive_cred_file)
    # upload file to Google Drive
    upload_file(gdrive, gdrive_folder_id, file_destination)
    print('Data saved in Google Drive')
    return

def save_patent_azure():
    print('Save to Azure has not been configured. Action skipped')
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
        case 4:
            print("invalid save option detected. See valid options in config.ini")
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
    #   --before/--after                                        || manually set the date range to query.
    #   --save                                                  || define if the raw data should be saved to local or cloud solution

    args = sys.argv[1:]                                     ## replace sys.argv with argparse
    d_range = False

     ## collect arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('--month', action='store_true', help = 'set search range to last month (default value)')
    parser.add_argument('--before', dest='before', type=lambda d: datetime.strptime(d, '%Y-%m-%d'), help = 'date input must use the format YYYY-MM-DD')
    parser.add_argument('--after', dest='after', type=lambda d: datetime.strptime(d, '%Y-%m-%d'), help = 'date input must use the format YYYY-MM-DD')
    parser.add_argument('--save', dest='save_to', type=str, help = "value determines how the data will be saved. See config.ini for default and valid options")

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

    ##### ERROR 4
    ## was --save used? 
    if pa.save_to is not None:
        confirm_valid = confirm_valid_save(pa.save_to)      ## if --save was used, check if the option was valid
        if(not confirm_valid):
            invalid_args(4)                                 ## if invalid, return error message
            return
        else:
            set_save_option(pa.save_to)                     ## if valid, override the default save option


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
    set_config()
    main()