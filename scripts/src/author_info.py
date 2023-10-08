import json
import pycountry
import pandas as pd

# Define a function to extract and format the author name
def extract_author_info(row):
    authors = row['authors']
    
    if isinstance(authors, list) and len(authors) > 0:
        last_author = authors[-1]
        first_name = last_author.get('first_name', '')
        last_name = last_author.get('last_name', '')
        affiliations = last_author.get('affiliations', [])
        
        if affiliations:
            institution = affiliations[0].get('name', '')
            country_code = affiliations[0].get('country_code', '')
            try:
                country_name = pycountry.countries.get(alpha_2=country_code).name
            except AttributeError:
                country_name = ''
            return f"{first_name} {last_name}", institution, country_name
        
    return '', '', ''