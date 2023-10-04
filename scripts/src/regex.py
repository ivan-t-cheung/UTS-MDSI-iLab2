import re

def define_tech_terms():
    "Defines a dictionary containing the keyword seach terms for the chosen emerging technologies."
    return [{'tech': 'quantum', 
            'keywords': ['quantum']},
            {'tech': 'semiconductors', 
            'keywords': ['semiconductor','semi-conductor','microchip','micro-chip','microprocessor','integrated circuit','memory chip']},
            {'tech': 'cell-based meats', 
            'keywords': ['cell-based meat','cell based meat','cell-grown meat','cell grown meat','cultivated meat','cultured meat','cellular agriculture','lab-produced meat','lab produced meat','lab-grown meat','lab grown meat','synthetic meat','hybrid meat','artificial meat']},
            {'tech': 'hydrogen power', 
            'keywords': ['green hydrogen','clean hydrogen','renewable hydrogen','hydrogen storage','hydrogen transport','hydrogen project','hydrogen production','hydrogen electrolyser', 'hydrogen electrolysis','hydrogen technology','hydrogen tech','hydrogen energy','hydrogen plant','hydrogen station','hydrogen network','hydrogen hub','hydrogen power','hydrogen powered','hydrogen-powered','hydrogen supply','hydrogen market','hydrogen economy','hydrogen infrastructure','hydrogen vehicle','hydrogen fueled']},
            {'tech': 'personalised medicine', 
            'keywords': ['personalised medicine','personalized medicine','personalized treatment','personalised treatment','personalised cancer','individual medicine','individualised medicine','individualized medicine','personalised therapy','personalized therapy','precision medicine','precision health care','precision health-care','theranostic','theragnostic','oncogenomic','onco-genomic']},
            ]

def keywords_to_pattern(keyword_list, spaces_optional=True, word_boundaries=False, group=False): 
    """
    Prepares a list of keyword terms for a transformation into a regex string.
    Args:
        keyword_list (list[string]): List of string literals used to create a regex pattern with OR logic.
        spaces_optional (bool): If True spaces will be converted to ".?". Allows for searching of text with whitespace removed.
        word_boundaries (bool): If True word boundaries are required at the start and end of keyword terms.
        group (bool): If True wrap regex in a grouping.
    Returns:
        Regex pattern string. 
    """
    # first escape any regex operators in the strings
    #keyword_list = [re.escape(keyword) for keyword in keyword_list]
    if spaces_optional:
    # replace spaces with wildcard characters to allow for searching in URLs etc
        keyword_list = [re.sub(' ', '.?', keyword) for keyword in keyword_list]
    if word_boundaries:
    # insert word boundary token at the start and end of each keyword term
        keyword_list = [fr'\b{keyword}\b' for keyword in keyword_list]
    # join the keywords into a single regex string
    regex_string = '|'.join(keyword_list)
    if group:
    # wrap with grouping
        regex_string = '(' + regex_string + ')'
    # return the regex string
    return regex_string

def add_regex_pattern(tech_terms):
    """
    Prepares a list of keyword terms for a transformation into a regex string, then compiled into a regex pattern object.
    Args:
        tech_terms (list[dict]): 
    Returns:
        None. Input list is modified in-place by adding 'regex' item to each dict.
    """
    for tech in tech_terms:
        regex_pattern = keywords_to_pattern(tech['keywords'])
        tech['regex'] = re.compile(regex_pattern, flags=re.IGNORECASE)

def grouped_pattern(tech_terms):
    patterns = [keywords_to_pattern(tech['keywords'], group=True) for tech in tech_terms]
    regex_pattern =  '|'.join(patterns)
    return re.compile(regex_pattern, flags=re.IGNORECASE)