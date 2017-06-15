import re

import functools as fnc
import pandas as pd


PATTERN = re.compile('Unnamed:*')

def drop_col_bool(colname, pat):
    """returns true if the colname matches the pattern"""
    matched = pat.match(colname)
    return matched is not None

def top_colname_trns(colname):
    "manipulate colnames to be machine friendly"
    return colname.lower().replace('/', '').replace(' ', '_')

def row_transform(pat, row):
    """pat matches empty items and row is the element of a dataframe. builds a nested 
    dictionary from the flat dataframe.
    """
    top = dict()
    top['financials'] = dict()
    top['institution_constants'] = dict()
    
    for tup in row.index:
        match_0, match_1 = pat.match(tup[0]), pat.match(tup[1])
        key = top_colname_trns(tup[0])
        if (match_0 is not None) and (match_1 is not None):
            continue
        elif ((match_1 is not None) and 
                  (key in ('reporting_period_end_date', 'fdic_certificate_number'))):
            top[key] = row[tup]
        elif match_1 is not None:
            top['institution_constants'][key] = row[tup]
        else:
            top['financials'][tup[0]] = row[tup]
    return top


# partially apply row_transform with a pattern for 
# matching unlabled column levels
transformer = fnc.partial(row_transform, PATTERN)


def transform_flat_text(filepath, fdic_codes):
    """conduct full transform on a text file returning a list of dictionaries"""
    df = pd.read_csv(filepath, delimiter='\t', header=[0, 1])
    
    # flatten the columns for filtering since the second column level is set
    # by position if null and we do not want to depend on column order
    tmpdf = df.copy()
    tmpdf.columns = tmpdf.columns.droplevel(1)
    
    filtered = df[tmpdf['FDIC Certificate Number'].isin(fdic_codes)]
    
    transform = filtered.apply(transformer, axis=1)
    
    return list(transform)