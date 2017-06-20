"""A class for tansforming queries against the financial records"""


import pandas as pd


__author__ = 'Marshall Markham'


class Transformer(object):
    """This class transforms objects queried from our financial data using
    the projection pattern below where RDFD2170 can be any MDRM identifier:
    
    projection = {
        'fdic_certificate_number':1, 
        'financial_institution_name': 1,
        'reporting_period_end_date': 1,
        'financials.RCFD2170': 1,
        '_id':0
    }
    
    parameters
    ----------
    obs_cursor : pymongo.cursor.CursorType
        a cursor from the financials collection derived from 
        the projection pattern above
    """
    def __init__(self, obs_cursor):
        self._obs_array = [item for item in obs_cursor]
        
    @property
    def obs_array(self):
        # define an imutable field
        return self._obs_array
    
    @staticmethod
    def _flatten(dct):
        # A function to unnest data
        def _go(dct, newdct):
            for key, value in dct.items():
                if isinstance(value, dict):
                    # if the value is of type dictionary recurse to pull the 
                    # values out of the sub-dictionary
                    _go(value, newdct)
                else:
                    newdct[key] = value
            return newdct
    
        return _go(dct, dict())
    
    @staticmethod
    def _column_transform(df):
        # return a new df with columns reordered and renamed
        known_cols = [
            'fdic_certificate_number', 
            'financial_institution_name', 
            'reporting_period_end_date'
        ]
        
        #find the name of the value column through set differencing
        value_col = list(set(df.columns).difference(set(known_cols)))
        known_cols.extend(value_col)
        
        newdf = df.loc[:, known_cols]
        newdf.columns = ['fdic_id', 'name', 'datestring', 'value_col']
        return newdf
    
    def _bank_name_transform(self, df):
        # NOTE: this mutates df
        # replace characters in name column to make bank names 
        # feasible column names
        stripped_chars = [',', '|', ':']
        def _go_str(name_col, char_array):
            if len(char_array) > 0:
                return _go_str(name_col.str.replace(char_array[0], ''), char_array[1:])
            return name_col
        
        tmp_col = df['name'].replace(' ', '_')
        df['institution'] = _go_str(tmp_col, stripped_chars)
        
    def transform(self):
        newobs = list(map(self._flatten, self.obs_array))
        df = pd.DataFrame(newobs)
        df_col_trans = self._column_transform(df)
        self._bank_name_transform(df_col_trans)
        subset = df_col_trans.loc[:, ['datestring', 'institution', 'value_col']]
        pivoted = subset.pivot(index='datestring', columns='institution', values='value_col')
        pivoted.index = pd.to_datetime(pivoted.index)
        return pivoted

