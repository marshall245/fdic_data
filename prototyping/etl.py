"""Define ETL classes for updating MongoDB instance with FDIC data"""


import re
import math

import pandas as pd

from copy import copy


__author__ = 'Marshall Markham'


class MDRMInfoETL(object):
    
    def __init__(self, filepath):
        self._filepath = filepath
        
    @staticmethod
    def column_name_transform(cols):
        """lower case column names and replace spaces with underscores. replace item_name column"""
        newcols = list(map(lambda strng: strng.replace(' ', '_').lower(), cols))
        
        finalcols = list()
        for col in newcols:
            if col != 'item_name':
                finalcols.append(col)
            else:
                finalcols.append('description')
        return finalcols

    @staticmethod
    def row_transform(row):
        """transform a row into a nested dictionary"""
        topkeys = ['mdrm_item', 'description']
        detailkeys = ['start_date', 'end_date', 'description', 'confidential', 'reporting_forms']

        obs = dict()
        for key in topkeys:
            obs[key] = row[key]

        obs['details'] = dict()
        for key in detailkeys:
            obs['details'][key] = row[key]

        return obs

    @staticmethod
    def readfile(filepath):
        """read in a tab seperated csv file"""
        return pd.read_csv(filepath, sep='\t')

    def transform(self):
        """read in and transform a csv file returning a list of dictionaries"""
        df = self.readfile(self._filepath)
        df.columns = self.column_name_transform(df.columns)
        dicts_df = df.apply(self.row_transform, axis=1)
        dicts_list = list(dicts_df)
        return dicts_list
    
    def load(self, mongo_collection, data):
        assert mongo_collection.count() == 0
        mongo_collection.insert_many(data)


class FDICDataETLBase(object):
    
    def __init__(self, filepath, fdic_codes=None):
        self._filepath = filepath
        self._fdic_codes = fdic_codes
        self.re_pattern = re.compile('Unnamed:*')

    def match(self, colname):
        """determine whether a column name matches the regular 
        expression on self.re_pattern
        """
        return self.re_pattern.match(colname) is not None

    def drop_unnamed_columns(self, df):
        """drop columns matching the pattern 'Unnamed:*'"""
        return df.loc[:, ~ df.columns.to_series().apply(self.match)]
    
    @staticmethod
    def column_name_transform(cols):
        """format column names to lower case and replace spaces with underscores"""
        transcols = [
            'Reporting Period End Date',
            'IDRSSD',
            'FDIC Certificate Number',
            'OCC Charter Number',
            'OTS Docket Number',
            'Primary ABA Routing Number',
            'Financial Institution Name',
            'Financial Institution Address',
            'Financial Institution City',
            'Financial Institution State',
            'Financial Institution Zip Code',
            'Financial Institution Filing Type',
            'Last Date/Time Submission Updated On'
        ]
        
        def one_col(col):
            # transform a single column name
            if col in transcols:
                return col.lower().replace(' ', '_').replace('/', '')
            return col
        
        newcols = map(one_col, cols)
        return list(newcols)

    @staticmethod
    def readfile(filepath):
        """read in a tab delimited csv file with two header rows.
        drop the second header row before returning"""
        df = pd.read_csv(filepath, sep='\t', header=[0, 1])
        df.columns = df.columns.droplevel(1)
        return df
    
    @staticmethod
    def row_transform(row):
        # not implemented
        raise AttributeError("this method is not implemented on the base class")
    
    def transform(self):
        """read in and transform a csv file returning a list of dictionaries"""
        # read data in
        df = self.readfile(self._filepath)
        
        # transform columns
        df = self.drop_unnamed_columns(df)
        df.columns = self.column_name_transform(df.columns)
        
        # subset df to requested FDIC IDs
        if self._fdic_codes is not None:
            df = df[df['fdic_certificate_number'].isin(self._fdic_codes)]
        
        # transform rows
        dicts = df.apply(self.row_transform, axis=1)
        return list(dicts)
    
    def load(self, mongo_collection, data):
        raise AttributeError("not implemented")
    

class FDICDataETL(FDICDataETLBase):

    @staticmethod
    def row_transform(row):
        """transform a row of a dataframe into a nested dictionary"""
        dropkeys = [
            'idrssd',
            'occ_charter_number',
            'ots_docket_number',
            'primary_aba_routing_number',
            'financial_institution_address',
            'financial_institution_city',
            'financial_institution_state',
            'financial_institution_zip_code',
            'financial_institution_filing_type'
        ]
        
        topkeys = [
            'reporting_period_end_date',
            'financial_institution_name',
            'fdic_certificate_number',
            'last_datetime_submission_updated_on'
        ]
        
        top = row[topkeys].to_dict()
        top['financials'] = row.drop(dropkeys + topkeys).to_dict()
        
        return top

    # def remove_nans(self, dct):
    #     """drop all key: values in a dictionary where the value is float('nan')"""
    #     for key, value in dct.items():
    #         if isinstance(value, dict):
    #             self.remove_nans(value)
    #         elif (value is None) or (isinstance(value, (float, int)) and math.isnan(value)):
    #             dct.pop(key)

    # def remove_nans(self, dct):
    #     """drop all key: values in a dictionary where the value is float('nan')"""
    #     def _go(dct, newdct):
    #         # this function recursively mutates newdct building up 
    #         # the values of dct that are not nan or empty
    #         for key, value in dct.items():
    #             if isinstance(value, dict):
    #                 newdct[key] = dict()
    #                 _go(value, newdct[key])
    #             else:
    #                 if value and not (isinstance(value, (float, int)) and math.isnan(value)):
    #                     newdct[key] = value

    #     return_dct = dict()
    #     _go(dct, return_dct)

    #     return return_dct

    def prep_update(self, dctold, dctnew):
        """mutate dctold overwriting the values that are contained in dctnew
        which are not null
        """
        for key, value in dctnew.items():
            if isinstance(value, dict):
                self.prep_update(dctold[key], value)
            elif value and not (isinstance(value, (int, float)) and math.isnan(value)):
                dctold[key] = value

    @staticmethod
    def build_prototype(obs_list, fdic_id_key, date_key):
        """build prototype for querying collection"""
        fdic_ids = {'$in': list({item[fdic_id_key] for item in obs_list})}
        datestrings = {'$in': list({item[date_key] for item in obs_list})}

        prototype_doc = {
            fdic_id_key: fdic_ids,
            date_key: datestrings
        }

        return prototype_doc

    def update_obs(self, mongo_collection, obs, fdic_id_key, date_key):
        """find and update an observation in a mongodb collection"""
        proto_doc = {fdic_id_key: obs[fdic_id_key], date_key: obs[date_key]}
        oldobs = mongo_collection.find_one(proto_doc)
        self.prep_update(oldobs, obs)
        insert_obs = {'$set': oldobs}
        mongo_collection.find_one_and_update(proto_doc, insert_obs)

    def load(self, mongo_collection, obs_list):
        """load data to mongo database collection adjusting for update vs insert"""
        fdic_id_key = 'fdic_certificate_number'
        date_key = 'reporting_period_end_date'

        prototype_doc = self.build_prototype(obs_list, fdic_id_key, date_key)

        bigcursor = mongo_collection.find(prototype_doc)
        indexes = [(item[fdic_id_key], item[date_key]) for item in bigcursor]

        for obs in obs_list:
            if (obs[fdic_id_key], obs[date_key]) in indexes:
                self.update_obs(mongo_collection, obs, fdic_id_key, date_key)
            else:
                mongo_collection.insert_one(obs)


class FDICCompanyInfoETL(FDICDataETLBase):
    
    def column_name_transform(self, cols):
        """format column names to lower case and replace spaces with underscores"""
        newcols = super().column_name_transform(cols)
        
        finalcols = list()
        for col in newcols:
            if col == 'reporting_period_end_date':
                finalcols.append('record_updated')
            else:
                finalcols.append(col)
        return finalcols
    
    @staticmethod
    def row_transform(row):
        """transform a row of a dataframe into a nested dictionary"""
        serial_nums = [
            'idrssd',
            'occ_charter_number',
            'ots_docket_number',
            'primary_aba_routing_number',
            'financial_institution_filing_type'
        ]
        
        loc_data = [
            'financial_institution_address',
            'financial_institution_city',
            'financial_institution_state',
            'financial_institution_zip_code',
        ]
        
        topkeys = [
            'record_updated',
            'financial_institution_name',
            'fdic_certificate_number'
        ]
        
        top = row[topkeys].to_dict()
        top['location'] = row[loc_data].to_dict()
        top['identifiers'] = row[serial_nums].to_dict()
        
        return top

    def load(self, mongo_collection, obs_list):
        """load records from obs_list to mongo_collection if the data 
        is not already present
        """
        comparison_dropkeys = ('_id', 'record_updated')

        fdic_code_map = {dct['fdic_certificate_number']:dct for dct in obs_list}
        
        proto_doc = {'fdic_certificate_number': {'$in': list(fdic_code_map.keys())}}
        cursor = mongo_collection.find(proto_doc)

        db_fdic_code_map = {dct['fdic_certificate_number']:dct for dct in cursor}

        for fdic_code, obs in fdic_code_map.items():
            if fdic_code in db_fdic_code_map.keys():
                comprecord = db_fdic_code_map[fdic_code]
                if not compare_observations(comparison_dropkeys, comprecord, obs):
                    mongo_collection.insert(obs)
            else:
                mongo_collection.insert(obs)


def compare_observations(dropkeys, dbobs, newobs):
    """recursively walk dictionaries to compare values
    
    parameters
    ----------
    dropkeys: List or Tuple
        an iterable containing strings or other iterables. the chain of keys 
        that yield an observation to be dropped from consideration
    dbobs: dict
        a dictionary for comparison
    newobs: dict
        a dictionary for comparison
    """
    dbobs = copy(dbobs)
    newobs = copy(newobs)
    
    # recursive function walks the dict and removes the
    # value described by dropkeys
    def _go_drop(dropitem, dct):
        if dropitem[0] not in dct.keys():
            return None
        if len(dropitem) == 1:
            return dct.pop(dropitem[0])
        _go_drop(dropitem[1:], dct[dropitem[0]])
    
    # for each item in dropkeys mutate the dictionaries removing
    # the values by calling _go_drop(...) defined above
    for item in dropkeys:
        if isinstance(item, str):
            _go_drop([item], dbobs)
            _go_drop([item], newobs)
        else:
            _go_drop(item, dbobs)
            _go_drop(item, newobs)

    return dbobs == newobs





