"""Populate the mongodb database"""


import os
import etl

import pymongo as mongo


__author__ = 'Marshall Markham'


# Module Constants
FDIC_IDS = [3510, 3511, 7213, 628, 32188]


if __name__ == '__main__':
    # communicate the beginning of the job
    print('beginning ETL job')

    # get/set path info
    local_dir = os.path.split(os.path.abspath(__file__))[0]
    data_dir = os.path.join(local_dir, '../financial_data_raw')

    # find data files
    data_files = os.listdir(data_dir)

    # access mongo instance
    # the name 'mongodb-app' is set in docker-compose.yaml file in this project
    client = mongo.MongoClient('mongodb-app')
    db = client.get_database('fdic_ffeic')

    # get/set collection connections
    ffeic_collection = db['ffeic_reports']
    company_collection = db['ffeic_company_info']

    # run ffeic etl to the report collection and the company_info collection
    for item in data_files:
        if item not in ('.ipynb_checkpoints', 'mdrm_codes'):
            filepath = os.path.join(data_dir, item)
            print(filepath)
            ffeic_etl = etl.FDICDataETL(filepath, FDIC_IDS)
            ffeic_etl.full_etl(ffeic_collection)
            company_etl = etl.FDICCompanyInfoETL(filepath, FDIC_IDS)
            company_etl.full_etl(company_collection)


    # set ffeic report indices
    ffeic_index_fields = [('fdic_certificate_number', mongo.ASCENDING), 
                            ('reporting_period_end_date', mongo.DESCENDING)]
    
    ffeic_collection.create_index(ffeic_index_fields, unique=True)
    ffeic_collection.create_index('fdic_certificate_number')
    ffeic_collection.create_index('reporting_period_end_date')

    # set ffeic_company_info index
    company_collection.create_index('fdic_certificate_number')
    company_collection.create_index('record_updated')

    # communicate end of script
    print('Succefully completed ETL job')
