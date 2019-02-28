import argparse
import json
import logging
import os
import platform
import sys
import time

from datetime import datetime
from oauth2client import client

import dcm_helper_copy
import dfareporting_utils
from db_helper_copy import dbConnection

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("__name__")


parser = argparse.ArgumentParser(description='Runs DCM Reach GUI Report and writes to database')
parser.add_argument('-t','--test', help='send to local db',action='store_true')
parser.add_argument('-m','--manual', help='choose campaign to refresh',action='store_true')
parser.add_argument('-p','--prof_id', help='dcm profile id',required=True) ## add 


args = parser.parse_args()


if args.prof_id:
    PROFILE_ID = args.prof_id
else:
    print("Please supply DCM profile id with flag -p")

with open('config.json', encoding='utf-8') as data_file:
        CREDS = json.loads(data_file.read())

if args.test:
    creds = CREDS['dev']
    logging.info("************* Write to Local DB ***********")
else:
    creds = CREDS['production']
    logging.info("************* Write to Cloud DB ***********")

db = dbConnection(creds['host'],
                        creds['db'],
                        creds['user'],
                        creds['password'],
                        3306)

def main():
    logger.info("REACH REFRESH START @ {}".format(datetime.now().strftime('%Y-%m-%d %H:%M:%S')))    
    flags = None  
    logger.info("Build DCM Service")
    service = dfareporting_utils.setup(flags)
    campaigns = db.get_campaigns()
    
    logger.debug(campaigns)    
    reports = {}
    
    if args.manual:
        print('Please Choose Campaign to Update')
        for idx,campaign in enumerate(campaigns):
            print('{}. {}'.format(idx,campaign.get('campaignName')))

        choice = input('Choice:? ')

        try:
            campaigns =[campaigns[int(choice)]]
            print('Selection: {} '.format(campaigns))
            time.sleep(2)
        except:
            print('nah')
            exit()
        
   
    for campaign in campaigns:
        name = campaign['campaignName']
        campaign_id = campaign['id']
        advertiser_id = campaign['advertiserId']
        rf_report_id = campaign['rfReportId']

        if rf_report_id == None:
            logger.info("Building Reach Report for {}".format(name))
            report = dcm_helper_copy.build_rf_report(name,advertiser_id,campaign_id)
            rf_report_id = dcm_helper_copy.insert_report(service,PROFILE_ID,report)
            resp = db.write_rf_report_id(rf_report_id,campaign_id)
            logger.debug(resp)

        try:
            logger.info("Running Reach Report {} for {}".format(rf_report_id,name))
            file_id = dcm_helper_copy.run_report(service,PROFILE_ID,rf_report_id)
            reports[rf_report_id] = file_id
            time.sleep(2)
        except:
            logger.error('Run Reach Report for {}'.format(name))
            pass
    
    #download loop
    for report_id,file_id in reports.items():
        logger.info("Downloading File {} for Report {}".format(report_id,file_id))
        dcm_helper_copy.download_file(service,report_id,file_id)   

    
    export_directory = 'exports'
    #process CSV's
    for export in [f for f in os.listdir(export_directory) if not f.startswith('.')]:
        if platform.system() == "Windows":
            path = export_directory + '\\' +  export ## please actually test this, Kyle
        else:
            path = export_directory + '/' +  export

        processed = dcm_helper_copy.process_rf_csv(path)
        logger.debug(processed)
        if processed != []:
            rows = db.write_rf_report(processed)
            logger.info("Measures Written: {}".format(rows))

        os.remove(path)

if __name__ == "__main__":
    main()
