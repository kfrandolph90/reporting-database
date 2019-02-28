import json
import dfareporting_utils
from oauth2client import client
from db_helper import dbConnection
from datetime import datetime
import sys
import logging
import dcm_helper
import argparse

parser = argparse.ArgumentParser(description='Update Moat')

parser.add_argument('-t','--test', help='send to local db',action='store_true')
parser.add_argument('-p','--prof_id', help='dcm profile id',required=True) ## add 
args = parser.parse_args()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("__name__")

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
    logger.info("DIMENSION REFRESH START @ {}".format(datetime.now().strftime('%Y-%m-%d %H:%M:%S')))    
    logger.info("Loaded Creds")
    flags = None  
    logger.info("Build DCM Service")
    service = dfareporting_utils.setup(flags)
    
    db = dbConnection(creds['host'],
                        creds['db'],
                        creds['user'],
                        creds['password'],
                        3306)
    
    campaigns = db.get_campaigns()
    logger.debug(campaigns)    
    campaign_ids = [row['id'] for row in campaigns]
    logger.debug(campaign_ids)   
     
    sites = dcm_helper.get_sites(service,PROFILE_ID,campaign_ids)
    logger.debug(sites)
    
    sites_written = db.write_sites(sites)
    logger.info('Sites Updated {}'.format(sites_written))   
    
    for campaign_id in campaign_ids: 
        creatives = dcm_helper.get_creatives(service,PROFILE_ID,campaign_id)        
        
        if creatives != []:
            #logger.debug(json.dumps(creatives, indent=4, sort_keys=True))    
            creatives_written = db.write_creative(creatives)
            logger.info('{} Creatives Updated: {}'.format(campaign_id,creatives_written))

        placements = dcm_helper.get_placements(service,PROFILE_ID,campaign_id)        
        
        if placements != []:
            placements_written = db.write_placements(placements)        
            logger.info('{} Placements Updated: {}'.format(campaign_id,placements_written))

        groups = dcm_helper.get_placement_groups(service,PROFILE_ID,campaign_id)

        if groups != []:
            logger.debug(json.dumps(groups, indent=4, sort_keys=True))  
            groups_written = db.write_groups(groups)
            logger.info('{} Placement Groups Updated {}'.format(campaign_id,groups_written))
        
        ads = dcm_helper.get_ads(service,PROFILE_ID,campaign_id) 
        
        if ads != []:
            logger.debug(json.dumps(ads, indent=4, sort_keys=True)) 
            ads_written = db.write_ads(ads)
            logger.info('{} Ads Updated {}'.format(campaign_id,ads_written))
        

if __name__ == "__main__":     
    main()