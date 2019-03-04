####################################################################################
# Insanity Documented Here:
# https://developers.google.com/api-client-library/python/apis/dfareporting/v3.1
# Or just email Kyle (kyle.randolph@essenceglobal.com)
####################################################################################

import csv
import io
import os
import logging
from datetime import datetime, date, timedelta
import time

from oauth2client import client
from googleapiclient import http

import dfareporting_utils

logger = logging.getLogger("__name__")

def get_sites(service,profile_id,campaign_ids):    
    logger.info('Getting DCM Sites')
    try:
    # Construct the request.
        request = service.sites().list(
                                        profileId=profile_id,
                                        campaignIds=campaign_ids,
                                        fields="sites(id,name)"
                                        )
        sites = []
        while True:
            # Execute request and print response.
            response = request.execute()
            sites.extend(response['sites'])
            if response.get('sites') and response.get('nextPageToken'):
                request = service.creatives().list_next(request, response)
            else:
                break
    
    except client.AccessTokenRefreshError:
        print('The credentials have been revoked or expired, please re-run the '
        'application to re-authorize')

    return sites

def get_creatives(service,profile_id,campaign_id):    
    logger.info("Getting Creatives for {}".format(campaign_id))
    try:
    # Construct the request.
        request = service.creatives().list(
                                        profileId=profile_id, 
                                        #archived=False, 
                                        campaignId=campaign_id,
                                        fields="creatives(advertiserId,id,name,type,mediaDuration)"
                                        )
        campaign_creatives = []
        while True:
            # Execute request and print response.
            response = request.execute()
            campaign_creatives.extend(response['creatives'])
            if response.get('creatives') and response.get('nextPageToken'):
                request = service.creatives().list_next(request, response)
            else:
                break
    
    except client.AccessTokenRefreshError:
        print('The credentials have been revoked or expired, please re-run the '
        'application to re-authorize')

    return campaign_creatives

def get_placements(service,profile_id,campaign_id): 
    logger.info("Getting Placements for {}".format(campaign_id))
    campaign_placements = []
    try:
    # Construct the request.
        request = service.placements().list(
                                    profileId=profile_id, 
                                    #archived=False, 
                                    campaignIds=campaign_id,
                                    fields= ("placements(advertiserId,"
                                            "campaignId,"
                                            "id,"
                                            "name,"
                                            "placementGroupId,"
                                            "siteId,"
                                            "pricingSchedule/pricingType,"
                                            "pricingSchedule/startDate,"
                                            "pricingSchedule/endDate)"
                                            )
    )
        while True:
            # Execute request and print response.
            response = request.execute()
            campaign_placements.extend(response['placements'])
            
            if response.get('placements') and response.get('nextPageToken'):
                request = service.placements().list_next(request, response)

            else:
                break
    except client.AccessTokenRefreshError:
        print('The credentials have been revoked or expired, please re-run the '
        'application to re-authorize')
    return campaign_placements

def get_placement_groups(service,profile_id,campaign_id):
    logger.info("Getting Placement Groups for {}".format(campaign_id)) 
    groups = []
    try:
    # Construct the request.
        request = service.placementGroups().list(
                                    profileId=profile_id, 
                                    archived=False, 
                                    campaignIds=campaign_id,
                                    fields= ("placementGroups(id,advertiserId,campaignId,name,placementGroupType,pricingSchedule(pricingPeriods(units,startDate,endDate)))")) 
        while True:
            # Execute request and print response.
            response = request.execute()
            groups.extend(response['placementGroups'])
            
            if response.get('placementGroups') and response.get('nextPageToken'):
                request = service.placementGroups().list_next(request, response)

            else:
                break
    except client.AccessTokenRefreshError:
        print('The credentials have been revoked or expired, please re-run the '
        'application to re-authorize')
    return groups

def get_ads(service, profile_id,campaign_id):
    logger.info("Getting Ads for {}".format(campaign_id))
    ads = []
    try:
    # Construct the request.
        request = service.ads().list(
                                    profileId=profile_id, 
                                    #archived=False, 
                                    campaignIds=campaign_id,
                                    fields= ("ads(advertiserId,"
                                            "campaignId,"
                                            "id,"
                                            "name)"
                                            )
    )
        while True:
            # Execute request and print response.
            response = request.execute()
            ads.extend(response['ads'])
            
            if response.get('ads') and response.get('nextPageToken'):
                request = service.ads().list_next(request, response)

            else:
                break
    except client.AccessTokenRefreshError:
        print('The credentials have been revoked or expired, please re-run the '
        'application to re-authorize')
    return ads
    
def build_report(name,filename,campaign_id,floodlight_id=None):
    '''
    # Function builds a new report in DCM. Triggered when no entry provided for Report ID in campaigns table
    '''
    
    report = {
            'name': name,
            'type': 'STANDARD',
            'fileName': filename,
            'format': 'CSV'
            }

    # Create a report criteria. 
    
    criteria = {
            'dateRange':    {
                            'kind': 'dfareporting#dateRange',
                            'relativeDateRange': 'LAST_7_DAYS' 
                            },

            'dimensions':   [
                            {'kind': 'dfareporting#sortedDimension','name': 'dfa:advertiserId'},
                            {'kind': 'dfareporting#sortedDimension','name': 'dfa:campaignId'},
                            {'kind': 'dfareporting#sortedDimension','name': 'dfa:placementId'},
                            {'kind': 'dfareporting#sortedDimension','name': 'dfa:creativeId'},
                            {'kind': 'dfareporting#sortedDimension','name': 'dfa:adId'},
                            {'kind': 'dfareporting#sortedDimension','name': 'dfa:date'}
                            ],

            'metricNames':  [
                            'dfa:impressions',
                            'dfa:clicks',
                            'dfa:activeViewViewableImpressions',
                            'dfa:activeViewMeasurableImpressions',
                            'dfa:activeViewEligibleImpressions',
                            'dfa:activityClickThroughConversions',
                            'dfa:activityViewThroughConversions',
                            'dfa:totalConversions',
                            'dfa:richMediaVideoPlays',
                            'dfa:richMediaVideoFirstQuartileCompletes',
                            'dfa:richMediaVideoThirdQuartileCompletes',
                            'dfa:richMediaVideoMidpoints',
                            'dfa:richMediaVideoCompletions',
                            'dfa:richMediaVideoViews',
                            'dfa:richMediaAverageVideoViewTime'
                            ],
            'dimensionFilters': [
                                {
                                    'dimensionName': 'dfa:campaignId',
                                    'kind': 'dfareporting#dimensionValue',
                                    'value': str(campaign_id)
                                }
                                
                                ]} 
    if floodlight_id != None:
        print(floodlight_id)
        criteria['dimensionFilters'].append({
                                'dimensionName': 'dfa:activityGroup',                            
                                'id': str(floodlight_id),
                                'kind': 'dfareporting#dimensionValue',
                                'matchType': 'EXACT'
                            })
        
    
    report['criteria'] = criteria

    

    return report

def build_rf_report(name,advertiser_id,campaign_id):

    report = {
            'name': name + "_RF",
            'type': 'REACH',
            'fileName': str(campaign_id) + '_RF_AUTO',
            'format': 'CSV'
            }

    reachCriteria = {
            'dateRange':    {
                            "kind": "dfareporting#dateRange",
                            "relativeDateRange": 'LAST_30_DAYS'
                            },
            'dimensions':   [{
                            "kind": "dfareporting#sortedDimension",
                            "name": "dfa:advertiser"
                            },
                            {
                            "kind": "dfareporting#sortedDimension",
                            "name": "dfa:country"
                            },
                            {
                            "kind": "dfareporting#sortedDimension",
                            "name": "dfa:campaign"
                            }],
            "metricNames": [
                           "dfa:uniqueReachTotalReach",
                           "dfa:uniqueReachAverageImpressionFrequency"
                            ],
            "dimensionFilters": [
                               {
                                "kind": "dfareporting#dimensionValue",
                                "dimensionName": "dfa:campaign",
                                "id": str(campaign_id),
                                "matchType": "EXACT"
                               },
                               {
                                "kind": "dfareporting#dimensionValue",
                                "dimensionName": "dfa:country",
                                "id": "256",
                                "matchType": "EXACT"
                               },
                               {
                                "kind": "dfareporting#dimensionValue",
                                "dimensionName": "dfa:advertiser",
                                "id": str(advertiser_id),
                                "matchType": "EXACT"
                                }]
            }

    report['reachCriteria'] = reachCriteria

    return report

def insert_report(service,profile_id,report):
    inserted_report = service.reports().insert(
                profileId=profile_id, body=report).execute()
    
    report_id = int(inserted_report['id'])    
    return int(report_id)

def run_report(service,profile_id,report_id):
    resp = service.reports().run(profileId=profile_id, 
                                reportId=report_id,
                                synchronous=True).execute()
    file_id = resp['id']

    return int(file_id)

def update_report_date(service,profile_id,report_id,startDate=None,endDate=None,dateRange=None):
    if dateRange is not None:
        body = {
        "reachCriteria": {
            "dateRange": {
                "kind": "dfareporting#dateRange",
                "relativeDateRange": dateRange,
                "startDate": None,
                "endDate": None
            }
        }
    }
    elif (startDate is not None) and (endDate is not None):
        body = {
            "reachCriteria": {
                "dateRange": {
                    "kind": "dfareporting#dateRange",
                    "relativeDateRange": None,
                    "startDate": startDate,
                    "endDate": endDate
                }
            }
        }

    else:
        print("No date given")
        exit()

    print(body)

    resp = service.reports().patch(profileId=profile_id,
                                    reportId=report_id,
                                    body=body).execute()

def download_file(service,report_id,file_id):     
    CHUNK_SIZE = 32 * 1024 * 1024
    
    try:
        # Retrieve the file metadata.
        report_file = service.files().get(reportId=report_id,
                                        fileId=file_id).execute()

        endDate = report_file['dateRange']['endDate']
        filename = report_file['fileName']
        extension = report_file['format'].lower()

    except Exception as e:
        logger.error(e)
        logger.error("Retry in 10s")
        time.sleep(10)    
        
    if report_file['status'] == 'REPORT_AVAILABLE':        
        
        filename = "exports/{} {}.{}".format(filename,endDate,extension) 
        out_file = io.FileIO(filename, mode='wb')

        # Create a get request.
        request = service.files().get_media(reportId=report_id, fileId=file_id)

        # Create a media downloader instance.
        # Optional: adjust the chunk size used when downloading the file.
        downloader = http.MediaIoBaseDownload(out_file, request,
                                                chunksize=CHUNK_SIZE)

        # Execute the get request and download the file.
        download_finished = False
        while download_finished is False:
            _, download_finished = downloader.next_chunk()

        print('File %s downloaded to %s'
                % (report_file['id'], os.path.realpath(out_file.name)))

    

def fixtype(x):
        try:
            return int(x)
        except ValueError:
            pass
        try:
            return float(x)
        except ValueError:
            return str(x)

def process_csv(filepath):
    data = []   
    with open(filepath) as f:
        reader = csv.reader(f)      
        write = False        
        for line in reader:        
            if 'Advertiser ID' in line:
                write = True
            elif 'Grand Total:' in line:
                break
            elif write == True:
                data.append(tuple(map(fixtype,line)))
    return data

def process_rf_csv(filepath):
    data = []   
    with open(filepath) as f:
        reader = csv.reader(f)      
        write = False        
        for line in reader:
            if 'Date Range' in line:
                range = line[1]
                endDate = datetime.strptime(range[range.find('-')+2:],'%m/%d/%y')
                endDate = str(endDate.date())
            elif 'Advertiser' in line and 'Country' in line:
                write = True
            elif 'Grand Total:' in line:
                break
            elif write == True:
                line = [endDate] + [line[0]] + line[2:]
                data.append(tuple(map(fixtype,line)))
    return data