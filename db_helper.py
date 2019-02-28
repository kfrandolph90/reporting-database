import pymysql
import logging

logger = logging.getLogger(__name__)

class dbConnection:    
    campaign_q = """
                SELECT  *
                FROM    campaigns
                WHERE   active = 1
                """

    insert_sites =  """
                    INSERT INTO dcm_sites (id, site)
                    VALUES (%s, %s)
                    ON DUPLICATE KEY UPDATE site=site
                    """
    insert_creative =   """
                        INSERT INTO dcm_creative (id, advertiserId,creativeName,creativeLength,creativeType)
                        VALUES (%s, %s, %s, %s, %s)
                        ON DUPLICATE KEY UPDATE creativeName = creativeName
                        """ 

    insert_placements =   ("INSERT INTO dcm_placements "
                            "(id,"
                            "advertiserId,"
                            "campaignId,"
                            "placementGroupId," 
                            "placementName," 
                            "siteId,"
                            "pricingType," 
                            "startDate," 
                            "endDate) "                            
                            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
                            "ON DUPLICATE KEY UPDATE endDate = endDate,pricingType=pricingType")

    insert_placements =   ("INSERT INTO dcm_placements "
                            "(id,"
                            "advertiserId,"
                            "campaignId,"
                            "placementGroupId," 
                            "placementName," 
                            "siteId,"
                            "pricingType," 
                            "startDate," 
                            "endDate) "                            
                            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
                            "ON DUPLICATE KEY UPDATE endDate = endDate") 
    
    insert_groups =   ("INSERT INTO dcm_groups "
                            "(id,"
                            "advertiserId,"
                            "campaignId,"
                            "placementGroupName,"
                            "placementGroupType,"
                            "startDate,"
                            "endDate,"
                            "units) "                  
                            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
                            "ON DUPLICATE KEY UPDATE placementGroupName = VALUES(placementGroupName),startDate=VALUES(startDate),endDate=VALUES(endDate),units=VALUES(units)")    
   
    insert_ads =    ("INSERT INTO dcm_ads "
                        "(id,advertiserId,campaignId,adName) "
                        "VALUES (%s,%s,%s,%s) "
                        "ON DUPLICATE KEY UPDATE adName = adName")
    
    update_report_id =  (
                        "UPDATE campaigns "
                        "SET reportId = %s "
                        "WHERE id = %s "
                        )

    update_rf_report_id = (
                          "UPDATE campaigns "
                          "SET rfReportId = %s "
                          "WHERE id = %s "
                          )

    insert_report = ("INSERT INTO dcm_measures "
                    "(`advertiserId`,`campaignId`,`placementId`,`creativeId`,"
                    "`adId`,`date`,`impressions`,`clicks`,"
                    "`activeViewViewableImpressions`,`activeViewMeasurableImpressions`,"
                    "`activeViewEligibleImpressions`,`click-throughConversions`,"
                    "`view-throughConversions`,`totalConversions`,"
                    "`videoPlays`,`videoFirstQuartileCompletions`,`videoThirdQuartileCompletions`,"
                    "`videoMidpoints`,`videoCompletions`,`videoViews`,`videoAverageViewTime`)"
                    "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) "
                    "ON DUPLICATE KEY UPDATE `impressions` = `impressions`"
                    )

    insert_rf_report = ("INSERT INTO dcm_reach "
                        "(`date`,`advertiser`,`campaign`,`totalReach`,`averageFrequency`)"
                        "VALUES (%s,%s,%s,%s,%s) "
                        "ON DUPLICATE KEY UPDATE `totalReach` = `totalReach`")

    insert_moat_display = ("INSERT INTO moat_display "
                            "(`active_in_view_time`,`date`,"
                            "`full_vis_2_sec_continuous_inview`,"
                            "`groupm_display_imps`,`impressions_analyzed`,"
                            "`l_full_visibility_measurable`,`level3_id`,"
                            "`measurable_impressions`) "
                            "VALUES (%s,%s,%s,%s,%s,%s,%s,%s) "
                            "ON DUPLICATE KEY UPDATE `impressions_analyzed` = `impressions_analyzed` "
                            )

    insert_moat_video = ("INSERT INTO moat_video "
                            "(`2_sec_video_in_view_impressions`,"
                            "`2_sec_video_in_view_percent`,"
                            "`ad_vis_and_aud_on_complete_percent`,"
                            "`date`,"
                            "`groupm_video_ots_completion`,"
                            "`human_and_groupm_video_payable_sum`,"
                            "`impressions_analyzed`,"
                            "`l_full_visibility_measurable`,"
                            "`level3_id`,"
                            "`measurable_impressions`,"
                            "`player_audible_full_vis_half_time_sum`,"
                            "`player_vis_and_aud_on_complete_sum`,"
                            "`valid_and_groupm_video_payable_sum`,"
                            "`viewable_gm_video_15cap_sum`) "
                            "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                            "ON DUPLICATE KEY UPDATE `measurable_impressions` = `measurable_impressions`")





    def __init__(self,host,database,username,password,port):
        self.host = host
        self.database = database
        self.username = username
        self.password = password
        self.port = port
    
    def connect(self):
        connection = pymysql.connect(host=self.host,
                                user=self.username,
                                password=self.password,                             
                                db=self.database,
                                charset='utf8mb4',
                                cursorclass=pymysql.cursors.DictCursor)
        self.connection = connection
        return "Connected!"
    
    def execute(self,query,values=None):
        self.connect()
        with self.connection.cursor() as cursor:
                if values:
                    cursor.execute(query,values)
                else:
                    cursor.execute(query)
                response = cursor.fetchall()
                logger.debug(cursor._last_executed)
                #rows = cursor.rowcount
        try:
            self.connection.commit()
            logger.info("Query Committed")
            self.connection.close()
        except Exception as e:
            logger.error("Error. Rolling Back.")
            self.connection.rollback()            
            self.connection.close()            
            logger.error(e)            
        
        return response
    
    def insertmany(self,query,values):
        self.connect()
        with self.connection.cursor() as cursor:
                cursor.executemany(query,values)
                #response = cursor.fetchall()
                rows = cursor.rowcount
        self.connection.commit()
        self.disconnect()
        return rows
    
    def disconnect(self):
        self.connection.close()
        self.connection = None
        return "Disconnected"
    
    def get_campaigns(self):        
        return self.execute(self.campaign_q)

    def write_sites(self,sites):
        logger.info('Writing Sites')
        values = [(int(site['id']),site['name']) for site in sites]
        return self.insertmany(self.insert_sites,values)
    
    def write_creative(self,creatives):
        logger.info('Writing Creatives')
        values = [(int(c['id']),
                    int(c['advertiserId']),
                    c['name'],
                    c.get('videoDuration'),
                    c['type']) for c in creatives]
        return self.insertmany(self.insert_creative,values)

    def write_placements(self,placements):
        logger.info('Writing Placements')
        values = [(int(p['id']),
                    int(p['advertiserId']),
                    int(p['campaignId']),
                    p.get('placementGroupId'),
                    p['name'],
                    int(p['siteId']),
                    p['pricingSchedule'].get('pricingType'),
                    p['pricingSchedule'].get('startDate'),
                    p['pricingSchedule'].get('endDate')
                    )
                    for p in placements]
        
        return self.insertmany(self.insert_placements,values)

    def write_ads(self,ads):
        logger.info('Writing ads')
        values = [(int(a['id']),
                    int(a['advertiserId']),
                    int(a['campaignId']),
                    a['name']       
                    )
                    for a in ads]
        return self.insertmany(self.insert_ads,values)

    def write_groups(self,groups):
        logger.info('Writing Placement Groups')
        values = [(int(g['id']),
                    int(g['advertiserId']),
                    int(g['campaignId']),                    
                    g['name'],                   
                    g['placementGroupType'],
                    g['pricingSchedule']['pricingPeriods'][0]['startDate'],
                    g['pricingSchedule']['pricingPeriods'][0]['endDate'],
                    int(g['pricingSchedule']['pricingPeriods'][0]['units']))
                    for g in groups]
        
        return self.insertmany(self.insert_groups,values)    
    
    def write_report_id(self,report_id,campaign_id):
        logger.info('Writing Report ID')
        return self.execute(self.update_report_id,(report_id,campaign_id))

    def write_rf_report_id(self,report_id,campaign_id):
        logger.info('Writing Reach Report ID')
        return self.execute(self.update_rf_report_id,(report_id,campaign_id))

    def write_report(self,report):
        logger.info('Writing Report')
        return self.insertmany(self.insert_report,report)

    def write_rf_report(self,report):
        logger.info('Writing Reach Report')
        return self.insertmany(self.insert_rf_report,report)

    def write_moat_display(self,data):
        logger.info('Writing Moat Display')
        values = [(d['active_in_view_time'],
                    d['date'],
                    d['full_vis_2_sec_continuous_inview'],
                    d['groupm_display_imps'],
                    d['impressions_analyzed'],
                    d['l_full_visibility_measurable'],
                    d['level3_id'],
                    d['measurable_impressions']) for d in data]        
        
        return self.insertmany(self.insert_moat_display,values)

    def write_moat_video(self,data):
        logger.info('Writing Moat Video')
        values = [(d['2_sec_video_in_view_impressions'],
                    d['2_sec_video_in_view_percent'],
                    d['ad_vis_and_aud_on_complete_percent'],
                    d['date'],
                    d['groupm_video_ots_completion'],
                    d['human_and_groupm_video_payable_sum'],
                    d['impressions_analyzed'],
                    d['l_full_visibility_measurable'],
                    d['level3_id'],
                    d['measurable_impressions'],
                    d['player_audible_full_vis_half_time_sum'],
                    d['player_vis_and_aud_on_complete_sum'],
                    d['valid_and_groupm_video_payable_sum'],
                    d['viewable_gm_video_15cap_sum']) for d in data]
        
        return self.insertmany(self.insert_moat_video,values)
    #def get_report
