/*
The following SQL queries build the schema for the NBCU reporting database
Can be run manually or invoked by using a build_schema function (<-- detail this later)
2019 - Kyle Randolph - kyle.randolph@essenceglobal.com
*/


CREATE TABLE `advertisers` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `name` varchar(255) DEFAULT NULL,
  `dcm_advertiser_id` bigint(11) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=8 DEFAULT CHARSET=utf8mb4;

CREATE TABLE `campaigns` (
  `id` bigint(11) NOT NULL DEFAULT '0',
  `advertiserId` bigint(11) DEFAULT NULL,
  `campaignName` varchar(255) DEFAULT NULL,
  `startDate` date DEFAULT NULL,
  `endDate` date DEFAULT NULL,
  `active` int(1) DEFAULT '0',
  `moatId` bigint(11) DEFAULT NULL,
  `reportId` bigint(20) DEFAULT NULL,
  `floodlightId` int(11) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE `dcm_ads` (
  `id` bigint(20) NOT NULL,
  `advertiserId` bigint(20) NOT NULL,
  `campaignId` bigint(20) NOT NULL,
  `adName` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE `dcm_creative` (
  `id` bigint(15) NOT NULL,
  `advertiserId` bigint(15) NOT NULL,
  `creativeName` varchar(255) DEFAULT NULL,
  `creativeLength` float DEFAULT NULL,
  `creativeType` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE `dcm_groups` (
  `id` int(11) NOT NULL DEFAULT '0',
  `advertiserId` bigint(15) DEFAULT NULL,
  `campaignId` bigint(15) DEFAULT NULL,
  `placementGroupName` varchar(255) DEFAULT NULL,
  `placementGroupType` varchar(255) DEFAULT NULL,
  `startDate` date DEFAULT NULL,
  `endDate` date DEFAULT NULL,
  `units` bigint(15) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE `dcm_measures` (
  `advertiserId` bigint(20) DEFAULT NULL,
  `campaignId` bigint(20) DEFAULT NULL,
  `placementId` bigint(11) DEFAULT NULL,
  `adId` bigint(20) DEFAULT NULL,
  `creativeId` bigint(20) DEFAULT NULL,
  `date` date DEFAULT NULL,
  `impressions` bigint(20) DEFAULT NULL,
  `clicks` bigint(20) DEFAULT NULL,
  `activeViewViewableImpressions` int(11) DEFAULT NULL,
  `activeViewMeasurableImpressions` int(11) DEFAULT NULL,
  `activeViewEligibleImpressions` int(11) DEFAULT NULL,
  `click-throughConversions` int(11) DEFAULT NULL,
  `view-throughConversions` int(11) DEFAULT NULL,
  `totalConversions` int(11) DEFAULT NULL,
  `videoPlays` int(11) DEFAULT NULL,
  `videoFirstQuartileCompletions` int(11) DEFAULT NULL,
  `videoThirdQuartileCompletions` int(11) DEFAULT NULL,
  `videoMidpoints` int(11) DEFAULT NULL,
  `videoCompletions` int(11) DEFAULT NULL,
  `videoViews` int(11) DEFAULT NULL,
  `videoAverageViewTime` double DEFAULT NULL,
  UNIQUE KEY `unique_index` (`campaignId`,`placementId`,`adId`,`creativeId`,`date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE `dcm_placements` (
  `id` int(11) NOT NULL DEFAULT '0',
  `advertiserId` bigint(15) DEFAULT NULL,
  `campaignId` bigint(15) DEFAULT NULL,
  `placementGroupId` bigint(15) DEFAULT NULL,
  `placementName` varchar(255) DEFAULT NULL,
  `pricingType` varchar(255) DEFAULT NULL,
  `siteId` bigint(15) DEFAULT NULL,
  `startDate` date DEFAULT NULL,
  `endDate` date DEFAULT NULL,
  `ignore_imps_clicks` int(1) DEFAULT '0',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE `dcm_sites` (
  `id` int(11) DEFAULT NULL,
  `site` varchar(255) DEFAULT NULL,
  UNIQUE KEY `id` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE `site_served` (
  `date` date DEFAULT NULL,
  `campaignname` varchar(255) DEFAULT NULL,
  `placementgroupid` bigint(15) DEFAULT NULL,
  `placementname` varchar(255) DEFAULT NULL,
  `adname` varchar(255) DEFAULT NULL,
  `creativename` varchar(255) DEFAULT NULL,
  `creativelength` float DEFAULT NULL,
  `creativetype` varchar(255) DEFAULT NULL,
  `impressions` bigint(20) DEFAULT NULL,
  `viewableimpressions` bigint(255) DEFAULT NULL,
  `clicks` bigint(20) DEFAULT NULL,
  `click-throughconversions` int(11) DEFAULT NULL,
  `view-throughconversions` int(11) DEFAULT NULL,
  `totalconversions` int(11) DEFAULT NULL,
  `videoplays` int(11) DEFAULT NULL,
  `videoviews` int(11) DEFAULT NULL,
  `videomidpoints` int(11) DEFAULT NULL,
  `videocompletions` int(11) DEFAULT NULL,
  `placementDisplayName` varchar(255) DEFAULT NULL,
  `packageDisplayName` varchar(255) DEFAULT NULL,
  `placementTag` varchar(255) DEFAULT NULL,
  `placementTag2` varchar(255) DEFAULT NULL,
  `packageTag` varchar(255) DEFAULT NULL,
  `site` varchar(255) DEFAULT NULL,
  `pricingType` varchar(255) DEFAULT NULL,
  `transactionTime` datetime(6) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE `dcm_placement_tags` (
  `placementId` int(11) unsigned NOT NULL,
  `placementDisplayName` varchar(255) DEFAULT NULL,
  `tag1` varchar(255) DEFAULT NULL,
  `tag2` varchar(255) DEFAULT NULL,
  `audience` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`placementId`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE `dcm_package_tags` (
  `packageId` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `packageDisplayName` varchar(255) DEFAULT NULL,
  `tag1` varchar(255) DEFAULT NULL,
  `tag2` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`packageId`)
) ENGINE=InnoDB AUTO_INCREMENT=222653200 DEFAULT CHARSET=utf8mb4;


CREATE VIEW `v_site_served`
AS SELECT
   `site_served`.`date` AS `date`,
   `site_served`.`campaignname` AS `campaignname`,
   `site_served`.`placementgroupid` AS `placementgroupid`,
   `site_served`.`placementname` AS `placementname`,
   `site_served`.`adname` AS `adname`,
   `site_served`.`creativename` AS `creativename`,
   `site_served`.`creativelength` AS `creativelength`,
   `site_served`.`creativetype` AS `creativetype`,
   `site_served`.`impressions` AS `impressions`,
   `site_served`.`clicks` AS `clicks`,
   `site_served`.`videoplays` AS `videoplays`,
   `site_served`.`videocompletions` AS `videocompletions`,if(isnull(`dcm_placement_tags`.`placementDisplayName`),`site_served`.`placementDisplayName`,
   `site_served`.`placementDisplayName`) AS `placementDisplayName`,
   `site_served`.`site` AS `site`,
   `site_served`.`pricingType` AS `pricingType`
FROM ((`site_served` left join `dcm_placements` on((left(`dcm_placements`.`placementName`,6) = left(`site_served`.`placementname`,6)))) left join `dcm_placement_tags` on((`dcm_placements`.`id` = `dcm_placement_tags`.`placementId`)));

CREATE VIEW `dcm_flat` AS 
(
   select
      `m`.`date` AS `date`,
      `c`.`campaignName` AS `campaignname`,
      `p`.`placementGroupId` AS `placementgroupid`,
      `p`.`id` AS `placementid`,
      `p`.`placementName` AS `placementname`,
      `a`.`adName` AS `adname`,
      `cr`.`id` AS `creativeid`,
      `cr`.`creativeName` AS `creativename`,
      `cr`.`creativeLength` AS `creativelength`,
      `cr`.`creativeType` AS `creativetype`,
      (
         case
            `p`.`ignore_imps_clicks` 
            when
               1 
            then
               0 
            else
               `m`.`impressions` 
         end
      )
      AS `impressions`, 
      (
         case
            `p`.`ignore_imps_clicks` 
            when
               1 
            then
               0 
            else
               `m`.`clicks` 
         end
      )
      AS `clicks`, `m`.`click-throughConversions`, `m`.`view-throughConversions`, `m`.`totalConversions` AS `totalconversions`, `m`.`videoPlays` AS `videoplays`, `m`.`videoViews` AS `videoviews`, `m`.`videoMidpoints` AS `videomidpoints`, `m`.`videoCompletions` AS `videocompletions`, `m`.`activeViewViewableImpressions` AS `viewableimpressions`, `plt`.`placementDisplayName` AS `placementdisplayname`, `pat`.`packageDisplayName` AS `packagedisplayname`, `plt`.`tag1` AS `placementtag`, `plt`.`tag2` AS `placementtag2`, `pat`.`tag1` AS `packagetag`, `s`.`site` AS `site`, `p`.`pricingType` AS `pricingtype`, `plt`.`audience` AS `audience`
   from
      (
(((((((`dcm_measures` `m` 
         left join
            `campaigns` `c` 
            on((`c`.`id` = `m`.`campaignId`))) 
         left join
            `dcm_ads` `a` 
            on((`a`.`id` = `m`.`adId`))) 
         left join
            `dcm_placements` `p` 
            on((`p`.`id` = `m`.`placementId`))) 
         left join
            `dcm_sites` `s` 
            on((`s`.`id` = `p`.`siteId`))) 
         left join
            `dcm_creative` `cr` 
            on((`cr`.`id` = `m`.`creativeId`))) 
         left join
            `dcm_package_tags` `pat` 
            on((`pat`.`packageId` = `p`.`placementGroupId`))) 
         left join
            `dcm_placement_tags` `plt` 
            on((`plt`.`placementId` = `m`.`placementId`))) 

));