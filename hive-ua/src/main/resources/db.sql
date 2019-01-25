-- Create database.
CREATE DATABASE IF NOT EXISTS logs;

-- Select database.
USE logs;

-- Create external bids table

CREATE EXTERNAL TABLE IF NOT EXISTS bids_ext (
 BidID STRING, Ts STRING, iPinYouID STRING, UserAgent STRING, IP STRING, RegionID STRING, CityID STRING,
 AdExchange STRING, publicDomain STRING, URL STRING, AnonymousURL STRING, AdSlotID STRING, AdSlotWidth STRING,
 AdSlotHeight STRING, AdSlotVisibility STRING, AdSlotFormat STRING, AdSlotFloorPrice STRING, CreativeID STRING,
 BiddingPrice STRING, AdvertiserID STRING, UserProfileIDs STRING)
COMMENT 'Bid logs dataset tsv'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION '/user/root/hive/bids';

-- Create external cities table

CREATE EXTERNAL TABLE IF NOT EXISTS cities_ext(id INT, name STRING)
COMMENT 'Cities tsv'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION '/user/root/hive/cities';

-- Add parseUA UDTF to classpath
add jar hdfs://sandbox-hdp.hortonworks.com:8020/user/root/hive/ParseUserAgentUDTF.jar;
create temporary function parseUA as 'com.vasileva.ua.ParseUserAgentUDTF';

-- Creating view of parsed values using our UDTF
DROP VIEW user_agents;
CREATE VIEW user_agents AS SELECT cityid, ua.device, ua.os, ua.browser
FROM bids_ext lateral view parseUA(useragent) ua as device, os, browser;

-- Getting top device, browser, os by cities
SELECT cities.name, dev.device, os.os, browsers.browser
FROM cities_ext cities
JOIN (SELECT u.cityid, u.device, ROW_NUMBER() OVER (Partition BY u.cityid ORDER BY d.cnt DESC) rnk
    FROM user_agents u
    JOIN (SELECT cityid, device, COUNT(device) cnt FROM user_agents GROUP BY cityid, device) d
    ON u.cityid = d.cityid AND u.device = d.device) dev
ON cities.id = dev.cityid
JOIN (SELECT u.cityid, u.os, ROW_NUMBER() OVER (Partition BY u.cityid ORDER BY d.cnt DESC) rnk
    FROM user_agents u
    JOIN (SELECT cityid, os, COUNT(os) cnt FROM user_agents GROUP BY cityid, os) d
    ON u.cityid = d.cityid AND u.os = d.os) os
ON cities.id = os.cityid
JOIN (SELECT u.cityid, u.browser, ROW_NUMBER() OVER (Partition BY u.cityid ORDER BY d.cnt DESC) rnk
    FROM user_agents u
    JOIN (SELECT cityid, browser, COUNT(browser) cnt FROM user_agents GROUP BY cityid, browser) d
    ON u.cityid = d.cityid AND u.browser = d.browser) browsers
ON cities.id = browsers.cityid
WHERE dev.rnk = 1 AND os.rnk = 1 AND browsers.rnk = 1;

-- The process of splitting data within table/partition into several files, storing records with the same value
-- to a single file is called bucketing. It improves JOIN performance, so we should choose such key for bucketing
-- that is used in JOIN. Bucketing involves some more additional perks, but we will skip it.

-- So, we should choose "cityid" as a bucketing key, as it is used in all of the joins.

-- Partitioning is used for distributing load horizontally in a logical fashion and as a result - performance
-- optimisation. Considering this and the fact that data will grow, we should choose the key(s) for partitioning
-- that have nearly equal distribution of values between partitions.

-- AdSlot* properties will not distribute equally as they depend on client banner
-- or popular banner placement on advertising platforms.

-- UserProfileID, CreativeID, BiddingPrice AdSlotFloorPrice, AnonymousURL, URL, Ts, BidID are alomost unique,
-- so they are not suitable for partitioning, may not be distributed equally, so they are not suitable for bucketing.

-- Lets run some queries to check the distribution of the values of the rest of the columns.

SELECT IP, COUNT(*) cnt FROM bids_ext GROUP BY IP;
-- IPs have unequal distribution, so as iPinYouID,

SELECT RegionID, COUNT(*) cnt FROM bids_ext GROUP BY RegionID;
SELECT AdvertiserID, COUNT(*) cnt FROM bids_ext GROUP BY AdvertiserID;
SELECT AdExchange, COUNT(*) cnt FROM bids_ext GROUP BY AdExchange;
SELECT publicDomain, COUNT(*) cnt FROM bids_ext GROUP BY publicDomain;
-- Values by all these columns are distributed better but far from equal

SELECT to_date(ts) bid_date, COUNT(*) cnt FROM bids_ext GROUP BY bid_date;

-- Values by date are distributed not better than previous tested columns, but, when dataset will grow we can import
-- new partitions separately, as one file in original dataset represents single day bids, so this option is better than
-- others.

-- Create internal bids table, based on previous decisions. We will compute the number of buckets based on the
-- date with the minimal amount of records, as HDFS better handles bigger files than smaller ones. The smallest
-- amount of records is 326 830, the overall amount of records is 78 298 236, size of base table in MB is 23 333.
-- Median value of records is about 2 million per day which corresponds to 600 MB. Lets assume that 2 buckets will
-- be optimal solution.
-- #1
CREATE TABLE IF NOT EXISTS bids_optimised (
 BidID STRING, Ts STRING, iPinYouID STRING, UserAgent STRING, IP STRING, RegionID STRING, CityID STRING,
 AdExchange STRING, publicDomain STRING, URL STRING, AnonymousURL STRING, AdSlotID STRING, AdSlotWidth STRING,
 AdSlotHeight STRING, AdSlotVisibility STRING, AdSlotFormat STRING, AdSlotFloorPrice STRING, CreativeID STRING,
 BiddingPrice STRING, AdvertiserID STRING, UserProfileIDs STRING)
COMMENT 'Bid logs dataset optimised'
PARTITIONED BY (bid_date STRING)
CLUSTERED BY (CityID)
SORTED BY (UserAgent)
INTO 2 BUCKETS
STORED AS TEXTFILE;

-- #2
CREATE TABLE IF NOT EXISTS bids_optimised (
 BidID STRING, Ts STRING, iPinYouID STRING, UserAgent STRING, IP STRING, CityID STRING,
 AdExchange STRING, publicDomain STRING, URL STRING, AnonymousURL STRING, AdSlotID STRING, AdSlotWidth STRING,
 AdSlotHeight STRING, AdSlotVisibility STRING, AdSlotFormat STRING, AdSlotFloorPrice STRING, CreativeID STRING,
 BiddingPrice STRING, AdvertiserID STRING, UserProfileIDs STRING)
COMMENT 'Bid logs dataset optimised'
PARTITIONED BY (RegionID STRING)
CLUSTERED BY (CityID)
SORTED BY (UserAgent)
INTO 2 BUCKETS
STORED AS TEXTFILE;

-- #3
CREATE TABLE IF NOT EXISTS bids_optimised (
 BidID STRING, Ts STRING, iPinYouID STRING, UserAgent STRING, IP STRING, CityID STRING,
 AdExchange STRING, publicDomain STRING, URL STRING, AnonymousURL STRING, AdSlotID STRING, AdSlotWidth STRING,
 AdSlotHeight STRING, AdSlotVisibility STRING, AdSlotFormat STRING, AdSlotFloorPrice STRING, CreativeID STRING,
 BiddingPrice STRING, AdvertiserID STRING, UserProfileIDs STRING, device STRING, os STRING, browser STRING)
COMMENT 'Bid logs dataset optimised'
PARTITIONED BY (RegionID STRING)
CLUSTERED BY (CityID)
SORTED BY (UserAgent)
INTO 2 BUCKETS
STORED AS TEXTFILE;

INSERT OVERWRITE TABLE bids_optimised PARTITION(bid_date)
SELECT BidID, Ts, iPinYouID, UserAgent, IP, RegionID, CityID, AdExchange, publicDomain, URL, AnonymousURL,
 AdSlotID, AdSlotWidth, AdSlotHeight, AdSlotVisibility, AdSlotFormat, AdSlotFloorPrice, CreativeID, BiddingPrice,
 AdvertiserID, UserProfileIDs, from_unixtime(UNIX_TIMESTAMP(ts,'yyyyMMddHHmmssSSS'), 'yyyy-MM-dd') bid_date
FROM bids_ext
ORDER BY CityID, UserAgent;



CREATE TABLE IF NOT EXISTS bids_orc (
 BidID STRING, Ts STRING, iPinYouID STRING, UserAgent STRING, IP STRING, RegionID STRING, CityID STRING,
 AdExchange STRING, Domain STRING, URL STRING, AnonymousURL STRING, AdSlotID STRING, AdSlotWidth STRING,
 AdSlotHeight STRING, AdSlotVisibility STRING, AdSlotFormat STRING, AdSlotFloorPrice STRING, CreativeID STRING,
 BiddingPrice STRING, AdvertiserID STRING, UserProfileIDs STRING)
COMMENT 'Bid logs dataset ORC'
PARTITIONED BY (RegionID TINYINT)
CLUSTERED BY (CityID)
SORTED BY (UserAgent)
INTO 5 BUCKETS
STORED AS ORC;

--not compressed
CREATE TABLE IF NOT EXISTS bids_orc (
 BidID STRING, Ts STRING, iPinYouID STRING, UserAgent STRING, IP STRING, RegionID STRING, CityID STRING,
 AdExchange STRING, Domain STRING, URL STRING, AnonymousURL STRING, AdSlotID STRING, AdSlotWidth STRING,
 AdSlotHeight STRING, AdSlotVisibility STRING, AdSlotFormat STRING, AdSlotFloorPrice STRING, CreativeID STRING,
 BiddingPrice STRING, AdvertiserID STRING, UserProfileIDs STRING)
COMMENT 'Bid logs dataset ORC'
PARTITIONED BY (RegionID TINYINT)
CLUSTERED BY (CityID)
SORTED BY (UserAgent)
INTO 5 BUCKETS
STORED AS ORC
tblproperties ("orc.compress"="NONE");

CREATE INDEX device_idx
ON TABLE bids_orc (device)
AS 'org.apache.hadoop.hive.ql.index.compact.CompactIndexHandler'
WITH DEFERRED REBUILD;
ALTER INDEX device_idx on bids_orc REBUILD;

CREATE INDEX browser_idx
ON TABLE bids_orc (browser)
AS 'org.apache.hadoop.hive.ql.index.compact.CompactIndexHandler'
WITH DEFERRED REBUILD;
ALTER INDEX browser_idx on bids_orc REBUILD;

CREATE INDEX os_idx
ON TABLE bids_orc (os)
AS 'org.apache.hadoop.hive.ql.index.compact.CompactIndexHandler'
WITH DEFERRED REBUILD;
ALTER INDEX os_idx on bids_orc REBUILD;
