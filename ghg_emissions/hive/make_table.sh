beeline 
!connect jdbc:hive2://hm-1.hpc.nyu.edu:10000/
use <NETID>; 

# Create GHG Emissions Table 
CREATE EXTERNAL TABLE ghg (iso STRING, source STRING, year  BIGINT, co2 DOUBLE) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE LOCATION '<path>';

# Create Tree Data Table 
CREATE EXTERNAL TABLE tree (iso STRING, year  BIGINT, treeLoss DOUBLE, co2 DOUBLE) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE LOCATION '<path>';
ALTER TABLE tree SET TBLPROPERTIES ("skip.header.line.count"="1");