# Create GHG Emissions Table 
CREATE EXTERNAL TABLE ghg (iso STRING, source STRING, year  BIGINT, co2 DOUBLE) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE LOCATION '<path>';

# Create Tree Data Table 
CREATE EXTERNAL TABLE tree (iso STRING, year  BIGINT, treeLoss DOUBLE, co2 DOUBLE) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE LOCATION '<path>';
ALTER TABLE tree SET TBLPROPERTIES ("skip.header.line.count"="1");

# Join Table 
CREATE TABLE combined as 
SELECT tree.iso as iso, tree.year as year, tree.treeloss as treeloss, ghg.source as sector, ghg.co2 as co2 
FROM tree JOIN ghg 
ON (tree.iso = upper(ghg.iso) and tree.year = ghg.year); 