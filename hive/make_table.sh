beeline 
!connect jdbc:hive2://hm-1.hpc.nyu.edu:10000/
use <NETID>; 

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

# Check that the table worked 
SELECT * FROM combined LIMIT 10;

# Count Rows 
SELECT COUNT(*) FROM combined; 

# Generate Output
INSERT OVERWRITE DIRECTORY '/user/<NETID>/<PATH>/<PATH>' 
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
<QUERY>

# Generate Output for Top 10 Countries By Deforestation Per Year 
INSERT OVERWRITE DIRECTORY '/user/<NETID>/<PATH>/<PATH>' 
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
SELECT * 
FROM (
    SELECT iso, year, treeLoss, sector, 
    rank() over (PARTITION BY year, sector ORDER BY treeLoss DESC) as rank 
    FROM combined
)
ranked_combined 
WHERE ranked_combined.rank < 11 and sector = 'total_including_lucf';

# Top Countries By GHG Emissions Per Year, Per Total GHG Emissions 
INSERT OVERWRITE DIRECTORY '/user/<NETID>/<PATH>/<PATH>' 
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
SELECT * 
FROM (
    SELECT iso, year, co2, sector, 
    rank() over (PARTITION BY year, sector ORDER BY co2 DESC) as rank 
    FROM combined
)
ranked_combined 
WHERE ranked_combined.rank < 11 and sector = 'total_including_lucf';

# Top Countries By GHG Emissions Per Year, Per Total, Per LUCF Sector 
INSERT OVERWRITE DIRECTORY '/user/<NETID>/<PATH>/<PATH>' 
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
SELECT * 
FROM (
    SELECT iso, year, co2, sector, 
    rank() over (PARTITION BY year, sector ORDER BY co2 DESC) as rank 
    FROM combined
)
ranked_combined 
WHERE ranked_combined.rank < 11 and sector = 'land-use_change_and_forestry';

# Generate the Correlation Coeffcient Table 
CREATE TABLE rate as 
SELECT iso, year, co2 / treeloss as coeff 
FROM combined 
WHERE sector = "land-use_change_and_forestry"; 

# Per Year, take the average global coeff rate 
INSERT OVERWRITE DIRECTORY '/user/<NETID>/<PATH>/<PATH>' 
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
SELECT year, AVG(coeff) as avg_coeff 
FROM rate 
WHERE treeloss >= 0
GROUP BY year; 

# Per Year, Per ISO, gather the countries with the highest coeff 
INSERT OVERWRITE DIRECTORY '/user/<NETID>/<PATH>/<PATH>' 
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
SELECT * FROM (
    SELECT *, 
    rank() over (PARTITION BY year ORDER by avg_coeff desc) as ranked 
    FROM (
        SELECT iso, year, AVG(coeff) as avg_coeff
        FROM rate
        WHERE treeLoss >= 0 
        GROUP BY year, iso
    ) grouped_rates
) ranked_rates 
WHERE ranked < 11; 