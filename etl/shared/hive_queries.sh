# Check that we have the proper data 
SELECT * FROM ghg LIMIT 10;
SELECT * FROM tree LIMIT 10;
SELECT * FROM combined LIMIT 10;

# Count Rows 
SELECT COUNT(*) FROM ghg; 
SELECT COUNT(*) FROM tree; 
SELECT COUNT(*) FROM combined; 

# Generate the Correlation Coeffcient Table 
CREATE TABLE rate as 
SELECT iso, year, co2 / treeloss as coeff 
FROM combined 
WHERE sector = "land-use_change_and_forestry"; 

# Top 10 Countries By Deforestation Per Year 
INSERT OVERWRITE DIRECTORY '/user/rmr557/project/output' 
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
WHERE ranked_combined.rank < 4 and sector = 'total_including_lucf';

# Top Countries By Total GHG Emissions Per Years 
INSERT OVERWRITE DIRECTORY '/user/rmr557/project/output' 
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
WHERE ranked_combined.rank < 4 and sector = 'total_including_lucf';

# Top Countries By GHG Emissions Per Year, Per Total, Per LUCF Sector 
INSERT OVERWRITE DIRECTORY '/user/rmr557/project/output' 
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
WHERE ranked_combined.rank < 4 and sector = 'land-use_change_and_forestry';

# Per Year, take the average global coeff rate 
INSERT OVERWRITE DIRECTORY '/user/rmr557/project/output' 
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
SELECT year, AVG(coeff) as avg_coeff 
FROM rate 
WHERE treeloss >= 0
GROUP BY year; 

# Per Year, Per ISO, gather the countries with the highest coeff 
INSERT OVERWRITE DIRECTORY '/user/rmr557/project/output' 
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
WHERE ranked < 4; 

# Generate Lag Table To Calculate YOY Growth 
CREATE TABLE lag_total AS
SELECT year, iso, co2, treeloss, 
        LAG(co2, 1, 0) OVER (PARTITION BY iso ORDER by year) as growth 
FROM combined
WHERE sector = "total_including_lucf"; 

CREATE TABLE lag_lucf AS
SELECT year, iso, co2, treeloss, 
        LAG(co2, 1, 0) OVER (PARTITION BY iso ORDER by year) as growth 
FROM combined
WHERE sector = "land-use_change_and_forestry"; 

CREATE TABLE lag_tree AS
SELECT year, iso, co2, treeloss, 
        LAG(treeloss, 1, 0) OVER (PARTITION BY iso ORDER by year) as growth 
FROM combined
WHERE sector = "total_including_lucf"; 

# Get YOY Growth In Terms of Treeloss (Globally)
INSERT OVERWRITE DIRECTORY '/user/rmr557/project/output' 
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
SELECT year, AVG(pct_change) as change_in_treeloss
FROM (
    SELECT *, 
        ( (treeloss - growth) / growth) * 100 as pct_change
    FROM lag_tree
) as intermediate 
GROUP BY year; 

# Get YOY Growth in Total CO2 Emissions 
INSERT OVERWRITE DIRECTORY '/user/rmr557/project/output' 
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
SELECT year, AVG(pct_change) as change_in_total_ghg
FROM (
    SELECT iso, year, co2, growth, 
        ( (co2 - growth) / growth) * 100 as pct_change
    FROM lag_total
) as intermediate 
WHERE intermediate.year > 2001
GROUP BY year; 

# Get YOY Growth in LUCF CO2 Emissions 
INSERT OVERWRITE DIRECTORY '/user/rmr557/project/output' 
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
SELECT year, AVG(pct_change) as change_in_lucf_ghg
FROM (
    SELECT iso, year, co2, growth, 
        ( (co2 - growth) / growth) * 100 as pct_change
    FROM lag_lucf
) as intermediate 
WHERE intermediate.year > 2001
GROUP BY year; 

# Get The Top Countries By Treeloss Change Per Year
INSERT OVERWRITE DIRECTORY '/user/rmr557/project/output' 
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
SELECT iso, year, pct_change, rank 
FROM (
    SELECT iso, year, pct_change, 
    rank() over (PARTITION BY year ORDER BY pct_change DESC) as rank 
    FROM (
        SELECT *, 
        ( (treeloss - growth) / growth) * 100 as pct_change
        FROM lag_tree
    ) as intermediate
) as final 
WHERE final.rank < 4 AND final.year > 2001; 