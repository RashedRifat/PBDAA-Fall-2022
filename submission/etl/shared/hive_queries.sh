# Check that we have the proper data 
SELECT * FROM ghg LIMIT 10;
SELECT * FROM tree LIMIT 10;
SELECT * FROM combined LIMIT 10;

# Count Rows 
SELECT COUNT(*) FROM ghg; 
SELECT COUNT(*) FROM tree; 
SELECT COUNT(*) FROM combined; 

# Top 10 Countries By Deforestation Per Year 
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

# Top Countries By Total GHG Emissions Per Years 
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