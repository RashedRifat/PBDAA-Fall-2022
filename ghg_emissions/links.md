# Resources

1. [ClimateWatch Data - Historical GHG Emissions Database](https://www.climatewatchdata.org/data-explorer/historical-emissions?historical-emissions-data-sources=cait&historical-emissions-gases=all-ghg&historical-emissions-regions=All%20Selected&historical-emissions-sectors=total-including-lucf%2Ctotal-including-lucf&page=1)
2. [GHG Emissions for the US](https://www.fs.usda.gov/rds/archive/catalog/RDS-2022-0052)
3. [Our World In Data](https://github.com/owid/co2-data)
4. [ISO Codes](https://github.com/lukes/ISO-3166-Countries-with-Regional-Codes/blob/master/all/all.csv)

## Data Cleaning  

* Use the Climate Change CAIT Data
  * Title: Climate Watch Historical GHG Emissions
  * Description: Climate Watch Historical Country Greenhouse Gas Emissions Data
* Data Cleaning Steps
  * Skip the first row of input (header)
  * For all columns
    * Strip whitespace 
    * transform word columns to be lowercase, with spaces replaced by underscores 
  * Remove all rows `except`
    * Total Including LUCF
      * Description: Total including land use change and forestry
    * Total Land-Use Change and Forestry
  * Remove the following columns
    * Source
    * Gas
    * All Years < 2001
* Code Notes 
  * Use Hadoop Map Reduce to process the data 
  * To process, use the ISO Code and the Category (combined as a string) as they key 
  * Keep the rest of columns as a comma separated string 
  * Reducer stage should just write out that data
    * Write out the row as a single key, the value should be a null row 
