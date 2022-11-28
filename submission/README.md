# PBDAA-Fall-2022

PBDAA-Fall-2022 Course Project (GHG Emissions and Deforestation).

 This README outlines the directory structure and also includes instructions as to how to build and run our code as well.

## Directory Structure

* `ana_code`: this directory contains all of the code required to run our visualizations, including input and output.
  * `input`: contains all of the required files to run the visualization. Note that the input is generated from Hive and has been renamed to friendly names for easier processes.
  * `output`: contains all output graphs from running the analytic
* `data_ingest`: this folder contains all of data and code required to ingest data for both our initial data cleaning and profiling, as well as Hive queries.
  * `hive_ingest`: this folder contains commands as to ingesting data within Hive. Details are noted below.
  * `initial_data`: this folder contains the initial (uncleaned) dataset for the project
  * `initial_data_ingest`: this folder contains commands on how to initially ingest data into HDFS to run the cleaning and profiling code.
* `etl`: this folder contains all of the code used within our ETL pipeline
  * `jfa8735`: this folder contains the code used to clean the deforestation/treeloss dataset.
    * `cleaning_code`: this folder contains the *.class,*.jar, *.java files used to clean the deforestation/treeloss dataset.
    * `output`: this folder contains the results of the initial data clean
  * `rmr557`: this folder contains the code used to clean the ghg_emissions dataset.
    * `cleaning_code`: this folder contains the *.class,*.jar, *.java files used to clean the ghg_emissions dataset.
    * `output`: this folder contains the results of the initial data clean
  * `shared`: this folder contains all of the Hive queries that is used to analyze the cleaned datasets
* `profiling_code`: this folder contains all of the code used to profile our datasets
  * `jfa8735`: this folder contains the *.class,*.jar, *.java files used to profile the deforestation/treeloss dataset.
  * `rmr557`: this folder contains the *.class,*.jar, *.java files used to profile the ghg_emissions dataset.
* `screenshots`: folder contains all of the screenshots taken of our analytic running
  * `cleaning_and_profiling`: this folder contains all of the screenshots related to cleaning and profiling our code.
    * `jfa8735`: this folder contains all screenshots used to clean and profile the deforestation/treeloss dataset.
    * `rmr557`: this folder contains all screenshots used to clean and profile the ghg_emissions dataset
  * `data_ingest`: this folder contains all of the screenshots displaying the ingestion of the data into Hive.
  * `hive_etl`: this folder contains all of the screenshots displaying the running of our analysis

## Running The Project

This section documents all of the steps required to run our project. Please note that the majority of our work has been completed in either MapReduce or Hive, from which we rename the output files to generate our visualizations.

### Initial Data Ingestion

* For initial data ingestion, please put the initial data within `\data_ingest\initial_data` within HDFS. Please be sure to note down your directory as this will be required to complete the later profiling and cleaning steps.
* For the specific command as to putting this data into HDFS, please run
  
      hdfs dfs -put localFile /hdfs/filepath 

  * You may also find these instructions within `\data_ingest\initial_data_ingest`

### Hive Data Ingestion

* To ingest data into Hive, please make sure that you know where your data is located within HDFS.
* Next, connect to your Hive environment. If you are using NYU HPC servers, please refer to [this article](https://sites.google.com/nyu.edu/nyu-hpc/training-support/tutorials/big-data-tutorial-hive?authuser=0) for instructions on connecting to Hive.
* Finally, once you are within Hive, execute the queries noted within the the `\data_ingest\hive_ingest\hive_commands.sh` file. This should be as simple as copy and paste!
  * Please note that if you are a grader, you should have access to the data that the authors have stored within HDFS under `rmr557/project/ghg` and `rmr557/project/deforestation`. If you do so, you should be apply to run the queries to ingest the data as is, without any need to specify any further paths.
  * Once these initial tables have been loaded into Hive, further queries down the line will generate more queries and tables as necessary.

### Profiling Data Steps

* To profile our data, please ensure that you are cognizant of where your initial data will be stored within HDFS.
* Next, execute the profiling code within `\profiling_code` directory. To do so, you will need to supply the input and output paths for where you data is stored within HDFS.
  * Note that the code here is separated into different directories based off of the NetID of the author.
    * jfa8735: treeloss data profiling
    * rmr557: ghg_emissions data profiling
* Your output will be generated at the specified path that you supplied.

### Cleaning Data Steps

* To clean our data, please ensure that you are cognizant of where your initial data will be stored within HDFS.
* Next, execute the profiling code within `\etl\NETID\cleaning_code` directory. To do so, you will need to supply the input and output paths for where you data is stored within HDFS.
  * Note that the code here is separated into different directories based off of the NetID of the author.
    * jfa8735: treeloss data profiling
    * rmr557: ghg_emissions data profiling
* Your output will be generated at the specified path that you supplied.
* Please note that the data made available to the graders of this project are in their clean form!

### Hive Queries and Output

* To run our Hive queries, please ensure that you are logged into your Hive environment. If you are using NYU HPC servers, please refer to [this article](https://sites.google.com/nyu.edu/nyu-hpc/training-support/tutorials/big-data-tutorial-hive?authuser=0) for instructions on connecting to Hive.
* Please ensure that you have followed the instructions above to ingest the cleaned data into your Hive table.
  * Please note that if you are a grader, you should have access to the data that the authors have stored within HDFS under `rmr557/project/ghg` and `rmr557/project/deforestation`. If you do so, you should be apply to run the queries to ingest the data as is, without any need to specify any further paths.
* To run our Hive queries, simply copy and paste the queries noted within the `etl/shared/hive_queries.sh`. Please note that the results of these queries (such as HDFS files) will require specific paths that you will need to supply.
  * Please note that if you are a grader, you should have access to the output directory that the authors have stored within HDFS under `rmr557/project/output`. If you do so, you should be apply to run the queries to ingest the data as is, without any need to specify any further paths. Note that output will be noted within the `rmr557/project/output` folder, as noted above.
* Please note that the authors, for ease of use in generating visualizations, have renamed the files generated by their analysis into friendlier names. This files will be use within the visualizations aspect of this project.
  * If you would like to view these code, please refer to the `ana_code\input` directory.

### Visualizations

* Please note that the authors, for ease of use in generating visualizations, have renamed the files generated by their analysis into friendlier names. This files will be use within the visualizations aspect of this project.
  * If you would like to view these files, please refer to the `ana_code\input` directory.
* Note that our visualizations are done within Python, specifically within a .ipynb notebook. To run this code, please follow the steps below.
  1. Install Python3 on your local computer.
  2. Navigate to `\ana_code`
  3. Install all of the required libraries from the requirements.txt file.
     * The use of a virtual environment is recommended here, if the user is able to implement it.
  4. Please ensure that there is an output folder at `ana_code\output`
  5. Run all cells within the main.ipynb notebook.
  6. You results should be generated within the `ana_code\output` directory.
