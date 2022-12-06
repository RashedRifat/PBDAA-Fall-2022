#!/bin/sh

hdfs dfs -rm -r -f hw7/part1/output
rm -rf *.class *.jar
javac -classpath `yarn classpath` -d . CountRecsMapper.java
javac -classpath `yarn classpath` -d . CountRecsReducer.java
javac -classpath `yarn classpath`:. -d . CountRecs.java
jar -cvf CountRecs.jar *.class
hadoop jar CountRecs.jar CountRecs project/ghg_emissions.csv /user/rmr557/hw7/part1/output
echo ""
hdfs dfs -cat hw7/part1/output/part-r-00000
