#!/bin/sh

hdfs dfs -rm -r -f hw7/part2/output
rm -rf *.class *.jar
javac -classpath `yarn classpath` -d . CleanMapper.java
javac -classpath `yarn classpath` -d . CleanReducer.java
javac -classpath `yarn classpath`:. -d . Clean.java
jar -cvf Clean.jar *.class
hadoop jar Clean.jar Clean project/ghg_emissions.csv /user/rmr557/hw7/part2/output
echo ""
hdfs dfs -tail hw7/part2/output/part-r-00000
