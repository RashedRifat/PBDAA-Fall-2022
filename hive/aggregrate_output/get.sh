#!/bin/sh 

hdfs dfs -get project/output
mv output/000000_0 ./
rm -rf output | echo "output dir does not existst" 
cat 000000_0
