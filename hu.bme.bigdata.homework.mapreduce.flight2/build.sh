#!/bin/bash

javac -cp /usr/lib/hadoop/*:/usr/lib/hadoop-mapreduce/* hu/bme/bigdata/homework/mapreduce/flight2/*.java hu/bme/bigdata/homework/mapreduce/flight2/mappers/*.java hu/bme/bigdata/homework/mapreduce/flight2/reducers/*.java -d build -Xlint
jar -cvf demo.jar -C build/ .
hadoop jar demo.jar hu.bme.bigdata.homework.mapreduce.flight2.MapReduceApplication input/2008.csv output1
