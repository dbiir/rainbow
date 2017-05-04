#!/bin/bash

java -jar cord-executor_jar/cord-executor.jar spark $1 hdfs://$1:9000/$2 hdfs://$1:9000/$3 resources/query.column /root/resources/spark_eva/
