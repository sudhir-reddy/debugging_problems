#!/usr/bin/env bash
#/bin/bash
if [ $# -eq 0 ]; then
	echo "Wrong Usage ! Pass Example to Run as arguments."
	exit 1
fi
export QUEUE=mlp
if [$1 -eq 2 ]; then
	export MAX_MEM_EXECUTOR=14G
else
	export MAX_MEM_EXECUTOR=1024m
fi
export JMX="-Dcom.sun.management.jmxremote.port=0 -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false"
spark-submit --master yarn --conf spark.executor.extraJavaOptions="$JMX -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/tmp/ -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps" --class org.flipkart.fdp.debugging_problems.ExampleLauncher --deploy-mode cluster --conf spark.task.maxFailures=1 --conf spark.executor.memory=$MAX_MEM_EXECUTOR --conf spark.yarn.maxAppAttempts=1 --queue $QUEUE debugging_problems-1.0-SNAPSHOT.jar $1
