#/bin/bash
if [ $# -eq 0 ]; then
	echo "Wrong Usage ! Pass Example to Run as arguments."
	exit 1
fi
export HADOOP_CONF_DIR=/tmp/conf
export SPARK_HOME=/var/lib/fk-pf-spark/
export YARN_CONF_DIR=/tmp/conf
export HADOOP_DIR=/usr/hdp/2.4.0.0-169/hadoop/
export QUEUE=mlp
export JMX="-Dcom.sun.management.jmxremote.port=0 -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false"
spark-submit --master yarn --conf spark.executor.extraJavaOptions="$JMX -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/tmp/ -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps" --class org.flipkart.fdp.debugging_problems.ExampleLauncher --deploy-mode cluster --conf spark.task.maxFailures=1 --conf spark.executor.memory=1024m --conf spark.yarn.maxAppAttempts=1 --queue $QUEUE debugging_problems-1.0-SNAPSHOT.jar $1
