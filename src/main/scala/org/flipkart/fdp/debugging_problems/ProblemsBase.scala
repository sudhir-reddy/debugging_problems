/**
  * Copyright 2019 Flipkart Internet Pvt. Ltd.
  * <p>
  * Licensed under the Apache License, Version 2.0 (the "License");
  * you may not use this file except in compliance with the License.
  * You may obtain a copy of the License at
  * <p>
  * http://www.apache.org/licenses/LICENSE-2.0
  * <p>
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */

package org.flipkart.fdp.debugging_problems

import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.util.Random

trait ProblemsBase extends Serializable {
  def run() : Unit

  def getSparkSession: SparkSession = {
    SparkSession.builder()
      .master("yarn")
      .config("spark.yarn.maxAppAttempts", "1")
      .appName("Debug Session Examples run by " + System.getenv("USER"))
      // --conf 'spark.executor.extraJavaOptions=-XX:+HeapDumpOnOutOfMemoryError -Xmx512m -XX:HeapDumpPath=/tmp/ -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -Xloggc:/tmp/ExampleLauncher-gc.log'
      // --conf 'spark.driver.extraJavaOptions=-XX:+HeapDumpOnOutOfMemoryError -Xmx512m -XX:HeapDumpPath=/tmp/ -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -Xloggc:/tmp/ExampleLauncher-gc.log'
      .getOrCreate()
  }

  def getRandomDF: DataFrame = {
    val data = 1 to 100 map(x =>  Row(1+Random.nextInt(100), 1+Random.nextInt(100), 1+Random.nextInt(100)))
    val schema = new StructType().add(StructField("col1", IntegerType, nullable = false))
        .add(StructField("col2", IntegerType, nullable = false))
        .add(StructField("col3", IntegerType, nullable = false))
    val sparkSession = getSparkSession
    val rdd = sparkSession.sparkContext.parallelize(data, 10)
    sparkSession.sqlContext.createDataFrame(rdd, schema)
  }

  def getRandomDFWithPartitions(partitionsCount: Int) : DataFrame = {
    getRandomDF.repartition(partitionsCount)
  }
}
