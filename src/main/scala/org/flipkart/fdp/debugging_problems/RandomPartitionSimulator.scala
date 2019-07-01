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

import org.apache.spark.TaskContext
import org.apache.spark.sql.Row
import org.slf4j.{Logger, LoggerFactory}

trait RandomPartitionSimulator extends ProblemsBase {
  val log: Logger

  // Pick a random partition
  val randPartitionId: Int = scala.util.Random.nextInt(10)

  def runPartition(x: Iterator[Row]): Unit

  override def run(): Unit = {
    log.info("RandPartition: " + randPartitionId)
    val df = getRandomDFWithPartitions(10)
    df.foreachPartition(x => {
      log.info("PARTITION: RandPartition: " + randPartitionId + ": " + TaskContext.get.partitionId())
      if(TaskContext.get.partitionId() == randPartitionId)
        runPartition(x)
      else
        runDefaultPartition(x)
    })
  }

  def runDefaultPartition(x: Iterator[Row]): Unit = {
    Thread.sleep(5000)
  }
}
