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

import java.util.UUID

import org.apache.spark.sql.Row
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable.ArrayBuffer

class OutOfMemoryProblem extends RandomPartitionSimulator {

  val log: Logger = LoggerFactory.getLogger(classOf[OutOfMemoryProblem])

  override def runPartition(x: Iterator[Row]): Unit = {
    var mapOfObjects = scala.collection.mutable.Map[String, HMO]()
    var oom = false
    var error: Error = null
    while(! oom) {
      val size = 1000000000
      try {
        val data = collection.mutable.ArrayBuffer.fill(size)(UUID.randomUUID().toString)
        mapOfObjects.put(UUID.randomUUID().toString, new HMO(data))
      }
      catch {
        case x: OutOfMemoryError =>
          oom = true
          error = x
      }
    }
    log.error("Something wrong ! Failed to create memory !", error)
    Thread.sleep(60000)
  }
// Huge Memory Object
  class HMO(data: ArrayBuffer[String])
}
