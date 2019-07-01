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

import java.io.{BufferedReader, File, FileReader}

import org.apache.spark.sql.Row

import scala.collection.mutable.ListBuffer

class TooManyFilesOpen extends RandomPartitionSimulator {

  override def runPartition(x: Iterator[Row]): Unit = {
    var filesMap: ListBuffer[BufferedReader] = new ListBuffer[BufferedReader]()
    var isTriggered = false
    while(! isTriggered) {
      try {
        filesMap += getFile
      } catch {
        case x: Throwable =>
          isTriggered = true
          println("Something gone wrong !!")
          //println("Failed with exception:" + x)
      }
    }
    Thread.sleep(100000)
    throw new RuntimeException("Failed to process data")
  }

  def getFile: BufferedReader = {
    val fileObject = new File("/etc/passwd" )
    val openReader = new BufferedReader(new FileReader(fileObject))
    openReader
    // Don't close file
  }

}
