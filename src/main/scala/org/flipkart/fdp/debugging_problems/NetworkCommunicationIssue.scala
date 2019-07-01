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

import java.io.{DataInputStream, DataOutputStream, EOFException, IOException, ObjectInputStream, ObjectOutputStream}
import java.net.{InetAddress, ServerSocket, Socket, SocketException}
import java.util.Calendar
import java.util.concurrent.Executors

import org.apache.spark.TaskContext
import org.apache.spark.sql.Row
import org.slf4j.{Logger, LoggerFactory}

import scala.util.Random

class NetworkCommunicationIssue extends RandomPartitionSimulator {
  val log: Logger = LoggerFactory.getLogger(classOf[NetworkCommunicationIssue])
  var serverIP: String = InetAddress.getLocalHost.getHostAddress
  var serverPort: Int = 0

  override def run(): Unit = {
    val serverSocket = openServerSocket
    serverIP = serverSocket.getInetAddress.getHostAddress
    serverPort = serverSocket.getLocalPort
    super.run() // Do Random Partitions
  }

  override def runPartition(x: Iterator[Row]): Unit = {
    startHealthCheckThread(TaskContext.get.partitionId(), isHealthy = false)
  }

  override def runDefaultPartition(x: Iterator[Row]): Unit = {
    startHealthCheckThread(TaskContext.get.partitionId(), isHealthy = true)
  }

  def startHealthCheckThread(partitionId: Int, isHealthy: Boolean): Unit = {
    new Thread("ClientThread") {
      @Override
      override def run(): Unit = {
        val clientSocket = new Socket(serverIP, serverPort)
        setName("ClientThread-" + clientSocket.getInetAddress + ":" + clientSocket.getLocalPort)
        val in = new ObjectInputStream(new DataInputStream(clientSocket.getInputStream))
        val out = new ObjectOutputStream(new DataOutputStream(clientSocket.getOutputStream))
        for(i <- 0 until 600){
          reportHealth(in, out, partitionId, isHealthy)
          if(isHealthy) {
            log.info("Processing Data")
            Thread.sleep(300)
          }
          else {
            log.error("Failed to process Data")
            Thread.sleep(1000)
          }
        }
        if(isHealthy)
          log.info("Finished processing data")
        else
          log.error("Failed to process data")
        out.writeUTF("BYE")
        out.close()
        in.close()
        clientSocket.close()
      }
    }.start()
  }

  def reportHealth(in: ObjectInputStream, out: ObjectOutputStream, partitionId:Int, bool: Boolean): Unit = {
    out.writeUTF(partitionId + " is healthy")
    out.flush()
    val response = in.readUTF()
    if(response != "OK")
      log.error("Something is wrong ! Health Check Ping Failed !")
  }

  def acceptHealth(in: ObjectInputStream, out: ObjectOutputStream): Unit = {
    var finished = false
    while(! finished) {
      val healthCheckStatus = in.readUTF()
      if (healthCheckStatus != null && healthCheckStatus.contains(randPartitionId.toString))
        out.writeUTF("Unknown Client:" + randPartitionId)
      else
        out.writeUTF("OK")
      out.flush()
      if(healthCheckStatus != null && healthCheckStatus.contains("BYE"))
        finished = true
    }
  }

  def openServerSocket: ServerSocket = {
    val serverSocket = new ServerSocket(0)  //Random port
    val pool = Executors.newFixedThreadPool(8)
    new Thread("Server") {
      @Override
      override def run(): Unit = {
        log.info("Server Started on " + serverSocket.getInetAddress + ":" + serverSocket.getLocalPort)
        log.info("Accepting Connections...")
        for(i <- 0 until 6000) {
          ServerWorkerThread(serverSocket.accept()).start()
        }
      }
    }.start()
    serverSocket
  }

  case class ServerWorkerThread(socket: Socket) extends Thread("ServerWorkerThread") {

    override def run(): Unit = {
      log.info("Accepted connection from: " + socket.getInetAddress + ":" + socket.getPort)
      setName("ServerWorkerThread-" + socket.getInetAddress + ":" + socket.getPort+"-Started_at_" + Calendar.getInstance().getTime)
      val rand = new Random(System.currentTimeMillis())
      try {
        val out = new ObjectOutputStream(new DataOutputStream(socket.getOutputStream))
        val in = new ObjectInputStream(new DataInputStream(socket.getInputStream))
        acceptHealth(in, out)
        out.close()
        in.close()
        socket.close()
      }
      catch {
        case e: SocketException =>
        case e: EOFException =>
          () // avoid stack trace when stopping a client with Ctrl-C
        case e: IOException =>
          log.error("Failed in ServerWorkerThread due to" + e.getMessage, e)
      }
      setName("ServerWorkerThread")
    }
  }
}
