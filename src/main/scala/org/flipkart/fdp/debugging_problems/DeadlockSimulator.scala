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

import java.lang.management.ManagementFactory
import java.util.UUID
import java.util.concurrent.locks.ReentrantLock

import org.apache.spark.TaskContext
import org.apache.spark.sql.Row

class DeadlockSimulator extends RandomPartitionSimulator {

  override def runPartition(x: Iterator[Row]): Unit = {
    val account1 = new Account(UUID.randomUUID().toString, 1000000)
    val account2 = new Account(UUID.randomUUID().toString, 1000000)
    // Start the second thread to simulate deadlock
    new Thread("Bank-Transfer-Thread-1") {
      @Override
      override def run() {
        while (true) {
          account1.transferFrom(account2, 1)
        }
      }
    }.start()
    // Start the second thread to simulate deadlock
    new Thread("Bank-Transfer-Thread-2") {
      @Override
      override def run() {
        while (true) {
          account2.transferFrom(account1, 1)
        }
      }
    }.start()

    // Start the deadlock detection code
    new Thread("Problem Detector Thread") {
      @Override
      override def run() {
        var ids: Array[Long] = null
        while(ids == null) {
          Thread.sleep(1000)
          val threadMXBean = ManagementFactory.getThreadMXBean
          ids = threadMXBean.findDeadlockedThreads
        }
        throw new RuntimeException("Something gone wrong !!")
        //println("SOmething gone wrong !!")
      }
    }.start()
    Thread.sleep(2*60*1000)
    if(randPartitionId == TaskContext.getPartitionId())
      throw new RuntimeException("Failed to do the transactions !")
  }

  class Account(val accountId: String, var balance: Long = 0) {
    val lock = new ReentrantLock()
    def credit(amount: Long): Long = {
      lock.lock()
      balance += amount
      lock.unlock()
      amount
    }

    // On success returns the amount debited and on failure returns 0
    def debit(amount: Long): Long = {
      if(balance-amount < 0) {
        //throw new RuntimeException("Insufficient balance")
        // For now return 0
        0
      }
      lock.lock()
      balance -= amount
      lock.unlock()
      amount
    }

    def transferFrom(fromAccount: Account, amount: Long): Unit = {
      println("Transferring Rs." + amount + " from: " + fromAccount.accountId + " to: " + this.accountId)
      fromAccount.lock.lock()
      this.lock.lock()
      try {
        credit(fromAccount.debit(amount))
        println("Transfer complete")
      }
      finally {
        this.lock.unlock()
        fromAccount.lock.unlock()
      }
    }
  }

}
