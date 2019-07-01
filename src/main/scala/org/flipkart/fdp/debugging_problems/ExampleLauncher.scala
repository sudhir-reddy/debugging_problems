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

object ExampleLauncher {

  val problems: Seq[ProblemsBase] = Seq(
    null, // To maintain the start from 1
    new OutOfMemoryProblem,
    new TooManyFilesOpen,
    new DeadlockSimulator,
    new NetworkCommunicationIssue
  )

  def main(args: Array[String]): Unit = {
    if(args.length != 1 || (problems.size-1) < args(0).toInt || args(0).toInt < 1)
      throw new IllegalArgumentException("Expecting the problem number as argument. Pass 1 - " + (problems.size-1))
    problems(args(0).toInt).run()
  }

}
