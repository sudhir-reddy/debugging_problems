# Debugging Excercises

This repository intends to help developers practise debugging, by simulating some of the commonly seen issues in production.
As of now this supports the below scenarios
* Out Of Memory Problem
* Too Many Files Open
* Deadlock Simulator
* Network Communication Issue

## Prerequisites
Understand basics of how to take/read thread dump, heap dump, using remote debugger, Flight Recorder/Visual VM/JConsole, basic system commands to get stats like disk/network/cpu/memory.

## Getting Started

This codebase uses Spark to launch the problems in a distributed environment.
Since Spark can run locally, you can get these running on your local setup or even in a distributed environment using YARN.

## Build
`mvn package`

This builds the jar and obfuscates the class names of the problems, so that the developer does not know which problem he is debugging.

## Usage
To run this on YARN, copy the jar built above and the `run_examples.sh` script to the gateway node and run below command
```
./run_examples.sh <PROBLEM_NUM>
```
Can give problem number from 1 to 4 as of now. See the mapping of the problem number to the code in `org.flipkart.fdp.debugging_problems.ExampleLauncher.problems`

To run locally, can launch the `org.flipkart.fdp.debugging_problems.ExampleLauncher` class with the right arguments.

## Debugging
Look at the failures in the Spark executor logs and identify the issue which is causing this to fail.
Use various tools mentioned in the _Prerequisites_ section to identify the problem.
Code should not be provided to the developers as it will become very easy to solve the problems.

## Contributing More Scenarios
Create a new class and add it to `org.flipkart.fdp.debugging_problems.ExampleLauncher.problems`