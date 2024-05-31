# Project Launch Instructions

This guide provides step-by-step instructions on how to launch the project. Follow these steps to set up and run the project successfully.

## Prerequisites

Ensure you have the following prerequisites installed on your system:

- Kafka
- Scala
- SBT

## Setup Instructions

1. **Start Zookeeper**: Navigate to the Kafka folder where the `bin` folder is located. Launch Zookeeper using the following command:
   ```bash
   bin/zookeeper-server-start.sh config/zookeeper.properties
   ```

2. Start Kafka Server: Open a new terminal window and navigate to the Kafka folder. Launch the Kafka server using the following command:

  ```bash
  bin/kafka-server-start.sh config/server.properties
  ```

3. Create Kafka Topic: Open another terminal window and navigate to the Kafka folder. Create the Kafka topic using the following command:

  ```bash
  bin/kafka-topics.sh --create --topic drone-data --bootstrap-server localhost:9092
  ```

## Running the Project

1. Navigate to part1 Folder: Go to the directory where part1 of the project is located.

2. Launch SBT: Open a terminal window and run the following command to launch SBT:

  ```bash
    sbt run
  ```

Verify Data Reception: After running the project, you can verify if the data is being received correctly by using the following command in the Kafka folder:

  ```bash
  bin/kafka-console-consumer.sh --topic drone-data --from-beginning --bootstrap-server localhost:9092
  ```
