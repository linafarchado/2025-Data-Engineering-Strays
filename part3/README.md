# Project Launch Instructions

This guide provides step-by-step instructions on how to launch the project. Follow these steps to set up and run the project successfully.

## Prerequisites

Ensure you have the following prerequisites installed on your system:

- Kafka
- Scala
- SBT
- Spark

## Setup Instructions

### Step 0: Go see the README in part 1 first.

### Step 1: Install Java Development Kit (JDK)


### Step 2: Install Kafka


### Step 3: Install Scala


### Step 4: Install SBT


### Step 2: Download Hadoop

Download the Hadoop binary package from the official website:

```sh
wget https://downloads.apache.org/hadoop/common/hadoop-3.3.1/hadoop-3.3.1.tar.gz
```

### Step 3: Extract Hadoop

Extract the downloaded tarball:

```sh
tar -xzvf hadoop-3.3.1.tar.gz
```

Move it to `/usr/local`:

```sh
sudo mv hadoop-3.3.1 /usr/local/hadoop
```

### Step 4: Configure Hadoop

Edit the Hadoop environment variables:

```sh
sudo nano /usr/local/hadoop/etc/hadoop/hadoop-env.sh
```

Add the following line at the end of the file:

```sh
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
```

### Step 5: Set Hadoop Environment Variables

Edit the `.bashrc` file to include Hadoop environment variables:

```sh
nano ~/.bashrc
```

Add the following lines at the end of the file:

```sh
# Hadoop Environment Variables
export HADOOP_HOME=/usr/local/hadoop
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin
```

Apply the changes:

```sh
source ~/.bashrc
```

### Step 6: Configure Hadoop XML Files

Edit the `core-site.xml` file:

```sh
sudo nano $HADOOP_HOME/etc/hadoop/core-site.xml
```

Add the following configuration:

```xml
<configuration>
    <property>
        <name>fs.defaultFS</name>
        <value>hdfs://localhost:9000</value>
    </property>
</configuration>
```

Edit the `hdfs-site.xml` file:

```sh
sudo nano $HADOOP_HOME/etc/hadoop/hdfs-site.xml
```

Add the following configuration:

```xml
<configuration>
    <property>
        <name>dfs.replication</name>
        <value>1</value>
    </property>
    <property>
        <name>dfs.name.dir</name>
        <value>file:///usr/local/hadoop/hadoopdata/hdfs/namenode</value>
    </property>
    <property>
        <name>dfs.data.dir</name>
        <value>file:///usr/local/hadoop/hadoopdata/hdfs/datanode</value>
    </property>
</configuration>
```

### Step 7: Format the Namenode

Format the HDFS namenode:

```sh
hdfs namenode -format
```

### Step 8: Start Hadoop Services

Start the Hadoop services:

```sh
start-dfs.sh
```

Verify that Hadoop is running by checking the processes:

```sh
jps
```

You should see something like this:

- NameNode
- DataNode
- SecondaryNameNode

### Step 9: Access HDFS Web Interface

Open a web browser and go to [http://localhost:9870](http://localhost:9870). You should see the HDFS web interface.

### Step 10: Create HDFS Directories

Create necessary directories in HDFS for storing data:

```sh
hdfs dfs -mkdir /user
hdfs dfs -mkdir /user/yourusername
```

Replace `yourusername` with your actual username.

### Step 11: Verify HDFS Setup

Verify that the directories are created:

```sh
hdfs dfs -ls /user/yourusername
```
