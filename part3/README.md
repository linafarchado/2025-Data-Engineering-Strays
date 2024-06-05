### Project Launch Instructions

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

### Step 5: Download Hadoop

Download the Hadoop binary package from the official website:

```sh
wget https://downloads.apache.org/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz
```

### Step 6: Extract Hadoop

Extract the downloaded tarball:

```sh
tar -xzvf hadoop-3.3.6.tar.gz
```

Move it to `/usr/local`:

```sh
sudo mv hadoop-3.3.6 /usr/local/hadoop
```

### Step 7: Configure Hadoop

Edit the Hadoop environment variables:

```sh
sudo vim /usr/local/hadoop/etc/hadoop/hadoop-env.sh
```

Add the following line at the end of the file:

```sh
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
```

### Step 8: Set Hadoop Environment Variables

Edit the `.bashrc` file to include Hadoop environment variables:

```sh
vim ~/.bashrc
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

### Step 9: Configure Hadoop XML Files

Edit the `core-site.xml` file:

```sh
sudo vim $HADOOP_HOME/etc/hadoop/core-site.xml
```

Replace the current configuration with the following configuration:

```xml
<configuration>
    <property>
        <name>fs.defaultFS</name>
        <value>hdfs://localhost:9000</value>
    </property>
    <property>
        <name>dfs.replication</name>
        <value>1</value>
    </property>
    <property>
        <name>dfs.client.block.write.replace-datanode-on-failure.policy</name>
        <value>NEVER</value>
    </property>
    <property>
        <name>dfs.client.block.write.replace-datanode-on-failure.enable</name>
        <value>false</value>
    </property>
</configuration>
```

Edit the `hdfs-site.xml` file:

```sh
sudo vim $HADOOP_HOME/etc/hadoop/hdfs-site.xml
```

Replace the current configuration with the following configuration:

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

### Step 10: SSH Key Setup

Before starting Hadoop services, ensure that SSH is set up correctly for seamless communication between Hadoop nodes.

1. **Install OpenSSH Server**:
    ```sh
    sudo apt update
    sudo apt install openssh-server
    ```

2. **Enable and Start SSH Service**:
    ```sh
    sudo systemctl start ssh
    sudo systemctl enable ssh
    ```

3. **Create a New RSA SSH Key**:
    ```sh
    ssh-keygen -t rsa -b 4096 -C "your_email@example.com"
    ```

4. **Add the SSH Key to Authorized Keys**:
    ```sh
    cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
    chmod 600 ~/.ssh/authorized_keys
    chmod 700 ~/.ssh
    ```

5. **Ensure Proper Permissions**:
    Connect to your device via SSH to ensure correct permissions:
    ```sh
    ssh username@device_name # for example flo@floubuntu
    chmod 700 ~/.ssh
    chmod 600 ~/.ssh/authorized_keys
    ```

    **Explanation**: These steps ensure that the SSH key-based authentication is set up correctly. The `chmod` commands set the appropriate permissions for the `.ssh` directory and the `authorized_keys` file, ensuring secure access. SSH key-based authentication is used by Hadoop for communication between nodes without needing to enter passwords.

### Step 11: Format the Namenode

Format the HDFS namenode:

```sh
hdfs namenode -format
```

### Step 12: Start Hadoop Services

Start the Hadoop services:

```sh
stop-dfs.sh
stop-yarn.sh
start-dfs.sh
start-yarn.sh
```

Verify that Hadoop is running by checking the processes:

```sh
jps
```

You should see something like this:

- NameNode
- DataNode
- SecondaryNameNode

### Step 13: Access HDFS Web Interface

Open a web browser and go to [http://localhost:9870](http://localhost:9870). You should see the HDFS web interface.

### Step 14: Create HDFS Directories

Create necessary directories in HDFS for storing data:

```sh
hdfs dfs -mkdir /user
hdfs dfs -mkdir /user/yourusername
```

Replace `yourusername` with your actual username.

### Step 15: Verify HDFS Setup

Verify that the directories are created:

```sh
hdfs dfs -ls /user/yourusername
```

### Step 16 : Change path in code

Change this line :

```scala
val hdfsPath = s"hdfs://localhost:9000/user/alexandre/dronedata/batch_$batchId.parquet"
```

Change the path ```/user/alexandre/``` with your path

## How to view data

You can install parquet-tools with 
```bash
pip install parquet-tools
```

```bash
hadoop fs -get /user/USERNAME/dronedata/NAME_OF_FILE.parquet tmp
```
This creates a "tmp" directory in you /home/your_username/ path containing the parquet file that was in the datalake.

Then you can see the parquet file with the following command :

```bash
parquet-tools show testing/NAME_OF_FILE.parquet
```