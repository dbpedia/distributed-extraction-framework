DBpedia Distributed Extraction Framework 
==================================

This is the distributed version of the [DBpedia Information Extraction Framework](https://github.com/dbpedia/extraction-framework/). It uses [Apache Spark](http://spark.apache.org) to extract structured data from Wikipedia in a parallel, distributed manner.

This is currently a work-in-progress, and the instructions are mostly intended for developers.

## Requirements
* Java 7
* Maven 3
* Apache Spark 0.9.1 built with Apache Hadoop 2.2.0

## Setup Apache Spark

```bash
$ wget http://d3kbcqa49mib13.cloudfront.net/spark-0.9.1-bin-hadoop2.tgz
$ tar xzf http://d3kbcqa49mib13.cloudfront.net/spark-0.9.1-bin-hadoop2.tgz
$ cd spark-0.9.1-bin-hadoop2
$ SCALA_HOME=/usr/share/java MAVEN_OPTS=\"-Xmx2g -XX:MaxPermSize=512M -XX:ReservedCodeCacheSize=512m\" mvn -Dhadoop.version=2.2.0 -Dprotobuf.version=2.5.0 -DskipTests clean package
```

Replace SCALA_HOME according to your machine settings. It is necessary to set enough memory for maven to make Spark compile successfully.

Add the hostnames of your slave nodes (after having downloaded Spark to all nodes) to conf/slaves. There's a bunch of attendant configurations needed for running on a cluster, like ensuring the firewall allows traffic on certain ports, ensuring passwordless access between the master and slave nodes, setting up HDFS and formatting the NameNode etc. Usually in a cluster of N nodes you would run Spark Master and Hadoop's NameNode on one node and the Spark Workers and Hadoop DataNodes on the remaining N-1 nodes.

Here's a sample `spark-env.sh` for a cluster where the slaves have 4 cores and 15G RAM each:
```bash
export SCALA_HOME=/usr/share/java[
export SPARK_MEM=2500m
export SPARK_WORKER_CORES=1
export SPARK_WORKER_INSTANCES=4
SPARK_JAVA_OPTS+=" -Dspark.local.dir=/mnt/spark"
export SPARK_JAVA_OPTS
export SPARK_MASTER_IP=192.168.0.100
export JAVA_HOME=/usr/lib/jvm/java-1.7.0-openjdk-1.7.0.65.x86_64
```

**Important**: Note that we have set cores (threads) per worker to 1 and set the number of workers equal to the number of cores on the machine. This is because:
* The implementation that Hadoop uses to decode bzip2 files - `CBZip2InputStream` - is not thead-safe (there's a JIRA for that: httips://issues.apache.org/jira/browse/HADOOP-10614). This means that allotting multiple threads to a single worker while using .bz2 input files will cause the jobs to fail.
* Multiple JVMs rather than a single huge JVM often increases performance.

While running tests we have found that setting `spark.executor.memory` to 2500m - 3000m is a good idea with the above sample configuration. It is given in the sample dist-config.properties file discussed in the next section.

And at the end:

```
sbin/start-all.sh
```
    
We have added a script for setting up Spark and Hadoop on Google Compute Engine with the optimal settings for this framework. You can find it in the **gce** directory.
    
Please refer to the [Spark official docs](http://spark.apache.org/docs/0.9.1/spark-standalone.html) for details on how to deploy Spark in standalone mode.

## How to Build

Clone the latest version of the repo and switch to stage branch:

    $ git clone https://github.com/dbpedia/distributed-extraction-framework.git
    $ cd distributed-extraction-framework
    $ mvn clean install -Dmaven.test.skip=true # Compiles the code without running tests

## Dump-based Distributed Extraction

Follow the instructions given below to download data for the extractions you need to perform. An example of the download.properties file is given at `download/src/test/resources/download.properties`

In the root directory run the following commands

    $ mvn clean install -Dmaven.test.skip=true # Compiles the code without running tests
    $ ./run download config=download.properties # Downloads the wikipedia dumps

**Points to keep in mind:**

1. Before performing extractions you will need a config.properties file for general extraction configuration and a dist-config.properties file for the distributed framework specific configuration (Spark, Hadoop, logging etc.). Examples are given at `extraction/src/test/resources/`.

2. The example `extraction/src/test/resources/dist-config.properties` file needs to be modified with a proper spark-home and spark-master (local[N] means N cores on the local node - you can change it to something like `spark://hostname:7077` to run it in distributed mode).

3. Prefer pages-articles-multistream.bz2 files to pages-articles.bz2 because they are more efficient for parallel extraction. The former can be decompressed in parallel using Hadoop's splittable Bzip2Codec. Of course, this does not matter when using the pages-articlesX.xml-pXXXXXXXXXXpXXXXXXXXXX.bz2 files (which will be the files of choice for distributed downloads).

4. **Important:** Finally, when running on a distributed cluster, it is essential that you set `spark.cores.max` (in dist-config.properties) to **N** \* **M** where N = total no. of slaves, M = `SPARK_WORKER_INSTANCES`. This is to ensure that Spark uses as many cores (over the entire cluster) as many workers there are.

Now perform parallel extractions on your Spark cluster:

    $ ./run extraction extraction/src/test/resources/config.properties extraction/src/test/resources/dist-config.properties


### Testing
Please see the [wiki page for Testing](https://github.com/dbpedia/distributed-extraction-framework/wiki/Testing) for detailed instructions on how to verify outputs of the distributed extraction framework by comparing them with that of the original.

## Distributed Downloads

This is still a work in progress and there are some issues that need to be solved.

Have a look at `download/src/test/resources/dist-download.properties` and `download/src/test/resources/download.properties`. You can create your own config files using them. Just make sure that they are present at the same path in all nodes of the cluster.

After cloning and building the framework on the master node, for each slave node, do this:
```
rsync -avhz --progress ~/.m2 $SLAVE:~/
rsync -avhz --progress /path/to/distributed-extraction-framework $SLAVE:/path/to/
../run download distconfig=/path/to/distributed-extraction-framework/download/src/test/resources/dist-download.properties config=/path/to/distributed-extraction-framework/download/src/test/resources/download.properties
```

You can find the worker logs at `/path/to/distributed-extraction-framework/logs` of each node.

