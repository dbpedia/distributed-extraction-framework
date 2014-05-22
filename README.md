DBpedia Distributed Extraction Framework 
==================================

This is the distributed version of the [DBpedia Information Extraction Framework](https://github.com/dbpedia/extraction-framework/). It uses [Apache Spark](http://spark.apache.org) to extract structured data from Wikipedia in a parallel, distributed manner.

This is currently a work-in-progress, and the instructions are mostly intended for developers.

## Requirements
* Java 7
* Maven 3
* Apache Spark 0.9.1 for Hadoop 2 (HDP2, CDH5) - download it from [here](http://d3kbcqa49mib13.cloudfront.net/spark-0.9.1-bin-hadoop2.tgz)

## Setup Apache Spark

    $ wget http://d3kbcqa49mib13.cloudfront.net/spark-0.9.1-bin-hadoop2.tgz
    $ tar xzf http://d3kbcqa49mib13.cloudfront.net/spark-0.9.1-bin-hadoop2.tgz
    $ cd spark-0.9.1-bin-hadoop2

Add the hostnames of your slave nodes (after having downloaded Spark to all nodes) to conf/slaves. Remember to set SPARK_HOME. 

    $ sbin/start-all.sh
    
Please see the [official docs](http://spark.apache.org/docs/latest/spark-standalone.html) for details on how to deploy Spark in standalone mode.

## How to Build

Clone the latest version of the repo and switch to nildev branch:

    $ git clone https://github.com/dbpedia/distributed-extraction-framework.git
    $ cd distributed-extraction-framework
    $ git checkout nildev
    $ mvn clean install -Dmaven.test.skip=true # Compiles the code without running tests

## Dump-based Distributed Extraction

Follow the instructions given below to download data for the extractions you need to perform. An example of the download.properties file is given at `distributed/src/test/resources/li.download.properties`

In the root directory run the following commands

    $ mvn clean install -Dmaven.test.skip=true # Compiles the code without running tests
    $ ./run download config=download-config-file # Downloads the wikipedia dumps

You can replace `download-config-file` with `distributed/src/test/resources/li.download.properties`, after changing the `base-dir` option in the `li.download.properties` to a directory where you want to perform the dataset downloads and extractions.

Before performing extractions you will need a config.properties file for general extraction configuration and a spark.config.properties file for Spark-specific configuration. Examples are given at `distributed/src/test/resources/`. The example config.properties has the setting `extractors=.PageIdExtractor,.RedirectExtractor`. You need to edit the base-dir before continuing.

The example `distributed/src/test/resources/spark.config.properties` file needs to be modified with a proper spark-home and spark-master (local[N] means N cores on the local node - you can change it to something like `spark://hostname:7077`)

Now perform parallel extractions on your Spark cluster:

    $ ./run extraction distributed/src/test/resources/config.properties distributed/src/test/resources/spark-config.properties


### Testing
If you do not want to run the unit tests, you may skip this subsection.

