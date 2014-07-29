Spark GCE
=========

Spark GCE is like Spark Ec2 but for those who run their cluster on Google Cloud.

  - Make sure you have installed and authenticated gcutils where you are running this script.
  - Helps you launch a spark cluster in the Google Cloud
  - Attaches 100GB empty disk to all nodes in the cluster
  - Installs and configures Spark and HDFS automatically
  - Starts the Shark server automatically

Spark GCE is a python script which will help you launch a spark cluster in the google cloud like the way the spark_ec2 script does for AWS.

Usage
-----

> ***spark_gce.py project-name number-of-slaves slave-type master-type identity-file zone cluster-name spark-mem workers-per-node cores-per-worker local-log-dir***
>
>>
>> - **project-name**: Name of the project where you are going to launch your spark cluster.
>>
>> - **number-of-slave**: Number of slaves that you want to launch.
>>
>> - **slave-type**: Instance type for the slave machines.
>>
>> - **master-type**: Instance type for the master node.
>>
>> - **identity-file**: Identity file to authenticate with your GCE instances, Usually resides at *~/.ssh/google_compute_engine* once you authenticate using gcutils.
>>
>> - **zone:** Specify the zone where you are going to launch the cluster.
>>
>> - **cluster-name**: Name the cluster that you are going to launch.
>>
>> - **spark-mem**: Amount of memory per Spark worker (as a JVM memory string eg. 2500m, 2g)
>>
>> - **workers-per-node**: Number of workers to run on each slave node
>>
>> - **cores-per-worker**: Number of cores each worker should use (optional, 1 by default)
>>
>> - **local-log-dir**: A local directory to download nmon logs from all the nodes (optional, empty, or no logging by default)
>>
>
> ***spark_gce.py project-name cluster-name [identity-fle local-log-dir] destroy***
>
>> - **project-name**: Name of the project where the spark cluster is at.
>> - **cluster-name**: Name of the cluster that you are going to destroy.
>> - **NOTE**: If you had specified a local-log-dir while starting the cluster, provide it here too, along with the identity-file, else skip both.


Installation
--------------

```sh
git clone git@github.com:dbpedia/distributed-extraction-framework.git
cd gce
python spark_gce.py
```
