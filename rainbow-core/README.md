# Rainbow Core

Rainbow Core contains a command line interface of rainbow.
A set of commands is provided to evaluating environmental features of
HDFS, calculate new optimized data layouts and generate useful SQL
statements to apply the data layouts in SQL-on-HDFS environment.

## Prepare

- Get at least one physical machine with a HDD drive and at least 8GB memory.

- Install Hadoop (1.2.1 and 2.7.3 tested), Hive (1.2.x tested), Spark (1.2.x and 1.3.x tested) / Presto (0.179 tested).
HDFS datanode should use HDD as storage media.

- Go to [Rainbow Benchmark](https://github.com/dbiir/rainbow/tree/master/rainbow-benchamrk)
  and follow the steps to generate benchmark data and put data in on HDFS.
  Here, we put data in the HDFS directory /rainbow/text/.
  Also find schema.txt and workload.txt in the benchmark.

## Cluster Configurations
In Hive, it is recommended to set these three settings to
control the block and row group size of Parquet:
```
dfs.block.size=268435456;
parquet.block.size=1073741824;
mapred.max.split.size=268435456;
```

These settings can be set by any of these three ways [here](https://cwiki.apache.org/confluence/display/Hive/AdminManual+Configuration).

If you use ORC instead of Parquet,
set orc.stripe.size instead of parquet.block.size.
More [Settings for ORC](https://orc.apache.org/docs/hive-config.html).

>
Tips:\
In [Parquet](http://parquet.apache.org/documentation/latest/),
1GB row groups, 1GB HDFS block size and 1 HDFS block per HDFS file
is recommended. But actually,
row group size is set by parquet.block.size, and this is the size of **IN MEMORY**
row group. If you also want to ensure that there is only one row group in a block,
it is a good idea to set parquet.block.size larger than mapred.max.split.size and
dfs.block.size. ITs default value may be really small, like 128MB.
>
In [ORC](https://orc.apache.org/docs/hive-config.html),
orc.stripe.size is something similar. Default ORC stripe size in Hive is
really small, like 64MB. Set it larger will boost query performance, and
will consume more memory at the same time.
>

In Hadoop's mapred-site.xml, add the following configurations:
```xml
  <property>
    <name>mapred.child.java.opts</name>
    <value>-Xmx4096m</value>
  </property>
  <property>
    <name>mapreduce.map.memory.mb</name>
    <value>4096</value>
  </property>
```

In Hadoop's hadoop-env.sh, add the following setting:
```sh
export HADOOP_HEAPSIZE=4096
```

In Presto's config.properties, set query.max-memory-per-node to no less than 4GB:
```sh
query.max-memory-per-node=4GB
```

In Spark's spark-defaults.conf, set spark.driver.memory to no less than 4GB:
```sh
spark.driver.memory    4g
```

In Spark's spark-env.sh, set SPARK_WORKER_MEMORY to no less than 4GB
```sh
export SPARK_WORKER_MEMORY=4g
```

## Build

Enter the directory of rainbow-core module which contains this README.md, run:
```bash
$ mvn clean
$ mvn package -DskipTests
```

Then you get a 'rainbow-core-xxx-full.jar' in the target subdirectory.
Now you are ready to run rainbow.

## Usage

To get usage instructions:
```bash
$ java -jar target/rainbow-core-xxx-full.jar -h
usage: ruc.iir.rainbow.core [-h] [-f CONFIG] -d PARAMS_DIR

Rainbow: Data Layout Optimization framework for very wide tables on HDFS.

optional arguments:
  -h, --help             show this help message and exit
  -f CONFIG, --config CONFIG
                         specify the path of configuration file
  -d PARAMS_DIR, --params_dir PARAMS_DIR
                         specify the directory of parameter files
Rainbow Core (https://github.com/dbiir/rainbow/tree/master/rainbow-core).
```

Argument -d is required. You can find template of the parameter files
in ./src/main/resources/params. **DO NOT** change to file names,
just set the parameters in these files following the comments.

For example, we can start rainbow like this:
```bash
java -jar target/rainbow-core-xxx-full.jar -d ./src/main/resources/params
```

You can use a different configuration file by -f argument.
If argument -f is not given, the default configuration file in the jar ball will be used.

Now we are going to do data layout optimization experiment step by step.

### Transform Format

### Build Cost Model

### Column Ordering

### Column Duplication

### Evaluation

