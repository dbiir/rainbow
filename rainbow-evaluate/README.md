# Workload Evaluation

After column ordering and / or duplication, we can
[Generate Queries](https://github.com/dbiir/rainbow/blob/master/rainbow-cli/README.md#evaluation) and
evaluation query performance by executing the queries in SQL-on-Hadoop systems.
But it is a boring job to execute hundreds of queries manually. `rainbow-evaluate` module
provides an easier way to evaluate query performance automatic.

## Prepare

- Finish the steps in [Rainbow CLI](https://github.com/dbiir/rainbow/blob/master/rainbow-cli/README.md).

## Build

Enter the root directory of rainbow, run:
```bash
$ mvn clean
$ mvn package
$ cd ./rainbow-evaluate
```

Note that the default Spark version used in Rainbow is `2.1.0`. If you are
willing to use Spark `1.x` (`1.3.x` recommended), edit spark version in 
`./rainbow-evaluate/pom.xml` before running `mvn package`.

Then you get `rainbow-evaluate-xxx-full.jar` in `target` subdirectory.
Now you are ready to start rainbow workload evaluation.

## Usage

To get usage information:
```bash
$ java -jar target/rainbow-evaluate-xxx-full.jar -h
```

There is only one argument required:
- `-p / --param_file`, specifies the parameter file to be used.

Template of the parameter file can be found in `./src/main/resources/params/`.
Edit the parameters before running workload evaluation.

You can also set a specific configuration file by -f argument.
If argument -f is not given, the default configuration file in the jar ball will be used.
More details of Rainbow configuration properties are discussed in 
[Rainbow Configuration](https://github.com/dbiir/rainbow/blob/master/rainbow-common/README.md).


To perform workload evaluations, run:
```bash
$ java -jar target/rainbow-evaluate-xxx-full.jar -p ./src/main/resources/params/WORKLOAD_EVALUATION.properties
```

Parameters in `WORKLOAD_EVALUATION.properties` are:
```
# LOCAL, SPARK1, SPARK2 or PRESTO
method=SPARK2

# PARQUET or ORC
format=PARQUET

# the path of unordered table directory on HDFS,
table.dir=/rainbow/parq

# the table name for presto, only needed when PRESTO method is used.
table.name=orc

# the file path of workload file
workload.file=/rainbow/workload.txt

# the local directory used to write evaluation results
log.dir=/tmp/log/dir

# true or false, whether or not to drop file cache on each node in the cluster
drop.cache=true

# the file path of drop_caches.sh
drop.caches.sh=/rainbow/drop_caches.sh
```

Currently, we only support automatic workload evaluation on **Parquet** format tables.

For the three evaluation `method`, LOCAL, SPARK1 and SPARK2:
- **LOCAL** is to read the accessed columns of a query from HDFS by a Parquet reader.
- **SPARK1** is to execute the queries in Spark 1 (1.3.x recommended). The duration of the first mapPartitions stage is
recorded as the read latency of the query. Such a latency includes task initialization, scheduling and garbage
collection overheads.
- **SPARK2** same with *SPARK1*, except executing the queries in Spark 2 (2.1.x recommended).

`format` is the format of the data to be read in evaluation. Can be `ORC` or `PARQUET`. 
But `ORC` is currently only supported in `SPARK2` method.

`table.dir` is the directory on HDFS which stores the Parquet files.

`table.name` is used by *PRESTO*, it should be a table in the database specified
by `presto.jdbc.url` in `rainbow.properties`. And this table must be stored
under `table.dir`.

`workload.file` is the path of workload file in [Rainbow CLI](https://github.com/dbiir/rainbow/blob/master/rainbow-cli/README.md).

`log.dir` is the directory on local fs to store the evaluation results.
A `spark_duration.csv` or `local_duration.csv` file (depends on which method been used) will be generated in this directory.
In LOCAL method, an `accessed_columns.txt` file is also generated. It records the Parquet column index and column name of
each query's accessed columns.

`drop.cache` indicates whether or not to drop file system cache on each node in the cluster after the execution of a query.
If you are using a small dataset to evaluate the effects of data layout optimization, it is a
good idea to drop cache on each node.

`drop.caches.sh` is the path of shell script file which is used to drop file system cache on each node
in the cluster. A very simple example of this file is:
```bash
# This is just a simple example of drop_caches.sh
#!/bin/bash

for ((i=1; i<=5; i+=1))
do
  ssh node$i "echo 3 > /proc/sys/vm/drop_caches"
  ssh node$i sync
done
```

You should ensure that the system user running Rainbow jars have the right permissions
to execute this script and clear the fs cache on the cluster nodes.