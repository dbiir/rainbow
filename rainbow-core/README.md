# Rainbow Core

Rainbow Core contains a command line interface of rainbow.
A set of commands is provided to evaluating environmental features of
HDFS, calculate new optimized data layouts and generate useful SQL
statements to apply the data layouts in SQL-on-HDFS environment.

## Prepare

- Get at least one physical machine with a HDD drive and at least 8GB memory.

- Install Hadoop (1.2.1 and 2.7.3 tested), Hive (1.2.x tested), Spark (1.2.x and 1.3.x tested) / Presto (0.179 tested).
HDFS datanode should use HDD as storage media.

- Go to [Rainbow Benchmark](https://github.com/dbiir/rainbow/tree/master/rainbow-benchmark)
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
```

We can simply start rainbow without any argument:
```bash
java -jar target/rainbow-core-xxx-full.jar
```

You can use a specific configuration file by specifying -f argument.
If argument -f is not given, the default configuration file in the jar ball will be used.

A template of rainbow configuration is [here](https://github.com/dbiir/rainbow/blob/master/rainbow-common/src/main/resources/rainbow.properties).
Set data.dir in rainbow.properties:
```
data.dir=/rainbow
```
It is the directory on HDFS to store the wide tables. The generated benchmark data
should be stored in {data.dir}/text/.

Now we are going to do data layout optimization experiment step by step.

### Data Transform

We use Hive to perform data transformation.

In Hive, to transform data format from text to other formats like Parquet,
you need a set of SQL statements.

Create a sub directories:
```bash
$ mkdir ./sql
$ mkdir ./bench
$ cp ../rainbow-benchmark/benchmark_data/*.txt ./bench/
```

In rainbow cli, use the following commands to generate the SQL statements:
```
rainbow> GENERATE_DDL -f TEXT -s ./bench/schema.txt -d ./sql/text_ddl.sql
rainbow> GENERATE_DDL -f PARQUET -s ./bench/schema.txt -t parq -d ./sql/parq_ddl.sql
rainbow> GENERATE_LOAD -r true -t parq -s ./bench/schema.txt -l ./sql/parq_load.sql
```

In Hive, execute text_ddl.sql, parq_ddl.sql and then parq_load.sql
to create a table in Parquet format and load data into the table.

### Build Seek Cost Model

Before layout optimization, we need a cost model to calculate the seek cost of a query.
To build this seek cost model, we have to know:
- the average chunk size of each column, and
- the seek cost function of HDFS.

Seek cost is a function of seek distance. In rainbow, there are three type of seek cost function:
- **LINEAR:** seek_cost = k * seek_distance.
- **POWER:** seek_cost = k * sqrt(seek_distance).
- **SIMULATED:** perform real seek operations in HDFS and fit the seek cost function.

POWER is the default seek cost function in Rainbow. It works well in most conditions.
In this document, we use POWER seek cost function so that we
do not need to evaluate the real seek cost of HDFS.

SIMULATED seek cost function is more accurate than POWER. To use this seek cost function,
you have to perform seek evaluation of different seek distances and save
the result in a seek_cost.txt file like [this](https://github.com/dbiir/rainbow/blob/master/rainbow-layout/src/test/resources/seek_cost.txt).
The first line is seek distance interval (in bytes), each line under the first line contains
the seek distance (in bytes) and the average seek cost (in milliseconds).
See [Seek Cost Evaluation](https://github.com/dbiir/rainbow/tree/master/rainbow-seek) for how to evaluate seek cost and get seek_cost.txt.

To calculate the average size of each column chunks:
```
rainbow> GET_COLUMN_SIZE -f PARQUET -s ./bench/schema.txt -p hdfs://localhost:9000/rainbow/parq/
```

Then you get a schema.txt.new file under ./bench/.
This file will be used instead of schema.txt in the following steps.

### Column Ordering

We can optimize the column order by ORDERING command:
```
rainbow> ORDERING -s ./bench/schema.txt.new -o ./bench/schema_ordered.txt -w ./bench/workload.txt
```

Here we used the default column ordering algorithm **SCOA**, the default seek cost function **POWER**,
and the default computation budget **200**. For full usage information of ORDERING:
```
rainbow> ORDERING -h
```

The ordered schema is stored in ./bench/schema_ordered.txt.

Generate the CREATE TABLE and LOAD DATA statements for ordered table:
```
rainbow> GENERATE_DDL -f PARQUET -s ./bench/schema_ordered.txt -t parq_ordered -d ./sql/parq_ordered_ddl.sql
rainbow> GENERATE_LOAD -r true -t parq_ordered -s ./bench/schema_ordered.txt -l ./sql/parq_ordered_load.sql
```

In Hive, execute parq_ordered_ddl.sql and parq_ordered_load.sql to create table parq_ordered and load data into the table.

### Column Duplication

The columns have been ordered to reduce seek cost. But there are some frequently accessed columns.
Seek cost can be further reduced by duplicating these columns.

We can duplicate frequently accessed columns by DUPLICATION command:
```
rainbow> DUPLICATION -s ./bench/schema_ordered.txt -ds ./bench/schema_dupped.txt -w ./bench/workload.txt -dw ./bench/workload_dupped.txt
```

Here we used the default column duplication algorithm **INSERTION**, the default seek cost function **POWER**,
and the default computation budget **3000**. For full usage information of DUPLICATION:
```
rainbow> DUPLICATION -h
```

The duplicated schema is stored in ./bench/schema_dupped.txt.
The query workload with duplicated columns is stored in ./bench/workload_dupped.txt.

Generate the CREATE TABLE and LOAD DATA statements for ordered table:
```
rainbow> GENERATE_DDL -f PARQUET -s ./bench/schema_dupped.txt -t parq_dupped -d ./sql/parq_dupped_ddl.sql
rainbow> GENERATE_LOAD -r true -t parq_dupped -s ./bench/schema_dupped.txt -l ./sql/parq_dupped_load.sql
```

In Hive, execute parq_dupped_ddl.sql and parq_dupped_load.sql to create table parq_dupped
and load data into the table.

### Column Redirection

After column duplication, a column may have a set of replicas. For example,
Column_6 may have two replicas, say Column_6_1 and Column_6_2, and Column_8 may have
three replicas Column_8_1, Column_8_2 and Column_8_3.

Query has to be redirected to proper column replicas. For example, query
```sql
SELECT Column_6, Column_8 FROM parq WHERE Column_6 = 3;
```
may be redirected as:
```sql
SELECT Column_6_1, Column_8_3 FROM parq WHERE Column_6_1 = 3;
```

To redirect columns for queries. We have to firstly build an
inverted bitmap index (see section 5.3 in the [paper](http://dl.acm.org/citation.cfm?id=3035930)):
```
rainbow> BUILD_INDEX -ds ./bench/schema_dupped.txt -dw ./bench/workload_dupped.txt
```

After that, we can redirect accessed column set for a query like:
```
rainbow> REDIRECT -q Q1 -s Column_6,Column_8
```

-q is optional. It is used to specify the query id, which is used to identify a query.
Note that Column_6 and Column_8 are existing columns in the original schema.txt file.
With the redirected column access pattern, queries can be easily rewritten to access the duplicated table.

### Evaluation

Finally, we are going to evaluate the query performance on the three different data layouts:
- original, has the same schema with our generated benchmark data, stored in table parq;
- ordered, with columns reordered, store in table parq_ordered;
- duplicated, with columns reordered and duplicated, stored in table parq_dupped.

We have to generate queries for the three data layouts:
```
rainbow> GENERATE_QUERY -t parq -n localhost -s ./bench/schema.txt.new -w ./bench/workload.txt -sq ./sql/spark_origin.sql -hq ./sql/hive_origin.sql
rainbow> GENERATE_QUERY -t parq_ordered -n localhost -s ./bench/schema.txt.new -w ./bench/workload.txt -sq ./sql/spark_ordered.sql -hq ./sql/hive_ordered.sql
rainbow> GENERATE_QUERY -t parq_dupped -n localhost -s ./bench/schema_dupped.txt -w ./bench/workload_dupped.txt -sq ./sql/spark_dupped.sql -hq ./sql/hive_dupped.sql
```

Now we have a set of spark_\*.sql and a set of hive_\*.sql.

spark_\*.sql files contain the script to be used in spark cli (not spark-sql cli).
Currently, we only support Parquet and Spark-1.2.x / 1.3.x.

hive_\*.sql files contain the queries written in HiveQL.
Actually, they are also standard SQL. So that they can be used in most of the systems such as
spark-sql cli, Presto, Hive and Impala.

It is recommended to use HiveQL in spark-sql cli or Presto. By evaluation queries, you should see
that queries on the ordered and dupped table runs much faster.

To evaluate the queries automatically on Spark and log the query elapsing time, see [Workload Evaluation](https://github.com/dbiir/rainbow/tree/master/rainbow-evaluate).