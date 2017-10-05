# Rainbow Core

Rainbow Core contains a command line interface which provides
a set of commands to calculate optimized data layouts and generate SQL
statements to apply the data layouts in SQL-on-Hadoop environment.

## Prepare

- Prepare at least one physical machine with a HDD drive and at least 8GB memory.
- Install Hadoop (2.2 or later recommended), Hive (2.0 or later recommended),
Spark (1.3.x or 2.1.x recommended) / Presto (0.179 or later recommended).
HDFS datanode should use HDD as storage media.
- Go to [Rainbow Benchmark](https://github.com/dbiir/rainbow/blob/master/rainbow-benchmark/README.md)
  and follow the steps to generate benchmark data. Put data in an HDFS directory,
  like `/rainbow/text/`. Find `schema.txt` and `workload.txt` in the benchmark.

## Cluster Configurations
In Hive, it is recommended to set these three settings to
control the block size and row group size of Parquet:
```
dfs.block.size=268435456;
parquet.block.size=1073741824;
mapred.max.split.size=268435456;
```

These settings can be set by any of the three ways [here](https://cwiki.apache.org/confluence/display/Hive/AdminManual+Configuration).

If you use ORC,
set `orc.stripe.size` instead of `parquet.block.size`.
See more [ORC Configuration in Hive](https://orc.apache.org/docs/hive-config.html).

>
> Tips:\
> In [Parquet](http://parquet.apache.org/documentation/latest/),
> 1GB row groups, 1GB HDFS block size and 1 HDFS block per HDFS file
> are recommended. But actually,
> row group size is set by parquet.block.size, and this is the size of an **IN-MEMORY**
> row group. If you want to ensure that there is only one row group in an HDFS block,
> it is a good idea to set parquet.block.size larger than mapred.max.split.size and
> dfs.block.size. Default parquet.block.size may be really small, like 128MB.
>
> In [ORC](https://orc.apache.org/docs/hive-config.html),
> orc.stripe.size is something similar with parquet.block.size.
> Default ORC stripe size in Hive is 64MB. Larger stripes will boost query performance, but
> will consume more memory when loading and querying data as well.
> Set it as large as your system could support.
>

In Hadoop's `mapred-site.xml`, add the following configurations:
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

In Hadoop's `hadoop-env.sh`, add the following setting:
```sh
export HADOOP_HEAPSIZE=4096
```

In Presto's `config.properties`, set query.max-memory-per-node to at least 4GB:
```sh
query.max-memory-per-node=4GB
```

In Spark's `spark-defaults.conf`, set spark.driver.memory to at least 4GB:
```sh
spark.driver.memory    4g
```

In Spark's `spark-env.sh`, set SPARK_WORKER_MEMORY to at least 4GB
```sh
export SPARK_WORKER_MEMORY=4g
```

## Build

Enter the root directory of rainbow, run:
```bash
$ mvn clean
$ mvn package
$ cd ./rainbow-cli
```

Then you get `rainbow-cli-xxx-full.jar` in `target` subdirectory.
Now you are ready to run rainbow cli.

## Usage

To get usage instructions:
```bash
$ java -jar target/rainbow-cli-xxx-full.jar -h
```

We can simply start rainbow cli without any argument:
```bash
java -jar target/rainbow-cli-xxx-full.jar
```

You can use a specific configuration file by specifying -f argument.
If argument -f is not given, the default configuration file in the jar ball will be used.

A template of rainbow configuration file can be found at `../rainbow-common/src/main/resources/rainbow.properties`.
Set `data.dir` in this file:
```
data.dir=/rainbow
```
It is the directory on HDFS to store the wide tables. The generated benchmark data
should be put in `{data.dir}/text/`.

Also set `namenode.host`, `namenode.port`, `spark.master`, `spark.app.port` and
`spark.driver.webapps.port` to be the same as your cluster configurations.

More details of Rainbow configuration properties are discussed in 
[Rainbow Configuration](https://github.com/dbiir/rainbow/blob/master/rainbow-common/README.md).

Now we are going to do data layout optimization experiments.

### Data Transform

We use Hive to perform data transformation.

In Hive, to transform data from text format to other formats, such as Parquet,
you need a set of SQL statements.

Create sub directories:
```bash
$ mkdir ./sql
$ mkdir ./bench
$ cp ../rainbow-benchmark/benchmark_data/*.txt ./bench/
```

Then in rainbow cli, use the following commands to generate the SQL statements:
```
rainbow> GENERATE_DDL -f TEXT -s ./bench/schema.txt -d ./sql/text_ddl.sql
rainbow> GENERATE_DDL -f PARQUET -s ./bench/schema.txt -t parq -d ./sql/parq_ddl.sql
rainbow> GENERATE_LOAD -r true -t parq -s ./bench/schema.txt -l ./sql/parq_load.sql
```

In Hive, execute `text_ddl.sql`, `parq_ddl.sql` and then `parq_load.sql`
to create a table in Parquet format and load data into the table.

### Build Seek Cost Model

Before layout optimization, we need a cost model to calculate the seek cost of a query.
To build this seek cost model, we have to know:
- the average chunk size of each column, and
- the seek cost function of HDFS.

Seek cost is a function of seek distance. In rainbow, there are three types of seek cost function:
- **LINEAR:** seek_cost = k * seek_distance.
- **POWER:** seek_cost = k * sqrt(seek_distance).
- **SIMULATED:** perform real seek operations in HDFS and fit the seek cost function.

POWER is the default seek cost function in Rainbow. It works well in most conditions.
Here, we are going to use POWER seek cost function so that we
do not need to evaluate the real seek cost of HDFS.

SIMULATED seek cost function could be more accurate than POWER. To use this seek cost function,
you have to perform seek evaluation of different seek distances and save
the result in a seek_cost.txt file like [this](https://github.com/dbiir/rainbow/blob/master/rainbow-layout/src/test/resources/seek_cost.txt).
The first line is seek distance interval (in bytes). Each line under the first line contains
the seek distance (in bytes) and the corresponding average seek cost (in milliseconds).
See [Seek Cost Evaluation](https://github.com/dbiir/rainbow/blob/master/rainbow-seek/README.md)
for details on how to evaluate seek cost and get seek_cost.txt.

To calculate the average size of each column chunks:
```
rainbow> GET_COLUMN_SIZE -f PARQUET -s ./bench/schema.txt -p /rainbow/parq/
```

Then you get a `schema.txt.new` file under ./bench/.
This file will be used instead of schema.txt in the following steps.

### Column Ordering

We can optimize the column order by ORDERING command:
```
rainbow> ORDERING -s ./bench/schema.txt.new -o ./bench/schema_ordered.txt -w ./bench/workload.txt
```

Here we use the default column ordering algorithm **SCOA**, the default seek cost function **POWER**,
and the default computation budget **200**. For full usage information of ORDERING:
```
rainbow> ORDERING -h
```

The ordered schema is stored in `./bench/schema_ordered.txt `.

The ordered schema in stored in `schema_ordered.txt`.
With this file, we can generate the CREATE TABLE and LOAD DATA statements for the ordered table:
```
rainbow> GENERATE_DDL -f PARQUET -s ./bench/schema_ordered.txt -t parq_ordered -d ./sql/parq_ordered_ddl.sql
rainbow> GENERATE_LOAD -r true -t parq_ordered -s ./bench/schema_ordered.txt -l ./sql/parq_ordered_load.sql
```

In Hive, execute `parq_ordered_ddl.sql` and `parq_ordered_load.sql` to create table parq_ordered and load data into the table.

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

The duplicated schema is stored in `./bench/schema_dupped.txt`.
The query workload with duplicated columns is stored in `./bench/workload_dupped.txt`.

Generate the CREATE TABLE and LOAD DATA statements for ordered table:
```
rainbow> GENERATE_DDL -f PARQUET -s ./bench/schema_dupped.txt -t parq_dupped -d ./sql/parq_dupped_ddl.sql
rainbow> GENERATE_LOAD -r true -t parq_dupped -s ./bench/schema_dupped.txt -l ./sql/parq_dupped_load.sql
```

In Hive, execute `parq_dupped_ddl.sql` and `parq_dupped_load.sql` to create table parq_dupped
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

After that, we can redirect accessed column set for the query:
```
rainbow> REDIRECT -q Q1 -s Column_6,Column_8
```

-q is optional. It is used to specify the query id, which is used to identify a query.
Note that Column_6 and Column_8 should be existing columns in the original schema.txt file.
With the redirected column access pattern, queries can be easily rewritten to access the duplicated table.

### Evaluation

Finally, we are going to evaluate the query performance on the three different data layouts:
- original, has the same schema with our generated benchmark data, stored in table parq;
- ordered, with columns reordered, store in table parq_ordered;
- duplicated, with columns reordered and duplicated, stored in table parq_dupped.

We have to generate queries for the three data layouts:
```
rainbow> GENERATE_QUERY -t parq -s ./bench/schema.txt.new -w ./bench/workload.txt -sq ./sql/spark_origin.sql -hq ./sql/hive_origin.sql
rainbow> GENERATE_QUERY -t parq_ordered -s ./bench/schema.txt.new -w ./bench/workload.txt -sq ./sql/spark_ordered.sql -hq ./sql/hive_ordered.sql
rainbow> GENERATE_QUERY -t parq_dupped -s ./bench/schema_dupped.txt -w ./bench/workload_dupped.txt -sq ./sql/spark_dupped.sql -hq ./sql/hive_dupped.sql
```

Now we have a set of `spark_\*.sql` files and a set of `hive_\*.sql` files in `./sql`.

`spark_\*.sql` files contain the queries to be used in spark cli (not spark-sql cli).
Currently, we only support Parquet and Spark-1.2.x / 1.3.x.

`hive_\*.sql` files contain the queries written in HiveQL.
Actually, they are standard SQL. So that they can be used in most systems such as
spark-sql cli, Presto, Hive and Impala.

It is recommended to use HiveQL in spark-sql cli or Presto. By evaluating queries, it could be seen
that query on the ordered and dupped tables runs much faster.

To evaluate the queries automatically on Spark and log the query elapsing time, see [Workload Evaluation](https://github.com/dbiir/rainbow/blob/master/rainbow-evaluate/README.md).