# Rainbow

Rainbow is a tool that helps improve the I/O performance of wide tables stored in columnar formats on HDFS.

### HDFS Column Store
Nowadays, very large amount of structured data is stored as wide two-dimension tables in columnar file formats on HDFS.
Popular columnar file formats include RC File, ORC, Parquet and Carbondata. They are widely supported in data analytical
systems over HDFS, such as Hive, Spark, Presto and Impala.

These file formats partition the data into row groups and place data inside
a row group in columnar manner. A row group is stored in an HDFS block. This is an efficient
way due to that it introduces most advantages of column store into Hadoop ecosystem
without affecting parallelism, scalability and fault-tolerance.

### Wide Tables
With these file formats, tables on HDFS are becoming very wide, from a few hundred columns to tens of thousands.
Wide Table has some good features:
- **Reduce Join Cost:** distributed joins are very expensive.
- **Easy Schema Modification:** new workloads and new schema requirements come out everyday. Adding new fields to a table is much easier than redesigning the schema following normal forms.
- **Application Friendly:** new features can be added to the data as a set of new fields without interrupting or modifying running applications.

Although everything looks good with wide tables stored as columnar formats on HDFS, the I/O efficiency and query
performance are far from optimal.

In an experimental example, given a 400GB, 1187-column table store as Parquet in a single node HDFS. The read bandwidth of HDFS is 100MB/s. A query took 907 seconds to read 8 columns (0.3% data, i.e. 1.2GB)
from the table. While ideally, it may take only 12 seconds.

Such a huge gap between ideal and reality is caused by disk seeks. The columns read by a query may not be continuous on disk so that seek cost becomes the major part of I/O cost.

We wrote a **[paper](http://dl.acm.org/citation.cfm?id=3035930)** on narrowing the gap and we have **Rainbow** to implement the solutions in
the paper.

>
Note: Rainbow does not contain any source code used in the original implementations described in section 5 and 6 of the paper.
Copyrights and patents of the original implementations are not included in Rainbow.
>

## How to Use it?

Rainbow is just a command line tool. It calculates the optimized data layout and generates Hive
SQL statements. The generated CREATE TABLE and LOAD DATA statements can be used in Hive to apply the optimized data layout.

Rainbow does not need to be installed. It does not change anything except your CREATE TABLE statements.

Follow the steps in [Rainbow Core](https://github.com/dbiir/rainbow/blob/master/rainbow-core/README.md) to use it.

## Project Structure
* **rainbow-core**
  schedules the other modules (except rainbow-benchmark module) to make them work together and provides a simple API to rainbow users.

* **rainbow-benchmark**
  provides a wide table data generator. The generated data has similar schema and data distribution with real-world data while sensitive contents in the schema and data are masked.

* **rainbow-common**
  contains the common interface definitions, functions and utils.

* **rainbow-evaluate**
  issues queries to Spark or performs read-column operation on local Parquet files to evaluate the end-to-end gain of a column ordering / duplication layout.

* **rainbow-layout**
  generates column ordering and duplication layout.

* **rainbow-redirect**
  redirects (rewrites) the accessed columns for a query when it is running on a duplication layout.

* **rainbow-seek**
  evaluates seek cost of a storage system (HDFS or local file system) and generates the seek cost function.
