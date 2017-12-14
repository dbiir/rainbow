# Rainbow

Rainbow is a tool that helps improve the I/O performance of wide tables stored in columnar formats on HDFS.

### Demonstration
[Demonstration Video](https://www.youtube.com/embed/6qaJBPZiHSA).
![Image](https://github.com/dbiir/rainbow/blob/master/docs/resources/images/video.png)

More information in our [project main page](https://dbiir.github.io/rainbow/).

### HDFS Column Store
In many industrial and academic data analytical applications, huge amount of structured data is stored as wide two-dimension tables in columnar file formats on HDFS.
Popular columnar file formats, including RC File, ORC, Parquet and Carbondata, are widely supported in data analytical
systems over HDFS such as Hive, Spark, Presto and Impala.

These file formats partition the data into row groups and place data inside
a row group in columnar manner. A row group is stored in an HDFS block. This is an efficient
way for that it introduces most advantages of column store into Hadoop ecosystem
without affecting parallelism, scalability and fault-tolerance.

### Wide Tables
With these file formats, tables on HDFS are becoming very wide, from a few hundred columns to tens of thousands.
Wide Table has some important advantages:
- **Join Cost Saving:** distributed joins are very expensive in HDFS environment. With wide tables, distributed joins are no longer needed.
- **Easy Schema Modification:** new workloads and new schema requirements are emerging everyday. Adding new fields to a wide table is much easier than redesigning the schema following normal forms.
- **Application Friendly:** new data features can be added to the table as a set of new fields without interrupting or modifying running applications.

Although everything looks good having the wide tables stored as columnar formats on HDFS, the I/O efficiency and query
performance are far from optimal.

In an experimental example, given a 400GB, 1187-column table stored as Parquet in a single node HDFS. The read bandwidth of HDFS is 100MB/s. A query took 907 seconds to read 8 columns (0.3% data, i.e. 1.2GB)
from the table. While ideally, it should take only 12 seconds.

Such a huge gap between ideal and reality is caused by disk seeks. The columns read by a query may not be continuous on disk so that seek cost becomes the major part of I/O cost.

We wrote a **[Paper (SIGMOD'17)](http://dl.acm.org/citation.cfm?id=3035930)** on narrowing the gap and we have **Rainbow** to implement the solutions in
the paper.

>
> Note: Rainbow does not contain any source code used in the original implementations described in section 5 and 6 of the paper.
> Copyrights and patents of the original implementations are not included in Rainbow.
>

## How to Use it?

Rainbow is just a command line tool. It calculates the optimized data layout and generates Hive
SQL statements. The generated CREATE TABLE and LOAD DATA statements can be used in Hive to apply the optimized data layout.

Rainbow does not need to be installed. It does not change anything except your CREATE TABLE statements.

Follow the steps in [Rainbow CLI](https://github.com/dbiir/rainbow/blob/master/rainbow-cli/README.md) to use the command line interface of Rainbow.

Or follow the steps in [Rainbow Web](https://github.com/dbiir/rainbow/blob/master/rainbow-web/README.md) to use the Web user interface of Rainbow.

## Contact us
For feedback and questions, feel free to email us:
* Haoqiong Bian bianhaoqiong@gmail.com
* Guodong Jin jelly.guodong.jin@gmail.com
* Youxian Tao taoyouxian@aliyun.com

Welcome to contribute and submit pull requests :)

## Project Structure
* **rainbow-cli**
  schedules the other modules and provides a command line interface of Rainbow.

* **rainbow-benchmark**
  provides a wide table data generator. The generated data has similar schema and data distribution with real-world data while sensitive contents in the schema and data are masked.

* **rainbow-common**
  contains the common interface definitions, functions and utils.

* **rainbow-evaluate**
  issues queries to Spark or performs read-column operation on Parquet files to evaluate the I/O gain of a column ordering / duplication layout.

* **rainbow-layout**
  generates column ordering and duplication layout.

* **rainbow-redirect**
  redirects (rewrites) the accessed columns for a query when it is running on a duplication layout.

* **rainbow-seek**
  evaluates seek cost of a storage system (HDFS or local file system) and generates the seek cost function.

