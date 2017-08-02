# rainbow
Column ordering and duplication framework for very wide tables stored on HDFS. See paper for details: 
http://dl.acm.org/citation.cfm?id=3035930

Project structure:

* **rainbow-core**
  schedules the other modules (except rainbow-benchmark module) to make them work together and provides a simple API to rainbow users.

* **rainbow-benchmark**
  provides a wide table data generator. The generated data has similar schema and data distribution with real-world data while sensitive contents in the schema and data are masked. This module can be used seprately.
  
* **rainbow-common**
  contains the common interface definitions, functions and utils.

* **rainbow-evaluate**
  issues queries to Spark or performs read-column operation on local Parquet files to evaluate the end-to-end gain of a column ordering / duplication layout.

* **rainbow-layout**
  generates column ordering and duplication layout.

* **rainbow-redirect**
  redirects (rewrites) the accessed columns for a query when it is running on a duplication layout.

* **rainbow-seek**
  evaluates seek cost of a storage system (HDFS for local file system) and generate the seek cost function.

