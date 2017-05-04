# rainbow
Column ordering and duplication framework.

Project structure:
* **rainbow-common**
  contains the common interface definitions, functions and utils.

* **rainbow-core**
  provides a RESTful API to framework users.

* **rainbow-evaluate**
  issues queries to Spark or performs read-column operation on local Parquet files to evaluate the end-to-end gain of a column ordering / duplication layout.

* **rainbow-layout**
  generates column ordering and duplication layout.

* **rainbow-redirect**
  redirects (rewrites) the accessed columns for a query when it is running on a duplication layout.

* **rainbow-seek**
  evaluates seek cost of a storage system (HDFS for local file system) and generate the seek cost function.


Full source code and documents will be submitted soon.
