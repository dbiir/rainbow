# Rainbow Configuration

A `rainbow.properties` file can be used to control the behavior of Rainbow.
There are many configuration properties in this file:

```
##### ---------- 1. Cluster Settings ---------- #####

# HDFS configuration
namenode.host=localhost
namenode.port=9000

# Spark configuration
spark.master=localhost
spark.app.port=7077
spark.driver.webapps.port=4040
spark.warehouse.dir=hdfs://localhost:9000/spark-warehouse
spark.executor.cores=4
spark.executor.memory=4g

# Presto configuration
presto.jdbc.url=jdbc:presto://presto00:8080/hive/rainbow
presto.user=test
presto.password=null
presto.ssl=false

# The directory on HDFS to store the wide tables
data.dir=/rainbow

# Table name of text format table
# This table is used as the data source in data loading and format transformation
text.table.name=text


#####---------- 2. Column Ordering Algorithm Settings ----------#####

# Column ordering algorithms
scoa=cn.edu.ruc.iir.rainbow.layout.algorithm.impl.ord.FastScoa
autopart=cn.edu.ruc.iir.rainbow.layout.algorithm.impl.ord.AutoPartC

# Settings for column ordering algorithms
scoa.cooling_rate=0.003
scoa.init.temperature=10000


#####---------- 3. Column Duplication Algorithm Settings ----------#####

# Column duplication algorithms
gravity=cn.edu.ruc.iir.rainbow.layout.algorithm.impl.dup.legacy.GravityDup
insertion=cn.edu.ruc.iir.rainbow.layout.algorithm.impl.dup.FastInsertionDup
refine=cn.edu.ruc.iir.rainbow.layout.algorithm.impl.dup.FastRefine

# Settings for column duplication algorithms
dup.storage.headroom=0.05
dup.max.duped.columns=200
insertion.candidate.column.num=300
insertion.select.stride=10
insertion.refine.stride=10
refine.cooling_rate=0.003
refine.init.temperature=0.0000001
refine.budget=100
refine.thread.num=4
gravity.divisions=100
gravity.gap=50
gravity.max.cluster.length=500


#####---------- 4. Column Redirection Settings ----------#####

# Name of the inverted index to be cached in memory
# This index is used in redirecting columns
# Currently, inverted index is the only index used in column redirection
inverted.index.name=inverted

# Used to mark a duplicated column in files.
# e.g. given DUP_MARK="_rainbow_dup_",
# a column named column1 with dupId 2 will be saved in a file as column1_rainbow_dup_2
# note that this should contain only characters which are legal in a SQL identifier.
# it will be used in generating SQL statements.
dup.mark=_rainbow_dup_
```

### Cluster Settings
Rainbow can generate SQL statements for creating, loading and querying data.
In these statements, cluster settings are used to specify the HDFS and Spark
URIs and paths.

Rainbow creates external tables in Hive. The tables are stored under `data.dir` on HDFS.

To load data into columnar format tables (Parquet or ORC),
a TEXT format table can be used as the data source. 
`text.table.name` is the TEXT table name in Hive.

### Column Ordering Algorithm Settings

Column ordering is the main technique used in Rainbow to optimize
data layout for wide tables. There are two types of column ordering 
algorithm in Rainbow: `autopart` and `scoa`.
We can specify the implementation for these two algorithms by the configuration properties
of the same name.

`cn.edu.ruc.iir.rainbow.layout.algorithm.impl.ord.FastScoa` is the 
best implementation for `scoa`. It is the optimized `SCOA` algorithm used 
in our [paper](http://dl.acm.org/citation.cfm?id=3035930).
`cn.edu.ruc.iir.rainbow.layout.algorithm.impl.ord.AutoPartC` 
is the best implementation for `autopart`. It is the baseline `AutoPartC` used in
out paper. AutoPartC combines the [autopart](http://www.cs.cmu.edu/~natassa/aapubs/conference/AutoPart.pdf) 
and [hill-climbing](http://dl.acm.org/citation.cfm?id=1315488). 
vertical partitioning algorithms. Layout optimization algorithms are loaded
using Java reflection, which make it easy to add new algorithms to Rainbow.

`scoa.cooling_rate` and `scoa.init.temperature` are the 
cooling rate and initial temperature of 
scoa. See Appendix C in our paper on tuning these two parameters.

### Column Duplication Algorithm Settings

Column duplication is used to further optimized the ordered data layout.
There are two types of duplication algorithms: `gravity`
and `insertion`. We can specify the best implementations for the algorithms.
`insertion` is the column duplication algorithm used in our paper. 
While `gravity` is just an experimental algorithm, which currently can not be used in 
real column duplication tasks. `refine` is a simulated-annealing-based 
algorithm used in the refinement stages of insertion duplication algorithm.

`dup.storage.headroom` is the available storage headroom in column duplication.
The default value 0.05 means 5%. `dup.max.duped.columns` is the maximum
number of column replicas can be produced in column duplication.

The `insertion` duplication algorithm duplicates the columns in multiple stages.
`insertion.candidate.column.num` is the number of candidate columns selected
for duplication in each stage. Make sure it is not greater than the number of columns
in your origin table schema. `insertion.select.stride` is the stride of a 
duplication stage. The default value 10 mean there will be a duplication stage
after each 10 columns been duplicated.

Similarly, there are multiple refinement stages in the insertion duplication
algorithm. A refinement state tries to further reduce the seek cost by reordering
the columns and column replicas.
`insertion.refine.stride` is the stride of a refinement stage.

`refine.cooling_rate` and `refine.init.temperature` are the cooling rate and
initial temperature for `refine`. Using a small initial temperature makes
the refine algorithm quickly converge to a stable state, like a greedy algorithm.
`refine.budget` is the computation budget of `refine`. The unit is second. 
`refine.thread.num` is the number of concurrent threads used to run refine algorithm.
Set it to the number of physical threads in your machine could be a good practice.


`gravity.divisions`, `gravity.gap` and `gravity.max.cluster.length` are the configuration
properties for `gravity` duplication algorithm. As we are not using this algorithm
in real column duplication tasks, just leave them there.


### Column Redirection Settings

After columns in the table schema been duplicated, a query had to
redirect the accessed columns to get benefits from column duplication.
To perform column redirection for queries, we have to firstly build an index.
More details are discussed in 
[Column Redirection](https://github.com/dbiir/rainbow/blob/master/rainbow-cli/README.md#column-redirection).

Instructions for configuration properties of column redirection are given in
their comments.