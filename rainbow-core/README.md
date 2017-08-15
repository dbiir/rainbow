# Rainbow Core

Rainbow Core contains a command line interface of rainbow.
A set of commands is provided to evaluating environmental features of
HDFS, calculate new optimized data layouts and generate useful SQL
statements to apply the data layouts in SQL-on-HDFS environment.

## Prepare

- Enter the directory of rainbow-benchmark module which contains this README.md.

- Find 'data_template.tar.gz' in dataset subdirectory and uncompress it to somewhere, like './benchmark_data/'.

data_template.tar.gz contains a ready-to-use template which can be used to generate a 1000-column wide table dataset.
You can modify schema.txt and the column templates in it to generate a different dataset.
workload.txt contains the query templates. If you modified schema.txt and column templates,
ensure the column names in workload.txt are valid.

## Build

In the directory of rainbow-core module, run:
```bash
$ mvn clean
$ mvn package
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
java -jar target/rainbow-core-xxx-full.jar -f ./src/main/resources/rainbow.properties -d ./src/main/resources/params
```

If argument -f is not given, the default configuration file in the jar ball will be used.

Now we are going to do data layout optimization experiment step by step.

### Generating Benchmark Data

Go to [Rainbow Benchmark](https://github.com/dbiir/rainbow/tree/master/rainbow-benchamrk)
and follow the steps to generate benchmark data and put data on HDFS.

### Hadoop and Hive Configurations
In Hive beeline, set the following settings before loading data:
```
set dfs.block.size=268435456;
set parquet.block.size=339131300;
set mapred.max.split.size=268435456;
```

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

