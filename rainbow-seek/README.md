# Seek COST EVALUATION

Rainbow improves query performance of wide tables on HDFS by reducing the disk seek cost.
Given a seek cost function of seek distance, rainbow reorders the location of columns and duplicates frequently
contended columns inside the row group so that co-accessed columns are likely to be read by
paying lower seek costs.

In rainbow, we use a feasible method to get a more accurate (compared with [LINEAR or POWER](https://github.com/dbiir/rainbow/tree/master/rainbow-core#build-seek-cost-model)) seek cost function
which is very important to column layout optimization. The method is to perform real seek operations
on HDFS with different seek distance, and fit the seek cost function with the recorded seek costs and
corresponding distances.

Such seek cost evaluation method is implemented in rainbow-seek module.
Follow the steps here to use it.

## Prepare

- Install Hadoop (1.2.1 and 2.7.3 recommended). HDFS datanode should use HDD as storage media.
- It is recommended to firstly
[Build Columnar Format Data on HDFS](https://github.com/dbiir/rainbow/blob/master/rainbow-core/README.md#data-transform).
- Set HDFS configurations in rainbow.properties like
[this](https://github.com/dbiir/rainbow/blob/master/rainbow-core/README.md#usage).

## Build

Enter the root directory of rainbow, run:
```bash
$ mvn clean
$ mvn package
$ cd ./rainbow-seek
```

Then you get `rainbow-seek-xxx-full.jar` in `target` subdirectory.
Now you are ready to run rainbow seek evaluation.

## Usage

To get usage information:
```bash
$ java -jar target/rainbow-seek-xxx-full.jar -h
```

There are two required arguments:
- `-c / --command`, specifies the command to execute, which can be GENERATE_FILE or SEEK_EVALUATION;
- `-p / param_file`, specifies the parameter file to be used by the command.

Template of the parameter files can be found in `./src/main/resources/params/`.
A parameter file contains the parameters to be used when executing the corresponding command.
Edit the parameters before executing a command.

### Generate File

To perform seek evaluations, you need a set of files on HDFS or local file system. It is recommended to
use real data files in a wide table.
But you can also generate a set of files by GENERATE_FILE command like this:
```bash
$ java -jar target/rainbow-seek-xxx-full.jar -c GENERATE_FILE -f ./src/main/resources/params/GENERATE_FILE.properties
```

In `GENERATE_FILE.properties`, set:
- `method` to specify whether you are going to generate the files on HDFS or local file system.
- `data.path` to specify the directory on HDFS or local fs to store generated files.
- `block.size` and `num.blocks` to control the total amount of generated data.

Here we use the default values:
```
method=HDFS
data.path=/rainbow/seek/
block.size=268435456
num.blocks=100
```

So that 100 files (one block per file)
are generated under `/rainbow/seek/` on HDFS.

### Seek Evaluation

On the set of files, we can perform seek cost evaluation like this:
```
$ java -jar target/rainbow-seek-xxx-full.jar -c SEEK_EVALUATION -f ./src/main/resources/params/SEEK_EVALUATION.properties
```

Parameters in `SEEK_EVALUATION.properties` are:
```
method=HDFS
# the directory on HDFS to store the generated files
data.path=/rainbow/seek/
# number of seeks to be performed in each segment
num.seeks=100
# the interval of seek distance in bytes
seek.distance.interval=51200
# the number of seek distance intervals in the seek cost function
num.intervals=100
# the length in bytes been read after each seek
read.length=100
# the number of files been skipped between two segments
num.skipped.files=10
# The local directory used to write evaluation results.
# This directory can not be exist and will be created by this method itself
log.dir=/rainbow/seek_eva/log
# the integer number of HDFS file the seek evaluation starts
start.file.num=0
# the integer number of HDFS file the seek evaluation ends
end.file.num=100
drop.caches.sh=/rainbow/drop_caches.sh
```

`segment` here means a subset of the files. We do not perform seek operations in very file
because that is too expensive. Only one file in each segment is used for seek evaluations.

Here we have 100 files in `/rainbow/seek/`. With
`start.file.num=0`, `end.file.num=100` and `num.skipped.files=10`,
rainbow will group the files into 10 segments and put 10 files in each segment.

`seek.distance.interval`, `num.intervals` and `read.length` are used to control
how the seek evaluations are performed. Rainbow will perform seeks with a set of different
distance, like 50KB, 100KB, 150KB... `seek.distance.interval` is the interval
of the series of distances. `read.length` is the bytes been read in a file after seeking to a offset.
This is used to simulate the real data reading pattern when executing queries.

`log.dir` is the directory on local fs to store the seek evaluation results.
A `seek_cost.txt` file will be generated in this directory.
This file is used to create [SIMULATED](https://github.com/dbiir/rainbow/blob/master/rainbow-core/README.md#build-seek-cost-model)
seek cost function for data layout optimization.

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

You should ensure that the system user running rainbow jars have the right permissions
to execute this script and clear the fs cache on the cluster nodes.