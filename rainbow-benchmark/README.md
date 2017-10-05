# Rainbow Benchmark

Rainbow Benchmark contains java based command line tool that helps generate data in wide tables.

>
> Note: Due to security and privacy considerations, data is randomly generated in Rainbow Benchmark.
> No details about the production data contents, schema and query workload in Microsoft Bing are
> allowed to be public. Although we take lessons from the patterns of real-world data schema and query workload,
> the workload and schema templates used in Rainbow are totally rewritten. They are different from the
> workload and data schema used in our [paper](http://dl.acm.org/citation.cfm?id=3035930).
>

## Prepare

- Enter the directory of rainbow-benchmark module which contains this README.md.
- Find `data_template.tar.gz` in dataset subdirectory and uncompress it to somewhere, like `./benchmark_data/`.

data_template.tar.gz contains a ready-to-use template which can be used to generate a 1000-column wide table dataset.
You can modify schema.txt and the column templates in it to generate a different dataset.
workload.txt contains the query templates. If you modified schema.txt and column templates,
ensure the column names in workload.txt are valid.

## Build

In the directory of rainbow-benchmark module, run:
```bash
$ mvn clean
$ mvn package
```

Then you get a `rainbow-benchmark-xxx-full.jar` in ./target directory.
Now you are ready to generate wide table data.

## Usage

To get usage information:
```bash
$ java -jar target/rainbow-benchmark-xxx-full.jar -h
```

For example, to generate a 4GB dataset with 4 concurrent threads:
```bash
$ java -jar target/rainbow-benchmark-xxx-full.jar --data_size=4096 --thread_num=4 --directory=./benchmark_data
```

Here, --directory argument specifies the directory of data template.

## Where is the Data

Data is generated under the directory of data template.
In this case, you can find the generated data in `./benchmark_data/rainbow_[timestamp]_4094MB/`.
There are a `memo.txt` and a `data` subdirectory in it.
And there are a number of data files generated under `data` directory, one by each thread.
Each line in `memo.txt` denotes the name of a file and the number of rows in the file.

## Next Step

- Put the generated data into HDFS, like:
```bash
$ hdfs dfs -mkdir -p /rainbow/text
$ hdfs dfs -put ./benchmark_data/data/* /rainbow/text/
```

- Go to [Rainbow CLI](https://github.com/dbiir/rainbow/blob/master/rainbow-cli/README.md) and follow the steps in it.