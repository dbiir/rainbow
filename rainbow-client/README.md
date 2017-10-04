# Rainbow Client

Rainbow Client contains java based command line tool that helps upload workloads of each pipline.

## Prepare

- Enter the directory of rainbow-client module which contains this README.md.
- Find `pno` of the pipline that you want to deal with, like `dbced032765f68732a5caa949fb4a1df`. 

## Build

In the directory of rainbow-client module, run:
```bash
$ mvn clean
$ mvn package
```

Then you get a `rainbow-client-xxx-full.jar` in ./target directory.
Now you are ready to upload workloads.

## Usage

To get usage information:
```bash
$ java -jar target/rainbow-client-xxx-full.jar -h
```

For example, to upload a workload with the piplineNo `dbced032765f68732a5caa949fb4a1df`, queryID `wqersadf`, weight `1` and columns `Column_1,Column_2,Column_5,Column_9`:
```bash
$ java -jar target/rainbow-client-xxx-full.jar --pno=dbced032765f68732a5caa949fb4a1df --i=qwerasdf --w=1 --c=Column_1,Column_2,Column_5,Column_9
```


## Where is the Data

Workload File is generated under the directory of `resources/pipline/pno` in `rainbow-manage` Module.


- Go to [Rainbow Manage](https://github.com/dbiir/rainbow/blob/master/rainbow-manage/README.md) and follow the steps in it.