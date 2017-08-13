# Rainbow Core

In Hive beeline, set the following settings before loading data:
```
set dfs.block.size=268435456;
set parquet.block.size=339131300;
set mapred.max.split.size=268435456;
```

In mapred-site.xml, add the following configuration:
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

In hadoop-env.sh, add the following setting:
```sh
export HADOOP_HEAPSIZE=4096
```

