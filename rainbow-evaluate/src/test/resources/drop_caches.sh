#!/bin/bash

for ((i=0;i<=4;i++))
do
  ssh root@presto0$i "echo 3 > /proc/sys/vm/drop_caches"
  ssh root@presto0$i sync
done