#!/bin/bash

for ((i=1; i<=5; i+=1))
do
  ssh node$i "echo 3 > /proc/sys/vm/drop_caches"
  ssh node$i sync
done
