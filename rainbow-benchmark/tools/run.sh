#!bin/bash
pkill -f GenServer
cd target
java -cp "rainbow-benchmark/WEB-INF/lib/*:rainbow-benchmark-jar-with-dependencies.jar:classes/__data" cn.edu.ruc.iir.rainbow.benchmark.server.GenServer

