hdfs-cache-tool
===============

Simple tool to iteratively cache a hdfs locations.

Compilation:
============
mvn clean package

Usage:
=====
hadoop jar ./target/hdfs-cache-tool-0.0.1-SNAPSHOT.jar -Dpath=<globPath; supports csv format> -DpoolName=<poolName> -Dttl=<ttl_in_ms (0) to indicate NEVER> -Dverbose=true/false(false by default)

path, poolName are mandatory parameters

e.g: hadoop jar ./target/hdfs-cache-tool-0.0.1-SNAPSHOT.jar -Dpath=/tmp/folder/*,/user/hive/*/2/* -DpoolName=test -Dverbose=true
e.g: hadoop jar ./target/hdfs-cache-tool-0.0.1-SNAPSHOT.jar -Dpath=/tmp/folder/* -DpoolName=test -Dverbose=true
e.g: hadoop jar ./target/hdfs-cache-tool-0.0.1-SNAPSHOT.jar -Dpath=/tmp/folder/* -DpoolName=test -Dttl=-1
