
# Run mapreduce job to dump hfiles
```
export HADOOP_CLASSPATH=`/apache/hbase/bin/hbase classpath`
/apache/hadoop/bin/hadoop jar dumptsdb-1.0-SNAPSHOT.jar com.adi.DumpTsdb -Dmapreduce.map.memory.mb=2048 -Dmapreduce.reduce.memory.mb=2048 -s '2017-12-01 00:00:00' -e '2018-01-17 23:00:00' -o /tmp/testoutdump
```
