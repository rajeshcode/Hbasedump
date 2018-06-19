package com.ebay.adi;

import com.google.common.primitives.Ints;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

import static java.lang.Math.toIntExact;

public class FilterTimeStampMapper extends TableMapper<ImmutableBytesWritable, KeyValue>  {

  private byte[] startTimeBytes;
  private byte[] endTimeBytes;
  public static String StartTimeName = "FILTER_START_TIMESTAMP";
  public static String EndTimeName = "FILTER_END_TIMESTAMP";

  // 4 byte as timestamp, start from byte 4, for more infor:  https://web.archive.org/web/20130518044109/http://opentsdb.net:80/schema.html
  private final int KEY_START_IDX = 3;

  private int debugReord = 0;
  private int maxDebugRecord = 0;

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    super.setup(context);

    Configuration conf = context.getConfiguration();

    long s = conf.getLong(StartTimeName, 0);
    long e = conf.getLong(EndTimeName, 2147483648L); // year 2038

    startTimeBytes = longTo4Byte(s);
    endTimeBytes = longTo4Byte(e);

    maxDebugRecord = conf.getInt("max_debug_record", 0);

    if (debugReord < maxDebugRecord) {
      System.out.println("startTimeBytes: " + Bytes.toString(startTimeBytes));
      System.out.println("endTimeBytes: " + Bytes.toString(startTimeBytes));
    }
  }

  @Override
  protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {

    byte[] keyBytes = key.get();
    if (keyBytes.length < 7) {
      return;
    }

    if (Bytes.compareTo(keyBytes, KEY_START_IDX, 4, startTimeBytes, 0, 4) < 0) {
      if (debugReord < maxDebugRecord) {
        System.out.println("less: " + Bytes.toString(keyBytes, KEY_START_IDX, 4));
        debugReord ++;
      }
      return;
    } else if (Bytes.compareTo(keyBytes, KEY_START_IDX, 4, endTimeBytes, 0, 4) > 0) {
      if (debugReord < maxDebugRecord) {
        System.out.println("great: " + Bytes.toString(keyBytes, KEY_START_IDX, 4));
        debugReord ++;
      }
      return;
    }

    for (Cell c: value.listCells()) {
      context.write(key, (KeyValue)c);
    }
  }

  private byte[] longTo4Byte(long l) {
    return Ints.toByteArray(toIntExact(l));
  }

  public static void initJob(String table, Scan scan, Job job) throws IOException {
    TableMapReduceUtil.initTableMapperJob(table, scan, FilterTimeStampMapper.class, ImmutableBytesWritable.class, KeyValue.class, job);
  }
}
