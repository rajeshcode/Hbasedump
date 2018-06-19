package com.ebay.adi;

import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;


public class DumpTsdb extends Configured implements Tool {

  public static final TableName tableName = TableName.valueOf("cmtsdbv2");

  private Options options = new Options();
  private CommandLine       commandLine;

  public int run(String[] args) throws Exception {

    System.out.println(args);

    GnuParser parser = new GnuParser();
    commandLine = parser.parse(options, args);

    System.out.println(commandLine.getArgs());
    System.out.println(commandLine.getOptionValue("s"));
    System.out.println(commandLine.getOptionValue("e"));
    System.out.println(commandLine.getOptionValue("o"));

    Configuration conf = this.getConf();
    conf.setLong(FilterTimeStampMapper.StartTimeName, getTimeStamp(commandLine.getOptionValue("s")));
    conf.setLong(FilterTimeStampMapper.EndTimeName, getTimeStamp(commandLine.getOptionValue("e")));


    conf.setDouble("mapreduce.job.reduce.slowstart.completedmaps", 0.99);

    Job job = Job.getInstance(conf, "Dump cmtsdbv2");
    job.setJarByClass(DumpTsdb.class);

    job.setSpeculativeExecution(false);

    Scan scan = new Scan();
    scan.setCaching(500);
    scan.setCacheBlocks(false);
    scan.setMaxVersions(3);

    FilterTimeStampMapper.initJob("cmtsdbv2", scan, job);

    Connection connection = ConnectionFactory.createConnection(this.getConf());
    Table table = connection.getTable(tableName);
    HFileOutputFormat2.configureIncrementalLoad(job, table, connection.getRegionLocator(tableName));
    FileOutputFormat.setOutputPath(job, new Path(commandLine.getOptionValue("o")));

    job.waitForCompletion(true);
    return 0;
  }

  public DumpTsdb(Configuration conf) {
    super(conf);

    options.addOption("s", "startTime", true, "start time in format yyyy-MM-dd HH:mm:ss");
    options.addOption("e", "endTime", true, "end time in format yyyy-MM-dd HH:mm:ss");
    options.addOption("o", "output", true, "output directory");
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = HBaseConfiguration.create();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

    ToolRunner.run(new DumpTsdb(conf), otherArgs);
  }

  private long getTimeStamp(String date) throws ParseException {
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    Date d = sdf.parse(date);
    return d.getTime() / 1000;
  }

}