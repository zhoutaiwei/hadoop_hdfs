package com.hadoop.yarn.logAnalysis;

import com.hadoop.yarn.tableJoin.TableBean;
import com.hadoop.yarn.tableJoin.TableDriver;
import com.hadoop.yarn.tableJoin.distributedCache.DistributedMaper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class LogDriver {
    public static void main(String[] args) {
        Configuration configuration = new Configuration();
        try {
            Job job = new Job(configuration);
            job.setJarByClass(LogDriver.class);

            job.setMapperClass(LogMapper.class);
            //设置分区数量为零，不需要reduce
            job.setNumReduceTasks(0);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(NullWritable.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(NullWritable.class);

            FileInputFormat.setInputPaths(job,new Path("E:\\hadoopfile\\inputformat\\input.txt"));
            FileOutputFormat.setOutputPath(job,new Path("E:\\hadoopfileout\\log\\05"));

            job.waitForCompletion(true);
            long value = job.getCounters().findCounter("map", "true").getValue();
            System.out.println("------"+value);
            long value1 = job.getCounters().findCounter("map", "false").getValue();
            System.out.println("------"+value1);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
        }
    }
}
