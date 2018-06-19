package com.hadoop.yarn.partitionAndSort;

import com.hadoop.yarn.FlowSort.OrderGroupCompartor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class OrderDriver {


    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = new Job(new Configuration());
        job.setJarByClass(OrderDriver.class);
        //分区
      job.setPartitionerClass(OrderPartition.class);
       job.setNumReduceTasks(4);

        job.setMapperClass(OrderMap.class);
        job.setReducerClass(OrderReduce.class);

        job.setGroupingComparatorClass(OrderGroupCompartor.class);
        job.setMapOutputKeyClass(OrderBean.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(DoubleWritable.class);

        FileInputFormat.setInputPaths(job,new Path("E:\\hadoop_info"));
        FileOutputFormat.setOutputPath(job,new Path("e:/hadoop_result13/"));

        job.waitForCompletion(true);
    }
}
