package com.hadoop.yarn.tableJoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class TableDriver  {
    public static void main(String[] args) {
        Configuration configuration = new Configuration();
        try {
            Job job = new Job(configuration);
            job.setJarByClass(TableDriver.class);

            job.setMapperClass(TableMapper.class);
            job.setReducerClass(TableReduce.class);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(TableBean.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(NullWritable.class);

            FileInputFormat.setInputPaths(job,new Path("E:\\hadoopfile\\table"));
            FileOutputFormat.setOutputPath(job,new Path("E:\\hadoopfileout\\table\\16"));

            job.waitForCompletion(true);

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
        }
    }
}
