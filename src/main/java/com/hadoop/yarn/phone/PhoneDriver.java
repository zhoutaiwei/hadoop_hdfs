package com.hadoop.yarn.phone;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class PhoneDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setMapperClass(PhoneMap.class);
        job.setReducerClass(PhoneReduce.class);
        job.setMapOutputKeyClass(Text.class);
        job.setOutputValueClass(PhoneBean.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(PhoneBean.class);
        FileInputFormat.setInputPaths(job,new Path("e:/phone_info.txt"));
        FileOutputFormat.setOutputPath(job,new Path("e:/phone08/info.txt"));
        job.waitForCompletion(true);
    }
}
