package com.hadoop.yarn.friend;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class FriendDriver02 {
    public static void main(String[] args) {
        try {
            Configuration configuration = new Configuration();
            Job job = new Job(configuration);

            job.setMapperClass(FriendMapper02.class);
            job.setReducerClass(FriendReduce02.class);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            FileInputFormat.setInputPaths(job,new Path("E:\\hadoopfile\\friend\\two"));
            FileOutputFormat.setOutputPath(job,new Path("E:\\hadoopfileout\\friend\\two\\03"));
            job.waitForCompletion(true);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
