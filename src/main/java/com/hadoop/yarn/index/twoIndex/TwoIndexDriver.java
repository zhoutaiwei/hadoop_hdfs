package com.hadoop.yarn.index.twoIndex;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class TwoIndexDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration config=new Configuration();
        Job job = new Job(config);

        job.setMapperClass(TwoIndexMapper.class);
        job.setReducerClass(TwoIndexReduce.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.setInputPaths(job,new Path("E:\\hadoopfile\\index\\two"));
        FileOutputFormat.setOutputPath(job,new Path("E:\\hadoopfileout\\index\\two\\08"));
        job.waitForCompletion(true);
    }
}
