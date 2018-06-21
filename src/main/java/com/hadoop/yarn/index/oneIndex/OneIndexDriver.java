package com.hadoop.yarn.index.oneIndex;

import com.hadoop.yarn.outputFormat.FileDriver;
import com.hadoop.yarn.outputFormat.FileMapper;
import com.hadoop.yarn.outputFormat.FileReduce;
import com.hadoop.yarn.outputFormat.OutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class OneIndexDriver {
    public static void main(String[] args) {
        Configuration configuration = new Configuration();
        try {
            Job job = new Job(configuration);
            job.setJarByClass(FileDriver.class);

            job.setMapperClass(oneIndexMapper.class);
            job.setReducerClass(oneIndexReduce.class);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(IntWritable.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(NullWritable.class);

            FileInputFormat.setInputPaths(job,new Path("E:\\hadoopfile\\index\\one"));
            FileOutputFormat.setOutputPath(job,new Path("E:\\hadoopfileout\\index\\one\\08"));
            job.waitForCompletion(true);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
        }
    }

}
