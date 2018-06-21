package com.hadoop.yarn.outputFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;

public class FileDriver {
    public static void main(String[] args) {
        Configuration configuration = new Configuration();
        try {
            Job job = new Job(configuration);
            job.setJarByClass(FileDriver.class);
        // 要将自定义的输出格式组件设置到 job 中
            job.setOutputFormatClass(OutputFormat.class);

            job.setMapperClass(FileMapper.class);
            job.setReducerClass(FileReduce.class);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(NullWritable.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(NullWritable.class);

            FileInputFormat.setInputPaths(job,new Path("E:\\hadoopfile\\outputformat"));
            FileOutputFormat.setOutputPath(job,new Path("E:\\hadoopfileout\\output/11"));
            job.waitForCompletion(true);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
        }


    }
}
