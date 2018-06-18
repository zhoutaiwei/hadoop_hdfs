package com.hadoop.yarn.FlowSort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class PhoneDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        //当有大量小文件时，可修改切片机制，将切片最小大小设为100，这样会将N个小文件合并成100M
       job.setInputFormatClass(CombineTextInputFormat.class);
        //在实际生产中可将其设为128M，100M
       CombineTextInputFormat.setMinInputSplitSize(job,2097152);
       CombineTextInputFormat.setMaxInputSplitSize(job,4194304);

       //设置reduce task合并文件的数量
       job.setPartitionerClass(Partition2.class);
       job.setNumReduceTasks(3);
        job.setMapperClass(SortPhoneMap.class);
        job.setReducerClass(SortPhoneReduce.class);

        job.setMapOutputKeyClass(PhoneBean.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(PhoneBean.class);

        FileInputFormat.setInputPaths(job,new Path("E:\\phone07"));
        FileOutputFormat.setOutputPath(job,new Path("e:/phone56/"));
        job.waitForCompletion(true);
    }
}
