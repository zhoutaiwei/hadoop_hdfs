package com.hadoop.yarn.test01;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;


public class Driver {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Job job = new Job(new Configuration());
		job.setJarByClass(Driver.class);
		job.setMapperClass(MapTask.class);
		job.setReducerClass(ReduceTask.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		FileOutputFormat.setCompressOutput(job, true);
		// 设置压缩的方式
		FileOutputFormat.setOutputCompressorClass(job, BZip2Codec.class);


		FileInputFormat.setInputPaths(job,new Path("e:/tmp/use.txt"));
		FileOutputFormat.setOutputPath(job,new Path("e:/tmp02/01"));
		boolean b = job.waitForCompletion(true);
		System.exit(b?0:1);
	}
}
