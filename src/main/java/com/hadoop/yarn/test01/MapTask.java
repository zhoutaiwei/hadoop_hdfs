package com.hadoop.yarn.test01;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MapTask extends Mapper<LongWritable, Text, Text, IntWritable> {
	Text k = new Text();
	IntWritable v = new IntWritable(1);

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		Text text=new Text();
		IntWritable iw=new IntWritable(1);
		String line = value.toString();
		String[] split = line.split(" ");
		for (int i = 0; i <split.length; i++) {
			System.out.println(split[i]);
			text.set(split[i]);
			context.write(text,iw);
		}
	}
}
