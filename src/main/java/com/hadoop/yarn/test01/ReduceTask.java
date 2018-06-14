package com.hadoop.yarn.test01;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class ReduceTask extends Reducer<Text, IntWritable, Text, IntWritable>{

		@Override
		protected void reduce(Text text, Iterable<IntWritable> count,Context context) throws IOException, InterruptedException {
			int sum=0;
			for ( IntWritable iw :count) {
				System.out.println(iw.get());
				sum+=iw.get();
			}
			context.write(text,new IntWritable(sum));
		}
}
