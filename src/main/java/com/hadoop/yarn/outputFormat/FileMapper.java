package com.hadoop.yarn.outputFormat;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FileMapper extends Mapper<LongWritable,Text,Text,NullWritable>{
    Text text=new Text();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split(" ");
        for (int i = 0; i < split.length; i++) {
            text.set(split[i]);
            context.write(text,NullWritable.get());
        }
    }
}
