package com.hadoop.yarn.index.oneIndex;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
// hadoop hive
//hadoop--c.txt 1
public class oneIndexMapper extends Mapper<LongWritable,Text,Text,IntWritable> {
    Text textValue=new Text();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        FileSplit fs = (FileSplit) context.getInputSplit();
        String name = fs.getPath().getName();
        String line = value.toString();
        String[] split = line.split(" ");
        for (int i = 0; i < split.length; i++) {
            name=split[i]+"--"+name;
            textValue.set(name);
            context.write(textValue,new IntWritable(1));
            name=fs.getPath().getName();
        }

    }
}
