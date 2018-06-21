package com.hadoop.yarn.index.twoIndex;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
//hadoop--a.txt 3
//hadoop--b.txt 3
//hadoop--c.txt 2
//pingping c.txt-->1 b.txt-->3 a.txt-->1

public class TwoIndexMapper extends Mapper<LongWritable,Text,Text,Text>{
    Text textKey=new Text();
    Text textValue=new Text();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split("--");
            textKey.set(split[0]);
            String count = split[1].replace(" ", "-->");
            textValue.set(count);
            System.out.println(textKey+"---------"+textValue);
            context.write(textKey,textValue);
    }
}
