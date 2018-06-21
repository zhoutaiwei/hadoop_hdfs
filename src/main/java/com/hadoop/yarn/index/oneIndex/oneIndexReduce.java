package com.hadoop.yarn.index.oneIndex;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class oneIndexReduce extends Reducer<Text,IntWritable,Text,NullWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum=0;
        String line;
        Text text=new Text();
        for ( IntWritable t : values) {
            sum+=t.get();
        }
        line=key.toString()+" "+sum;
        text.set(line);
        context.write(text,NullWritable.get());
    }
}
