package com.hadoop.yarn.friend;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FriendReduce02 extends Reducer<Text,Text,Text,Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String name="";
        for (Text text : values) {
            name+= text.toString()+" ";
        }
        context.write(key,new Text(name));
    }
}
