package com.hadoop.yarn.friend;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class FriendReduce extends Reducer<Text,Text,Text,Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        Iterator<Text> iterator = values.iterator();
        String friend="";
        while (iterator.hasNext()){
             friend+= iterator.next().toString()+",";

        }
        context.write(key,new Text(friend));
    }
}
