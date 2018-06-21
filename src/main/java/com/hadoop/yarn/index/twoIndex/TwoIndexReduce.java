package com.hadoop.yarn.index.twoIndex;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

//pingping c.txt-->1 b.txt-->3 a.txt-->1
public class TwoIndexReduce extends Reducer<Text,Text,Text,NullWritable> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Iterator<Text> iterator = values.iterator();
        String count="";
        while (iterator.hasNext()){
             count+=" "+iterator.next().toString();

        }
        context.write(new Text(key.toString()+count),NullWritable.get());
    }
}
