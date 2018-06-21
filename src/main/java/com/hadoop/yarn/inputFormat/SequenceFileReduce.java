package com.hadoop.yarn.inputFormat;

import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class SequenceFileReduce extends Reducer<Text,BytesWritable,Text,BytesWritable> {
    @Override
    protected void reduce(Text key, Iterable<BytesWritable> values, Context context) throws IOException, InterruptedException {
        Iterator<BytesWritable> iterator = values.iterator();
        context.getCounter("","").increment(1);
        context.write(key,iterator.next());
    }

}
