package com.hadoop.yarn.partitionAndSort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class OrderReduce extends Reducer<OrderBean,IntWritable,IntWritable ,DoubleWritable> {

    @Override
    protected void reduce(OrderBean key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        context.write(new IntWritable(key.getOrder_id()) ,new DoubleWritable(key.getPrice()));

    }
}
