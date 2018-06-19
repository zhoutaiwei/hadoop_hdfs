package com.hadoop.yarn.partitionAndSort;

import org.apache.commons.collections.bag.SynchronizedSortedBag;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
//001 part_02 625.6
public class OrderMap extends Mapper<LongWritable,Text,OrderBean,IntWritable> {
    OrderBean orderBean = new OrderBean();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split("\t");
        int id=Integer.valueOf(split[0]);
        double price=Double.valueOf(split[2]);

        orderBean.setOrder_id(id);
        orderBean.setPrice(price);
        System.out.println("map:"+orderBean );
        context.write(orderBean,new IntWritable(1));
    }
}
