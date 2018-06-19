package com.hadoop.yarn.partitionAndSort;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class OrderPartition extends Partitioner<OrderBean, IntWritable> {
    public int getPartition(OrderBean orderBean, IntWritable intWritable, int i) {
        int order_id = orderBean.getOrder_id();
        int part=3;
        if(order_id==1){
            part=0;
        }else if(order_id==2){
            part=1;
        }else  if (order_id==3){
            part=2;
        }
        return part;
    }
}
