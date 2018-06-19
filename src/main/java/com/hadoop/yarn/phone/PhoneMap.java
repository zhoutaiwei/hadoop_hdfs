package com.hadoop.yarn.phone;

import org.apache.commons.collections.bag.SynchronizedSortedBag;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

//40 15659617908 cac36884-b 7052 7052
public class PhoneMap extends Mapper<LongWritable, Text, Text, PhoneBean> {
    Text k = new Text();
    PhoneBean pb;

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] split = line.split(" ");
            String phnoeNumber = split[1];
            String upFlow = split[3];
            String downFlow = split[4];
            pb = new PhoneBean(Long.valueOf(upFlow), Long.valueOf(downFlow));
            k.set(phnoeNumber);

        context.write(k, pb);
    }
}
