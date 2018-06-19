package com.hadoop.yarn.FlowSort;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

//40 15659617908 cac36884-b 7052 7052
public class SortPhoneMap extends Mapper<LongWritable, Text, PhoneBean, Text> {
    Text v = new Text();
    PhoneBean pb;

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split("\t");
            String s = split[split.length - 1];
            Long sumFlow = Long.parseLong(s);
            String callNum = split[0];
            Long upFlow = Long.parseLong(split[1]);
            Long downFlow = Long.parseLong(split[2]);
            pb=new PhoneBean(upFlow,downFlow);
            v.set(callNum);
            context.write(pb,v);

    }
}
