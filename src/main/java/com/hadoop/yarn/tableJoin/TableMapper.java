package com.hadoop.yarn.tableJoin;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class TableMapper extends Mapper<LongWritable,Text,Text,TableBean> {
    TableBean tb=new TableBean();
    Text text =new Text();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        FileSplit split = (FileSplit) context.getInputSplit();
        String name = split.getPath().getName();
        String[] splits =value.toString().split("\t");
        if(name.contains("order")){//订单表

            tb.setId(splits[0]);
            tb.setOrder_id(splits[1]);
            tb.setOrder_mount(Integer.parseInt(splits[2]));
            tb.setOrder_name("");
            tb.setOrder_flag("0");

            text.set(splits[1]);
        }else{
            tb.setId("");
            tb.setOrder_id(splits[0]);
            tb.setOrder_name(splits[1]);
            tb.setOrder_mount(0);
            tb.setOrder_flag("1");
            text.set(splits[0]);
        }
        System.out.println(tb);
        context.write(text,tb );
    }
}
