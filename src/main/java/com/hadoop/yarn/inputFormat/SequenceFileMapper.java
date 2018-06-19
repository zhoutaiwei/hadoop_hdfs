package com.hadoop.yarn.inputFormat;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class SequenceFileMapper extends Mapper<NullWritable, BytesWritable, Text,BytesWritable>{
    Text text=new Text();
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        //获取文件切片信息
        FileSplit inputSplit = (FileSplit) context.getInputSplit();
        //获取切片名称
        String name = inputSplit.getPath().getName();
        //设置text输出
        text.set(name);
    }

    @Override
    protected void map(NullWritable key, BytesWritable value, Context context) throws IOException, InterruptedException {
        context.write(text,value);

    }

}
