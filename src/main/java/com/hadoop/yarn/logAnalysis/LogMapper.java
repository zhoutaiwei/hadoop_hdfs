package com.hadoop.yarn.logAnalysis;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class LogMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
    Text text=new Text();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        boolean result = parseLog(line, context);
        if (result){
            text.set(line);
            context.write(text,NullWritable.get());
        }

    }

    private boolean parseLog(String line,Context context) {
        if (line != null) {
            if (line.length() >= 20) {
                context.getCounter("map","true").increment(1L);
                return true;
            }else{
                context.getCounter("map","false").increment(1L);
                return false;
            }
        }
        return false;
    }


}
