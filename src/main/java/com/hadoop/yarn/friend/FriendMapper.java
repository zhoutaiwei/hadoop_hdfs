package com.hadoop.yarn.friend;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FriendMapper extends Mapper<LongWritable,Text,Text,Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split(":");
        String person = split[0];
        String[] friends=split[1].split(",");
        for (int i = 0; i < friends.length; i++) {
            System.out.println("---"+friends[i]);
            context.write(new Text(friends[i]),new Text(person));

        }
    }
}
