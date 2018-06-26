package com.hadoop.yarn.friend;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;

/**
 * A	R,A,O,B,N,V,S,T,H,A,M,I,K,U,L,
   B	U,P,F,B,A,U,V,N,
 */
public class FriendMapper02 extends Mapper<LongWritable,Text,Text,Text>{
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split("\t");
        String name = split[0];
        String[] friends = split[1].split(",");
        Arrays.sort(friends);
        for (int i = 0; i < friends.length-1; i++) {
            for (int j = i+1; j < friends.length; j++) {
                context.write(new Text(friends[i]+"--"+friends[j]),new Text(name));
            }
            
        }
    }
}
