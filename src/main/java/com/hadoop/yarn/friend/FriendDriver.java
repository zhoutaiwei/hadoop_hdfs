package com.hadoop.yarn.friend;

import com.hadoop.yarn.index.twoIndex.TwoIndexMapper;
import com.hadoop.yarn.index.twoIndex.TwoIndexReduce;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class FriendDriver {
    public static void main(String[] args) {
        try {
            Configuration config=new Configuration();
            Job job = new Job(config);

            job.setMapperClass(FriendMapper.class);
            job.setReducerClass(FriendReduce.class);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            FileInputFormat.setInputPaths(job,new Path("E:\\hadoopfile\\friend\\one"));
            FileOutputFormat.setOutputPath(job,new Path("E:\\hadoopfileout\\friend\\one\\18"));
            job.waitForCompletion(true);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
}
