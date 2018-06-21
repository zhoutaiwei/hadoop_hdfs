package com.hadoop.yarn.tableJoin.distributedCache;

import com.hadoop.yarn.tableJoin.TableBean;
import com.hadoop.yarn.tableJoin.TableDriver;
import com.hadoop.yarn.tableJoin.TableMapper;
import com.hadoop.yarn.tableJoin.TableReduce;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class distributedCacheDriver {
    public static void main(String[] args) {
        Configuration configuration = new Configuration();
        try {
            Job job = new Job(configuration);
            job.setJarByClass(distributedCacheDriver.class);

            job.setMapperClass(DistributedMaper.class);
            //设置分区数量为零，不需要reduce
            job.setNumReduceTasks(0);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(TableBean.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(NullWritable.class);

            FileInputFormat.setInputPaths(job,new Path("E:\\hadoopfile\\table_cache"));
            FileOutputFormat.setOutputPath(job,new Path("E:\\hadoopfileout\\table\\17"));

            job.waitForCompletion(true);

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
        }
    }
}
