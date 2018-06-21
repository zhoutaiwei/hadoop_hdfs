package com.hadoop.yarn.outputFormat;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * 可根据不同的条件将指定的数据放至指定的文件中
 */
public class FileRecordWrite  extends RecordWriter<Text,NullWritable> {
    FSDataOutputStream stream01;
    FSDataOutputStream stream02;
    public FileRecordWrite(TaskAttemptContext context) throws IOException {

        //获取文件系统
        FileSystem fs = FileSystem.get(context.getConfiguration());
        //创建输出流
         stream01 = fs.create(new Path("E:\\hadoopfileout\\output/hadoop"));
         stream02 = fs.create(new Path("E:\\hadoopfileout\\output/other"));
    }


    public void write(Text text, NullWritable nullWritable) throws IOException, InterruptedException {
        String info = text.toString();
        if(info.contains("hadoop")){
            stream01.write(info.getBytes());
        }else {
            stream02.write(info.getBytes());
        }
    }

    public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        IOUtils.closeStream(stream01);
        IOUtils.closeStream(stream02);
    }
}
