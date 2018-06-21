package com.hadoop.yarn.inputFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class WholeRecordReader extends RecordReader<NullWritable, BytesWritable> {
    FileSplit split;
    Configuration conf;
    boolean processed = false;
    BytesWritable value = new BytesWritable();

    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        split = (FileSplit) inputSplit;
        conf = taskAttemptContext.getConfiguration();
    }

    public boolean nextKeyValue() throws IOException, InterruptedException {
        //如果文件没被使用
        if (!processed) {
            processed = true;//标识文件正在被使用
            //定义缓存区
            byte[] bytes = new byte[(int) split.getLength()];
            System.out.println("-----------------"+split.getLength());
            FileSystem fs=null;
            FSDataInputStream fis=null;
            try {
                //获取文件系统
                Path path = split.getPath();
                fs = path.getFileSystem(conf);
                //读取文件数据
                fis = fs.open(path);
                //读取文件数据,将fis中的数据存放到bytes中
                IOUtils.readFully(fis, bytes, 0, bytes.length);
                //输出文件内容
                value.set(bytes, 0, bytes.length);

                return true;
            } catch (IOException e) {
                e.printStackTrace();
            }finally {
                IOUtils.closeStream(fis);

            }
        }
        return false;
    }

    public NullWritable getCurrentKey() throws IOException, InterruptedException {
        return NullWritable.get();
    }

    public BytesWritable getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    public float getProgress() throws IOException, InterruptedException {
        return processed ? 1 : 0;
    }

    public void close() throws IOException {

    }
}
