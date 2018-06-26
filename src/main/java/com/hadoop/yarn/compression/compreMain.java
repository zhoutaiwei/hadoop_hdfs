package com.hadoop.yarn.compression;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;

import javax.management.ReflectionException;
import java.io.*;

public class compreMain {
    public static void main(String[] args) throws IOException {


        /**
         * 在driver中开启map的压缩
         */
        Configuration configuration = new Configuration();
        // 开启 map 端输出压缩
        configuration.setBoolean("mapreduce.map.output.compress", true);
    // 设置 map 端输出压缩方式
        configuration.setClass("mapreduce.map.output.compress.codec", BZip2Codec.class,CompressionCodec.class);

        Job job = new Job(configuration);
        // 设置 reduce 端输出压缩开启
        FileOutputFormat.setCompressOutput(job, true);
        // 设置压缩的方式
        FileOutputFormat.setOutputCompressorClass(job, BZip2Codec.class);
        // FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
        // FileOutputFormat.setOutputCompressorClass(job, DefaultCodec.class);


   //     compression("E:\\hadoopfile\\compression\\ddddd","org.apache.hadoop.io.compress.BZip2Codec");
    //    deccompression("E:\\hadoopfile\\compression\\ddddd.bz2");
    }

    /**
     * 文件压缩
     * @param fileName
     * @param method
     */
    public static void compression(String fileName, String method) {
        FileInputStream fis = null;
        CompressionOutputStream cos = null;
        FileOutputStream fos = null;
        try {
            //获取输入流
            fis=new FileInputStream(fileName);

            //获取压缩类
            Class<?> aClass = Class.forName(method);
            CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(aClass, new Configuration());
            //获取输出流
            fos=new FileOutputStream(fileName+codec.getDefaultExtension());
           cos= codec.createOutputStream(fos);
           //流拷贝
            IOUtils.copyBytes(fis,cos,1024*1024*5,false);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            IOUtils.closeStream(cos);
            IOUtils.closeStream(fos);
            IOUtils.closeStream(fis);
        }
    }
    public static void deccompression(String fileName){
        //判断文件能否被解压
        CompressionCodecFactory factory = new CompressionCodecFactory(new Configuration());
        CompressionCodec codec = factory.getCodec(new Path(fileName));
        if(codec==null){
            System.out.println("cannot find codec for file:"+fileName);
            return;
        }
        CompressionInputStream cis=null;
        FileOutputStream fos=null;
        try {
             cis = codec.createInputStream(new FileInputStream(fileName));
             fos = new FileOutputStream(fileName + ".decodec");
             IOUtils.copyBytes(cis,fos,1024*1024*5,false);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
