package com.hadoop.yarn.tableJoin.distributedCache;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

public class DistributedMaper extends Mapper<LongWritable,Text,Text,NullWritable> {
    Map<String ,String > map=new HashMap<String, String>();
    /**
     * 将product中的数据放到map集合中
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {

        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(new FileInputStream("E:\\hadoopfile\\table\\product"), "utf-8"));
        String line=null;
        while((line=bufferedReader.readLine())!=null){
            String[] split = line.split("\t");
            map.put(split[0],split[1]);
        }
        Set<String> set = map.keySet();
        Iterator<String> iterator = set.iterator();

    }

    Text text=new Text();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] split = line.split("\t");
        String orader_id=split[1];
        String name = map.get(orader_id);
        text.set(line+"\t"+name);
        context.write(text,NullWritable.get());
    }
}
