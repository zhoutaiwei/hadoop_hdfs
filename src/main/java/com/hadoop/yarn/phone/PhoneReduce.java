package com.hadoop.yarn.phone;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class PhoneReduce extends Reducer<Text,PhoneBean,Text,PhoneBean>{


    @Override
    protected void reduce(Text key, Iterable<PhoneBean> values, Context context) throws IOException, InterruptedException {
        Long upFlowSum=0L;
        Long downFlowSum=0L;
        for (PhoneBean pb:values) {
            System.out.println(pb);
            upFlowSum+=pb.upFlow;
            downFlowSum+=pb.downFlow;
        }
        PhoneBean phoneBean= new PhoneBean(upFlowSum,downFlowSum);
        context.write(key,phoneBean);
    }
}
