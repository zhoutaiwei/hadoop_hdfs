package com.hadoop.yarn.phone;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class ProvincePartition extends Partitioner<Text,PhoneBean> {
    public int getPartition(Text text, PhoneBean phoneBean, int i) {
        String substring = text.toString().substring(0, 3);
        int partition=3;
      if(substring.startsWith("13")){
          partition=0;
      }else if (substring.startsWith("15")){
          partition=1;
      }else if (substring.startsWith("18")){
          partition=2;
      }

        return partition;
    }
}
