package com.hadoop.yarn.tableJoin;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;

public class TableReduce extends Reducer<Text, TableBean, TableBean, NullWritable> {
    @Override
    protected void reduce(Text key, Iterable<TableBean> values, Context context) throws IOException, InterruptedException {
        TableBean productBean =new TableBean();
        List<TableBean> list = new ArrayList<TableBean>();
        for (TableBean tb : values) {
            System.out.println(tb.getOrder_flag());
            if (tb.getOrder_flag().equals("0")) {
                list.add(tb);
            } else {
                try {
                    BeanUtils.copyProperties(productBean,tb);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
        for (TableBean tbean : list) {
            tbean.setOrder_name(productBean.getOrder_name());
            context.write(tbean,NullWritable.get());
        }
    }
}
