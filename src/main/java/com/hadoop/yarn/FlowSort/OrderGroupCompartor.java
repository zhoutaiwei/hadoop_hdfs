package com.hadoop.yarn.FlowSort;

import com.hadoop.yarn.partitionAndSort.OrderBean;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * 自定义分组,这样可只要一个数据（一个文件内）
 */
public class OrderGroupCompartor extends WritableComparator {

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        if(a==null||b==null){
            return 0;
        }
        OrderBean bean = (OrderBean) a;
        OrderBean bean1 = (OrderBean) b;
        if(bean.getOrder_id()>bean1.getOrder_id()){
            return 1;
        }else if(bean.getOrder_id()<bean1.getOrder_id()){
            return -1;
        }else {
            return 0;
        }
    }

    /**
     * 指定类型
     */
    public OrderGroupCompartor() {
        super(OrderBean.class,true);
    }
}
