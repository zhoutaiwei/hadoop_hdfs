package com.hadoop.yarn.partitionAndSort;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class OrderBean implements WritableComparable<OrderBean> {

    int order_id;
    double price;
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(order_id);
       dataOutput.writeDouble(price);
        System.out.println("write:"+order_id+".."+price);
    }

    public void readFields(DataInput dataInput) throws IOException {
        order_id= dataInput.readInt();
        price=dataInput.readDouble();
        System.out.println("read:"+order_id+".."+price);
    }

    public OrderBean() {
    }

    public OrderBean(int order_id, double price) {
        this.order_id = order_id;
        this.price = price;
    }

    public void setOrder_id(int order_id) {
        this.order_id = order_id;
    }

    public void setPrice(double price) {
        this.price = price;
    }

    public int getOrder_id() {
        return order_id;
    }

    public double getPrice() {
        return price;
    }

    @Override
    public String toString() {
        return  order_id +"\t" + price ;
    }

    public int compareTo(OrderBean o) {
        if(this.order_id > o.order_id){
            return 1;
        }else if(this.order_id < o.order_id){
            return -1;
        }else   if(this.price>o.price){
            return -1;
        }else if(this.price<o.price){
            return 1;
        }else{
            return 0;
        }
    }
}
