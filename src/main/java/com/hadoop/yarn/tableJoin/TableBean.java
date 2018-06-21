package com.hadoop.yarn.tableJoin;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TableBean  implements Writable{
    String id;//ID
    String order_id;//订单id
    String  order_name;//产品名称
    int order_mount;//产品数量
    String order_flag;//标记，用于标记产品表和订单表


    public void write(DataOutput out) throws IOException {
        out.writeUTF(id);
        out.writeUTF(order_id);
        out.writeUTF(order_name);
        out.writeInt(order_mount);
        out.writeUTF(order_flag);
    }

    public void readFields(DataInput in) throws IOException {
        id=in.readUTF();
        order_id=in.readUTF();
        order_name=in.readUTF();
        order_mount=in.readInt();
        order_flag=in.readUTF();
    }

    public TableBean(String id, String order_id, String order_name, int order_mount, String order_flag) {
        this.id = id;
        this.order_id = order_id;
        this.order_name = order_name;
        this.order_mount = order_mount;
        this.order_flag = order_flag;
    }

    public TableBean() {
    }

    public void setId(String id) {
        this.id = id;
    }

    public void setOrder_id(String order_id) {
        this.order_id = order_id;
    }

    public void setOrder_name(String order_name) {
        this.order_name = order_name;
    }

    public void setOrder_mount(int order_mount) {
        this.order_mount = order_mount;
    }

    public void setOrder_flag(String order_flag) {
        this.order_flag = order_flag;
    }

    public String getId() {
        return id;
    }

    public String getOrder_id() {
        return order_id;
    }

    public String getOrder_name() {
        return order_name;
    }

    public int getOrder_mount() {
        return order_mount;
    }

    public String getOrder_flag() {
        return order_flag;
    }

    @Override
    public String toString() {
        return  order_id + '\t' + order_name + '\t'  + order_mount;
    }
}
