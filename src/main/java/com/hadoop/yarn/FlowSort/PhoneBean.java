package com.hadoop.yarn.FlowSort;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class PhoneBean implements WritableComparable<PhoneBean> {
    Long sumFlow;
    Long upFlow;
    Long downFlow;
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(sumFlow);
        dataOutput.writeLong(upFlow);
        dataOutput.writeLong(downFlow);
    }

    public void readFields(DataInput dataInput) throws IOException {
        sumFlow = dataInput.readLong();
        upFlow = dataInput.readLong();
        downFlow = dataInput.readLong();
    }

    public PhoneBean(Long upFlow, Long downFlow) {
        this.upFlow = upFlow;
        this.downFlow = downFlow;
        this.sumFlow=upFlow+downFlow;
    }

    public PhoneBean() {
    }

    public Long getsumFlow() {
        return sumFlow;
    }

    public Long getUpFlow() {
        return upFlow;
    }

    public Long getDownFlow() {
        return downFlow;
    }

    public void setsumFlow(Long sumFlow) {
        this.sumFlow = sumFlow;
    }

    public void setUpFlow(Long upFlow) {
        this.upFlow = upFlow;
    }

    public void setDownFlow(Long downFlow) {
        this.downFlow = downFlow;
    }

    @Override
    public String toString() {
        return upFlow + "\t" + downFlow +"\t" + sumFlow;
    }

    public int compareTo(PhoneBean o) {
        long i1 = this.sumFlow - o.sumFlow;
        if(i1>0){
            return -1;
        }else if(i1<0){
            return  1;
        }else{
            return (int) (this.upFlow-o.upFlow);
        }
    }

}
