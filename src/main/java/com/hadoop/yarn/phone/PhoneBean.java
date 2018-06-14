package com.hadoop.yarn.phone;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class PhoneBean implements Writable {
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
        return
                upFlow +
                "   " + downFlow +"   " + sumFlow;
    }
}
