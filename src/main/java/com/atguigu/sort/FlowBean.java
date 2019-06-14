package com.atguigu.sort;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class FlowBean implements WritableComparable<FlowBean> {
	
	private long upFlow;//上行流量
	private long downFlow;//下行流量
	private long sumFlow;//总流量
	
	public FlowBean() {
		super();
	}

	public FlowBean(long upFlow, long downFlow,long sumFlow) {
		super();
		this.upFlow = upFlow;
		this.downFlow = downFlow;
		this.sumFlow = sumFlow;
	}

	public long getUpFlow() {
		return upFlow;
	}

	public void setUpFlow(long upFlow) {
		this.upFlow = upFlow;
	}

	public long getDownFlow() {
		return downFlow;
	}

	public void setDownFlow(long downFlow) {
		this.downFlow = downFlow;
	}

	public long getSumFlow() {
		return sumFlow;
	}

	public void setSumFlow(long sumFlow) {
		this.sumFlow = sumFlow;
	}

	@Override
	public String toString() {
		return  upFlow + "\t" + downFlow + "\t" +sumFlow ;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(upFlow);
		out.writeLong(downFlow);
		out.writeLong(sumFlow);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		upFlow= in.readLong();
		downFlow=in.readLong();
		sumFlow=in.readLong();
	}

	@Override
	public int compareTo(FlowBean bean) {
		//核心比较
		int result;
		if(this.sumFlow>bean.getSumFlow()) {
			result=-1;
		}else if(this.sumFlow<bean.getSumFlow()) {
			result=1;
		}else {
			result=0;
		}
		return result;
	}

}
