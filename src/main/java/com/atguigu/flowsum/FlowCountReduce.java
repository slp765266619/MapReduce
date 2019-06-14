package com.atguigu.flowsum;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FlowCountReduce extends Reducer<Text, FlowBean, Text, FlowBean> {
	FlowBean bean=new FlowBean();
	@Override
	protected void reduce(Text k, Iterable<FlowBean> values, Context context)
			throws IOException, InterruptedException {
		long sum_upFlow=0;
		long sum_downFlow=0;
		
		for (FlowBean flowBean : values) {
			sum_upFlow+= flowBean.getUpFlow();
			sum_downFlow+=flowBean.getDownFlow();
		}
		bean.set(sum_upFlow,sum_downFlow);
		context.write(k, bean);
	}
}
