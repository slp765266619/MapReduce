package com.atguigu.partition;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class ProvinecePartitioner extends Partitioner<Text, FlowBean> {

	@Override
	public int getPartition(Text key, FlowBean value, int numPartitions) {
		
		//获取手机号前三位
		String prephone = key.toString().substring(0, 3);
		int partition=4;
		if ("136".equals(prephone)) {
			partition=0;
		}else if("137".equals(prephone)) {
			partition=1;
		}else if("138".equals(prephone)) {
			partition=2;
		}else if("139".equals(prephone)) {
			partition=3;
		}else {
			partition=4;
		}
		
		return partition;
	}

}
