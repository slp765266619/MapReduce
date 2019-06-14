package com.atguigu.sort;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FlowCountSortMapper extends  Mapper<LongWritable, Text, FlowBean, Text>{
	FlowBean k = new FlowBean();
	Text v = new Text();
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, FlowBean, Text>.Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		String[] fields = line.split("\t");
		String phoneNumber=fields[0];
		long upflow = Long.parseLong(fields[1]);
		long downflow = Long.parseLong(fields[2]);
		long sumflow = Long.parseLong(fields[3]);
		k.setUpFlow(upflow);
		k.setDownFlow(downflow);
		k.setSumFlow(sumflow);
		v.set(phoneNumber);
		context.write(k, v);
	}

}
