package com.atguigu.flowsum;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FlowCountMapper extends Mapper<LongWritable, Text, Text, FlowBean> {
		Text k=new Text();
	    FlowBean bean = new FlowBean();
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			//获取一行
			String line = value.toString();
			//切割数据
			String[] fields = line.split("\t");
			//封装对象
			k.set(fields[1]);
			long upFlow=Long.parseLong(fields[fields.length-3]);
			long downFlow=Long.parseLong(fields[fields.length-2]);
			bean.setUpFlow(upFlow);
			bean.setDownFlow(downFlow);
			
			//输出
			context.write(k, bean);
		}

}
