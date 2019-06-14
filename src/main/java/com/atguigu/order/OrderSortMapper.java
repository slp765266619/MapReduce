package com.atguigu.order;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class OrderSortMapper extends Mapper<LongWritable, Text, OrderBean, NullWritable> {
	OrderBean v=new OrderBean();
	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, OrderBean, NullWritable>.Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		String[] fields = line.split("\t");
		v.setOrder_id(Integer.parseInt(fields[0]));
		v.setPrice(Double.parseDouble(fields[2]));
		context.write(v, NullWritable.get());
	}
}
