package com.atguigu.table;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class TableMapper extends Mapper<LongWritable, Text, Text, TableBean> {
	String filename;
	TableBean v=new TableBean();
	Text k=new Text();
	@Override
	protected void setup(Mapper<LongWritable, Text, Text, TableBean>.Context context)
			throws IOException, InterruptedException {
		//获取文件的名称
		FileSplit inputSplit = (FileSplit) context.getInputSplit();
		 filename = inputSplit.getPath().getName();
		
	}
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, TableBean>.Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		if(filename.startsWith("order")) {
			String[] fields = line.split("\t");
			v.setId(fields[0]);
			v.setPid(fields[1]);
			v.setAmount(Integer.parseInt(fields[2]));
			v.setPname("");
			v.setFlag("order");
			k.set(fields[1]);
		}else {
			String[] fields = line.split("\t");
			v.setId("");
			v.setPid(fields[0]);
			v.setAmount(0);
			v.setPname(fields[1]);
			v.setFlag("pd");
			k.set(fields[0]);
		}
		context.write(k, v);
	}
}
