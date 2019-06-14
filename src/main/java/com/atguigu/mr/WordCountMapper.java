package com.atguigu.mr;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountMapper extends Mapper<LongWritable,Text, Text, IntWritable> {
	Text k=new Text();
	IntWritable v=new IntWritable(1);
	@Override
	protected void map(LongWritable key, Text value, 
			Context context)
			throws IOException, InterruptedException {
		
		String line = value.toString();
		String[] wrods = line.split(" ");
		for (String word : wrods) {
			k.set(word);
			context.write(k, v);
		}
	}

}
