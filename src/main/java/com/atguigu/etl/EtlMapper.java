package com.atguigu.etl;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class EtlMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, NullWritable>.Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		boolean result=parseLog(line,context);
		if(!result) {
			return;
		}
		context.write(value, NullWritable.get());
	}

	private boolean parseLog(String line, Mapper<LongWritable, Text, Text, NullWritable>.Context context) {
		String[] fields = line.split(" ");
		
		if(fields.length>11) {
			context.getCounter("map", "true").increment(1);
			return true;
		}
		context.getCounter("map", "false").increment(1);
		return false;
	}
}
