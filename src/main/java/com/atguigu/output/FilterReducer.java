package com.atguigu.output;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FilterReducer extends Reducer<Text, NullWritable, Text, NullWritable> {
	Text k=new Text();
	@Override
	protected void reduce(Text key, Iterable<NullWritable> values,
			Reducer<Text, NullWritable, Text, NullWritable>.Context context) throws IOException, InterruptedException {
		String line=key.toString()+"\r\n";
		k.set(line);
		for (NullWritable nullWritable : values) {
			context.write(k, NullWritable.get());
		}
		
	}
}
