package com.atguigu.custominpu;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;


public class SequenceFileDriver {
	public static void main(String[] args) throws Exception {
		args=new String[] {"d:/input/inputword","d:/output"};
		Configuration con = new Configuration();
		Job job = Job.getInstance(con);
		// 设置jar
		job.setJarByClass(SequenceFileDriver.class);
		// 设置Mapper类
		job.setMapperClass(SequenceFileMapper.class);
		// 设置Reduce类
		job.setReducerClass(SequenceFileReducer.class);
		// 设置输入的InoutFormat
		job.setInputFormatClass(WholeFileInputformat.class);
		// 设置输出的OutputFormat
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		// 设置Map输出
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(BytesWritable.class);
		// 设置最终输出的key和value
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(BytesWritable.class);
		// 设置输入路径和输出路径
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		// 提交
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
