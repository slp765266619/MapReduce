package com.atguigu.keyinputformat;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueLineRecordReader;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class KVTextDriver {
	public static void main(String[] args) throws Exception {
		args=new String[] {"d:/input/inputword","d:/output"};
		//1.获取job对象
		Configuration conf=new Configuration();
		// 设置切割符
		conf.set(KeyValueLineRecordReader.KEY_VALUE_SEPERATOR, " ");
		Job job = Job.getInstance(conf);
		
		//2.设置jar存储路径
		job.setJarByClass(KVTextDriver.class);
		//3。关联mapper和reduccer
		job.setMapOutputKeyClass(KVTextMapper.class);
		job.setReducerClass(KVTextReducer.class);
		//4.设置mapper输出的key和value
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		//5.设置最终输出的key和value类型
		job.setOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		//6.设置输入输出路径
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		//7.提交job
		System.exit(job.waitForCompletion(true)?0:1);
	}
}
