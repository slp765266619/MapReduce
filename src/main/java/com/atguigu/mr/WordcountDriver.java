package com.atguigu.mr;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordcountDriver {
	public static void main(String[] args) throws Exception {
		args = new String[] { "d:/input/combiner", "d:/output" };
		// 1 获取配置信息以及封装任务
		Configuration configuration = new Configuration();
		configuration.setBoolean("mapreduce.map.output.compress", true);
		configuration.setClass("mapreduce.map.output.compress.codec", BZip2Codec.class, CompressionCodec.class);
		Job job = Job.getInstance(configuration);
		// 2 设置jar加载路径
		job.setJarByClass(WordcountDriver.class);
		// 3 设置map和reduce类
		job.setMapperClass(WordCountMapper.class);
		job.setReducerClass(WordcountReducer.class);
		// 设置map输出
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		// 5 设置最终输出kv类型
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
//		job.setNumReduceTasks(2);
//		job.setCombinerClass(WordcountCombiner.class);
		// 6 设置输入和输出路径
		FileInputFormat.setInputPaths(job, new Path(args[0]));
	
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		// 设置reduce端输出压缩开启
		FileOutputFormat.setCompressOutput(job, true);
		// 设置压缩的方式
		FileOutputFormat.setOutputCompressorClass(job, BZip2Codec.class);
		// 7.提交
		boolean result = job.waitForCompletion(true);
		System.exit(result ? 0 : 1);

	}
}
