package com.atguigu.table;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class TableDriver {
	public static void main(String[] args) throws Exception {
		args=new String[] {"d:/input/table","d:/output"};
		// 1 获取配置信息以及封装任务
		Configuration configuration = new Configuration();
		Job job = Job.getInstance(configuration);
		// 2 设置jar加载路径
		job.setJarByClass(TableDriver.class);
		// 3 设置map和reduce类
		job.setMapperClass(TableMapper.class);
		job.setReducerClass(TableReducer.class);
		// 设置map输出
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(TableBean.class);

		// 5 设置最终输出kv类型
		job.setOutputKeyClass(TableBean.class);
		job.setOutputValueClass(NullWritable.class);
//		job.setNumReduceTasks(2);
//		job.setCombinerClass(WordcountCombiner.class);
		// 6 设置输入和输出路径
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		// 7.提交
		boolean result = job.waitForCompletion(true);
		System.exit(result ? 0 : 1);

	}
}