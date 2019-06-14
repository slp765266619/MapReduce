package com.atguigu.partition;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class FlowCountDriver {
	
	public static void main(String[] args) throws Exception {
		args=new String[] {"d:/input/partition","d:/output"};
		Configuration conf=new Configuration();
		//获取job对象
		Job job = Job.getInstance(conf);
		//设置jar路径
		job.setJarByClass(FlowCountDriver.class);
		//关联mapper和reducer
		job.setMapperClass(FlowCountMapper.class);
		job.setReducerClass(FlowCountReduce.class);
		//设置mapper输出的key和value类型
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(FlowBean.class);
		//设置最终输出的key和value类型
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FlowBean.class);
		//设置自定义分区
		job.setPartitionerClass(ProvinecePartitioner.class);
		job.setNumReduceTasks(5);
		//设置输入输出路径
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		//提交job
		boolean result = job.waitForCompletion(true);
		System.exit(result?0:1);
	}
}
