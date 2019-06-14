package com.atguigu.output;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class FilterDriver {
	public static void main(String[] args) throws Exception {
		args=new String[] {"d:/input/outputfmart","d:/output"};
		// 1 获取配置信息以及封装任务
		Configuration configuration = new Configuration();
		Job job = Job.getInstance(configuration);
		// 2 设置jar加载路径
		job.setJarByClass(FilterDriver.class);
		// 3 设置map和reduce类
		job.setMapperClass(FilterMapper.class);
		job.setReducerClass(FilterReducer.class);
		// 设置map输出
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);

		// 5 设置最终输出kv类型
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		
		job.setOutputFormatClass(FilterOutPutFamrt.class);
		// 6 设置输入和输出路径
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		// 7.提交
		boolean result = job.waitForCompletion(true);
		System.exit(result ? 0 : 1);

	}
}
