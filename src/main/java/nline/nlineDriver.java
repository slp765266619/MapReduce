package nline;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class nlineDriver {
	public static void main(String[] args) throws Exception {
		args=new String[] {"d:/input/inputword","d:/output"};
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		//使用NLineInputFormat读取
		NLineInputFormat.setNumLinesPerSplit(job, 3);
		job.setInputFormatClass(NLineInputFormat.class);
		// 设置Jar包依赖路径
		job.setJarByClass(nlineDriver.class);
		// 设置Mapper和Reducer依赖路径
		job.setMapperClass(nlineMapper.class);
		job.setReducerClass(nlineReducer.class);
		//设置map输出格式
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		//设置最终输出格式
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		//设置输入流路径
		//设置输出路径
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		//提交job
		System.exit(job.waitForCompletion(true)?0:1);
	}

}
