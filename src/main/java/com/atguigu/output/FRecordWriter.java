package com.atguigu.output;

import java.io.IOException;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class FRecordWriter extends RecordWriter<Text, NullWritable> {
	FSDataOutputStream fosatguigu;
	FSDataOutputStream fosother;

	public FRecordWriter(TaskAttemptContext job) {

		try {
			// 获取文件系统
			FileSystem fs = FileSystem.get(job.getConfiguration());
			// 创建到atguigu的输出流
			fosatguigu = fs.create(new Path("d:/atguigu.log"));
			// 创建其他类型的输出流
			fosother = fs.create(new Path("d:/other.log"));

		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	@Override
	public void write(Text key, NullWritable value) throws IOException, InterruptedException {
		// 写出时，判断文件中是否包含atguigu
		if (key.toString().contains("atguigu")) {
			fosatguigu.write(key.toString().getBytes());
		}else {
			fosother.write(key.toString().getBytes());
		}
	}

	@Override
	public void close(TaskAttemptContext context) throws IOException, InterruptedException {
		IOUtils.closeStream(fosatguigu);
		IOUtils.closeStream(fosother);
	}

}
