package com.atguigu.custominpu;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class WholeRecordReader extends RecordReader<Text,BytesWritable> {

	FileSplit split;
	Configuration configuration;
	Text k=new Text();
	BytesWritable v = new BytesWritable();
	boolean isProgress=true;
	@Override
	public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
		this.split=(FileSplit) split;
		configuration= context.getConfiguration();
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		if(isProgress) {
			byte[] buf=new byte[(int) split.getLength()];
			//核心业务模块
			//获取fs对象
			Path path = split.getPath();
			FileSystem fs = path.getFileSystem(configuration);
			//获取输入流
			FSDataInputStream fis = fs.open(path);
			//
			IOUtils.readFully(fis, buf, 0, buf.length);
			//封装V
			v.set(buf,0,buf.length);
			//封装k
			k.set(path.toString());
			//关闭资源
			IOUtils.closeStream(fis);
			isProgress=false;
			return true;
		}
		return false;
	}

	@Override
	public Text getCurrentKey() throws IOException, InterruptedException {
		// 
		
		return k;
	}

	@Override
	public BytesWritable getCurrentValue() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		
		return v;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		
	}
	
}
