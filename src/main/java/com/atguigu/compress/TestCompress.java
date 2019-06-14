package com.atguigu.compress;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.util.ReflectionUtils;

public class TestCompress {
	public static void main(String[] args) throws Exception {
//		compress("d:/大数据内容/Hadoop视频/更多干货资料.jpg","org.apache.hadoop.io.compress.BZip2Codec");
		compress("d:/大数据内容/Hadoop视频/2.资料.zip","org.apache.hadoop.io.compress.BZip2Codec");
	}

	private static void compress(String filename, String method) throws Exception {
		FileInputStream fis=	new FileInputStream(new File(filename));
		Class codecClass = Class.forName(method);
		Configuration conf=new Configuration();
		CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, conf);
		
		//获取输出流
		FileOutputStream fos=new FileOutputStream(new File(filename+codec.getDefaultExtension()));
		CompressionOutputStream cos = codec.createOutputStream(fos);
		//
//		IOUtils.copyBytes(fis, cos,1024*1024*5, conf);
		IOUtils.copyBytes(fis, cos, 1024*1024*5, false);
		IOUtils.closeStream(cos);
		IOUtils.closeStream(fos);
		IOUtils.closeStream(fis);
	}
}
