package com.assignment.hw4_Part5;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class DistinctMapper extends Mapper<Object, Text, Text, NullWritable>{
	
	private Text outIp = new Text();
	
	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException{
		String[] fields = value.toString().split("- -");
		String ip = fields[0];
		outIp.set(ip);
		context.write(outIp, NullWritable.get());
	}
}
