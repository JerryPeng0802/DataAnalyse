package com.jerypeng.data;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DataAnalyseMapper_2 extends Mapper<LongWritable, Text, Text, Text> {

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		String linevalue = value.toString();
		String line [] = linevalue.split("\t");
		String key_1 = line[0];
		String value_1 = line[1]+"\t"+line[2]+"\t"+line[3]+"\t"+line[4]+"\t"+line[5];
		context.write(new Text(key_1),new Text(value_1) );
	}

}
