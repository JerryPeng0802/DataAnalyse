package com.jerypeng.data;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class DataAnalyseDriver {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		
		 // 判断output文件夹是否存在，如果存在则删除  
        Path path = new Path(args[1]);// 取第1个表示输出目录参数（第0个参数是输入目录）  
        FileSystem fileSystem = path.getFileSystem(conf);// 根据path找到这个文件  
        if (fileSystem.exists(path)) {  
            fileSystem.delete(path, true);// true的意思是，就算output有东西，也一带删除  
        }  
		Job job = Job.getInstance(conf, "DataAnalyse");
		job.setJarByClass(com.jerypeng.data.DataAnalyseDriver.class);
		// TODO: specify a mapper
		job.setMapperClass(DataAnalyseMapper.class);
		// TODO: specify a reducer
		job.setReducerClass(DataAnalyseReducer.class);

		// TODO: specify output types
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		// TODO: specify input and output DIRECTORIES (not files)
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		if (!job.waitForCompletion(true))
			return;
	}

}
