package com.jerypeng.data;

import java.util.Random;

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
		if (args.length != 3) {  
            System.err.println("Usage: wordcount <in> <out> <out>");  
            System.exit(2);  
        }  
		// 判断output文件夹是否存在，如果存在则删除  
        Path path = new Path(args[1]);
        Path path1 = new Path(args[2]);							// 取第1,2个表示输出目录参数（第0个参数是输入目录）  
        
        FileSystem fileSystem = path.getFileSystem(conf);
        FileSystem fileSystem1 = path1.getFileSystem(conf);		 // 根据path找到这个文件  
        if (fileSystem.exists(path)) {  
            fileSystem.delete(path, true);						// true的意思是，就算output有东西，也一带删除  
        }
        if (fileSystem1.exists(path1)) {  
            fileSystem1.delete(path1, true);	
        }
        
		//Job job1 = new Job(conf);
        Job job1 = Job.getInstance(conf,"TheFirstJob");
		job1.setMapperClass(DataAnalyseMapper_1.class);
		job1.setReducerClass(DataAnalyseReducer_1.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);
		
		// 定义一个临时目录，先将任务的输出结果写到临时目录中, 下一个job以临时目录为输入目录。  
        FileInputFormat.addInputPath(job1, new Path(args[0]));  
        Path tempDir = new Path("hdfs://localhost:9000/"  
                + Integer.toString(new Random().nextInt(Integer.MAX_VALUE)));  
        FileOutputFormat.setOutputPath(job1, tempDir);  
  
        if (job1.waitForCompletion(true)) {			//如果第一轮MapReduce完成再做这里的代码  
            //Job job2 = new Job(conf); 
            Job job2 = Job.getInstance(conf,"TheSecondJob");
            
            //设置第二轮MapReduce的相应处理类与输入输出  
            job2.setMapperClass(DataAnalyseMapper_2.class);  
            job2.setReducerClass(DataAnalyseReducer_2.class);  
            job2.setOutputKeyClass(Text.class);  
            job2.setOutputValueClass(Text.class);  
            
            FileInputFormat.addInputPath(job2, tempDir);  
            FileOutputFormat.setOutputPath(job2, new Path(args[1]));            
//            System.exit(job2.waitForCompletion(true) ? 0 : 1);  
         
        Job job3 = Job.getInstance(conf,"TheThirdJob");
        
        //设置第二轮MapReduce的相应处理类与输入输出  
        job3.setMapperClass(DataAnalyseMapper_3.class);  
        job3.setReducerClass(DataAnalyseReducer_3.class);  
        job3.setOutputKeyClass(Text.class);  
        job3.setOutputValueClass(Text.class);  
        
        FileInputFormat.addInputPath(job3, tempDir);  
        FileOutputFormat.setOutputPath(job3, new Path(args[2]));
        
        //搞完删除刚刚的临时创建的输入目录  
        FileSystem.get(conf).deleteOnExit(tempDir);
        if(job2.waitForCompletion(true)){
        	System.exit(job3.waitForCompletion(true)? 0 : 1);  
        }
        	
        
        } 	
	}
}
