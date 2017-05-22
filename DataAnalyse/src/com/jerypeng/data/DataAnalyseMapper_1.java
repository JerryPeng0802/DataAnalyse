package com.jerypeng.data;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

public class DataAnalyseMapper_1 extends Mapper<LongWritable, Text, Text, Text> {

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		String alldata = value.toString();
		JSONObject jsondata = JSONObject.fromObject(alldata);
		String id = jsondata.getString("id");
		String time = jsondata.getString("time");
		String data = jsondata.getString("data");
		
		
		JSONArray jsona= JSONArray.fromObject(data);
		@SuppressWarnings("unchecked")
		Iterator<JSONObject> js = jsona.iterator();
		while(js.hasNext()){
			JSONObject JsonDataItem = js.next();
			String mac = JsonDataItem.getString("mac");
			String range = JsonDataItem.getString("range");
			
			context.write(new Text(id+"\t"+mac),new Text(time+"\t"+range) );
	}
		//context.write(new Text(id), new Text(data));
		
		

	}

}
