package com.jerypeng.data;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

public class DataAnalyseReducer extends Reducer<Text, Text, Text, Text> {

	public static final double COMEIN_RANGE = 10;
	public static final double MAX_RANGE = 1000;
	public static final long COMEIN_TIME = 1;
	
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		// process values
		String LatestTime = null;
		String OldestTime = null;
		long StayTime = 0;
		double range = MAX_RANGE;
		boolean flag = false;
		for(Text value :values){
			String lineValue = value.toString();
			String [] data = lineValue.split("\t");
			double nowrange = Double.parseDouble(data[1]);
			
			if(LatestTime ==null||CalculateTime.CalculateTimeGap(LatestTime, data[0])<0){
				LatestTime = data[0];
			}
			if(OldestTime ==null||CalculateTime.CalculateTimeGap(OldestTime, data[0])<0){
				OldestTime = data[0];
			}
			if(CalculateTime.CalculateTimeGap(OldestTime, data[0])<=COMEIN_TIME && nowrange<=COMEIN_RANGE){
				OldestTime = data[0];
				if(nowrange<=range)
					range = nowrange;
				flag = true;
			}
			else if(CalculateTime.CalculateTimeGap(OldestTime, data[0])>COMEIN_TIME && nowrange<=COMEIN_RANGE){
				if(flag == true){
					StayTime = CalculateTime.CalculateTimeGap(LatestTime, OldestTime);
					context.write(key,new Text(OldestTime+"\t"+LatestTime+"\t"+String.valueOf(StayTime)+"\t"+String.valueOf(range)));
					flag = false;
					LatestTime = data[0];
					OldestTime = data[0];
					range = nowrange;
				}
			}
//			else if(CalculateTime.CalculateTimeGap(OldestTime, data[0])>COMEIN_TIME && nowrange>COMEIN_RANGE){
//				if(flag == true){
//					StayTime = CalculateTime.CalculateTimeGap(OldestTime, data[0]);
//					context.write(key,new Text(OldestTime+"\t"+LatestTime+"\t"+String.valueOf(StayTime)+"\t"+String.valueOf(range)));
//					flag = false;
//					LatestTime = data[0];
//					OldestTime = data[0];
//					range = nowrange;
//				}
//				OldestTime = data[0];
//				if(nowrange<=range)
//					range = nowrange;	
//			}
//			else{
//				OldestTime = data[0];
//				if(nowrange<=range)
//					range = nowrange;
//			}
		}
		StayTime = CalculateTime.CalculateTimeGap(LatestTime, OldestTime);
		context.write(key,new Text(OldestTime+"\t"+LatestTime+"\t"+String.valueOf(StayTime)+"\t"+String.valueOf(range)));
		
		
	}

}
