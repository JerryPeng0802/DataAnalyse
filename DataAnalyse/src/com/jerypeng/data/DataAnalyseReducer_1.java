package com.jerypeng.data;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class DataAnalyseReducer_1 extends Reducer<Text, Text, Text, Text> {

	
	public static final double COMEIN_RANGE = 10;
	public static final double MAX_RANGE = 1000;
	public static final long COMEIN_TIME = 1;
	
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		// process values
		String LatestTime_IN = null;
		String OldestTime_IN = null;
		String LatestTime_OUT = null;
		String OldestTime_OUT = null;
		long StayTime = 0;
		double range = MAX_RANGE;
		boolean flag = false;
		ArrayList <String> array = new ArrayList<String>();
		for(Text value : values){
			array.add(value.toString());
		}
		Collections.sort(array);
		Iterator<String> it = array.iterator();
		
		while(it.hasNext()){
			String [] data = it.next().split("\t");
			String nowtime = data[0];
			double nowrange = Double.parseDouble(data[1]);
			flag = false;
			if(nowrange <= COMEIN_RANGE){
				if(LatestTime_IN == null && OldestTime_IN == null)
					LatestTime_IN = OldestTime_IN =nowtime;
				flag = true;
				if(CalculateTime.CalculateTimeGap(nowtime, LatestTime_IN) < COMEIN_TIME){
					LatestTime_IN = nowtime;
					if(nowrange <= range)
						range = nowrange;
				}
				else if(CalculateTime.CalculateTimeGap(nowtime, LatestTime_IN) > COMEIN_TIME){
					OldestTime_IN = nowtime;
					LatestTime_IN = nowtime;
					range = nowrange;
				}	
			}
			else{
				if(flag == true){
					StayTime = CalculateTime.CalculateTimeGap(LatestTime_IN,OldestTime_IN);
					String value_tem = OldestTime_IN+"\t"+LatestTime_IN+"\t"
					+String.valueOf(StayTime)+"\t"+String.valueOf(range);
					context.write(key,new Text(value_tem));	
//					flag = false;
					continue;
				}
				if(LatestTime_OUT == null &&OldestTime_OUT == null)	
					LatestTime_OUT = OldestTime_OUT =nowtime;
				LatestTime_OUT = nowtime;
				if(nowrange <= range)
					range = nowrange;
			}
			
		}
		if(flag == true){
			StayTime = CalculateTime.CalculateTimeGap(LatestTime_IN,OldestTime_IN);
			String value_tem = OldestTime_IN+"\t"+LatestTime_IN+"\t"
			+String.valueOf(StayTime)+"\t"+String.valueOf(range);
			context.write(key,new Text(value_tem));	
		}
		else{
			StayTime = CalculateTime.CalculateTimeGap(LatestTime_OUT,OldestTime_OUT);
			String value_tem = OldestTime_OUT+"\t"+LatestTime_OUT+"\t"
			+String.valueOf(StayTime)+"\t"+String.valueOf(range);
			context.write(key,new Text(value_tem));	
		}	
	}

}
