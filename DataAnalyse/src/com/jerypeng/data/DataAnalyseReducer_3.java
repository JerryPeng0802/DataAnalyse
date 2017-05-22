package com.jerypeng.data;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class DataAnalyseReducer_3 extends Reducer<Text, Text, Text, Text> {

	public static final double COMEIN_RANGE = 10;
	public static final long NEWOLDCUSTOMER = 10;//10day
//	public static final long LOWACTIVITY = 6;	//6day
	public static final long MEDIUMACTIVITY = 4;
	public static final long HIGHACTIVITY = 2;
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		// process values
		
		long CycleTime = 0;
		String Ativity = null;  		//0--low  1--medium   2--high
		String NewOldCustomer = null;  //0 present new customer
		String LastTime = "0";
		String NowTime = "0";
		String StayTime = null;
		double range =0.0;
		ArrayList <String> array = new ArrayList<String>();
		for(Text value : values){
			array.add(value.toString());
		}
		Collections.sort(array);
		Iterator<String> it = array.iterator();
		while(it.hasNext()){
			String data [] = it.next().split("\t");
			String now = data[0];
			StayTime = data[2];
			double nowrange = Double.parseDouble(data[3]);
			//if(nowrange <= COMEIN_RANGE){
				if(LastTime == null && NowTime == null)
					LastTime = NowTime = now;
				LastTime = NowTime;
				NowTime = now;
				range = nowrange;
			//}
		}
		if(range <= COMEIN_RANGE){
			CycleTime = CalculateTime.CalculateTimeGapByDay(NowTime , LastTime);
			if(CycleTime > NEWOLDCUSTOMER )
				NewOldCustomer = "0";
			else NewOldCustomer = "1";
			if(CycleTime <= HIGHACTIVITY )
				Ativity = "2";
			else if(CycleTime <= MEDIUMACTIVITY)
				Ativity = "1";
			else Ativity = "0";
			
			String value_1 = String.valueOf(CycleTime)+"\t"+NewOldCustomer+"\t"+
			Ativity+"\t"+StayTime;
			
			context.write(key, new Text(value_1));
		}
		
	}

}
