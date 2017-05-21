package com.jerypeng.data;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class DataAnalyseReducer_2 extends Reducer<Text, Text, Text, Text> {
	
	public static final double COMEIN_RANGE = 10;
	//public static final double MAX_RANGE = 1000;
	public static final long COMEIN_SHORT = 2;
	public static final long COMEIN_LONG = 20;

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		// process values
		int AllCome = 0;
		int ComeIn = 0;
		int ComeInShort = 0;
		int ComeInLong = 0;
		double ComeInRate = 0.0;
		double ComeInShortRate = 0.0;
		double ComeInLongRate = 0.0;
		for (Text value : values) {
			String stringvalue = value.toString();
			String data [] = stringvalue.split("\t");
			double nowrange = Double.parseDouble(data[4]);
			long StayTime = Long.parseLong(data[3]);
			
			AllCome++;
			if(nowrange <= COMEIN_RANGE){
				ComeIn++;
				if(StayTime <= COMEIN_SHORT)
					ComeInShort++;
				if(StayTime >= COMEIN_LONG)
					ComeInLong++;	
			}
		}
		
		ComeInRate = ComeIn*1.0/AllCome;
		ComeInShortRate = ComeInShort*1.0/ComeIn;
		ComeInLongRate = ComeInLong*1.0/ComeIn;
		//入店量+入店率+跳出率+深访率
		String outvalue = String.valueOf(AllCome)+"\t"+String.valueOf(ComeIn)+"\t"+
		String.valueOf(ComeInRate)+"\t"+String.valueOf(ComeInShortRate)+"\t"+
		String.valueOf(ComeInLongRate);
		
		context.write(key, new Text(outvalue));

	}

}
