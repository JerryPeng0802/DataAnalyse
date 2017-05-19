package com.jerypeng.data;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

public class CalculateTime {
	
//	private Date LatestTime;
//	private Date OldestTime;
	//return minutes			date1-date2
	public static long CalculateTimeGap(Date date1,Date date2){
		return((date1.getTime()-date2.getTime())/(1000*60));
	}
	
	public static long CalculateTimeGap(String date1,String date2){
		long minutes = 0;
		SimpleDateFormat format = new SimpleDateFormat("EEE MMM dd kk:mm:ss YYYY",Locale.ENGLISH);
		try {
			minutes = (format.parse(date1).getTime()-format.parse(date2).getTime())/(1000*60);
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return minutes;
	}

}
