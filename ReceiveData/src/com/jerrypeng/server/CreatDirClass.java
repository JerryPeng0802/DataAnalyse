package com.jerrypeng.server;

import java.io.File;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

public class CreatDirClass {
	private File dir;
	
	public File CreatDir(Date date){
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMddHH");
		String now=simpleDateFormat.format(date);
		String year = now.substring(0,4);
		String month = now.substring(4,6);
		String day = now.substring(6,8);
		String hour = now.substring(8,10);
		dir =new File("/home/jerrypeng/"+year+"/"+month+"/"+day);
		if(!dir.exists())
			dir.mkdirs();
		File file = new File(dir,hour+".txt");
		return file;
	}
	public File CreatDir(String date){
		SimpleDateFormat format = new SimpleDateFormat("EEE MMM dd kk:mm:ss yyyy",Locale.ENGLISH);
		Date inputtime = new Date();
		try {
			inputtime = format.parse(date);
			
		} catch (ParseException e) {
			e.printStackTrace();
		}
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMddHH");
		String now=simpleDateFormat.format(inputtime);
		String year = now.substring(0,4);
		String month = now.substring(4,6);
		String day = now.substring(6,8);
		String hour = now.substring(8,10);
		dir =new File("/home/Data/"+year+"/"+month+"/"+day);
		if(!dir.exists())
			dir.mkdirs();
		File file = new File(dir,hour+".txt");
		return file;
	}
}
