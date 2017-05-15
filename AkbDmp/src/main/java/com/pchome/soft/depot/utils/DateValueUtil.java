package com.pchome.soft.depot.utils;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.LinkedHashMap;

import org.springframework.stereotype.Component;

@Component
public class DateValueUtil  {

	private static DateValueUtil dateValueUtil=new DateValueUtil();

	public static final int YESTERDAY = -1;
	public static final int TOMORROW = 1;
	public static final int TODAY = 0;
	public static final String DBPATH ="dbpath";
	public static final String DATETEMPLATE0 ="YYYYMMdd";
	
	
	private Calendar cal=null;

	private  DateValueUtil(){
		cal=Calendar.getInstance();
	}

	public void reloadCalender(){
		cal=Calendar.getInstance();
	}

	public static DateValueUtil getInstance(){
		return dateValueUtil;
	}

	public void setDay(int dayFlag){
		cal.add(Calendar.DATE, dayFlag);
	}

	public String getDateHour(){
		reloadCalender();

		String str=String.valueOf(cal.get(Calendar.HOUR_OF_DAY));
		if(str.length()==1){
			str="0"+str;
		}


		return str;
	}

	public String getDateMinute(){
		reloadCalender();

		String str=String.valueOf(cal.get(Calendar.MINUTE));
		if(str.length()==1){
			str="0"+str;
		}
		return str;	
	}

	public String getDateYear(){
		reloadCalender();
		return String.valueOf(cal.get(Calendar.YEAR));
	}

	public String getDateMonth(){
		reloadCalender();
		return String.valueOf(cal.get(Calendar.MONTH)+1);
	}

	public String getDateDay(){
		reloadCalender();
		return String.valueOf(cal.get(Calendar.DAY_OF_MONTH));
	}


	public String getDateWeek(){
		reloadCalender();
		return String.valueOf(cal.get(Calendar.DAY_OF_WEEK));
	}

	public String getPreWeekFirstDate(){

		int d=Integer.parseInt(getDateWeek())+6;
		return DateValueUtil.getInstance().dateToString(DateValueUtil.getInstance().getDateForStartDateAddDay(DateValueUtil.getInstance().getDateValue(DateValueUtil.TODAY, DateValueUtil.DBPATH),-d));

	}

	public String getPreWeekLastDate(){
		int d=Integer.parseInt(getDateWeek());
		return DateValueUtil.getInstance().dateToString(DateValueUtil.getInstance().getDateForStartDateAddDay(DateValueUtil.getInstance().getDateValue(DateValueUtil.TODAY, DateValueUtil.DBPATH),-d));

	}



	public String getDateMonthO(){
		reloadCalender();
		int m=cal.get(Calendar.MONTH)+1;
		String mm="";
		if(m<10) mm = "0"+m; else mm = m+"";
		return mm;
	}

	public String getDateDayO(){
		reloadCalender();
		int m=cal.get(Calendar.DAY_OF_MONTH);
		String mm="";
		if(m<10) mm = "0"+m; else mm = m+"";
		return mm;
	}

	public String getDateAdPath(){
		reloadCalender();
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy/M/d");
		String data=sdf.format(cal.getTime());
		return data;
	}

	public String getDateValue(int dayFlag,String dateType){
		//dayFlag= -1  昨天 , 0 今天, 1 明天
		reloadCalender();

		cal.add(Calendar.DATE, dayFlag);

		String data="";

		if(dateType.equals("year")){
			data=String.valueOf(cal.get(Calendar.YEAR));
		}

		if(dateType.equals("month")){
			data=String.valueOf(cal.get(Calendar.MONTH)+1);
		}

		if(dateType.equals("day")){
			data=String.valueOf(cal.get(Calendar.DAY_OF_MONTH));
		}

		if(dateType.equals("month0")){
			int m=cal.get(Calendar.MONTH)+1;
			String mm="";
			if(m<10) mm = "0"+m; else mm = m+"";
			data=mm;
		}

		if(dateType.equals("day0")){
			int m=cal.get(Calendar.DAY_OF_MONTH);
			String mm="";
			if(m<10) mm = "0"+m; else mm = m+"";
			data=mm;
		}

		if(dateType.equals("dbpath")){

			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
			data=sdf.format(cal.getTime());

		}

		if(dateType.equals("")){

			SimpleDateFormat sdf = new SimpleDateFormat("yyyy/M/d");
			data=sdf.format(cal.getTime());

		}

		return data;

	}

	//字串轉日期
	public Date stringToDate(String dateStr){
		DateFormat format1 = new SimpleDateFormat("yyyy-MM-dd");  
		Date returnDate=null;
		try {
			returnDate=format1.parse(dateStr);
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 

		return  returnDate;  
	}

	//日期轉字串
	public String dateToString(Date date){

		DateFormat format1 = new SimpleDateFormat("yyyy-MM-dd");  
		String str=format1.format(date);




		return  str;  
	}


	//兩日期相差的天數
	public long getDateDiffDay(String startDate,String endDate){

		long diffDate=0;

		Date s = stringToDate(startDate);
		Date e = stringToDate(endDate);

		diffDate   =   (e.getTime() - s.getTime())   /   (3600   *   24   *   1000); 

		return diffDate + 1;

	}


	//取得指定的日期加上天數後的日期
	public Date getDateForStartDateAddDay(String startDate,int day ){

		Calendar aday=Calendar.getInstance();
		aday.setTime(DateValueUtil.getInstance().stringToDate(startDate));
		aday.add(Calendar.DATE, day);
		String str=dateToString(aday.getTime());

		return stringToDate(str);
	}

	//open flash chart x軸日期格式 01/12
	public String getChartXlabelDate(Date pDate){

		String result="";
		Calendar aday=Calendar.getInstance();
		aday.setTime(pDate);



		int m=aday.get(Calendar.MONTH)+1;
		String mm="";
		if(m<10) mm = "0"+m; else mm = m+"";


		int d=aday.get(Calendar.DAY_OF_MONTH);
		String dd="";
		if(d<10) dd = "0"+d; else dd = d+"";

		result=dd+"/"+mm;

		return result;

	}




	public String getThisMonthFirstDate(){
		GregorianCalendar calendar=new GregorianCalendar();
		int  dayOfMonth = calendar.get(GregorianCalendar.DATE);
		calendar.add(GregorianCalendar.DATE,  - dayOfMonth + 1 );
		String  begin = new  java.sql.Date(calendar.getTime().getTime()).toString();

		return begin;
	}

	public String getPreMonthFirstDate(){
		GregorianCalendar calendar=new GregorianCalendar();
		calendar.set(calendar.get(GregorianCalendar.YEAR),calendar.get(GregorianCalendar.MONTH), 1 );
		calendar.add(GregorianCalendar.DATE,  - 1 );
		//String end = new  java.sql.Date(calendar.getTime().getTime()).toString();

		int  month = calendar.get(GregorianCalendar.MONTH) + 1 ;
		String mm="";
		if(month<10) mm = "0"+month; else mm = month+"";
		String begin = calendar.get(GregorianCalendar.YEAR) + "-" + mm + "-01" ;

		return begin;
	}

	public String getPre3MonthFirstDate(){

		int m=Integer.parseInt(getDateMonth());
		int y=Integer.parseInt(getDateYear());

		if(m >=1 && m <=3){
			m=10;
			y=y-1;
		}

		if(m >=4 && m <=6){
			m=1;
		}

		if(m >=7 && m <=9){
			m=4;
		}

		if(m >=10 && m <=12){
			m=7;
		}

		String mm="";
		if(m<10) mm = "0"+m; else mm = m+"";


		String begin=String.valueOf(y)+"-"+mm+"-"+"01";




		return begin;
	}

	public String getPre6MonthFirstDate(){

		int m=Integer.parseInt(getDateMonth());
		int y=Integer.parseInt(getDateYear());

		int lm=m-6;
		if(lm<=0){
			m=12-java.lang.Math.abs(lm);
			y=y-1;
		}else{
			m=lm;
		}



		String mm="";
		if(m<10) mm = "0"+m; else mm = m+"";


		String begin=String.valueOf(y)+"-"+mm+"-"+"01";




		return begin;
	}


	public String getPre3MonthLastDate(){

		int m=Integer.parseInt(getDateMonth());
		int y=Integer.parseInt(getDateYear());

		if(m >=1 && m <=3){
			m=12;
			y=y-1;
		}

		if(m >=4 && m <=6){
			m=3;
		}

		if(m >=7 && m <=9){
			m=6;
		}

		if(m >=10 && m <=12){
			m=9;
		}

		String mm="";
		if(m<10) mm = "0"+m; else mm = m+"";


		Calendar c   = new GregorianCalendar(y,m,0);



		int d=c.getActualMaximum(Calendar.DATE);


		String dd="";
		if(d<10) dd = "0"+d; else dd = d+"";

		String begin=String.valueOf(y)+"-"+mm+"-"+dd;




		return begin;
	}



	public String getPreMonthLastDate(){
		GregorianCalendar calendar=new GregorianCalendar();
		calendar.set(calendar.get(GregorianCalendar.YEAR),calendar.get(GregorianCalendar.MONTH), 1 );
		calendar.add(GregorianCalendar.DATE,  - 1 );
		String end = new  java.sql.Date(calendar.getTime().getTime()).toString();

		return end;
	}




	public LinkedHashMap<String,String> getDateRangeMap(){

		LinkedHashMap<String,String> rangeMap=new LinkedHashMap<String,String>();
		String showString="";
		String dateString="";
		//1自訂
		rangeMap.put("自定區間","self");

		//2今天
		showString="今天("+getDateValue(DateValueUtil.TODAY,DateValueUtil.DBPATH)+" 到 "+getDateValue(DateValueUtil.TODAY,DateValueUtil.DBPATH)+")";
		dateString=getDateValue(DateValueUtil.TODAY,DateValueUtil.DBPATH)+","+getDateValue(DateValueUtil.TODAY,DateValueUtil.DBPATH);
		rangeMap.put(showString,dateString);

		//3昨天
		showString="昨天("+getDateValue(DateValueUtil.YESTERDAY,DateValueUtil.DBPATH)+" 到 "+getDateValue(DateValueUtil.YESTERDAY,DateValueUtil.DBPATH)+")";
		dateString=getDateValue(DateValueUtil.YESTERDAY,DateValueUtil.DBPATH)+","+getDateValue(DateValueUtil.YESTERDAY,DateValueUtil.DBPATH);
		rangeMap.put(showString,dateString);

		//3上週
		showString="上週("+getPreWeekFirstDate()+" 到 "+getPreWeekLastDate()+")";
		dateString=getPreWeekFirstDate()+","+getPreWeekLastDate();
		rangeMap.put(showString,dateString);

		//4上月
		showString="上月("+getPreMonthFirstDate()+" 到 "+getPreMonthLastDate()+")";
		dateString=getPreMonthFirstDate()+","+getPreMonthLastDate();
		rangeMap.put(showString,dateString);


		//5本月至今
		showString="本月至今("+getThisMonthFirstDate()+" 到 "+getDateValue(DateValueUtil.TODAY,DateValueUtil.DBPATH)+")";
		dateString=getThisMonthFirstDate()+","+getDateValue(DateValueUtil.TODAY,DateValueUtil.DBPATH);
		rangeMap.put(showString,dateString);

		//6上一季
		showString="上一季("+getPre3MonthFirstDate()+" 到 "+getPre3MonthLastDate()+")";
		dateString=getPre3MonthFirstDate()+","+getPre3MonthLastDate();
		rangeMap.put(showString,dateString);

		//7過去7天
		showString="過去7天("+dateToString(getDateForStartDateAddDay(getDateValue(DateValueUtil.TODAY,DateValueUtil.DBPATH),-7))+" 到 "+getDateValue(DateValueUtil.TODAY,DateValueUtil.DBPATH)+")";
		dateString=dateToString(getDateForStartDateAddDay(getDateValue(DateValueUtil.TODAY,DateValueUtil.DBPATH),-7))+","+getDateValue(DateValueUtil.TODAY,DateValueUtil.DBPATH);
		rangeMap.put(showString,dateString);


		//6過去30天
		showString="過去30天("+dateToString(getDateForStartDateAddDay(getDateValue(DateValueUtil.TODAY,DateValueUtil.DBPATH),-30))+" 到 "+getDateValue(DateValueUtil.TODAY,DateValueUtil.DBPATH)+")";
		dateString=dateToString(getDateForStartDateAddDay(getDateValue(DateValueUtil.TODAY,DateValueUtil.DBPATH),-30))+","+getDateValue(DateValueUtil.TODAY,DateValueUtil.DBPATH);
		rangeMap.put(showString,dateString);



		return rangeMap;



	}


	
	//取得日期格式 YYYYMMdd
	public String getFateFormatTemplate0() throws ParseException{
		reloadCalender();
		DateFormat format = new SimpleDateFormat(DateValueUtil.DATETEMPLATE0);
		return format.format(Calendar.getInstance().getTime());
	}

	public static void main(String avg[]){



//		LinkedHashMap<String,String> test=DateValueUtil.getInstance().getDateRangeMap();
//
//		for(String s:test.keySet()){
//			System.out.println(s);
//			System.out.println(test.get(s));
//		}
//
//		int a=(Integer.parseInt(DateValueUtil.getInstance().getDateWeek()));

		//int a=-9;

//		System.out.println("6m="+DateValueUtil.getInstance().getPre6MonthFirstDate());
//		System.out.println(DateValueUtil.getInstance().getPreMonthFirstDate());
		//System.out.println(DateValueUtil.getInstance().dateToString(DateValueUtil.getInstance().getDateForStartDateAddDay(DateValueUtil.getInstance().getDateValue(DateValueUtil.TODAY, DateValueUtil.DBPATH),-a)));

		//System.out.println(DateValueUtil.getInstance().getDateWeek());
		//System.out.println(DateValueUtil.getInstance().getDateDiffDay("2010-01-01","2010-12-31" ));

		//Calendar test=Calendar.getInstance();
		//test.setTime(DateValueUtil.getInstance().stringToDate("2010-08-01"));

		//for(int i=0;i<10;i++){
		//test.setTime(DateValueUtil.getInstance().stringToDate("2010-08-01"));
		//test.add(Calendar.DATE, i);
		//test.getTime();
		//System.out.println(DateValueUtil.getInstance().dateToString(test.getTime()));
		//}


	}


}
