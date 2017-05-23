package com.pchome.soft.depot.utils;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

import org.springframework.stereotype.Component;

//import com.pchome.pcbapi.api.call.newsfeed.bean.NewsFeedQueryTimeBean;

@Component
public class DateFormatUtil {

	
	public static final String Template = "yyyy-MM-dd";
	public static final String Template2 = "yyyy-MM-dd HH:mm:ss";
	public static final String Template3 = "yyyy-MM-dd HH:mm";
	
	SimpleDateFormat simpleDateFormat = null;

	public SimpleDateFormat getDateTemplate() {
		simpleDateFormat = new SimpleDateFormat(Template);
		return simpleDateFormat;
	}

	public SimpleDateFormat getDateTemplate2() {
		simpleDateFormat = new SimpleDateFormat(Template2);
		return simpleDateFormat;
	}
	
	public long getLongByTemplate2(String date) throws Exception {
		simpleDateFormat = new SimpleDateFormat(Template2);
		Date dateTime = simpleDateFormat.parse(date);
		return dateTime.getTime() / 1000;
	}
	
	public long getLongByTemplate(String date) throws Exception {
		simpleDateFormat = new SimpleDateFormat(Template);
		Date dateTime = simpleDateFormat.parse(date);
		return dateTime.getTime() / 1000;
	}
	
	
	/**
	 * 取得時間傳入時間與相差小時數時間
	 * */
//	public NewsFeedQueryTimeBean getLongTime(Long time,int hour) throws Exception{
//		Calendar calendar = Calendar.getInstance();
//		NewsFeedQueryTimeBean newsFeedQueryTimeBean = new NewsFeedQueryTimeBean();
//		if(time == null){
//			calendar.add(Calendar.HOUR, - hour);
//			long start = calendar.getTimeInMillis();
//			calendar.add(Calendar.HOUR, hour);
//			long end = calendar.getTimeInMillis();
//			newsFeedQueryTimeBean.setUtStartTimestamp(start);
//			newsFeedQueryTimeBean.setUtcEndTimestamp(end);
//		}
//		if(time != null){
//			calendar.setTimeInMillis(time);
//			calendar.add(Calendar.HOUR, -hour);
//			long start = calendar.getTimeInMillis();
//			calendar.add(Calendar.HOUR, hour);
//			long end = calendar.getTimeInMillis();
//			newsFeedQueryTimeBean.setUtStartTimestamp(start);
//			newsFeedQueryTimeBean.setUtcEndTimestamp(end);
//		}
//		return newsFeedQueryTimeBean;
//	}
	
	/**
	 * UTC轉local time
	 * */
	public String utcToLocalDateFormat(long utcTime) throws Exception{
		long localtime = System.currentTimeMillis();
		Date localDate = new Date(localtime);
		DateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		Date date = new Date(utcTime+TimeZone.getDefault().getOffset(localDate.getTime()));
		format.setTimeZone(TimeZone.getTimeZone(TimeZone.getDefault().getID()));
		return format.format(date);
	}
		
	/**
	 * local time 轉UTC
	 * */
	public long getUtcTime() throws Exception{
        Calendar calendar = Calendar.getInstance() ;
        int zoneOffset = calendar.get(java.util.Calendar.ZONE_OFFSET);
        int dstOffset = calendar.get(java.util.Calendar.DST_OFFSET);
        calendar.add(java.util.Calendar.MILLISECOND, - (zoneOffset + dstOffset));
        return calendar.getTimeInMillis();
	}
	
	/**
	 * 時間加上分鐘
	 * */
	public long getTimeAddMinute(long time, int Minute) throws Exception{
		 Calendar calendar = Calendar.getInstance();
		 Date date =  new Date(time);
		 calendar.setTime(date);
		 calendar.add(Calendar.MINUTE, + Minute);
		 return calendar.getTimeInMillis();
	}
	
	/**
	 * 檢查日期格式
	 * */
	@SuppressWarnings("unused")
	public Boolean checkDateFormat(String date) {
		try {
			SimpleDateFormat dFormat = new SimpleDateFormat("yyyy-MM-dd");
			dFormat.setLenient(false);
			Date formatDate = dFormat.parse(date);
			return true;
		} catch (ParseException e) {
			return false;
		}
	}
	
	/**
	 * 取得目前日期時間後格式化
	 * */
	public String getCurrentDateTimeTemplate3() {
		SimpleDateFormat sdFormat = new SimpleDateFormat(Template3);
		Date date = new Date();
		String strDate = sdFormat.format(date);
		return strDate;
	}
}
