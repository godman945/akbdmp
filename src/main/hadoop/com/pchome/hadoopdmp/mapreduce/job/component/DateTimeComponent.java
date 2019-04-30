package com.pchome.hadoopdmp.mapreduce.job.component;

import java.text.ParseException;
import java.text.SimpleDateFormat;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.InputSplit;

import com.pchome.hadoopdmp.mapreduce.job.factory.DmpLogBean;

public class DateTimeComponent {

	Log log = LogFactory.getLog("DateTimeComponent");
	
	public net.minidev.json.JSONObject datetimeTransformHour(net.minidev.json.JSONObject dmpJSon) throws Exception {
		
		
		
		
		return dmpJSon;
		
		
//		String dateTime = dmpDataBean.getDateTime();
//		boolean date = isValidDate(dateTime);
//		
//		if (!date){
//			dmpDataBean.setHour("null");
//			dmpDataBean.setTimeInfoSource("null");
//			dmpDataBean.setTimeInfoClassify("N");
//			return dmpDataBean;
//		}
//		
//		String hour = dateTime.split(" ")[1].split(":")[0];
//		dmpDataBean.setHour(hour);
//		dmpDataBean.setTimeInfoSource("datetime");
//		dmpDataBean.setTimeInfoClassify("Y");
//		return dmpDataBean;
	}
	
	
	public static boolean isValidDate(String str) {
		boolean convertSuccess = true;
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		try {
			format.setLenient(false);
			format.parse(str);
		} catch (ParseException e) {
			convertSuccess = false;
		}
		return convertSuccess;
	}

}