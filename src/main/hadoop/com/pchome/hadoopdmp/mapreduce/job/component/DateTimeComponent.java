package com.pchome.hadoopdmp.mapreduce.job.component;

import java.text.ParseException;
import java.text.SimpleDateFormat;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.pchome.hadoopdmp.mapreduce.job.factory.DmpLogBean;

public class DateTimeComponent {

	Log log = LogFactory.getLog("DateTimeComponent");

	public DmpLogBean datetimeTransformHour(DmpLogBean dmpDataBean) throws Exception {
		String dateTime = dmpDataBean.getDateTime();
		boolean date = isValidDate(dateTime);
		
		if (!date){
			dmpDataBean.setDateTime("null");
			dmpDataBean.setDateTimeSource("null");
			return dmpDataBean;
		}
		
		String hour = dateTime.split(" ")[1].split(":")[0];
		dmpDataBean.setDateTime(hour);
		dmpDataBean.setDateTimeSource("datetime");
		return dmpDataBean;
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