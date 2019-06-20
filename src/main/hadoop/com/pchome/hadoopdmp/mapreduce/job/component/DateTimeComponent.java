package com.pchome.hadoopdmp.mapreduce.job.component;

import java.text.ParseException;
import java.text.SimpleDateFormat;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.InputSplit;

import com.pchome.hadoopdmp.mapreduce.job.dmplog.DmpLogMapper;
import com.pchome.hadoopdmp.mapreduce.job.factory.DmpLogBean;

public class DateTimeComponent {

	Log log = LogFactory.getLog("DateTimeComponent");
	
	public net.minidev.json.JSONObject datetimeTransformHour(net.minidev.json.JSONObject dmpJSon) throws Exception {
		dmpJSon.put("time_info_source", DmpLogMapper.record_hour);
		dmpJSon.put("time_info_classify", "Y");
		return dmpJSon;
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