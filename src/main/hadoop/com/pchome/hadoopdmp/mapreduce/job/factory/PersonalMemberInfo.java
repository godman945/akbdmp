//package com.pchome.hadoopdmp.mapreduce.job.factory;
//
//import java.io.BufferedReader;
//import java.io.InputStreamReader;
//import java.net.URL;
//import java.net.URLConnection;
//import java.text.SimpleDateFormat;
//import java.util.Calendar;
//import java.util.Date;
//import java.util.Map;
//
//import com.jayway.jsonpath.JsonPath;
//
//public class PersonalMemberInfo extends APersonalInfo {
//
//	SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
//	
//	public Object personalData(Map<String, Object> map) throws Exception {
//		StringBuffer url = new StringBuffer();
//		url.append("http://member.pchome.com.tw/findMemberInfo4ADAPI.html?ad_user_id=");
//		url.append(map.get("memid"));
//		String prsnlData = httpGet(url.toString());
//		
//		String sex = JsonPath.parse(prsnlData).read("sexuality");
//		String age = String.valueOf(getAge(sdf.parse(JsonPath.parse(prsnlData).read("birthday").toString())));
//		map.put("sex", sex);
//		map.put("age", age);
//		return map;
//	}
//
//	public String httpGet(String myURL) {
//		StringBuilder sb = new StringBuilder();
//		URLConnection urlConn = null;
//		InputStreamReader in = null;
//		try {
//			URL url = new URL(myURL);
//			urlConn = url.openConnection();
//			if (urlConn != null)
//				urlConn.setReadTimeout(60 * 1000);
//			if (urlConn != null && urlConn.getInputStream() != null) {
//				in = new InputStreamReader(urlConn.getInputStream(), "UTF-8");
//				BufferedReader bufferedReader = new BufferedReader(in);
//				if (bufferedReader != null) {
//					int cp;
//					while ((cp = bufferedReader.read()) != -1) {
//						sb.append((char) cp);
//					}
//					bufferedReader.close();
//				}
//			}
//			in.close();
//		} catch (Exception e) {
//			// log.error(e.getMessage());
//		}
//
//		return sb.toString();
//	}
//
//	
//	
//	public int getAge(Date birthDay) {
//		Calendar cal = Calendar.getInstance();
//
//		if (cal.before(birthDay)) {
//			return -1;
//		}
//
//		int yearNow = cal.get(Calendar.YEAR);
//		int monthNow = cal.get(Calendar.MONTH)+1;
//		int dayOfMonthNow = cal.get(Calendar.DAY_OF_MONTH);
//
//		cal.setTime(birthDay);
//		int yearBirth = cal.get(Calendar.YEAR);
//		int monthBirth = cal.get(Calendar.MONTH)+1;
//		int dayOfMonthBirth = cal.get(Calendar.DAY_OF_MONTH);
//
//		int age = yearNow - yearBirth;
//
//		if (monthNow <= monthBirth) {
//			if (monthNow == monthBirth) {
//				if (dayOfMonthNow < dayOfMonthBirth) {
//					age--;
//				}
//			} else {
//				age--;
//			}
//		}
//
//		return age;
//	}
//}