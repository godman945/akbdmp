package com.pchome.hadoopdmp.mapreduce.job.component;

import java.net.InetAddress;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.maxmind.geoip2.model.CityResponse;
import com.pchome.hadoopdmp.mapreduce.job.component.IpAddress.IpAdd;
import com.pchome.hadoopdmp.mapreduce.job.dmplog.DmpLogMapper;
import com.pchome.hadoopdmp.mapreduce.job.factory.DmpLogBean;

public class GeoIpComponent {

	Log log = LogFactory.getLog("GeoIpComponent");
	private static String ip = ""; 
	private static IpAdd ipAdd = new IpAdd();
	private static CityResponse response = null;
	public net.minidev.json.JSONObject ipTransformGEO(net.minidev.json.JSONObject dmpJSon) throws Exception {
		response = null;
		// 判斷是否為正確ip格式
		ip = dmpJSon.getAsString("ip");
		if (!ipAdd.isIP(ip)) {
			dmpJSon.put("country", "null");
			dmpJSon.put("city", "null");
			dmpJSon.put("area_info_source", "null");
			dmpJSon.put("area_info_classify", "null");
			return dmpJSon;
		}
		// ip轉換國家、城市
		InetAddress ipAddress = DmpLogMapper.ipAddress.getByName(ip);
		try {
			response = DmpLogMapper.reader.city(ipAddress);
		} catch (Exception e) {
			log.info("The address is not in the database:"+ip);
		}
		if (response == null) {
			dmpJSon.put("country", "null");
			dmpJSon.put("city", "null");
			dmpJSon.put("area_info_source", "null");
			dmpJSon.put("area_info_classify", "N");
			return dmpJSon;
		}
		String countryStr = response.getCountry().getName();
		String cityStr = response.getCity().getNames().get("en");
		dmpJSon.put("country", countryStr);
		dmpJSon.put("city", cityStr);
		dmpJSon.put("area_info_source", "ip");
		if( (StringUtils.isNotBlank(countryStr)) && (!StringUtils.equals(countryStr, "null")) && (StringUtils.isNotBlank(cityStr)) && (!StringUtils.equals(cityStr, "null")) ){
			dmpJSon.put("area_info_classify", "Y");
		}else{
			dmpJSon.put("area_info_classify", "N");
		}
		return dmpJSon;
		
//		String ip = dmpDataBean.getIp();
//		String countryStr = "null";
//		String cityStr = "null";
//
//		// 判斷是否為正確ip格式
//		IpAdd ipAdd = new IpAdd();
//		if (!ipAdd.isIP(ip)) {
//			dmpDataBean.setCountry("null");
//			dmpDataBean.setCity("null");
//			dmpDataBean.setAreaInfoSource("null");
//			dmpDataBean.setAreaInfoClassify("null");
//			return dmpDataBean;
//		}
//
//		// ip轉換國家、城市
//		CityResponse response = null;
//		InetAddress ipAddress = DmpLogMapper.ipAddress.getByName(ip);
//		try {
//			response = DmpLogMapper.reader.city(ipAddress);
//		} catch (Exception e) {
//			System.out.println("The address is not in the database");
//		}
//
//		if (response == null) {
//			dmpDataBean.setCountry("null");
//			dmpDataBean.setCity("null");
//			dmpDataBean.setAreaInfoSource("null");
//			dmpDataBean.setAreaInfoClassify("N");
//			return dmpDataBean;
//		}
//
//		countryStr = response.getCountry().getName();
//		cityStr = response.getCity().getNames().get("en");
//
//		dmpDataBean.setCountry(countryStr);
//		dmpDataBean.setCity(cityStr);
//		dmpDataBean.setAreaInfoSource("ip");
//		
//		if( (StringUtils.isNotBlank(countryStr)) && (!StringUtils.equals(countryStr, "null"))
//			&& (StringUtils.isNotBlank(cityStr)) && (!StringUtils.equals(cityStr, "null")) ){
//			dmpDataBean.setAreaInfoClassify("Y");
//		}else{
//			dmpDataBean.setAreaInfoClassify("N");
//		}
//
//		return dmpDataBean;
	}
}