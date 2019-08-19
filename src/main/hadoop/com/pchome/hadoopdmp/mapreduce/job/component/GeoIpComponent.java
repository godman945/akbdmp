package com.pchome.hadoopdmp.mapreduce.job.component;

import java.net.InetAddress;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.maxmind.geoip2.model.CityResponse;
import com.pchome.hadoopdmp.mapreduce.job.component.IpAddress.IpAdd;
import com.pchome.hadoopdmp.mapreduce.job.dmplog.DmpLogMapper;

import net.minidev.json.JSONObject;

public class GeoIpComponent {

	Log log = LogFactory.getLog("GeoIpComponent");
	private static String ip = ""; 
	private static IpAdd ipAdd = new IpAdd();
	private static CityResponse response = null;
	public JSONObject ipTransformGEO(JSONObject dmpJSon_997) throws Exception {
		System.out.println("11111111111----:"+dmpJSon_997);
		// 判斷是否為正確ip格式
		System.out.println("ZZZZZZZZZZZZZz000000");
		System.out.println(dmpJSon_997.get("trigger_type"));
		System.out.println(dmpJSon_997.get("ip"));
		ip = dmpJSon_997.getAsString("ip");
		System.out.println("ZZZZZZZZZZZZZz");
		if (!ipAdd.isIP(ip)) {
			System.out.println("area_info_classify ><><><>< null");
			dmpJSon_997.put("area_info_classify", "");
			return dmpJSon_997;
		}
		System.out.println("2222222222222");
		// ip轉換國家、城市
		InetAddress ipAddress = DmpLogMapper.ipAddress.getByName(ip);
		try {
			response = DmpLogMapper.reader.city(ipAddress);
		} catch (Exception e) {
			System.out.println("The address is not in the database:" + ip);
			throw e;
		}
		System.out.println("33333333333");
		if (response == null) {
			dmpJSon_997.put("area_info_classify", "N");
			return dmpJSon_997;
		}
		String countryStr = response.getCountry().getName();
		String cityStr = response.getCity().getNames().get("en");
		dmpJSon_997.put("area_country", countryStr);
		dmpJSon_997.put("area_city", cityStr);
		System.out.println("44444444444");
		return dmpJSon_997;
	}
}