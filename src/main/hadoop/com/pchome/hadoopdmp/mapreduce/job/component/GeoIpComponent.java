package com.pchome.hadoopdmp.mapreduce.job.component;

import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import com.maxmind.geoip2.model.CityResponse;
import com.pchome.hadoopdmp.mapreduce.job.component.IpAddress.IpAdd;
import com.pchome.hadoopdmp.mapreduce.job.dmplog.DmpLogMapper;

public class GeoIpComponent {

	Log log = LogFactory.getLog("GeoIpComponent");
	private static String ip = ""; 
	private static IpAdd ipAdd = new IpAdd();
	private static Map<String,CityResponse> ipMap = new HashMap<String,CityResponse>();
	
	public net.minidev.json.JSONObject ipTransformGEO(net.minidev.json.JSONObject dmpJSon) throws Exception {
		// 判斷是否為正確ip格式
		ip = dmpJSon.getAsString("ip").toString();
		if(ipMap.containsKey(ip)) {
			CityResponse cityResponse = ipMap.get(ip);
			if(cityResponse == null) {
				dmpJSon.put("area_info_classify", "");
				return dmpJSon;
			}else {
				String countryStr = cityResponse.getCountry().getName();
				String cityStr = cityResponse.getCity().getNames().get("en");
				dmpJSon.put("area_country", countryStr);
				dmpJSon.put("area_city", cityStr);
			}
		}else {
			if (!ipAdd.isIP(ip)) {
				dmpJSon.put("area_info_classify", "");
				ipMap.put(dmpJSon.getAsString("ip").toString(), null);
				return dmpJSon;
			}
			// ip轉換國家、城市
			InetAddress ipAddress = InetAddress.getByName(ip);
			try {
				CityResponse cityResponse = DmpLogMapper.reader.city(ipAddress);
				String countryStr = cityResponse.getCountry().getName();
				String cityStr = cityResponse.getCity().getNames().get("en");
				dmpJSon.put("area_country", countryStr);
				dmpJSon.put("area_city", cityStr);
				ipMap.put(dmpJSon.getAsString("ip").toString(), cityResponse);
			} catch (Exception e) {
				ipMap.put(dmpJSon.getAsString("ip").toString(), null);
				System.out.println("The address is not in the database:" + ip);
				throw e;
			}
			
		}
		return dmpJSon;
	}
}