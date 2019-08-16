package com.pchome.hadoopdmp.mapreduce.job.component;

import java.net.InetAddress;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.maxmind.geoip2.model.CityResponse;
import com.pchome.hadoopdmp.mapreduce.job.component.IpAddress.IpAdd;
import com.pchome.hadoopdmp.mapreduce.job.dmplog.DmpLogMapper;

public class GeoIpComponent {

	Log log = LogFactory.getLog("GeoIpComponent");
	private static String ip = ""; 
	private static IpAdd ipAdd = new IpAdd();
	private static CityResponse response = null;
	public net.minidev.json.JSONObject ipTransformGEO(net.minidev.json.JSONObject dmpJSon_997) throws Exception {
		// 判斷是否為正確ip格式
		ip = dmpJSon_997.getAsString("ip");
		if (!ipAdd.isIP(ip)) {
			dmpJSon_997.put("area_info_classify", "null");
			return dmpJSon_997;
		}
		// ip轉換國家、城市
		InetAddress ipAddress = DmpLogMapper.ipAddress.getByName(ip);
		try {
			response = DmpLogMapper.reader.city(ipAddress);
		} catch (Exception e) {
			System.out.println("The address is not in the database:" + ip);
			throw e;
		}
		if (response == null) {
			dmpJSon_997.put("area_info_classify", "N");
			return dmpJSon_997;
		}
		String countryStr = response.getCountry().getName();
		String cityStr = response.getCity().getNames().get("en");
		dmpJSon_997.put("area_country", countryStr);
		dmpJSon_997.put("area_city", cityStr);
		return dmpJSon_997;
	}
}