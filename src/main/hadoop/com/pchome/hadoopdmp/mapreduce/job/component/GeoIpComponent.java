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
	public net.minidev.json.JSONObject ipTransformGEO(net.minidev.json.JSONObject dmpJSon) throws Exception {
		// 判斷是否為正確ip格式
		
		log.info(">>>>>>>>>>>>>>>>>>>>>"+dmpJSon.getClass());
		
		ip = dmpJSon.getAsString("ip").toString();
		if (!ipAdd.isIP(ip)) {
			dmpJSon.put("area_info_classify", "");
			return dmpJSon;
		}
		// ip轉換國家、城市
		InetAddress ipAddress = InetAddress.getByName(ip);
		try {
			response = DmpLogMapper.databaseReader.city(ipAddress);
		} catch (Exception e) {
//			System.out.println("The address is not in the database:" + ip);
			throw e;
		}
		if (response == null) {
			dmpJSon.put("area_info_classify", "N");
			return dmpJSon;
		}
		String countryStr = response.getCountry().getName();
		String cityStr = response.getCity().getNames().get("en");
		dmpJSon.put("area_country", countryStr);
		dmpJSon.put("area_city", cityStr);
		return dmpJSon;
	}
}