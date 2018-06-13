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

	public DmpLogBean ipTransformGEO(DmpLogBean dmpDataBean) throws Exception {
		String ip = dmpDataBean.getIp();
		String countryStr = "null";
		String cityStr = "null";

		// 判斷是否為正確ip格式
		IpAdd ipAdd = new IpAdd();
		if (!ipAdd.isIP(ip)) {
			dmpDataBean.setCountry("null");
			dmpDataBean.setCity("null");
			dmpDataBean.setAreaInfoSource("null");
			dmpDataBean.setAreaInfoClassify("null");
			return dmpDataBean;
		}

		// ip轉換國家、城市
		CityResponse response = null;
		InetAddress ipAddress = DmpLogMapper.ipAddress.getByName(ip);
		try {
			response = DmpLogMapper.reader.city(ipAddress);
		} catch (Exception e) {
			System.out.println("The address is not in the database");
		}

		if (response == null) {
			dmpDataBean.setCountry("null");
			dmpDataBean.setCity("null");
			dmpDataBean.setAreaInfoSource("null");
			dmpDataBean.setAreaInfoClassify("N");
			return dmpDataBean;
		}

		countryStr = response.getCountry().getName();
		cityStr = response.getCity().getNames().get("en");

		dmpDataBean.setCountry(countryStr);
		dmpDataBean.setCity(cityStr);
		dmpDataBean.setAreaInfoSource("ip");
		
		if( (StringUtils.isNotBlank(countryStr)) && (!StringUtils.equals(countryStr, "null"))
			&& (StringUtils.isNotBlank(cityStr)) && (!StringUtils.equals(cityStr, "null")) ){
			dmpDataBean.setAreaInfoClassify("Y");
		}else{
			dmpDataBean.setAreaInfoClassify("N");
		}

		return dmpDataBean;
	}
}