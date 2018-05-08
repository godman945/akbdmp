package com.pchome.hadoopdmp.mapreduce.job.component;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.pchome.hadoopdmp.mapreduce.job.factory.DmpLogBean;

import eu.bitwalker.useragentutils.Browser;
import eu.bitwalker.useragentutils.OperatingSystem;
import eu.bitwalker.useragentutils.UserAgent;

public class DeviceComponent {

	Log log = LogFactory.getLog("DeviceComponent");

	public DmpLogBean parseUserAgentToDevice(DmpLogBean dmpDataBean) throws Exception {
		
		if (StringUtils.isBlank(dmpDataBean.getUserAgent())){
			dmpDataBean.setDeviceInfo("null");
			dmpDataBean.setDevicePhoneInfo("null");
			dmpDataBean.setDeviceOsInfo("null");
			dmpDataBean.setDeviceBrowserInfo("null");
			return dmpDataBean;
		}
		
		UserAgent userAgent = UserAgent.parseUserAgentString(dmpDataBean.getUserAgent());
		Browser browser = userAgent.getBrowser();
		OperatingSystem operatingSystem = userAgent.getOperatingSystem();
		
		dmpDataBean.setDeviceInfo(operatingSystem.getDeviceType().toString());
		dmpDataBean.setDevicePhoneInfo(operatingSystem.getManufacturer().toString());
		dmpDataBean.setDeviceOsInfo(operatingSystem.getGroup().toString());
		dmpDataBean.setDeviceBrowserInfo(browser.getName());
		
		
//		System.out.println("访问设备类型:"+operatingSystem.getDeviceType());// “device_info”:{ //enum DeviceType
////		System.out.println("浏览器生产厂商:"+browser.getManufacturer());
//		System.out.println("操作系统生产厂商:"+operatingSystem.getManufacturer());//“device_phone_info”:{ //enum Manufacturer
//		System.out.println("操作系统家族:"+operatingSystem.getGroup());//“device_os_info”:{ //enum OperatingSystem
//		System.out.println("浏览器名稱:"+browser.getName());//“device_browser_info”:{ //enum Browser
		
		
		
		return dmpDataBean;
	}
	
}