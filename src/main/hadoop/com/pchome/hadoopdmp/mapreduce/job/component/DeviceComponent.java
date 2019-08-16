package com.pchome.hadoopdmp.mapreduce.job.component;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.bitwalker.useragentutils.Browser;
import eu.bitwalker.useragentutils.OperatingSystem;
import eu.bitwalker.useragentutils.UserAgent;

public class DeviceComponent {

	Log log = LogFactory.getLog("DeviceComponent");
	private static UserAgent userAgent = null;
	private static Browser browser  = null;
	private static OperatingSystem operatingSystem = null;
	public net.minidev.json.JSONObject parseUserAgentToDevice(net.minidev.json.JSONObject dmpJSon) throws Exception {
		if (StringUtils.isBlank(dmpJSon.getAsString("user_agent"))){
			dmpJSon.put("device_info_classify", "");
			return dmpJSon;
		}else {
			userAgent = null;
			browser  = null;
			operatingSystem = null;
			userAgent = UserAgent.parseUserAgentString(dmpJSon.getAsString("user_agent"));
			browser = userAgent.getBrowser();
			operatingSystem = userAgent.getOperatingSystem();
			
			dmpJSon.put("device_info", operatingSystem.getDeviceType().toString());
			dmpJSon.put("device_phone_info", operatingSystem.getManufacturer().toString());
			dmpJSon.put("device_os_info", operatingSystem.getGroup().toString());
			dmpJSon.put("device_browser_info", browser.getGroup().toString());
			dmpJSon.put("device_info_source", "user-agent");
			if ((!StringUtils.equals(dmpJSon.getAsString("device_info"),"UNKNOWN")) && 
			   (!StringUtils.equals(dmpJSon.getAsString("device_phone_info"),"UNKNOWN")) &&
			   (!StringUtils.equals(dmpJSon.getAsString("device_os_info"),"UNKNOWN")) && 
			   (!StringUtils.equals(dmpJSon.getAsString("device_browser_info"),"UNKNOWN"))){
					dmpJSon.put("device_info_classify", "Y");
				}else{
					dmpJSon.put("device_info_classify", "N");
				}
			return dmpJSon;
		}
	}
}