package com.pchome.hadoopdmp.mapreduce.job.component;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import eu.bitwalker.useragentutils.Browser;
import eu.bitwalker.useragentutils.OperatingSystem;
import eu.bitwalker.useragentutils.UserAgent;

public class DeviceComponent {
	
	private static Logger log = Logger.getLogger(DeviceComponent.class);
	private static UserAgent userAgent = null;
	private static Browser browser  = null;
	private static OperatingSystem operatingSystem = null;
	private static Pattern pattern = Pattern.compile(";\\s?(\\S*?\\s?\\S*?)\\s?(Build)?/");
	private static String mobile_type = "";  
	
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
			Matcher matcher = pattern.matcher(dmpJSon.getAsString("user_agent"));    
			mobile_type = "";    
			if (matcher.find()) {    
				mobile_type = matcher.group(1).trim();
			}    
			if (mobile_type.indexOf("AppleWebKit") >= 0) {
				mobile_type = "";
			}    
			dmpJSon.put("mobile_type", mobile_type);
			
			
			
			
			
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