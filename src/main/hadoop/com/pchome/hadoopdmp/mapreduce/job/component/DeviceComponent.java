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
			dmpDataBean.setDeviceInfoSource("null");
			dmpDataBean.setDeviceInfoClassify("null");
			return dmpDataBean;
		}
		
		UserAgent userAgent = UserAgent.parseUserAgentString(dmpDataBean.getUserAgent());
		Browser browser = userAgent.getBrowser();
		OperatingSystem operatingSystem = userAgent.getOperatingSystem();
		
		dmpDataBean.setDeviceInfo(operatingSystem.getDeviceType().toString());
		dmpDataBean.setDevicePhoneInfo(operatingSystem.getManufacturer().toString());
		dmpDataBean.setDeviceOsInfo(operatingSystem.getGroup().toString());
		dmpDataBean.setDeviceBrowserInfo(browser.getName());
		dmpDataBean.setDeviceInfoSource("user-agent");
		
		if ( (!StringUtils.equals(dmpDataBean.getDeviceInfo(),"UNKNOWN")) && (!StringUtils.equals(dmpDataBean.getDevicePhoneInfo(),"UNKNOWN"))
			 &&	(!StringUtils.equals(dmpDataBean.getDeviceOsInfo(),"UNKNOWN")) && (!StringUtils.equals(dmpDataBean.getDeviceBrowserInfo(),"UNKNOWN")) ){
			dmpDataBean.setDeviceInfoClassify("Y");
		}else{
			dmpDataBean.setDeviceInfoClassify("N");
		}
	
		return dmpDataBean;
	}
}