package com.pchome.akbdmp.api.data.check;

import javax.servlet.http.HttpServletRequest;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.aspectj.lang.ProceedingJoinPoint;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.ModelAndView;

import com.pchome.akbdmp.api.data.enumeration.DmpApiPermissionsIPEnum;

@Component
public class CheckAdmIntercept extends AInterceptCheckData {

	Log log = LogFactory.getLog(CheckAdmIntercept.class);
	
	
	
	@Override
	public Object checkData(HttpServletRequest request,ProceedingJoinPoint proceedingJoinPoint) throws Exception {
		String ip = request.getHeader("x-forwarded-for");
		if(ip == null || ip.length() == 0 || "unknown".equalsIgnoreCase(ip)) {
			ip = request.getHeader("Proxy-Client-IP");
		}
		if(ip == null || ip.length() == 0 || "unknown".equalsIgnoreCase(ip)) {
			ip = request.getHeader("WL-Proxy-Client-IP");
		}
		if(ip == null || ip.length() == 0 || "unknown".equalsIgnoreCase(ip)) {
			ip = request.getRemoteAddr();
		}
		
		boolean flag = false;
		for (DmpApiPermissionsIPEnum dmpApiPermissionsIPEnum : DmpApiPermissionsIPEnum.values()) {
			if(ip.equals(dmpApiPermissionsIPEnum.getIp())){
				flag = true;
				return flag;
			}
		}
		return flag;
	}

}