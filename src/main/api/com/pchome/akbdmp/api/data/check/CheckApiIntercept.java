package com.pchome.akbdmp.api.data.check;

import javax.servlet.http.HttpServletRequest;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.aspectj.lang.ProceedingJoinPoint;
import org.springframework.stereotype.Component;

import com.pchome.akbdmp.api.data.enumeration.DmpApiPermissionsEnum;

@Component
public class CheckApiIntercept extends AInterceptCheckData {

	Log log = LogFactory.getLog(CheckApiIntercept.class);

	@Override
	public Object checkData(HttpServletRequest request,ProceedingJoinPoint proceedingJoinPoint) throws Exception {
		request.setAttribute("device", "pc");
		if (request.getHeader("User-Agent").toUpperCase().indexOf("WINDOWS") > 0) {
			request.setAttribute("device", "pc");
		}
		if (request.getHeader("User-Agent").toUpperCase().indexOf("IOS") > 0) {
			request.setAttribute("device", "ios");
		}
		if (request.getHeader("User-Agent").toUpperCase().indexOf("ANDROID") > 0) {
			request.setAttribute("device", "android");
		}
		
		boolean flag = false;
		for (DmpApiPermissionsEnum pcbookApiPermissionsEnum : DmpApiPermissionsEnum.values()) {
			if(proceedingJoinPoint.getSignature().getName().equals(pcbookApiPermissionsEnum.getMethod())){
				flag = pcbookApiPermissionsEnum.isApprove();
				break;
			}
		}
		return flag;
	}

}