package com.pchome.akbdmp.api.call.listen.intercept;


import javax.servlet.http.HttpServletRequest;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.Signature;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.springframework.stereotype.Component;
import org.springframework.util.StopWatch;

import com.pchome.akbdmp.api.data.enumeration.DmpApiPermissionsEnum;


@Component
@Aspect
public class ApiIntercept {

	Log log = LogFactory.getLog(ApiIntercept.class);

	@Around("@within(org.springframework.web.bind.annotation.RestController)")
	public Object callApi(ProceedingJoinPoint proceedingJoinPoint) throws Throwable {
		long time1,time2;
		time1 = System.currentTimeMillis();
		StopWatch stopWatch = new StopWatch();
		stopWatch.start();
		Object returnObject;
		boolean flag = false;
		try {
			Object[] args = proceedingJoinPoint.getArgs();
			HttpServletRequest request = (HttpServletRequest) args[0];
//			log.info(request.getHeader("User-Agent"));
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
			for (DmpApiPermissionsEnum pcbookApiPermissionsEnum : DmpApiPermissionsEnum.values()) {
				if(proceedingJoinPoint.getSignature().getName().equals(pcbookApiPermissionsEnum.getMethod())){
					flag = pcbookApiPermissionsEnum.isApprove();
					break;
				}
			}
		} finally {
			stopWatch.stop();
		}
		if(!flag){
			return "{\"status\":\"API不允許呼叫\"}";
		}
		returnObject = proceedingJoinPoint.proceed();
		time2 = System.currentTimeMillis();
		final Signature signature = proceedingJoinPoint.getSignature();
//		log.info(signature.getDeclaringTypeName() + "." + signature.getName()+">>>>花費:"+((double) time2 - time1) / 1000+"秒");
		return returnObject;
	}
}