package com.pchome.akbdmp.api.call.listen.intercept;


import javax.servlet.http.HttpServletRequest;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.StopWatch;
import org.springframework.web.servlet.ModelAndView;

import com.pchome.akbdmp.api.data.check.AInterceptCheckData;
import com.pchome.akbdmp.api.data.check.CheckInterceptFactory;
import com.pchome.akbdmp.api.data.enumeration.DmpApiPermissionsEnum;
import com.pchome.akbdmp.api.data.enumeration.DmpCheckObjNameEnum;


@Component
@Aspect
public class ApiIntercept {

	Log log = LogFactory.getLog(ApiIntercept.class);

	@Autowired
	private CheckInterceptFactory checkInterceptFactory;
	
	
	@Around("@within(org.springframework.web.bind.annotation.RestController)")
	public Object callApi(ProceedingJoinPoint proceedingJoinPoint) throws Throwable {
//		long time1,time2;
//		time1 = System.currentTimeMillis();
		StopWatch stopWatch = new StopWatch();
		stopWatch.start();
		try {
			Object[] args = proceedingJoinPoint.getArgs();
			HttpServletRequest request = (HttpServletRequest) args[0];
			
			String requestURI = request.getRequestURI();
			if(requestURI.indexOf("/AkbDmp/adm") >= 0){
				AInterceptCheckData aInterceptCheckData = checkInterceptFactory.getaCheckData(DmpCheckObjNameEnum.CHECK_ADM_INTERCEPT);
				boolean flag =  (boolean) aInterceptCheckData.checkData(request,proceedingJoinPoint);
				if(!flag){
					ModelAndView modelAndView = new ModelAndView();
					modelAndView.addObject("login", "flase");
					modelAndView.addObject("ERR", "IP不允許...");
					modelAndView.setViewName("login");
					return modelAndView;
				}
			}else if(requestURI.indexOf("/AkbDmp/api") >= 0){
				AInterceptCheckData aInterceptCheckData = checkInterceptFactory.getaCheckData(DmpCheckObjNameEnum.CHECK_API_INTERCEPT);
				boolean flag = (boolean) aInterceptCheckData.checkData(request,proceedingJoinPoint);
				if(!flag){
					return "{\"status\":\"API不允許呼叫\"}";
				}
			}
			Object returnObject;
			returnObject = proceedingJoinPoint.proceed();
//			final Signature signature = proceedingJoinPoint.getSignature();
//			time2 = System.currentTimeMillis();
			return returnObject;
//			log.info(signature.getDeclaringTypeName() + "." + signature.getName()+">>>>花費:"+((double) time2 - time1) / 1000+"秒");
		} finally {
			stopWatch.stop();
		}
	}
}