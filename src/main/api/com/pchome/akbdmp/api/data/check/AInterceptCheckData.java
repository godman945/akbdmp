package com.pchome.akbdmp.api.data.check;

import javax.servlet.http.HttpServletRequest;

import org.aspectj.lang.ProceedingJoinPoint;

public abstract class AInterceptCheckData {
	private AInterceptCheckData aInterceptCheckData;
	
	public abstract Object checkData(HttpServletRequest requeston,ProceedingJoinPoint proceedingJoinPoint) throws Exception;

	public AInterceptCheckData getaCheckData(Object obj) {
		return aInterceptCheckData;
	}
}
