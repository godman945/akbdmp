package com.pchome.akbdmp.api.data.check;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.pchome.akbdmp.api.data.enumeration.DmpCheckObjNameEnum;

@Component
public class CheckInterceptFactory {

	@Autowired
	private CheckAdmIntercept checkAdmIntercept;
	
	@Autowired
	private CheckApiIntercept checkApiIntercept;
	
	public AInterceptCheckData getaCheckData(DmpCheckObjNameEnum akbDmpCheckObjNameEnum) throws Exception {
		switch (akbDmpCheckObjNameEnum) {
		case CHECK_ADM_INTERCEPT:
			return checkAdmIntercept;
		case CHECK_API_INTERCEPT:
			return checkApiIntercept;
		default:
			break;
		}
		return null;
	}
}