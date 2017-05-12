package com.pchome.akbdmp.api.data.check;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.pchome.akbdmp.api.data.enumeration.DmpCheckObjNameEnum;

@Component
@Scope("request")
public class CheckDataFactory {

	
	@Autowired
	CheckAdSowLimit checkAdSowLimit;
	
	public ACheckData getaCheckData(DmpCheckObjNameEnum akbDmpCheckObjNameEnum) throws Exception {

		switch (akbDmpCheckObjNameEnum) {
		case CHECK_ADSHOW_LIMIT:
			return checkAdSowLimit;
		default:
			break;
		}
		return null;
	}
}