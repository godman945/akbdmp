package com.pchome.soft.depot.utils;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.stereotype.Component;

@Component
public class IsNumericUtil {

	Log log = LogFactory.getLog(IsNumericUtil.class);

	public boolean isNumeric(String string) {
		if (string != null && !"".equals(string.trim())){
			return string.matches("^[0-9]*$");
		}else{
			return false;
		}
	}
}
