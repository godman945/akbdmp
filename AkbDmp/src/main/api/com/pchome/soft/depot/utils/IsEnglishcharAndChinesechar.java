package com.pchome.soft.depot.utils;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.stereotype.Component;

@Component
public class IsEnglishcharAndChinesechar {

	Log log = LogFactory.getLog(IsEnglishcharAndChinesechar.class);

	public boolean isEnglishcharAndChinesechar(String string) {
		if (string != null && !"".equals(string.trim())){
			return string.matches("^[\u4e00-\u9fa5_a-zA-Z0-9\\s]+$");
		}else{
			return false;
		}
	}
}
