package com.pchome.soft.depot.utils;

import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.ObjectMapper;

@Component
public class BeanFormatUtil {

	protected static ObjectMapper mapper = new ObjectMapper();
	
	public String beanToString(Object obj) throws Exception{
		return mapper.writeValueAsString(obj);
	}
}
