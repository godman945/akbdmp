package com.pchome.akbdmp.api.call.base.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.ObjectMapper;


@Component
@Scope("request")
public class BaseController {

	@Autowired
	ObjectMapper objectMapper;
	
	public String getReturnData(Object data) throws Exception{
		return objectMapper.writeValueAsString(data);
	}
}
