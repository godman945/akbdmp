package com.pchome.akbdmp.api.call.base.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.ModelAndView;

import com.fasterxml.jackson.databind.ObjectMapper;


@Component
@Scope("request")
public class BaseController {

	@Autowired
	private ObjectMapper objectMapper;
	
	private ModelAndView modelAndView = new ModelAndView();
	
	
	public String getReturnData(Object data) throws Exception{
		return objectMapper.writeValueAsString(data);
	}

	public ModelAndView getModelAndView() {
		return modelAndView;
	}

	public void setModelAndView(ModelAndView modelAndView) {
		this.modelAndView = modelAndView;
	}
	
	
}
