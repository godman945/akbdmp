package com.pchome.akbdmp.spring.config.freemarker;

import javax.servlet.ServletException;


import freemarker.ext.servlet.FreemarkerServlet;

public class MemberFreemarkerServlet extends FreemarkerServlet {
    /**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Override
    public void init() throws ServletException {
        super.init();
        getConfiguration().setTemplateExceptionHandler(new MemberFreemarkerTemplateHandler());
    }
}