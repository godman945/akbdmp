package com.pchome.soft.depot.utils;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.mail.internet.MimeMessage;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.mail.javamail.JavaMailSenderImpl;
import org.springframework.mail.javamail.MimeMessageHelper;
import org.springframework.stereotype.Component;

@Component
public class EmailUtil {

	Log log = LogFactory.getLog(EmailUtil.class);
	
	@Value("${email.host}")
	private String host;
	
	@Value("${email.default.sender}")
	private String sender;
	
	
	//檢查email格式
	public boolean checkEmailFormat(String email) throws Exception {
		String regEx = "^\\w+([-+.]\\w+)*@\\w+([-.]\\w+)*\\.\\w+([-.]\\w+)*$";
		Pattern pattern = Pattern.compile(regEx);
		Matcher matcher = pattern.matcher(email);
		boolean flag = matcher.find();
		log.info(">>>>>> check email format: email:" + email+":"+flag);
		return flag;
	}
	
	
	//發送email
	public boolean sendEmail(String subject, String mailTo, String content) throws Exception {
		log.info(">>>>>> send email mailTo:" + mailTo);
		log.info(">>>>>> send email content:" + content);
		JavaMailSenderImpl javaMailSender = new JavaMailSenderImpl();
		javaMailSender.setHost(host);
		MimeMessage mime = javaMailSender.createMimeMessage();
		MimeMessageHelper mimeMessageHelper = new MimeMessageHelper(mime, "UTF-8");
		mimeMessageHelper.setSubject(subject);
		mimeMessageHelper.setFrom(sender);
		mimeMessageHelper.setTo(mailTo);
		mimeMessageHelper.setText(content,true);
		javaMailSender.send(mime);
		log.info(">>>>>> send email success mailTo:" + mailTo);
		return true;
	}
}
