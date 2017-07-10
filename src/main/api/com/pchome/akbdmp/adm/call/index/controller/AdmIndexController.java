package com.pchome.akbdmp.adm.call.index.controller;

import java.net.URLEncoder;
import java.util.List;

import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.web.bind.annotation.CookieValue;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.ModelAndView;

import com.pchome.akbdmp.api.call.base.controller.BaseController;
import com.pchome.akbdmp.data.mysql.pojo.AdmCategoryAudienceAnalyze;
import com.pchome.akbdmp.data.mysql.pojo.AdmMenu;
import com.pchome.akbdmp.mysql.db.service.adclass.IAdmCategoryAudienceAnalyzeService;
import com.pchome.akbdmp.mysql.db.service.user.IAdmMenuService;
import com.pchome.akbdmp.mysql.db.service.user.IAdmUserService;

import net.minidev.json.JSONObject;
import sun.misc.BASE64Decoder;
import sun.misc.BASE64Encoder;

@RestController
@Scope("request")
public class AdmIndexController extends BaseController {

	Log log = LogFactory.getLog(AdmIndexController.class);

	@Autowired
	private IAdmUserService admUserService;

	@Autowired
	private IAdmMenuService admMenuService;

	@Autowired
	private IAdmCategoryAudienceAnalyzeService admCategoryAudienceAnalyzeService;
	
	
	
	// @CrossOrigin(origins = {"http://pcbwebstg.pchome.com.tw"})
	@RequestMapping(value = "/adm/index", method = RequestMethod.GET)
	public ModelAndView dmpIndex(
			HttpServletRequest request, 
			ModelAndView modelAndView,
			@CookieValue(value = "pchome_dmp_adm", required = false, defaultValue = "") String dmpAdmCookie,
			@RequestParam(defaultValue = "", required = false) String localStorage
			) {
		try {
			if (StringUtils.isNotBlank(dmpAdmCookie)) {
				String loginInfo = new String((new BASE64Decoder()).decodeBuffer(dmpAdmCookie));
				String[] user = loginInfo.split("_");
				boolean flag = admUserService.checkUser(user[0], user[1]);
				if (flag) {
					modelAndView = new ModelAndView("forward:/index.html");
					modelAndView.addObject("login", "false");
					return modelAndView;
				}else{
					modelAndView.setViewName("login");
					modelAndView.addObject("login", "false");
					return modelAndView;
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		modelAndView.setViewName("login");
		modelAndView.addObject("login", "false");
		return modelAndView;
	}
	
	
	@RequestMapping(value = "/index.html", method = RequestMethod.GET)
	public ModelAndView index(HttpServletRequest request, ModelAndView modelAndView,
			@CookieValue(value = "pchome_dmp_adm", required = false, defaultValue = "") String dmpAdmCookie,
			@RequestParam(defaultValue = "", required = false) String localStorage) {
		try {
			List<AdmCategoryAudienceAnalyze> admCategoryAudienceAnalyzeList = admCategoryAudienceAnalyzeService.loadAll();
			List<AdmMenu> admMenuList = admMenuService.loadAll();
			modelAndView.addObject("admMenuList", admMenuList);
			modelAndView.addObject("admCategoryAudienceAnalyzeList", admCategoryAudienceAnalyzeList);
			modelAndView.addObject("login", "true");
			modelAndView.setViewName("homePage");
			return modelAndView;

		} catch (Exception e) {
			e.printStackTrace();
		}
		modelAndView.setViewName("login");
		modelAndView.addObject("login", "false");
		return modelAndView;
	}
	
	
	@RequestMapping(value = "/adm/userlogin", method = RequestMethod.POST)
	public String dmpUserlogin(
			HttpServletRequest request, 
			HttpServletResponse response,
			ModelAndView modelAndView,
			@RequestParam(defaultValue = "", required = false) String account,
			@RequestParam(defaultValue = "", required = false) String password
			) {
		JSONObject result = new JSONObject();
		try {
			boolean flag = admUserService.checkUser(account, password);
			if (flag) {
				String data = (new BASE64Encoder()).encodeBuffer((account + "_" + password).getBytes());
				result.put("result", "OK");
				result.put("msg", data);
				result.put("url", "index.html");
				log.info(">>>>>> account:"+account);
				log.info(">>>>>> password:"+password);
				log.info(">>>>>> write cookie:"+data);
				log.info(">>>>>> write cookie time:"+60*60);
				Cookie cookie = new Cookie("pchome_dmp_adm",URLEncoder.encode(data, "UTF-8"));
				cookie.setMaxAge(60*60);
				response.addCookie(cookie);
				return result.toString();
			} else {
				result.put("result", "FAIL");
				result.put("msg", "not found");
				return result.toString();
			}
		} catch (Exception e) {
			e.printStackTrace();
			result.put("result", "FAIL");
			result.put("msg", "system error");
			return result.toString();
		}
	}
	
	@RequestMapping(value = "/adm/userlogout", method = RequestMethod.POST)
	public String dmpUserlogout(
			@CookieValue(value = "pchome_dmp_adm", required = false, defaultValue = "") String dmpAdmCookie,
			HttpServletRequest request,
			HttpServletResponse response,
			ModelAndView modelAndView
			) {
		JSONObject result = new JSONObject();
		try {
			 
			Cookie cookie = new Cookie("pchome_dmp_adm",null);
			cookie.setMaxAge(0);
			response.addCookie(cookie);
			
			result.put("result", "FAIL");
			result.put("msg", "system error");
			return result.toString();
	        
		} catch (Exception e) {
			e.printStackTrace();
			result.put("result", "FAIL");
			result.put("msg", "system error");
			return result.toString();
		}
	}
	
}
