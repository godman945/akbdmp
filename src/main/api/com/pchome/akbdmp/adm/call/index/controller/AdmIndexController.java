package com.pchome.akbdmp.adm.call.index.controller;

import java.util.ArrayList;
import java.util.List;

import javax.servlet.http.HttpServletRequest;

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
	@RequestMapping(value = "/login.html", method = RequestMethod.GET)
	public ModelAndView admLogin(HttpServletRequest request, ModelAndView modelAndView,
			@CookieValue(value = "pchome_dmp_adm", required = false, defaultValue = "") String dmpAdmCookie,
			@RequestParam(defaultValue = "", required = false) String localStorage) {
		JSONObject result = new JSONObject();
		try {
			if (StringUtils.isNotBlank(localStorage)) {
				String loginInfo = new String((new BASE64Decoder()).decodeBuffer(localStorage));
				String[] user = loginInfo.split("_");
				boolean flag = admUserService.checkUser(user[0], user[1]);
				if (flag) {
					ModelAndView modelAndView2 = new ModelAndView("redirect:/index.html");
					return modelAndView2;
				}
			}else{
				modelAndView.setViewName("login");
				modelAndView.addObject("login", "false");
			}

			
		} catch (Exception e) {
			e.printStackTrace();
		}
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

	// @CrossOrigin(origins = {"http://pcbwebstg.pchome.com.tw"})
	@RequestMapping(value = "/checklogin", method = RequestMethod.POST)
	public String checkLogin(HttpServletRequest request, ModelAndView modelAndView,
			@RequestParam(defaultValue = "", required = false) String account,
			@RequestParam(defaultValue = "", required = false) String password) {
		JSONObject result = new JSONObject();
		try {
			boolean flag = admUserService.checkUser(account, password);
			if (flag) {
				String data = (new BASE64Encoder()).encodeBuffer((account + "_" + password).getBytes());
				result.put("result", "OK");
				result.put("msg", data);
				result.put("url", "index.html");
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
	
	
	
	
	@RequestMapping(value = "/test.html", method = RequestMethod.POST)
	public ModelAndView test(HttpServletRequest request, ModelAndView modelAndView,
			@RequestParam(defaultValue = "", required = false) String localStorage
		) {
		try {
			if (StringUtils.isNotBlank(localStorage)) {
				String loginInfo = new String((new BASE64Decoder()).decodeBuffer(localStorage));
				String[] user = loginInfo.split("_");
				boolean flag = admUserService.checkUser(user[0], user[1]);
				if (true) {
					ModelAndView modelAndView2 = new ModelAndView("redirect:/index.html");
					return modelAndView2;
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
			
		}
		modelAndView.setViewName("login");
		modelAndView.addObject("login", "false");
		return modelAndView;
	}
	
	
	
	
}
