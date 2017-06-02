package com.pchome.akbdmp.api.call.adshowlimit.controller;

import javax.servlet.http.HttpServletRequest;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.jayway.jsonpath.JsonPath;
import com.pchome.akbdmp.api.call.adshowlimit.bean.AdShowLimitBean;
import com.pchome.akbdmp.api.call.base.controller.BaseController;
import com.pchome.akbdmp.api.data.check.ACheckData;
import com.pchome.akbdmp.api.data.check.CheckDataFactory;
import com.pchome.akbdmp.api.data.enumeration.DmpAdShowLimitParamaterEnum;
import com.pchome.akbdmp.api.data.enumeration.DmpApiReturnCodeEnum;
import com.pchome.akbdmp.api.data.enumeration.DmpCheckObjNameEnum;
import com.pchome.akbdmp.api.data.enumeration.DmpReturnKeyEnum;
import com.pchome.akbdmp.api.data.returndata.ReturnData;

@RestController
@Scope("request")
public class AdShowLimitController extends BaseController {

	@Autowired
	RedisTemplate<String, Object> redisTemplate;

	@Autowired
	CheckDataFactory checkDataFactory;
	
	Log log = LogFactory.getLog(AdShowLimitController.class);

	// @CrossOrigin(origins = {"http://pcbwebstg.pchome.com.tw"})
	@RequestMapping(value = "/api/getAdShowLimit", method = RequestMethod.POST, headers = "Accept=application/json;charset=UTF-8")
	@ResponseBody
	public Object adShowLimit(HttpServletRequest request,
			@RequestParam(defaultValue = "", required = false) String[] adKey
			) throws Exception {
		try {
			log.info(">>>>>> call getAdShowLimit : adKey:" + adKey);
			JSONObject paramaterJson = new JSONObject();
			paramaterJson.put(DmpAdShowLimitParamaterEnum.AD_KEY.getKey(), adKey);
			ACheckData aCheckData = checkDataFactory.getaCheckData(DmpCheckObjNameEnum.CHECK_ADSHOW_LIMIT);
			Object obj = aCheckData.checkData(paramaterJson);
			String checkResult = getReturnData(obj);
			Boolean checkFlag = JsonPath.read(checkResult, DmpReturnKeyEnum.STATUS.getKey());
			if(!checkFlag){
				return getReturnData(obj);
			}
			
			AdShowLimitBean adShowLimitBean = new AdShowLimitBean();
			for (Object key : adKey) {
				int adLimit = (int) ((redisTemplate.opsForValue().get(key) == null) ? 0 : Integer.parseInt(redisTemplate.opsForValue().get(key).toString()));
				adShowLimitBean.getAdShowLimitMap().put(key.toString(), adLimit);
			}
			
			ReturnData returnData = new ReturnData();
			returnData.setCode(DmpApiReturnCodeEnum.API_CODE_S001.getCode());
			returnData.setResult(adShowLimitBean.getAdShowLimitMap());
			returnData.setStatus(DmpApiReturnCodeEnum.API_CODE_S001.isStatus());
			return returnData;
		} catch (Exception e) {
			log.error(">>>>" + e.getMessage());
			ReturnData returnData = new ReturnData();
			returnData.setCode(DmpApiReturnCodeEnum.API_CODE_E002.getCode());
			returnData.setResult(e.getMessage());
			returnData.setStatus(DmpApiReturnCodeEnum.API_CODE_E002.isStatus());
			return getReturnData(returnData);
		}
	}
}
