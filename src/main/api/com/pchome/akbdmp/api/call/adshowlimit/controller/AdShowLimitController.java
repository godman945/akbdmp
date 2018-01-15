package com.pchome.akbdmp.api.call.adshowlimit.controller;

import java.util.Arrays;

import javax.servlet.http.HttpServletRequest;

import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Scope;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
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

import redis.clients.jedis.Jedis;

@RestController
@Scope("request")
public class AdShowLimitController extends BaseController {

	@Autowired
	RedisTemplate<String, Object> redisTemplate;

	@Autowired
	CheckDataFactory checkDataFactory;
	
	@Value("${spring.profiles.active}")
	private String active;
	
	@Autowired
	private JedisConnectionFactory jedisConnectionFactory;
	
	Log log = LogFactory.getLog(AdShowLimitController.class);

	// @CrossOrigin(origins = {"http://pcbwebstg.pchome.com.tw"})
	/**
	 * 1.廣告顯示頻次API
	 * 2.回傳查詢的頻次數
	 * */
	@RequestMapping(value = "/api/getAdShowLimit", method = RequestMethod.POST, headers = "Accept=application/json;charset=UTF-8")
	@ResponseBody
	public Object adShowLimit(HttpServletRequest request,
			@RequestParam(defaultValue = "", required = false) String adKey
			) throws Exception {
		try {
			String [] keyArray = adKey.split(",");
			JSONObject paramaterJson = new JSONObject();
			paramaterJson.put(DmpAdShowLimitParamaterEnum.AD_KEY.getKey(), keyArray);
			ACheckData aCheckData = checkDataFactory.getaCheckData(DmpCheckObjNameEnum.CHECK_ADSHOW_LIMIT);
			Object obj = aCheckData.checkData(paramaterJson);
			String checkResult = getReturnData(obj);
			Boolean checkFlag = JsonPath.read(checkResult, DmpReturnKeyEnum.STATUS.getKey());
			if(!checkFlag){
				return getReturnData(obj);
			}
			
			boolean adKeyFlag = false;
			AdShowLimitBean adShowLimitBean = new AdShowLimitBean();
			for (String key : keyArray) {
					
				if(key == null || key.equals("")){
					adKeyFlag = true;
				}
				if(active.equals("stg")){
					String [] adKeyArray = key.toString().split("_");
					if(adKeyArray.length < 6){
						adKeyFlag = true;
					}
					String searchKey = "stg:akb:adfc:" + key;
					int adLimit = (int) ((redisTemplate.opsForValue().get(searchKey) == null) ? 0 : Integer.parseInt(IOUtils.toString(jedisConnectionFactory.getClusterConnection().get(searchKey.toString().getBytes()))));
					adShowLimitBean.getAdShowLimitMap().put(key.toString(), adLimit);
				}
				if(active.equals("prd")){
					String [] adKeyArray = key.toString().split("_");
					if(adKeyArray.length < 6){
						adKeyFlag = true;
					}
					String searchKey = "prd:akb:adfc:" + key;
					int adLimit = (int) ((redisTemplate.opsForValue().get(searchKey) == null) ? 0 : Integer.parseInt(IOUtils.toString(jedisConnectionFactory.getClusterConnection().get(searchKey.toString().getBytes()))));
					adShowLimitBean.getAdShowLimitMap().put(key.toString(), adLimit);
				}
			}
				
			if(adKeyFlag){
				log.error(">>>>>> Fail adkey:"+Arrays.asList(adKey));
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
