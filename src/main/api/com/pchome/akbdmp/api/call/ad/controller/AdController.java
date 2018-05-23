package com.pchome.akbdmp.api.call.ad.controller;

import java.util.HashSet;
import java.util.concurrent.TimeUnit;

import javax.servlet.http.HttpServletRequest;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.JSONArray;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Scope;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.pchome.akbdmp.api.call.base.controller.BaseController;
import com.pchome.akbdmp.api.data.enumeration.DmpApiReturnCodeEnum;
import com.pchome.akbdmp.api.data.enumeration.DmpLogInfoKeyEnum;
import com.pchome.akbdmp.api.data.returndata.ReturnData;
import com.pchome.akbdmp.spring.config.bean.allbeanscan.SpringAllConfig;
import com.pchome.soft.depot.utils.KafkaUtil;

@RestController
@Scope("request")
public class AdController extends BaseController {

	@Autowired
	private RedisTemplate<String, Object> redisTemplate;

	@Value("${spring.profiles.active}")
	private String active;
	
	@Value("${dmp.api.topic}")
	private String dmpApiTopic;
	
	@Autowired
	private KafkaUtil kafkaUtil;
	
	@Value("${redis.call.map}")
	private String redisCallmapKey;
	
	@Value("${redis.callfc}")
	private String redisCallfcKey;
	
	@Value("${redis.class}")
	private String redisClassKey;
	
	@Value("${redis.frequency}")
	private long redisFrequency;
	
	@Value("${radis.dmpinfo}")
	private String radisDmpinfoKey;
	
	
	/**
	 * 1.REDIS PRD MAP:prd:dmp:callmap:[uuid | pcid]
	 * 2.REDIS STG MAP:stg:dmp:callmap:[uuid | pcid]
	 * 3.REDIS PRD 分類頻次:prd:dmp:callfc:[uuid | pcid]
	 * 4.REDIS STG 分類頻次 :stg:dmp:callfc:[uuid | pcid]
	 * 5.REDIS STG 分類:prd:dmp:class:[uuid | pcid]
	 * 6.REDIS PRD 分類:stg:dmp:class:[uuid | pcid]
	 * */
	Log log = LogFactory.getLog(AdController.class);
	// @CrossOrigin(origins = {"http://pcbwebstg.pchome.com.tw"})
	@RequestMapping(value = "/api/adclassApi", method = RequestMethod.GET, headers = "Accept=application/json;charset=UTF-8")
	@ResponseBody
	public Object adclassApi(
			HttpServletRequest request,
			@RequestParam(defaultValue = "", required = false) String memid,
			@RequestParam(defaultValue = "", required = false) String uuid
			) throws Exception {
		try {
			
			String key  = "";
			String result = "{\"ad_class\":[],\"behavior\":\"\",\"sex\":\"\",\"age\":\"\"}";
			if(StringUtils.isBlank(memid) && StringUtils.isBlank(uuid)){
				result = "{\"ad_class\":[],\"behavior\":\"\",\"sex\":\"\",\"age\":\"\"}";
				return result;
			}
			
			if(StringUtils.isNotBlank(memid)){
				key = memid;
			}else if(StringUtils.isNotBlank(uuid)){
				key = uuid;
			}
			
			String mapKey = redisCallmapKey+key;
			String fcKey = redisCallfcKey+key;
			String classKey = redisClassKey+key;
			
			//map不存在key則呼叫kafka建立redis key反之只取redis key
			redisTemplate.opsForValue().get(mapKey);
			if(redisTemplate.opsForValue().get(mapKey) == null){
				kafkaUtil.sendMessage(dmpApiTopic, "", key);
				redisTemplate.opsForValue().set(redisCallmapKey+key, key, 1,TimeUnit.DAYS);
				return result;
			}
			
			if(redisTemplate.opsForValue().get(mapKey) != null){
				String redisDmpClassValue = (String) redisTemplate.opsForValue().get(classKey);
				if(StringUtils.isBlank(redisDmpClassValue)){
					return result;
				}
				
				if(redisTemplate.opsForValue().get(fcKey) == null || (Integer)redisTemplate.opsForValue().get(fcKey) > redisFrequency){
					JSONObject json = new JSONObject(redisDmpClassValue); 
					json.put("ad_class", new JSONArray());
					json.put("behavior", "");
					result = json.toString();
					return result;
				}
				
				result = redisDmpClassValue;
				redisTemplate.opsForValue().increment(fcKey, 1);
			}
			return result;
		} catch (Exception e) {
			log.error(">>>>" + e.getMessage());
			e.printStackTrace();
			ReturnData returnData = new ReturnData();
			returnData.setCode(DmpApiReturnCodeEnum.API_CODE_E002.getCode());
			returnData.setResult(e.getMessage());
			returnData.setStatus(DmpApiReturnCodeEnum.API_CODE_E002.isStatus());
			return getReturnData(returnData);
		}
	}
	
	
	/**
	 * Dmp info api
	 * */
	@RequestMapping(value = "/api/dmpInfoApi", method = RequestMethod.GET, headers = "Accept=application/json;charset=UTF-8")
	@ResponseBody
	public Object dmpInfoApi(
			HttpServletRequest request,
			@RequestParam(defaultValue = "", required = false) String memid,
			@RequestParam(defaultValue = "", required = false) String uuid
			) throws Exception {
		try {
			
			JSONObject resultJson = new JSONObject();
			String result = "";
			String key = "";
			for (DmpLogInfoKeyEnum infoKeyEnum : DmpLogInfoKeyEnum.values()) {
				resultJson.put(infoKeyEnum.getMongoKey(),new HashSet());
			}
			result = resultJson.toString();
			if(StringUtils.isBlank(memid) && StringUtils.isBlank(uuid)){
				return result;
			}
			
			if(StringUtils.isNotBlank(memid)){
				key = memid;
			}else if(StringUtils.isNotBlank(uuid)){
				key = uuid;
			}
			
			String radisKey = radisDmpinfoKey + key;
			Object obj = redisTemplate.opsForValue().get(radisKey);
			if(obj == null){
				kafkaUtil.sendMessage(dmpApiTopic, "", "<PCHOME_API>"+key);
				return result;
			}else{
				result =  (String) obj;
			}
			return result;
		} catch (Exception e) {
			log.error(">>>>" + e.getMessage());
			e.printStackTrace();
			ReturnData returnData = new ReturnData();
			returnData.setCode(DmpApiReturnCodeEnum.API_CODE_E002.getCode());
			returnData.setResult(e.getMessage());
			returnData.setStatus(DmpApiReturnCodeEnum.API_CODE_E002.isStatus());
			return getReturnData(returnData);
		}
	}
}
