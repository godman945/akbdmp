package com.pchome.akbdmp.api.call.ad.controller;

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
	
	
	
	@RequestMapping(value = "/api/alex", method = RequestMethod.GET, headers = "Accept=application/json;charset=UTF-8")
	@ResponseBody
	public Object alex9(
			HttpServletRequest request
			) throws Exception {
		ReturnData returnData = new ReturnData();
		try {
			returnData.setResult("SSSS");
			kafkaUtil.sendMessage("TEST", "", "FFFF");
			return getReturnData(returnData);
		}catch(Exception e){
			e.printStackTrace();
		}
		return getReturnData(returnData);
	}
	
	public void a(){
		kafkaUtil.sendMessage("TEST", "", "CC");
	}
	
	public static void main(String args[]){
		System.setProperty("spring.profiles.active", "prd");
		ApplicationContext ctx = new AnnotationConfigApplicationContext(SpringAllConfig.class);
		RedisTemplate redisTemplate = (RedisTemplate) ctx.getBean(RedisTemplate.class);
		
		
		redisTemplate.delete("stg:dmp:class:zzz1929");
		redisTemplate.delete("stg:dmp:class:zzz1929");
		redisTemplate.delete("stg:dmp:class:zzz1929");
		
		
		
		
		
		
		
//		redisTemplate.delete("adclass_api_nico19732001");
	}
	
}
