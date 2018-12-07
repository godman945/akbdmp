package com.pchome.akbdmp.api.call.ad.controller;

import java.util.HashMap;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;

import javax.servlet.http.HttpServletRequest;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.JSONArray;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Scope;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.pchome.akbdmp.api.call.base.controller.BaseController;
import com.pchome.akbdmp.api.data.enumeration.DmpApiReturnCodeEnum;
import com.pchome.akbdmp.api.data.enumeration.DmpLogInfoKeyEnum;
import com.pchome.akbdmp.api.data.returndata.ReturnData;
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
	
	@Value("${radis.retargeting}")
	private String radisRetargetingKey;
	
	/**
	 * 1.REDIS PRD MAP:prd:dmp:callmap:[uuid | pcid]
	 * 2.REDIS STG MAP:stg:dmp:callmap:[uuid | pcid]
	 * 3.REDIS PRD 分類頻次:prd:dmp:callfc:[uuid | pcid]
	 * 4.REDIS STG 分類頻次 :stg:dmp:callfc:[uuid | pcid]
	 * 5.REDIS STG 分類:prd:dmp:class:[uuid | pcid]
	 * 6.REDIS PRD 分類:stg:dmp:class:[uuid | pcid]
	 * 7.REDIS STG RETARGETING stg:pa:track:[uuid | pcid]
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
			JSONObject result = new JSONObject();
			result.put("ad_class", new JSONArray());
			result.put("behavior", "");
			result.put("sex", "");
			result.put("age", "");
			result.put("retargeting_prod", new HashMap<>());
			if(StringUtils.isBlank(memid) && StringUtils.isBlank(uuid)){
				return result.toString();
			}
			if(StringUtils.isNotBlank(memid)){
				key = memid;
			}else if(StringUtils.isNotBlank(uuid)){
				key = uuid;
			}
			String mapKey = redisCallmapKey+key;
			String fcKey = redisCallfcKey+key;
			String classKey = redisClassKey+key;
//			map不存在key則呼叫kafka建立redis key反之只取redis key
//			redisTemplate.opsForValue().get(mapKey);
			if(redisTemplate.opsForValue().get(mapKey) == null){
				kafkaUtil.sendMessage(dmpApiTopic, "", key);
				redisTemplate.opsForValue().set(redisCallmapKey+key, key, 1,TimeUnit.DAYS);
				return result;
			}
			//dmp有資料
			Object redisDmpClassValue = redisTemplate.opsForValue().get(classKey);
			if(redisDmpClassValue != null && StringUtils.isNotBlank(redisDmpClassValue.toString())){
				if(StringUtils.isBlank(redisDmpClassValue.toString())){
					return result;
				}
				result = new JSONObject(redisDmpClassValue.toString());
				redisTemplate.opsForValue().increment(fcKey, 1);
			}
			//再行銷商品資料
			String retargetingKey = radisRetargetingKey + key;
			Object obj = redisTemplate.opsForValue().get(retargetingKey);
			if(obj != null){
				String retargeting = obj.toString();
				JSONObject retargetingAdJson = new JSONObject(retargeting);
				result.put("retargeting_prod", retargetingAdJson);
			}
			
			//頻次資料
			Object fckeyTimes = redisTemplate.opsForValue().get(fcKey);
			if(fckeyTimes == null || (Integer)fckeyTimes > redisFrequency){
				System.out.println((Integer) fckeyTimes);
				result.put("ad_class", new JSONArray());
				result.put("behavior", "");
				result.put("sex", "");
				result.put("age", "");
			}
			if(!active.equals("prd")){
				log.info("memid:"+memid);
				log.info("uuid:"+uuid);
				log.info("result:"+result);
			}
			return result.toString();
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
	
	
	
	
//	@RequestMapping(value = "/api/prodAdTest", method = RequestMethod.POST,produces = MediaType.APPLICATION_JSON_VALUE, headers = "Accept=application/json;charset=UTF-8")
	@RequestMapping(value = "/api/prodAdTest", method = RequestMethod.POST,produces = MediaType.APPLICATION_JSON_VALUE, headers = "Accept=application/x-www-form-urlencoded;charset=UTF-8")
	@ResponseBody
	public Object prodAdTest(
			HttpServletRequest request
			) throws Exception {
		try {
			String data = IOUtils.toString(request.getInputStream(), "UTF8");
			JSONObject json = new JSONObject();
			if(StringUtils.isNotBlank(data)){
				String array[] = data.split("&");
				for (String obj : array) {
					String detail[] = obj.split("=");
					String key = detail[0];
					String value = detail[1];
					json.put(key, value);
				}
			}
			kafkaUtil.sendMessage("akb_prod_ad_stg", "", json.toString());
			return "success";
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
