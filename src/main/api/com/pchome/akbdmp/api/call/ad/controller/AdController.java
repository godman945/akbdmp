package com.pchome.akbdmp.api.call.ad.controller;

import javax.servlet.http.HttpServletRequest;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Scope;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
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

import redis.clients.jedis.Jedis;

@RestController
@Scope("request")
public class AdController extends BaseController {

	@Autowired
	private RedisTemplate<String, Object> redisTemplate;

	@Value("${spring.profiles.active}")
	private String active;
	
	@Value("${dmp.api.topic}")
	private String topicName;
	
	@Autowired
	private KafkaUtil kafkaUtil;
	
	
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
			String result = "";
			if(StringUtils.isNotBlank(memid)){
				key = memid;
			}else if(StringUtils.isNotBlank(uuid)){
				key = uuid;
			}
			
			log.info(redisTemplate.opsForValue().get("adclass_api_"+key));
			result = (String) redisTemplate.opsForValue().get("adclass_api_"+key);
			//呼叫kafka
			if(StringUtils.isBlank(result)){
				result = "{\"ad_class\":[],\"behavior\":\"\",\"sex\":\"\",\"age\":\"\"}";
				kafkaUtil.sendMessage(topicName, "", key);
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
		System.setProperty("spring.profiles.active", "local");
		ApplicationContext ctx = new AnnotationConfigApplicationContext(SpringAllConfig.class);
		AdController AdController = (AdController) ctx.getBean(AdController.class);
		AdController.a();
	}
	
}
