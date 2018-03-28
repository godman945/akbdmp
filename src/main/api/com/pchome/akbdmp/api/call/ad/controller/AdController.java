package com.pchome.akbdmp.api.call.ad.controller;

import java.util.HashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;

import javax.servlet.http.HttpServletRequest;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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
	private String topicName;
	
	@Autowired
	private KafkaUtil kafkaUtil;
	
	@Autowired
 	private HashMap<String,Object> sendKafkaMap;
	
//	@Autowired
// 	private long sendKafkaMap;
//	
//	
//	@Autowired
// 	private long apiSendCount;
//	
//	@Autowired
// 	private long repeatCount;
	
	
	
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
			
//			log.info(redisTemplate.opsForValue().get("adclass_api_"+key));
			
//			AdclassApiThreadProcess adclassApiThreadProcess = new AdclassApiThreadProcess(key,sendKafkaMap);
//			ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(500);
//			Future<String> adclassApiThreadProcessResult = executor.submit(adclassApiThreadProcess);
//			boolean flag = true;
//			while (flag) {
//				String a = adclassApiThreadProcessResult.get();
//				System.out.println(a);
//				if(StringUtils.isNotBlank(a)){
//					flag = false;
//					executor.shutdown();
//				}
//			}
			
			//判斷
			long apiSendCount = (long) sendKafkaMap.get("apiSendCount");
			apiSendCount = apiSendCount + 1 ;
			sendKafkaMap.put("apiSendCount", apiSendCount);
			if(sendKafkaMap.containsKey(key)){
				long repeatCount = (long) sendKafkaMap.get("repeatCount");
				repeatCount = repeatCount + 1;
				sendKafkaMap.put("repeatCount", repeatCount);
				
				log.info(">>>>>>key:"+key);
				result = (String) redisTemplate.opsForValue().get("adclass_api_"+key);
				if(StringUtils.isBlank(result)){
					result = "{\"ad_class\":[],\"behavior\":\"\",\"sex\":\"\",\"age\":\"\"}";
				}
			}else{
				sendKafkaMap.put(key, key);
				long kafkaCount = (long) sendKafkaMap.get("kafkaCount");
				long count = (long) sendKafkaMap.get("count");
				count = count + 1;
				kafkaCount = kafkaCount + 1;
				sendKafkaMap.put("kafkaCount", kafkaCount);
				sendKafkaMap.put("count", count);
				
				log.info(">>>>>>key:"+key);
				result = (String) redisTemplate.opsForValue().get("adclass_api_"+key);
				if(StringUtils.isBlank(result)){
					result = "{\"ad_class\":[],\"behavior\":\"\",\"sex\":\"\",\"age\":\"\"}";
					kafkaUtil.sendMessage(topicName, "", key);
				}
			}
			
//			result = (String) redisTemplate.opsForValue().get("adclass_api_"+key);
			//呼叫kafka
//			if(StringUtils.isBlank(result)){
////				log.info(">>>>>>key:"+key);
//				result = "{\"ad_class\":[],\"behavior\":\"\",\"sex\":\"\",\"age\":\"\"}";
////				kafkaUtil.sendMessage(topicName, "", key);
//			}
			
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
		System.out.println(redisTemplate.opsForValue().get("adclass_api_nico19732001"));
//		redisTemplate.delete("adclass_api_nico19732001");
	}
	
}
