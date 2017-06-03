package com.pchome.akbdmp.api.call.lifecheck.controller;

import javax.servlet.http.HttpServletRequest;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Scope;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.pchome.akbdmp.api.call.base.controller.BaseController;
import com.pchome.akbdmp.api.data.check.CheckDataFactory;
import com.pchome.akbdmp.api.data.enumeration.DmpApiReturnCodeEnum;
import com.pchome.akbdmp.api.data.enumeration.DmpLifeCheckEnum;
import com.pchome.akbdmp.api.data.returndata.ReturnData;

import redis.clients.jedis.Jedis;

@RestController
@Scope("request")
public class LifeCheckController extends BaseController {

	@Autowired
	RedisTemplate<String, Object> redisTemplate;

	@Autowired
	CheckDataFactory checkDataFactory;

	@Value("${spring.profiles.active}")
	private String active;

	Log log = LogFactory.getLog(LifeCheckController.class);

	// @CrossOrigin(origins = {"http://pcbwebstg.pchome.com.tw"})
	@RequestMapping(value = "/api/LifeCheck", method = RequestMethod.GET, headers = "Accept=application/json;charset=UTF-8")
	@ResponseBody
	public Object LifeCheck(HttpServletRequest request) throws Exception {
		try {
			log.info(">>>>>> call LifeCheck");
			if (active.equals("stg")) {
				Jedis jedis = new Jedis("redisdev.mypchome.com.tw");
				jedis.set(DmpLifeCheckEnum.CHECK_KEY.getKey(), DmpLifeCheckEnum.CHECK_KEY.getValue());
				boolean flag = StringUtils.isNotBlank(jedis.get(DmpLifeCheckEnum.CHECK_KEY.getKey()));
				jedis.close();
				ReturnData returnData = new ReturnData();
				if (flag) {
					returnData.setCode(DmpApiReturnCodeEnum.API_CODE_S001.getCode());
					returnData.setResult(DmpApiReturnCodeEnum.API_CODE_S001.getContent());
					returnData.setStatus(DmpApiReturnCodeEnum.API_CODE_S001.isStatus());
					return returnData;
				} else {
					returnData.setCode(DmpApiReturnCodeEnum.API_CODE_E003.getCode());
					returnData.setResult(DmpApiReturnCodeEnum.API_CODE_E003.getContent());
					returnData.setStatus(DmpApiReturnCodeEnum.API_CODE_E003.isStatus());
					return returnData;
				}
			} else {
				redisTemplate.opsForValue().set(DmpLifeCheckEnum.CHECK_KEY.getKey(), DmpLifeCheckEnum.CHECK_KEY.getValue());
				boolean flag = StringUtils.isNotBlank(redisTemplate.opsForValue().get(DmpLifeCheckEnum.CHECK_KEY.getKey()).toString());
				ReturnData returnData = new ReturnData();
				if (flag) {
					returnData.setCode(DmpApiReturnCodeEnum.API_CODE_S001.getCode());
					returnData.setResult(DmpApiReturnCodeEnum.API_CODE_S001.getContent());
					returnData.setStatus(DmpApiReturnCodeEnum.API_CODE_S001.isStatus());
					return returnData;
				} else {
					returnData.setCode(DmpApiReturnCodeEnum.API_CODE_E003.getCode());
					returnData.setResult(DmpApiReturnCodeEnum.API_CODE_E003.getContent());
					returnData.setStatus(DmpApiReturnCodeEnum.API_CODE_E003.isStatus());
					return returnData;
				}
			}

		} catch (Exception e) {
			log.error(">>>>" + e.getMessage());
			ReturnData returnData = new ReturnData();
			returnData.setCode(DmpApiReturnCodeEnum.API_CODE_E003.getCode());
			returnData.setResult(e.getMessage());
			returnData.setStatus(DmpApiReturnCodeEnum.API_CODE_E003.isStatus());
			return getReturnData(returnData);
		}
	}
}
