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
	RedisTemplate<String, String> redisTemplate;

	@Autowired
	CheckDataFactory checkDataFactory;

	@Value("${spring.profiles.active}")
	private String active;

	Log log = LogFactory.getLog(LifeCheckController.class);

	// @CrossOrigin(origins = {"http://pcbwebstg.pchome.com.tw"})
	@RequestMapping(value = "/api/LifeCheck", method = RequestMethod.GET)
	@ResponseBody
	public Object LifeCheck(HttpServletRequest request) throws Exception {
		try {
			log.info(">>>>>> call LifeCheck");
			if (active.equals("stg")) {
				Jedis jedis = new Jedis("redisdev.mypchome.com.tw");
				jedis.set(DmpLifeCheckEnum.CHECK_KEY.getKey(), DmpLifeCheckEnum.CHECK_KEY.getValue());
				boolean flag = StringUtils.isNotBlank(jedis.get(DmpLifeCheckEnum.CHECK_KEY.getKey()));
				jedis.del(DmpLifeCheckEnum.CHECK_KEY.getKey());
				jedis.close();
				if (flag) {
					return "OK";
				} else {
					return "ERROR";
				}
			} else {
				redisTemplate.opsForValue().set(DmpLifeCheckEnum.CHECK_KEY.getKey(), DmpLifeCheckEnum.CHECK_KEY.getValue());
				boolean flag = StringUtils.isNotBlank(redisTemplate.opsForValue().get(DmpLifeCheckEnum.CHECK_KEY.getKey()).toString());
				redisTemplate.delete(DmpLifeCheckEnum.CHECK_KEY.getKey());
				if (flag) {
					return "OK";
				} else {
					return "ERROR";
				}
			}

		} catch (Exception e) {
			log.error(">>>>" + e.getMessage());
			return "ERROR";
		}
	}
}
