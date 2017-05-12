package com.pchome.soft.depot.utils;

import java.net.InetSocketAddress;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import net.spy.memcached.MemcachedClient;


//@Component
public class MemcachedUtil {

	Log log = LogFactory.getLog(MemcachedUtil.class);
	
	private int maxIdle = 60 * 60 * 24 * 6;
	
	
	private MemcachedClient memcachedClient;
	
	public MemcachedUtil() throws Exception{
		init();
	}
	
	public void init() throws Exception {
		this.memcachedClient = new MemcachedClient(new InetSocketAddress("192.168.1.54", 11211));
	}
	
	/**
	 * key:key
	 * Object:寫入物件
	 * */
	public void addCache(String key, Object value) throws Exception{
		try {
//			String cacheString = mapper.writeValueAsString(value);
//			log.info("key:"+key + " value:" + value);
			this.memcachedClient.set(key, maxIdle, value);
		} catch (Exception e) {
			log.error(">>>>"+e.getMessage());
		}
	}

	public Object getCache(String key) throws Exception{
		try {
//			log.info(memcachedClient.get(key));
			return this.memcachedClient.get(key);
		} catch (Exception e) {
			log.error(">>>>"+e.getMessage());
			return null;
		}

	}
	
	public void deleteCache(String key) throws Exception{
		try {
			this.memcachedClient.delete(key);
		} catch (Exception e) {
			log.error(">>>>"+e.getMessage());
		}
	}
	
	public MemcachedClient getMemcachedClient() {
		return memcachedClient;
	}

}
