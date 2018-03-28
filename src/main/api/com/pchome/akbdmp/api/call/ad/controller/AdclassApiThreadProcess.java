package com.pchome.akbdmp.api.call.ad.controller;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;

//package com.pchome.akbpfp.data.threadprocess;
//
//import java.util.List;
//import java.util.concurrent.Callable;
//
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
//import com.pchome.akbpfp.db.service.ad.IPfpAdService;
//import com.pchome.akbpfp.db.vo.ad.PfpAdAdVideoViewSumVO;
//import com.pchome.akbpfp.db.vo.ad.PfpAdAdVideoViewVO;
//import com.pchome.akbpfp.db.vo.ad.PfpAdAdViewConditionVO;
//import com.pchome.enumerate.thread.EnumAdThreadType;
//
//import net.sf.json.JSONArray;
//import net.sf.json.JSONObject;




public class AdclassApiThreadProcess implements Callable<String> {

	Log log = LogFactory.getLog(AdclassApiThreadProcess.class);
	
	private Map<String,String> map = null;
	
//	private ThreadServiceBean threadServiceBean;
//	private JSONObject conditionJson;
//	private EnumAdThreadType enumAdThreadType;
	
//	public AdclassApiThreadProcess(JSONObject conditionJson,EnumAdThreadType enumAdThreadType,ThreadServiceBean threadServiceBean) {
//		this.threadServiceBean = threadServiceBean;
//		this.conditionJson = conditionJson;
//		this.enumAdThreadType = enumAdThreadType;
	String key = "";
	public AdclassApiThreadProcess(String key,Map<String,String> map) {
		this.key = key;
		this.map = map;
	}

	public synchronized String call() throws Exception {
		long time = System.currentTimeMillis();
//		log.info(">>>>>>>>>>>>>>>map size:"+map.size());
		map.put(String.valueOf(time), String.valueOf(time));
		
		
		
//		System.out.println(map);
		
		return "SSS";
	}
}