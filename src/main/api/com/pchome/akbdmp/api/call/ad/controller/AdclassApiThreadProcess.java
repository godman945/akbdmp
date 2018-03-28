package com.pchome.akbdmp.api.call.ad.controller;
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
	
	private Map<String,Object> map = null;
	
//	private ThreadServiceBean threadServiceBean;
//	private JSONObject conditionJson;
//	private EnumAdThreadType enumAdThreadType;
	
//	public AdclassApiThreadProcess(JSONObject conditionJson,EnumAdThreadType enumAdThreadType,ThreadServiceBean threadServiceBean) {
//		this.threadServiceBean = threadServiceBean;
//		this.conditionJson = conditionJson;
//		this.enumAdThreadType = enumAdThreadType;
	String key = "";
	public AdclassApiThreadProcess(String key,Map<String,Object> map) {
		this.key = key;
		this.map = map;
	}

	public synchronized String call() throws Exception {
//		long time = System.currentTimeMillis();
//		log.info(">>>>>>>>>>>>>>>map size:"+map.size());
		if(map.containsKey(key)){
			long repeatCount = (long) map.get("repeatCount");
			repeatCount = repeatCount + 1;
			map.put("repeatCount", repeatCount);
		}else{
			map.put(key, key);
			long kafkaCount = (long) map.get("kafkaCount");
			long count = (long) map.get("count");
			count = count + 1;
			kafkaCount = kafkaCount + 1;
			map.put("kafkaCount", kafkaCount);
			map.put("count", count);
		}
		
//		map.put(String.valueOf(time), String.valueOf(time));
		
		
		
//		System.out.println(map);
		
		return "SSS";
	}
}