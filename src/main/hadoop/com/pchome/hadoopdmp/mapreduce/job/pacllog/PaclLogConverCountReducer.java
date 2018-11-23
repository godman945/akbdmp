package com.pchome.hadoopdmp.mapreduce.job.pacllog;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.kafka.clients.producer.Producer;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Component;

import com.pchome.soft.util.MysqlUtil;

import net.minidev.json.JSONObject;
import net.minidev.json.parser.JSONParser;

@Component
public class PaclLogConverCountReducer extends Reducer<Text, Text, Text, Text> {

	private static Log log = LogFactory.getLog("DmpLogReducer");

	private Text keyOut = new Text();

	private Text valueOut = new Text();

	public static String record_date;

	public static Producer<String, String> producer = null;

	public RedisTemplate<String, Object> redisTemplate = null;

	public int count;

	public JSONParser jsonParser = null;

	public String redisFountKey;

	public Map<String, JSONObject> kafkaDmpMap = null;

	public Map<String, Integer> redisClassifyMap = null;

	private MysqlUtil mysqlUtil = null;
	
	private StringBuffer convertCondition = new StringBuffer();
	
	private Map<String,Set<String>> convertResultMap = new HashMap<>();
	
	public void setup(Context context) {
		log.info(">>>>>> Reduce  setup>>>>>>>>>>>>>>env>>>>>>>>>>>>"+ context.getConfiguration().get("spring.profiles.active"));
		try {
			String url = "jdbc:mysql://kddbdev.mypchome.com.tw:3306/akb_video";
			String jdbcDriver = "com.mysql.jdbc.Driver";
			String user = "keyword";
			String password =  "K1y0nLine";
			mysqlUtil = MysqlUtil.getInstance();
			mysqlUtil.setConnection(url, user, password);
		} catch (Throwable e) {
			log.error("reduce setup error>>>>>> " + e);
		}
	}

	@Override
	public void reduce(Text mapperKey, Iterable<Text> mapperValue, Context context) {
		try {
			convertCondition.setLength(0);
			String convertSeq = mapperKey.toString();
			log.info(">>>>>>>>>convertSeq:"+convertSeq);
			ResultSet resultSet = mysqlUtil.query(" select * from pfp_code_convert_rule where 1 = 1 and convert_seq = '"+convertSeq+"' ");
			while(resultSet.next()){
				String rouleId = resultSet.getString("convert_rule_id");
				if(resultSet.isLast()){
					convertCondition.append(rouleId);
				}else{
					convertCondition.append(rouleId).append(":");
				}
//				log.info(">>>>>>"+resultSet.getString("convert_rule_id"));
//				log.info(">>>>>>"+resultSet.getString("convert_rule_way"));
//				log.info(">>>>>>"+resultSet.getString("convert_rule_value"));
			}
			log.info(">>>>>>>>>convertCondition:"+convertCondition.toString());
			log.info("--------------");
			//整理條件內容與總計
			Set<String> convertConditionSet = new HashSet<String>();			 
			String convertConditionArray[] = convertCondition.toString().split(":");
			for (String rouleId : convertConditionArray) {
				convertConditionSet.add(rouleId+"_0");
			}
			convertResultMap.put(convertSeq, convertConditionSet);
			
			//開始計算條件出現次數
			for (Text text : mapperValue) {
				String paclRouleId = text.toString();
//				log.info(">>>>>>>>>"+rouleId);
				String data = "";
				if(convertCondition.toString().indexOf(paclRouleId.toString()) >= 0){
					for (String setStr : convertConditionSet) {
						String converArray[] = setStr.split("_");
						String rouleId = converArray[0];
						int count = Integer.parseInt(converArray[1]);
						if(paclRouleId.equals(rouleId)){
							convertConditionSet.remove(rouleId+"_"+count);
							count ++;
							data = rouleId+"_"+String.valueOf(count);
							convertConditionSet.add(data);
							break;
						}
					 }
				 }
			}
			log.info(">>>>>>>>>>>>>>>>"+convertResultMap);
			//統計轉換次數
			for (Entry<String, Set<String>> entry : convertResultMap.entrySet()) {
				int min = 0;
				for (String str : entry.getValue()) {
					String converArray[] = str.split("_");
					int count = Integer.parseInt(converArray[1]);
					if(count == 0){
						min = 0;
						break;
					}
					if(min == 0){
						min = count;
					}else if(count < min){
						min = count;
					}
				}
				log.info("============="+entry.getKey()+" convert count:"+min);
			}
			
			
			
			log.info("-*****************-");
		} catch (Throwable e) {
			log.error("reduce error>>>>>> " + e);
		}
	}
	public void cleanup(Context context) {
		try {
			mysqlUtil.closeConnection();
		} catch (Throwable e) {
			log.error("reduce cleanup error>>>>>> " + e);
		}
	}
	
	
	
	public static void main(String args[]){
		MysqlUtil mysqlUtil = MysqlUtil.getInstance();
		try{
			List<String> a = new ArrayList<String>();
			a.add("RLE20180724000000001");
			a.add("RLE20180724000000001");
			a.add("RLE20180724000000001");
			a.add("RLE20180724000000002");
			a.add("RLE20180724000000002");
			a.add("ALEX");
			a.add("ALEX");
			a.add("ALEX");
			
//			String url = "jdbc:mysql://kddbdev.mypchome.com.tw:3306/akb_video";
//			String jdbcDriver = "com.mysql.jdbc.Driver";
//			String user = "keyword";
//			String password =  "K1y0nLine";
//			mysqlUtil.setConnection(url, user, password);
//			
//			String convertSeq = "CAC20181112000000001";
//			
			StringBuffer convertCondition = new StringBuffer("RLE20180724000000001:RLE20180724000000002:ALEX");
//			ResultSet resultSet = mysqlUtil.query(" select * from pfp_code_convert_rule where 1 = 1 and convert_seq = '"+convertSeq+"' ");
//			while(resultSet.next()){
//				String rouleId = resultSet.getString("convert_rule_id");
//				if(resultSet.isLast()){
//					convertCondition.append(rouleId);
//				}else{
//					convertCondition.append(rouleId).append(":");
//				}
//				log.info(">>>>>>"+resultSet.getString("convert_rule_id"));
//				log.info(">>>>>>"+resultSet.getString("convert_rule_way"));
//				log.info(">>>>>>"+resultSet.getString("convert_rule_value"));
//			}
			
			System.out.println(convertCondition.toString());
			
			 Map<String,Set<String>> map = new HashMap<>();
			 
			 
			 Set<String> set = new HashSet<String>();			 
			 String convertConditionArray[] = convertCondition.toString().split(":");
			 for (String string : convertConditionArray) {
				 set.add(string+"_0");
			 }
			 map.put("CAC20181112000000001", set);
			 String data = "";
			 for (String str : a) {
				 data = "";
				 if(convertCondition.toString().indexOf(str) >= 0){
					 for (String setStr : set) {
						String converArray[] = setStr.split("_");
						String rouleId = converArray[0];
						int count = Integer.parseInt(converArray[1]);
						if(str.equals(rouleId)){
							set.remove(rouleId+"_"+count);
							count ++;
							data = rouleId+"_"+String.valueOf(count);
							set.add(data);
							break;
						}
					 }
				 }
			}
			 
			System.out.println(map);
			for (Entry<String, Set<String>> entry : map.entrySet()) {
				int min = 0;
				System.out.println(entry.getKey());
				System.out.println(entry.getValue());
				
				for (String str : entry.getValue()) {
					String converArray[] = str.split("_");
					String rouleId = converArray[0];
					int count = Integer.parseInt(converArray[1]);
					if(count == 0){
						min = 0;
						break;
					}
					if(min == 0){
						min = count;
					}else if(count < min){
						min = count;
					}
				}
				System.out.println(min);
			}
			
//			mysqlUtil.closeConnection();
		}catch(Exception e){
			e.printStackTrace();
		}
		
		
		
		
		
		
		
		
		
		
		
//		System.setProperty("spring.profiles.active", "stg");
//		ApplicationContext ctx = new AnnotationConfigApplicationContext(SpringAllHadoopConfig.class);
//		
//		
//		 String text ="a,a,b,c,a";
//		 int count = 0;
//		 int start = 0;
//		 String sub = "a";
//		 while((start = text.indexOf(sub,start))>=0){
//	            start += sub.length();
//	            count ++;
//		 }
//		 System.out.println(count);
	}
}
