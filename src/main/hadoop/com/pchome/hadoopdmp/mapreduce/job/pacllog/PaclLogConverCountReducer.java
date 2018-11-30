package com.pchome.hadoopdmp.mapreduce.job.pacllog;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
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
	
	public static Map<String, PcalConditionBean> convertConditionMap = new HashMap<String, PcalConditionBean>();

	private MysqlUtil mysqlUtil = null;
	
	private StringBuffer convertWriteInfo = new StringBuffer();
	
	private StringBuffer sql = new StringBuffer();
	
	private Map<String,Set<String>> convertResultMap = new HashMap<>();
	
	private Set<String> convertConditionSet = new HashSet<String>();	
	
	private static PcalConditionBean pcalConditionBean;
	
	private static String paclSymbol = String.valueOf(new char[] { 9, 31 });
	
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
			String key = mapperKey.toString();
			log.info(">>>>>>init mapperKey:"+key);
			String convertSeq = key.split("_")[0];
			String uuid = key.split("_")[1];
			if(convertConditionMap.get(convertSeq) == null){
				log.info(">>>>>>convertConditionMap data not exist!!");
				sql.append(" SELECT c.convert_seq, ");
				sql.append(" 	c.click_range_date,  ");
				sql.append(" 	c.imp_range_date,  ");
				sql.append(" 	c.convert_price, ");
				sql.append(" 	c.convert_status, ");
				sql.append(" 	c.convert_belong, ");
				sql.append(" 	c.convert_num_type, ");
				sql.append(" 	GROUP_CONCAT(convert_rule_id SEPARATOR ':')convert_rule_id  ");
				sql.append(" FROM   pfp_code_convert_rule r,  ");
				sql.append(" 		pfp_code_convert c ");
				sql.append(" WHERE  1 = 1 ");
				sql.append(" AND c.convert_seq = '").append(convertSeq).append("'");
				sql.append(" AND r.convert_seq = c.convert_seq  ");
				sql.append(" GROUP BY r.convert_seq  ");
				
				ResultSet resultSet = mysqlUtil.query(sql.toString());
				while(resultSet.next()){
					int clickRangeDate = resultSet.getInt("click_range_date");
					int impRangeDate = resultSet.getInt("imp_range_date");
					String convertPrice = resultSet.getString("convert_price");
					String convertStatus = resultSet.getString("convert_status");
					String convertBelong = resultSet.getString("convert_belong");
					int convertNumType = resultSet.getInt("convert_num_type");
					String convertRule = resultSet.getString("convert_rule_id");
					pcalConditionBean = new PcalConditionBean();
					pcalConditionBean.setClickRangeDate(clickRangeDate);
					pcalConditionBean.setImpRangeDate(impRangeDate);
					pcalConditionBean.setConvertPrice(convertPrice);
					pcalConditionBean.setConvertStatus(convertStatus);
					pcalConditionBean.setConvertNumType(convertNumType);
					pcalConditionBean.setConvertBelong(convertBelong);
					pcalConditionBean.setConvertRule(convertRule);
					convertConditionMap.put(convertSeq, pcalConditionBean);
					log.info(">>>>>>convertConditionMap:"+convertConditionMap);
				}
			}else{
				log.info(">>>>>>convertConditionMap data exist!!");
				pcalConditionBean = convertConditionMap.get(convertSeq);
			}
			
			//整理條件內容與總計
					 
			String convertConditionArray[] = pcalConditionBean.getConvertRule().split(":");
			for (String rouleId : convertConditionArray) {
				convertConditionSet.add(rouleId+"_0");
			}
			
			//開始計算條件出現次數
			for (Text text : mapperValue) {
				String paclRouleId = text.toString();
				String data = "";
				if(pcalConditionBean.getConvertRule().indexOf(paclRouleId.toString()) >= 0){
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
			
			//統計轉換次數
			int min = 0;
			for (String rouleIdCountStr : convertConditionSet) {
				String converArray[] = rouleIdCountStr.split("_");
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
			int convertPriceCount = 0;
			//1:每次 2:一次
			if(pcalConditionBean.getConvertNumType() == 1){
				convertPriceCount = (int)Double.parseDouble(pcalConditionBean.getConvertPrice()) * min;
			}else if(pcalConditionBean.getConvertNumType() == 2){
				convertPriceCount = (int)Double.parseDouble(pcalConditionBean.getConvertPrice());
			}
			log.info("============="+convertConditionSet+" convert count:"+min);
			keyOut.set(uuid);
			convertWriteInfo.append(paclSymbol).append(pcalConditionBean.getClickRangeDate());
			convertWriteInfo.append(paclSymbol).append(pcalConditionBean.getImpRangeDate());
			convertWriteInfo.append(paclSymbol).append(convertPriceCount);
			convertWriteInfo.append(paclSymbol).append(pcalConditionBean.getConvertPrice());
			convertWriteInfo.append(paclSymbol).append(pcalConditionBean.getConvertBelong());
			convertWriteInfo.append(paclSymbol).append(convertSeq);
			valueOut.set(convertWriteInfo.toString());
			log.info(">>>>>>write:"+convertWriteInfo.toString());
			context.write(keyOut, valueOut);
			
			convertConditionSet.clear();
			convertWriteInfo.setLength(0);
			sql.setLength(0);
			convertResultMap.clear();
		} catch (Throwable e) {
			log.error("reduce error>>>>>> " + e);
		}
	}
	public void cleanup(Context context) {
		try {
			mysqlUtil.closeConnection();
		} catch (Throwable e) {
			convertConditionSet.clear();
			convertWriteInfo.setLength(0);
			sql.setLength(0);
			log.error("reduce cleanup error>>>>>> " + e);
		}
	}
	
	
	
	public static void main(String args[]){
		
		
//		PcalConditionBean PcalConditionBean = new PcalConditionBean();
//		Set<String> a = new HashSet<>();
//		a.add("A01");
//		a.add("A02");
//		a.add("A03");
//		
//		PcalConditionBean.setConvertConditionSet(a);
//		
//		Set<String> b = PcalConditionBean.getConvertConditionSet();
//		
//		Iterator<String> h =  b.iterator();
//		while (h.hasNext()) {
//			String dat  = h.next();
//			if(dat.equals("A03")){
//				String j = dat;
//				j =j +"_AAAA";
//				h.remove();
//				b.add(j);
//			}
//			
//		}
//		System.out.println(b);
//		System.out.println(PcalConditionBean.getConvertConditionSet());
		
		
//		for (String string : b) {
//			if(string.equals("A02")){
//				PcalConditionBean.getConvertConditionSet().remove("A02");
//			}
//		}
//		
//		System.out.println(b);
//		System.out.println(PcalConditionBean.getConvertConditionSet());
		
//		MysqlUtil mysqlUtil = MysqlUtil.getInstance();
//		try{
//			List<String> a = new ArrayList<String>();
//			a.add("RLE20180724000000001");
//			a.add("RLE20180724000000001");
//			a.add("RLE20180724000000001");
//			a.add("RLE20180724000000002");
//			a.add("RLE20180724000000002");
//			a.add("ALEX");
//			a.add("ALEX");
//			a.add("ALEX");
//			
////			String url = "jdbc:mysql://kddbdev.mypchome.com.tw:3306/akb_video";
////			String jdbcDriver = "com.mysql.jdbc.Driver";
////			String user = "keyword";
////			String password =  "K1y0nLine";
////			mysqlUtil.setConnection(url, user, password);
////			
////			String convertSeq = "CAC20181112000000001";
////			
//			StringBuffer convertCondition = new StringBuffer("RLE20180724000000001:RLE20180724000000002:ALEX");
////			ResultSet resultSet = mysqlUtil.query(" select * from pfp_code_convert_rule where 1 = 1 and convert_seq = '"+convertSeq+"' ");
////			while(resultSet.next()){
////				String rouleId = resultSet.getString("convert_rule_id");
////				if(resultSet.isLast()){
////					convertCondition.append(rouleId);
////				}else{
////					convertCondition.append(rouleId).append(":");
////				}
////				log.info(">>>>>>"+resultSet.getString("convert_rule_id"));
////				log.info(">>>>>>"+resultSet.getString("convert_rule_way"));
////				log.info(">>>>>>"+resultSet.getString("convert_rule_value"));
////			}
//			
//			System.out.println(convertCondition.toString());
//			
//			 Map<String,Set<String>> map = new HashMap<>();
//			 
//			 
//			 Set<String> set = new HashSet<String>();			 
//			 String convertConditionArray[] = convertCondition.toString().split(":");
//			 for (String string : convertConditionArray) {
//				 set.add(string+"_0");
//			 }
//			 map.put("CAC20181112000000001", set);
//			 String data = "";
//			 for (String str : a) {
//				 data = "";
//				 if(convertCondition.toString().indexOf(str) >= 0){
//					 for (String setStr : set) {
//						String converArray[] = setStr.split("_");
//						String rouleId = converArray[0];
//						int count = Integer.parseInt(converArray[1]);
//						if(str.equals(rouleId)){
//							set.remove(rouleId+"_"+count);
//							count ++;
//							data = rouleId+"_"+String.valueOf(count);
//							set.add(data);
//							break;
//						}
//					 }
//				 }
//			}
//			 
//			System.out.println(map);
//			for (Entry<String, Set<String>> entry : map.entrySet()) {
//				int min = 0;
//				System.out.println(entry.getKey());
//				System.out.println(entry.getValue());
//				
//				for (String str : entry.getValue()) {
//					String converArray[] = str.split("_");
//					String rouleId = converArray[0];
//					int count = Integer.parseInt(converArray[1]);
//					if(count == 0){
//						min = 0;
//						break;
//					}
//					if(min == 0){
//						min = count;
//					}else if(count < min){
//						min = count;
//					}
//				}
//				System.out.println(min);
//			}
//			
////			mysqlUtil.closeConnection();
//		}catch(Exception e){
//			e.printStackTrace();
//		}
		
		
		
		
		
		
		
		
		
		
		
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
