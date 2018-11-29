package com.pchome.hadoopdmp.mapreduce.job.pacllog;

import java.sql.ResultSet;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
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
public class PaclLogConverCountReducer2 extends Reducer<Text, Text, Text, Text> {

	private static Log log = LogFactory.getLog("DmpLogReducer");

	private Text keyOut = new Text();

	private Text valueOut = new Text();

	public static String record_date;

	public static Producer<String, String> producer = null;

	public RedisTemplate<String, Object> redisTemplate = null;

	public int count;

	public String redisFountKey;

	public Map<String, JSONObject> kafkaDmpMap = null;

	public Map<String, Integer> redisClassifyMap = null;
	
	public static Map<String, PcalConditionBean> convertConditionMap = new HashMap<String, PcalConditionBean>();

	private MysqlUtil mysqlUtil = null;
	
	private StringBuffer convertWriteInfo = new StringBuffer();
	
	private StringBuffer sql = new StringBuffer();
	
	private static String[] convertConditionArray = null;	
	
	private static List<JSONObject> dataList = new ArrayList<JSONObject>();
	
	private static Date date = new Date();
	
	private static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
	
//	private JSONObject logJson = new JSONObject();
	
	private static boolean flagKdcl = false;
	
	private static boolean flagPacl = false;
	
	private JSONParser jsonParser = new JSONParser(JSONParser.MODE_PERMISSIVE);
	
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
			dataList.clear();
//			convertConditionArray = null;
			flagKdcl = false;
			flagPacl = false;
			if(key.equals("2f59086e290c6a4a513834ba16f563e6")){
				for (Text text : mapperValue) {
					String value = text.toString();
					log.info(">>>"+value);
					JSONObject logJson = (JSONObject) jsonParser.parse(value);
					
					
					log.info("fileName:"+logJson.getAsString("fileName"));
					log.info("kdcl:"+logJson.getAsString("fileName").contains("kdcl"));
					log.info("pacl:"+logJson.getAsString("fileName").contains("part-r-00000"));
					
					if(logJson.getAsString("fileName").contains("kdcl")){
						dataList.add(logJson);
						flagKdcl = true;
					}
					if(logJson.getAsString("fileName").contains("part-r-00000")){
						dataList.add(logJson);
						flagKdcl = true;
					}
//					logJson.clear();
//					if(value.contains("kdcl")){
//						dataList.add(value);
//						flagKdcl = true;
//					}
//					if(value.contains("part-r-00000")){
//						flagPart = true;
//						convertConditionArray = value.split(",");
//					}
				}
				
				log.info("flagKdcl>>>>>>>"+flagKdcl);
				log.info("flagPacl>>>>>>>"+flagPacl);
				log.info("list>>>>>>>"+dataList);
			}
	
			
			
			
			if(flagKdcl && flagPacl){
				log.info("##>>>>>>key:"+key);
				for (JSONObject json : dataList) {
					log.info(json);
				}
			}
			
			
			
			
			
			
			
			
			
			
			
			
			
//			if(flagKdcl && flagPart){
//				log.info("##>>>>>>key:"+key);
//				
//				String clickRangeDate = convertConditionArray[0];
//				String impRangeDate = convertConditionArray[1];
//				String convertPriceCount = convertConditionArray[2];
//				String convertPric = convertConditionArray[3];
//				//1:最終 2:最初
//				String convertBelong = convertConditionArray[4];
//				String convertSeq = convertConditionArray[5];
//				
//				String adDateBelong = "";
//				String adSeqBelong = "";
//				
//				
//				
//				for (String str : dataList) {
//					String[] kdclDataArray = str.split(",");
//					String kdclDate = kdclDataArray[0];
//					String kdclAdseq = kdclDataArray[1];
//					String kdclType = kdclDataArray[2];
//					long day = (long) (date.getTime() - sdf.parse(kdclDate).getTime()) / (1000 * 60 * 60 *24);
//					log.info(">>>>>>>>>>>>>:"+str);
//					log.info(">>>>>>>>>>>>>adseq:"+kdclAdseq);
//					log.info(">>>>>>>>>>>>>kdclType:"+kdclType);
//					log.info(">>>>>>>>>>>>>range day:"+day);
//					long d = 0;
//					if(kdclType.equals("ck")){
//						d = Long.valueOf(clickRangeDate.split(":")[1]);
//					}else if(kdclType.equals("pv")){
//						d = Long.valueOf(impRangeDate.split(":")[1]);
//					}
//					log.info(">>>>>>>>>>>>>range day flag:"+(day <= d));
//					
//					if(day <= d){
//						if(convertBelong.equals("1") && sdf.parse(kdclDate).compareTo(anotherDate) >= 0){
//							
//						}
//						if(convertBelong.equals("2")){
//							
//						}
//					}
					
					
//					log.info("***************************");
//				}
//			}
		} catch (Throwable e) {
			log.error("reduce error>>>>>> " + e);
		}
	}
	public void cleanup(Context context) {
		try {
			mysqlUtil.closeConnection();
		} catch (Throwable e) {
			convertWriteInfo.setLength(0);
			sql.setLength(0);
			log.error("reduce cleanup error>>>>>> " + e);
		}
	}
	
	
	
}
