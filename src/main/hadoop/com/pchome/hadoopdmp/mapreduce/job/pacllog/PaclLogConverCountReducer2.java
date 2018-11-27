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
public class PaclLogConverCountReducer2 extends Reducer<Text, Text, Text, Text> {

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
			log.info(">>>>>>key1:"+key);
			boolean flagKdcl = false;
			boolean flagPart = false;
			for (Text text : mapperValue) {
				String value = text.toString();
				log.info(">>>>>1:"+value.toString());
				if(value.contains("kdcl")){
					flagKdcl = true;
				}
				if(value.contains("part-r-00000")){
					flagPart = true;
				}
			}
			
			if(flagKdcl && flagPart){
				log.info("key:"+key);
				for (Text text : mapperValue) {
					log.info(">>>>>:"+text.toString());
				}
			}
			
			
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
	
	
	
}
