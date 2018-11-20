package com.pchome.hadoopdmp.mapreduce.job.pacllog;

import java.sql.ResultSet;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.kafka.clients.producer.Producer;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Component;

import com.pchome.hadoopdmp.spring.config.bean.allbeanscan.SpringAllHadoopConfig;
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
			String convertSeq = mapperKey.toString();
			log.info(">>>>>>>>>convertSeq:"+convertSeq);
			ResultSet resultSet = mysqlUtil.query(" select * from pfp_code_convert_rule where 1 = 1 and convert_seq = '"+convertSeq+"' ");
			while(resultSet.next()){
				log.info(">>>>>>"+resultSet.getString("convert_rule_id"));
				log.info(">>>>>>"+resultSet.getString("convert_rule_way"));
				log.info(">>>>>>"+resultSet.getString("convert_rule_value"));
			}
			log.info("--------------");
			for (Text text : mapperValue) {
				log.info(">>>>>>>>>"+text.toString());
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
		System.setProperty("spring.profiles.active", "stg");
		ApplicationContext ctx = new AnnotationConfigApplicationContext(SpringAllHadoopConfig.class);
		
		
		
		
		
		 String text ="a,a,b,c,a";

		 int count = 0;
		 int start = 0;
		 String sub = "a";
		 while((start = text.indexOf(sub,start))>=0){
	            start += sub.length();
	            count ++;
		 }
		 System.out.println(count);
	}
}
