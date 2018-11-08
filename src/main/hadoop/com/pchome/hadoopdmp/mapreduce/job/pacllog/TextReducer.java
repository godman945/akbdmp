package com.pchome.hadoopdmp.mapreduce.job.pacllog;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Component;

import com.pchome.hadoopdmp.spring.config.bean.allbeanscan.SpringAllHadoopConfig;

@Component
public class TextReducer extends Reducer<Text, Text, Text, Text> {

	private static Log log = LogFactory.getLog(TextReducer.class);
	
	private StringBuffer sqlStringBuffer = null;
	
	private StringBuffer valueStringBuffer = null;
	
	private PreparedStatement preparedStatement = null;
	
	private Text keyOut = new Text();
	
	private Text valueOut = new Text();

	private Map<String,Object> pfpCodeConvertMap = new HashMap<String,Object>();
	
	private RedisTemplate<String, Object> redisTemplate = null;
	
	private Connection conn = null;
	
	private String url = null;
	
	private String jdbcDriver = null;
	
	private String user = null;
	
	private String password =  null;
	
	private int maxRangeDay = 0;
	
	
	public void setup(Context context) {
		try {
				String env = context.getConfiguration().get("spring.profiles.active");
				log.info(">>>>>> Reduce  setup>>>>>>>>>>>>>>env>>>>>>>>>>>>"+env);
				if(env.equals("prd")){
					
				}else{
					System.setProperty("spring.profiles.active", env);
					ApplicationContext ctx = new AnnotationConfigApplicationContext(SpringAllHadoopConfig.class);
					this.redisTemplate = (RedisTemplate<String, Object>) ctx.getBean("redisTemplate");
					String url = "jdbc:mysql://kddbdev.mypchome.com.tw:3306/akb_video";
					String jdbcDriver = "com.mysql.jdbc.Driver";
					String user = "keyword";
					String password =  "K1y0nLine";
					conn = DriverManager.getConnection(url, user, password);
				}
				sqlStringBuffer = new StringBuffer();
				valueStringBuffer = new StringBuffer();
		} catch (Throwable e) {
			log.error("reduce setup error>>>>>> " +e);
		}
	}

	public void reduce(Text mapperKey, Iterable<Text> mapperValue, Context context) {
		try {
			Iterator<Text> itr = mapperValue.iterator();
			int times = 0;
			while (itr.hasNext()) {
				itr.next();
				times = times + 1;
            }
			
			String reducekey = mapperKey.toString();
			String[] paclInfoArray = reducekey.split("<PCHOME>");
			String convId = paclInfoArray[0];
			String uuid = paclInfoArray[1];
			
			pfpCodeConvertMap = findCodeConvert(convId);
			if(pfpCodeConvertMap.size() > 0){
				float convert_price = (float) pfpCodeConvertMap.get("convert_price");
				int click_range_date = (int) pfpCodeConvertMap.get("click_range_date");
				int imp_range_date = (int) pfpCodeConvertMap.get("imp_range_date");
				String convert_num_type = (String) pfpCodeConvertMap.get("convert_num_type");
				int maxDay = (click_range_date >= imp_range_date) ? click_range_date : imp_range_date;
				if(maxRangeDay < maxDay){
					maxRangeDay = maxDay;
				}
				String convert_belong = (String) pfpCodeConvertMap.get("convert_belong");
				float price = convert_price * times;
				valueStringBuffer.setLength(0);
				keyOut.set(convId);
				valueStringBuffer.append("uuid_").append(uuid).append("<PCHOME>click_range_date_").append(click_range_date).append("<PCHOME>imp_range_date_").append(imp_range_date).append("<PCHOME>convert_belong_").append(convert_belong).append("<PCHOME>price_").append(price).append("<PCHOME>times_").append(times).append("<PCHOME>convert_num_type_").append(convert_num_type);
				valueOut.set(valueStringBuffer.toString());
				context.write(keyOut, valueOut);
			}
		} catch (Throwable e) {
			log.error("reduce error>>>>>> " +e);
		}
	}
	
	private Map<String,Object> findCodeConvert(String convertSeq) throws Exception{
		pfpCodeConvertMap.clear();
		sqlStringBuffer.setLength(0);
		sqlStringBuffer.append(" select * from pfp_code_convert where 1=1 and convert_seq = ? ");
		preparedStatement = conn.prepareStatement(sqlStringBuffer.toString());
		preparedStatement.setString(1, convertSeq);
		ResultSet rs = preparedStatement.executeQuery();
		while (rs.next()) {
			String convert_status = rs.getString("convert_status");
			if(convert_status.equals("1")){
				int click_range_date = rs.getInt("click_range_date");
				int imp_range_date = rs.getInt("imp_range_date");
				String convert_belong = rs.getString("convert_belong");
				float convert_price = rs.getFloat("convert_price");
				String convert_num_type = rs.getString("convert_num_type");
				pfpCodeConvertMap.put("click_range_date", click_range_date);
				pfpCodeConvertMap.put("imp_range_date", imp_range_date);
				pfpCodeConvertMap.put("convert_belong", convert_belong);
				pfpCodeConvertMap.put("convert_price", convert_price);
				pfpCodeConvertMap.put("convert_num_type", convert_num_type);
				
			}
		}
		preparedStatement.close();
		rs.close();
		return pfpCodeConvertMap;
	}
	
	public void cleanup(Context context) {
		try{
			redisTemplate.opsForValue().set("pacl_conv_max_rangeday", String.valueOf(maxRangeDay), 4,TimeUnit.HOURS);
			conn.close();
		} catch (Exception e) {
			log.error("reduce cleanup error>>>>>> " + e);
		}
	}
	
	public static void main(String[] args) throws Exception {
		ApplicationContext ctx = new AnnotationConfigApplicationContext(SpringAllHadoopConfig.class);
		RedisTemplate redisTemplate = (RedisTemplate<String, Object>) ctx.getBean("redisTemplate");
		System.out.println(redisTemplate.opsForValue().get("pacl_conv_max_rangeday"));
	}
}
