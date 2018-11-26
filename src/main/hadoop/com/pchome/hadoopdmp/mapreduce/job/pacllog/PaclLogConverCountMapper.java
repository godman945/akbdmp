package com.pchome.hadoopdmp.mapreduce.job.pacllog;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.springframework.stereotype.Component;

@Component
public class PaclLogConverCountMapper extends Mapper<LongWritable, Text, Text, Text> {
	Log log = LogFactory.getLog("DmpLogMapper");
	
	private Text keyOut = new Text();

	private static String paclSymbol = String.valueOf(new char[] { 9, 31 });
	
	@Override
	public void setup(Context context) {
		log.info(">>>>>> Mapper  setup >>>>>>>>>>>>>>env>>>>>>>>>>>>"+context.getConfiguration().get("spring.profiles.active"));
		try {
			
		} catch (Exception e) {
			log.error("Mapper setup error>>>>>> " +e);
		}
	}

	@Override
	public void map(LongWritable offset, Text value, Context context) {
		try {
			//讀取kdcl、Campaign資料
			log.info("raw_data : " + value);
			String valueStr = value.toString();
			String arrayData[] = valueStr.split(paclSymbol);
			log.info("arrayData size : " + arrayData.length);
			String uuid = arrayData[2];
			String type = arrayData[11];
			String convId = arrayData[12];
			String rouleId = arrayData[13];
			if(type.equals("convert")){
				keyOut.set(convId+"_"+uuid);
				context.write(keyOut, new Text(rouleId.replace(";", "")));
			}
		} catch (Exception e) {
			log.error("Mapper error>>>>>> " +e); 
		}
	}
}
