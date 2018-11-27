package com.pchome.hadoopdmp.mapreduce.job.pacllog;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.springframework.stereotype.Component;

@Component
public class PaclLogConverCountMapper extends Mapper<LongWritable, Text, Text, Text> {
	Log log = LogFactory.getLog("DmpLogMapper");
	
	private Text keyOut = new Text();

	private static String paclSymbol = String.valueOf(new char[] { 9, 31 });
	
	private static SimpleDateFormat sdf =  new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	
	private StringBuffer mapperValue = new StringBuffer();
	
	private static Calendar calendar = null;
	@Override
	public void setup(Context context) {
		log.info(">>>>>> Mapper  setup >>>>>>>>>>>>>>env>>>>>>>>>>>>"+context.getConfiguration().get("spring.profiles.active"));
		try {
			calendar = Calendar.getInstance();
			calendar.setTime(new Date());
			calendar.add(Calendar.DAY_OF_MONTH, -1); 
		} catch (Exception e) {
			log.error("Mapper setup error>>>>>> " +e);
		}
	}

	@Override
	public void map(LongWritable offset, Text value, Context context) {
		mapperValue.setLength(0);
		try {
			InputSplit inputSplit=(InputSplit)context.getInputSplit(); 
			String filename = ((FileSplit)inputSplit).getPath().getName();
			String valueStr = value.toString();
			String arrayData[] = valueStr.split(paclSymbol);
			
//			log.info("Path:"+((FileSplit)inputSplit).getPath());
//			log.info("filename:"+filename);
			if(filename.equals("pacl_2018_11_26.txt.lzo")){
//				log.info("raw_data : " + valueStr);
//				log.info("arrayData size : " + arrayData.length);
				String uuid = arrayData[2];
				String type = arrayData[11];
				String convId = arrayData[12];
				String rouleId = arrayData[13];
				if(type.equals("convert")){
					keyOut.set(convId+"_"+uuid);
					context.write(keyOut, new Text(rouleId.replace(";", "")));
				}
			}else{
				if(filename.contains("kdcl")){
					log.info(">>>>>>kdcl log");
//					log.info("raw_data : " + value);
//					log.info("arrayData size : " + arrayData.length);
					String date = arrayData[0];
					String uuid = arrayData[2];
					String type = arrayData[13];
					String adSeq = arrayData[16];
//					log.info(">>>>>>date:"+date);
//					log.info(">>>>>>uuid:"+uuid);
//					log.info(">>>>>>type:"+type);
					keyOut.set(uuid);
					mapperValue.append(date).append(",").append(adSeq).append(",").append(type).append(",").append(filename);
					context.write(keyOut, new Text(mapperValue.toString()));
				}else{
					log.info(">>>>>>conv log");
//					log.info("raw_data : " + value);
//					log.info("arrayData size : " + arrayData.length);
					String uuid = arrayData[0];
					String clickRangeDate = arrayData[1];
					String impRangeDate = arrayData[2];
					String convertPriceCount = arrayData[3];
					String convertPric = arrayData[4];
					String convertBelong = arrayData[5];
					String convertSeq = arrayData[6];
					
					mapperValue.append(clickRangeDate).append(",").append(impRangeDate).append(",").append(convertPriceCount);
					mapperValue.append(",").append(convertPric).append(",").append(convertBelong).append(",").append(convertSeq).append(",").append(filename);
					keyOut.set(uuid);
					context.write(keyOut, new Text(mapperValue.toString()));
					
				}
			}
		} catch (Exception e) {
			log.error("Mapper error>>>>>> " +e); 
		}
	}
}
