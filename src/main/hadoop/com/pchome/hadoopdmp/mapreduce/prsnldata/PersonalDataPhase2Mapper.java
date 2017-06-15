package com.pchome.hadoopdmp.mapreduce.prsnldata;


import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.pchome.hadoopdmp.enumerate.EnumPersonalDataPhase2Job;
import com.pchome.hadoopdmp.factory.job.AncestorJob;
import com.pchome.hadoopdmp.factory.job.FactoryPersonalDataPhase2;


public class PersonalDataPhase2Mapper extends Mapper<LongWritable, Text, Text, Text> {

	private static String SYMBOL = String.valueOf(new char[]{9, 31});
	private Log log = LogFactory.getLog(this.getClass());

	private Text keyOut = new Text();
	private Text valueOut = new Text();

	public static String record_date;
	public static Map<String,combinedValue> clsfyCraspMap = new HashMap<String,combinedValue>();

	@Override
	public void setup(Context context) {
		record_date = context.getConfiguration().get("job.date");
		//log.info("record_date: " + record_date);

		// read data : ClsfyGndAgeCrspTable.txt
		try{
			Configuration conf = context.getConfiguration();
	        org.apache.hadoop.fs.Path[] path = DistributedCache.getLocalCacheFiles(conf);
	        log.info("ClsfyGndAgeCrspTable path[0]:" + path[0].toString());
	        Path clsfyTable = Paths.get(path[0].toString());
			Charset charset = Charset.forName("UTF-8");
			List<String> lines = Files.readAllLines(clsfyTable, charset);
			for (String line : lines) {
				String[] tmpStrAry = line.split(";");		//0001000000000000;M,35
				String[] tmpStrAry2 = tmpStrAry[1].split(",");
				clsfyCraspMap.put(
					tmpStrAry[0],
					new combinedValue(
						tmpStrAry[1].split(",")[0],
						tmpStrAry2.length>1?tmpStrAry2[1]:""
						)
					);
			}
			log.info("ClsfyGndAgeCrspTable map size:" + clsfyCraspMap.size());
		}catch(Exception e) {
			log.error("ClsfyGndAgeCrspTable error:\n" + e.getMessage());
//			e.printStackTrace();
//			log.error("ClsfyGndAgeCrspTable error:\n" + e.pr);
		}
	}

	@Override
	public void map(LongWritable offset, Text value, Context context) {

//		log.info("value=" + value);

		if (StringUtils.isBlank(value.toString())) {
			log.info("value is blank");
			return;
		}

		String[] values = value.toString().split(SYMBOL);

		AncestorJob job = null;
		String key = null;
		String val = null;

		for (EnumPersonalDataPhase2Job enumPersonalDataPhase2Job: EnumPersonalDataPhase2Job.values()) {
			try {
				job = FactoryPersonalDataPhase2.getInstance( enumPersonalDataPhase2Job );

				key = job.getKey(values);
				val = job.getValue(values);

//				log.info("key:" + key + "  value:" + val);

				if (StringUtils.isBlank(key)) {
					log.info("key is blank");
					continue;
				}
				if (StringUtils.isBlank(val)) {
					log.info("val is blank");
					continue;
				}

				keyOut.set(key);
				valueOut.set(val);

				context.write(keyOut, valueOut);

			} catch (Exception e) {
				log.error(value, e);
			}

		}

	}

	public class combinedValue {
		public String gender;
		public String age;

		public combinedValue(String gender, String age) {
			this.gender = gender;
			this.age = age;
		}
	}
}
