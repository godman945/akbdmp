package com.pchome.hadoopdmp.mapreduce.prsnldata;


import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.pchome.hadoopdmp.enumerate.EnumPersonalDataPhase1Job;
import com.pchome.hadoopdmp.factory.job.AncestorJob;
import com.pchome.hadoopdmp.factory.job.FactoryPersonalData;

public class PersonalDataMapper extends Mapper<LongWritable, Text, Text, Text> {

//	private static int LOG_LENGTH = 30;
	private static String SYMBOL = String.valueOf(new char[]{9, 31});
	private Log log = LogFactory.getLog(this.getClass());

	private Text keyOut = new Text();
	private Text valueOut = new Text();

	public static String record_date;

	@Override
	public void setup(Context context) {
		record_date = context.getConfiguration().get("job.date");
		//log.info("record_date: " + record_date);
	}

	@Override
	public void map(LongWritable offset, Text value, Context context) {

//		log.info("value=" + value);

		if (StringUtils.isBlank(value.toString())) {
			log.info("value is blank");
			return;
		}

		String[] values = value.toString().split(SYMBOL);
//		if (values.length < LOG_LENGTH) {
//			log.info("values.length < " + LOG_LENGTH);
//			return;
//		}

		AncestorJob job = null;
		String key = null;
		String val = null;

		for (EnumPersonalDataPhase1Job enumPersonalDataPhase1Job: EnumPersonalDataPhase1Job.values()) {
			try {
				job = FactoryPersonalData.getInstance( enumPersonalDataPhase1Job );

				key = job.getKey(values);
				val = job.getValue(values);

				if (StringUtils.isBlank(key)) {
//					log.info("key is blank");
					continue;
				}
				if (StringUtils.isBlank(val)) {
//					log.info("val is blank");
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

}
