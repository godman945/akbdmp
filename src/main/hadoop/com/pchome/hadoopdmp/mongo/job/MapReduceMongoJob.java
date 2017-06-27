package com.pchome.hadoopdmp.mongo.job;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.bson.BSONObject;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

import com.mongodb.hadoop.MongoInputFormat;
import com.mongodb.hadoop.util.MongoConfigUtil;
import com.pchome.hadoopdmp.data.mysql.pojo.AdmCategory;
import com.pchome.hadoopdmp.data.mysql.pojo.AdmCategoryGroup;
import com.pchome.hadoopdmp.mysql.db.service.category.IAdmCategoryGroupService;
import com.pchome.hadoopdmp.spring.config.bean.allbeanscan.SpringAllHadoopConfig;
public class MapReduceMongoJob {
	private static Log log = LogFactory.getLog("MapReduceMongoJob");
	
	
	public static class ReadWeblogsFromMongo extends Mapper<Object, BSONObject, Text, Text> {
		
		private IAdmCategoryGroupService admCategoryGroupService;
		
		Map<String,String> categoryMap = new HashMap<>();
		
		public void setup(Context context) {
			try {
				log.info(">>>>>> setup:");
				
				
				System.setProperty("spring.profiles.active", "stg");
				ApplicationContext ctx = new AnnotationConfigApplicationContext(SpringAllHadoopConfig.class);
				admCategoryGroupService = ctx.getBean(IAdmCategoryGroupService.class);
			
				List<AdmCategoryGroup> admGroupList = admCategoryGroupService.loadAll();
				for (AdmCategoryGroup admCategoryGroup : admGroupList) {
					Set<AdmCategory> admCategorySet = admCategoryGroup.getAdmCategories();
					String key = "";
					List<AdmCategory> admCategoryList = new ArrayList<>(admCategorySet);
					int admCategorySize = admCategoryList.size();
					for (AdmCategory admCategory : admCategoryList) {
						if(admCategoryList.indexOf(admCategory) == admCategorySize){
							key = key + admCategory.getAdClass();
						}else{
							key = key + admCategory.getAdClass()+"_";
						}
					}
					
					if(StringUtils.isNotBlank(key)){
						categoryMap.put(key, admCategoryGroup.getGroupId()+"_TOTAL");
					}
				}
			
				log.info(">>>>>> categoryMap:"+categoryMap);
			} catch (Exception e) {
				log.error(">>>>> mapper e : " + e.getMessage());
			}
			
		}
		
		public void map(Object key, BSONObject value, Context context) throws IOException, InterruptedException {
			try {
				String update_date = value.get("update_date").toString();
				String category_info_str = value.get("category_info").toString();
				String user_id = value.get("user_id").toString();
				Map<String, Object> user_info = (Map<String, Object>) value.get("user_info");
				List<Map<String, Object>> category_info = (List<Map<String, Object>>) value.get("category_info");
				String userType = user_info.get("type").toString();
				Map<String, Set<String>> allMap = new HashMap<String, Set<String>>();

				for (Map<String, Object> category : category_info) {
					String ad_class = category.get("category").toString();
					String categoryKey = ad_class + "_" + userType.toUpperCase();
					context.write(new Text(categoryKey), new Text());

					//process parent
//					for (Entry<String, String> entry : categoryMap.entrySet()) {
//						log.info(">>>>>> entry_key:"+entry.getKey());
//						log.info(">>>>>> entry_value:"+entry.getValue());
//						
//						if(entry.getKey().contains(ad_class)){
//							context.write(new Text(entry.getValue()), new Text(user_id));
//						}
//					}
				}
				
//				Set<String> data = new HashSet<>();
//				for (Map.Entry<String, Set<String>> entry : allMap.entrySet()) {
//					if (entry.equals("0015022500000000")) {
//						data.addAll(entry.getValue());
//					}
//					if (entry.equals("0015022720350000")) {
//						data.addAll(entry.getValue());
//					}
//				}
//				context.write(new Text("0015022500000000_0015022720350000_TOTAL"),	new Text(String.valueOf(data.size())));

			} catch (Exception e) {
				log.error(">>>>> mapper e : " + e.getMessage());
			}
		}
	}

	public static class MyReducer extends Reducer<Text, Text, Text, Text> {
		private static Set<String> data = new HashSet<>();
		
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			try {
				data.clear();
				
				if (key.toString().indexOf("TOTAL") > 0) {
					
					String [] array = key.toString().split("_");
					String parentKey = array[0];
					int sum = 0;
					for (Text text : values) {
						data.add(text.toString());
						sum = sum + 1;
					}
					
					log.info(">>>>> reduce key: " + key);
					log.info(">>>>> reduce dataSize: " + data.size());
					log.info(">>>>> reduce sum: " + sum);
					context.write(new Text(parentKey), new Text(String.valueOf(data.size())));
				} else {
					int sum = 0;
					for (Text text : values) {
						sum = sum + 1;
					}
					log.info(">>>>> reduce key: " + key);
					log.info(">>>>> reduce sum: " + sum);
					context.write(key, new Text(String.valueOf(sum)));
				}
			} catch (Exception e) {
				log.error(">>>>> mapper e : " + e.getMessage());
			}
		}
	}

	public static void main(String[] args) throws Exception {
		final Configuration conf = new Configuration();
		MongoConfigUtil.setInputURI(conf, "mongodb://192.168.1.37:27017/pcbappdev.class_count");
		// conf.set("mongo.input.query",
		// "{'update_date':{'$gt':{'$date':'2017-06-01 23:59:59'}}}");
		// conf.set("mongo.input.query", "{'update_date':{'$gt':'2017-06-19
		// 23:59:59'}}");
		MongoConfigUtil.setCreateInputSplits(conf, false);

		System.out.println("Configuration: " + conf);
		final Job job = new Job(conf, "alex_test");
		Path out = new Path("/home/webuser/alex/mongo");
		FileOutputFormat.setOutputPath(job, out);
		job.setJarByClass(MapReduceMongoJob.class);
		job.setMapperClass(ReadWeblogsFromMongo.class);
		job.setReducerClass(MyReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setInputFormatClass(MongoInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setNumReduceTasks(1);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
//		new ReadWeblogsFromMongo().setup(null);
	}
}