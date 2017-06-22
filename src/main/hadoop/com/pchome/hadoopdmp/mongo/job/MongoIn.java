package com.pchome.hadoopdmp.mongo.job;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.bson.BSONObject;

import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.hadoop.MongoInputFormat;
import com.mongodb.hadoop.MongoOutputFormat;
import com.mongodb.hadoop.io.BSONWritable;
import com.mongodb.hadoop.util.MongoConfigUtil;

public class MongoIn {

	public static class MyMapper extends Mapper<Object, BSONObject, Text, Text> {
		public void map(Object key, BSONObject value, Context context) throws IOException, InterruptedException {
			String uuid = value.get("url").toString();
//			String behavior = value.get("behavior").toString();
//			String record_date = value.get("record_date").toString();
//			String date = value.get("date").toString();
//			String time = value.get("time").toString();
//			String ip = value.get("ip").toString();
//			String output = behavior+"     "+record_date;
			context.write(new Text(uuid), new Text(uuid));
		}
	}
	
	public class MyReducer extends Reducer<Text, Text, Text, BSONWritable> {
		
		public void reduce(Text key, Iterable<BSONWritable> values, Context context) throws IOException, InterruptedException {
			
//			MongoClient mongo = new MongoClient( "mgodev.mypchome.com.tw" , 27017 );
//			DB db = mongo.getDB("dmp");
			
//			DBCollection table = db.getCollection("hadoop");
//			BasicDBObject document = new BasicDBObject();
//			document.put("uuid", 123);
//			document.put("content", 456);
//			table.insert(document);
			
//			 DBObject builder = new BasicDBObjectBuilder().start()
//		                .add("uuid", values).get();
			
			 
           BSONObject outDoc = new BasicDBObjectBuilder().start()
        		              .add("url", values)
	                          .get();
	       BSONWritable pkeyOut = new BSONWritable(outDoc);
	       context.write(key, pkeyOut); 
			 
			
//			output.put("values", values);
//			context.write(key, new BSONWritable(output));
			
//			BSONWritable bsonWritable = new BSONWritable();  
//	        BasicDBObject document = new BasicDBObject();  
//	        document.put("_id",new ObjectId());  
//	        document.put("1","1");  
//	        bsonWritable.setDoc(document);  
//	        context.write(key, bsonWritable);  
		}
		
	}
	

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.set("mongo.input.uri", "mongodb://192.168.1.37:27017/pcbappdev.class_url");
//        conf.set("mongo.output.uri", "mongodb://webuser:axw2mP1i@192.168.1.37:27017/dmp.hadoop_out");
        
//		MongoConfigUtil.setInputURI(conf, "mongodb://webuser:axw2mP1i@192.168.1.37:27017/dmp.hadoop_out");
//		MongoConfigUtil.setInputURI(conf, "mongodb://webuser:axw2mP1i@192.168.1.37:27017/dmp.class_count");
		MongoConfigUtil.setCreateInputSplits(conf, false);
		Job job = new Job(conf, "MongoIN-0622");
		
//		MongoConfigUtil.setOutputURI(conf, "mongodb://webuser:axw2mP1i@192.168.1.37:27017/dmp.hadoop");
		
		
		job.setJarByClass(MongoIn.class);
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
//		job.setOutputValueClass(BSONWritable.class);
		
		job.setInputFormatClass(MongoInputFormat.class);
//		job.setOutputFormatClass(MongoOutputFormat.class);
		
		job.setOutputFormatClass(TextOutputFormat.class);
		/* 輸出資料的HDFS路徑 */
		FileOutputFormat.setOutputPath(job, new Path("/home/webuser/bessie/bessie_file"));
		
		
		job.setNumReduceTasks(1);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}