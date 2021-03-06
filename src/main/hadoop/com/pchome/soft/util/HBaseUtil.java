package com.pchome.soft.util;

import java.io.File;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.json.JSONObject;



public class HBaseUtil {
	
	private static HBaseUtil singleton = new HBaseUtil();
	
	private static Log log = LogFactory.getLog("HBaseUtil");
	
	HBaseAdmin admin = null;
	
	Configuration conf = HBaseConfiguration.create();
	
	HBaseUtil() {
		try{
			
			log.info("**********HBASE INIT 2*********");
			
			conf = HBaseConfiguration.create();
			conf.set("hbase.zookeeper.quorum", "192.168.2.150,192.168.2.151,192.168.2.152");
			conf.set("hbase.zookeeper.property.clientPort", "3333");
			conf.set("hbase.master", "192.168.2.149:16010");   
//			File workaround = new File(".");         
//			System.getProperties().put("hadoop.home.dir", workaround.getAbsolutePath());         
			conf = HBaseConfiguration.create(conf);
			Connection connection = ConnectionFactory.createConnection(conf);
			admin = (HBaseAdmin) connection.getAdmin();
		}catch(Exception e){
			log.error(e.getMessage());
		}
	}
	
	synchronized static public HBaseUtil getInstance() {
		log.info("**********HBASE INIT 1*********");
		return singleton;
	}
	
	
	 /**
	  * 刪除資料表
	  * */
	 public void deleteTable(String tableName) throws Exception {
		 log.info("delete hbase table:"+tableName);
		 if(admin.tableExists(tableName)){
			 admin.disableTable(tableName);
			 admin.deleteTable(tableName);
		 }
	 }
	 
	 /**
	  * 建立資料表
	  * */
	 public void creatTable(String tableName,String[] family) throws Exception {
		 log.info("create hbase table:"+tableName);
		 if(!admin.tableExists(tableName)){
			 HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
			 for (int i = 0; i < family.length; i++) {
				 hTableDescriptor.addFamily(new HColumnDescriptor(family[i]));
		     }
			 System.out.println(hTableDescriptor.getFamilies());
			 admin.createTable(hTableDescriptor);
			 System.out.println("create table Success!");
		 }
		 
	 }  
	 
	 public void putData(String tableName,String rowKey,String family,String qualifier,String value) throws Exception{
		 HTable table = new HTable(conf, Bytes.toBytes(tableName));
		 int region = Math.abs(rowKey.hashCode()) % 10;
		 rowKey = "0"+region+"|"+rowKey;
		 Get get = new Get(Bytes.toBytes(rowKey));
		 get.addColumn(Bytes.toBytes("type"), Bytes.toBytes("retargeting"));
		 Result result = table.get(get);
		 String row = Bytes.toString(result.getRow());
		 
		 //row存在更新value否則建立一筆新row
		 if(row != null){
			 JSONObject hbaseValueJson = new JSONObject(Bytes.toString(result.getValue(family.getBytes(), qualifier.getBytes())));
			 JSONObject logJsonKey = new JSONObject(value);
			 Iterator<String> keys = logJsonKey.keys();
			 while (keys.hasNext()) {
				 String logkey = (String)keys.next();
				 String logValue = logJsonKey.getString(logkey);
				 if(hbaseValueJson.has(logkey)){
					 hbaseValueJson.put(logkey, logValue);
				 }else{
					 hbaseValueJson.put(logkey, logValue);
				 }
			 }
			 Put put = new Put(Bytes.toBytes(rowKey));
			 put.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes.toBytes(hbaseValueJson.toString()));
			 table.put(put);
		 }else{
			 Put put = new Put(Bytes.toBytes(rowKey));
			 put.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes.toBytes(value));
			 table.put(put);
		 }
	 }
	 
	 public JSONObject getData(String tableName,String rowKey,String family,String qualifier) throws Exception{
		 HTable table = new HTable(conf, Bytes.toBytes(tableName));
		 int region = Math.abs(rowKey.hashCode()) % 10;
		 rowKey = "0"+region+"|"+rowKey;
		 Get get = new Get(Bytes.toBytes(rowKey));
		 get.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier));
		 Result result = table.get(get);
		 String row = Bytes.toString(result.getRow());
		 if(row == null){
			 return null;
		 }else{
			 log.info("HBASE:"+Bytes.toString(result.getValue(family.getBytes(), qualifier.getBytes())));
			 return new JSONObject(Bytes.toString(result.getValue(family.getBytes(), qualifier.getBytes())));
		 }
	 }
	 
	 
	 
	 public void scanAll(String tableName) throws Exception{
		 HTable table = new HTable(conf, Bytes.toBytes(tableName));
		 Scan scan = new Scan();
		 ResultScanner rs = table.getScanner(scan);
            for (Result r : rs) {
                for (KeyValue kv : r.list()) {
                    log.info("row:" + Bytes.toString(kv.getRow()));
                    log.info("family:" + Bytes.toString(kv.getFamily()));
                    log.info("qualifier:"  + Bytes.toString(kv.getQualifier()));
                    log.info("value:" + Bytes.toString(kv.getValue()));
                    log.info("timestamp:" + kv.getTimestamp());
                    log.info("-------------------------------------------");
                }
            }
	 }
	 
	 
	    public static void main(String args[]){
	    	 try {
	    		 HBaseUtil hbaseUtil = HBaseUtil.getInstance();
	    		 
	    		 JSONObject json = new JSONObject();
	    		 json.put("traceId001_prodId008", "2018-08-10");
	    		 hbaseUtil.putData("pacl_retargeting","alex","type","retargeting",json.toString());
	    		 
	    		 
	    		 
	    		 org.json.JSONObject hbaseValue = hbaseUtil.getData("pacl_retargeting", "alex", "type", "retargeting");
	    		 System.out.println(hbaseValue);
	    		 
	    		 
	    		 
			} catch (Exception e) {
				e.printStackTrace();
				System.out.println(e);
			}  
	    }
	
	}