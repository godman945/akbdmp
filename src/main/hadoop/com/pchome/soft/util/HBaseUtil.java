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
import org.springframework.stereotype.Component;



@Component
public class HBaseUtil {
	Log log = LogFactory.getLog(KafkaUtil.class);
	private Configuration config;
	HBaseAdmin admin = null;
	Configuration conf = HBaseConfiguration.create();
	public HBaseUtil() throws Exception {  
		conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "192.168.2.150,192.168.2.151,192.168.2.152");
		conf.set("hbase.zookeeper.property.clientPort", "3333");
		conf.set("hbase.master", "192.168.2.149:16010");
		conf.set("hbase.rpc.timeout", "10000");
		conf.set("hbase.client.scanner.timeout.period", "10000");
		conf.set("hbase.cells.scanned.per.heartbeat.check", "10000");
		conf.set("zookeeper.session.timeout", "10000");
		conf.set("phoenix.query.timeoutMs", "10000");
		conf.set("phoenix.query.keepAliveMs", "10000");
		conf.set("hbase.client.retries.number", "3");
		conf.set("hbase.client.pause", "1000");
		conf.set("zookeeper.recovery.retry", "1");
		
		
		File workaround = new File(".");         
		System.getProperties().put("hadoop.home.dir", workaround.getAbsolutePath());         
		new File("./bin").mkdirs();         
		new File("./bin/winutils.exe").createNewFile();
		conf = HBaseConfiguration.create(conf);
		Connection connection = ConnectionFactory.createConnection(conf);
		admin = (HBaseAdmin) connection.getAdmin();
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
	    		 HBaseUtil hbaseUtil = new HBaseUtil();
	    		 JSONObject json = new JSONObject();
	    		 System.out.println(hbaseUtil.getData("pacl_retargeting_prd", "0b4224b8-3427-4a82-a29d-3eee83de521f","type", "retargeting"));
//	    		 json.put("traceId001_prodId008", "2018-08-10");
//	    		 hbaseUtil.putData("pacl_retargeting","nico","type","retargeting",json.toString());
			} catch (Exception e) {
				e.printStackTrace();
				System.out.println(e);
			}  
	    }
	
	}