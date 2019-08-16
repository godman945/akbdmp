package com.pchome.hadoopdmp.mapreduce.job.dmplog;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.StringTokenizer;

import org.apache.commons.logging.Log;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text; 
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.log4j.Logger; 
 
public class TokenizerMapper extends Mapper<LongWritable, Text, Text, Text> {
 
    private final static IntWritable one = new IntWritable(1); 
    private Text word = new Text(); 
    private static Logger log = Logger.getLogger(TokenizerMapper.class);
    private static String kdclSymbol = String.valueOf(new char[] { 9, 31 });
    private static String[] values  = null;
    private static net.minidev.json.JSONObject json = new net.minidev.json.JSONObject();
    
    private static int count = 0;
    public synchronized void map(LongWritable offset, Text value, Context context)  {
    	try {
    		System.out.println(value.toString());
    		
    		json.put("alex", "5555");
    		
    		System.out.println(json.get("alex"));
    		this.values = value.toString().split(kdclSymbol,-1);
    		if(count == 0) {
    			for (String string : values) {
    				System.out.println(string);
    			}
    			count = count + 1;
    		}
    		System.out.println("-----");
//    		String value2 = new String(value.getBytes(), 0, value.getLength(), "utf-8");	
//    		System.out.println(value2);
    	}catch(Exception e) {
    		e.printStackTrace();
    	}
    	

    	
    	
//    	log.info(value.toString());
    	
//        StringTokenizer itr = new StringTokenizer(value.toString()); 
//        while (itr.hasMoreTokens()) { 
//            word.set(itr.nextToken()); 
//            context.write(word, one); 
//        } 
    } 
} 