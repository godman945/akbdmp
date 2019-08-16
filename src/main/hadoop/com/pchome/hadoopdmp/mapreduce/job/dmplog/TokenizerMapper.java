package com.pchome.hadoopdmp.mapreduce.job.dmplog;

import java.io.IOException; 
import java.util.StringTokenizer; 
 
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text; 
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context; 
 
public class TokenizerMapper extends 
        Mapper<Object, Text, Text, IntWritable> { 
 
    private final static IntWritable one = new IntWritable(1); 
    private Text word = new Text(); 
 
    public synchronized void map(LongWritable offset, Text value, Context context) {
    	
    	System.out.println(value.toString());
    	
    	
    	
//        StringTokenizer itr = new StringTokenizer(value.toString()); 
//        while (itr.hasMoreTokens()) { 
//            word.set(itr.nextToken()); 
//            context.write(word, one); 
//        } 
    } 
} 