package com.pchome.hadoopdmp.mongo.job;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.bson.BSONObject;

import com.mongodb.hadoop.MongoInputFormat;
import com.mongodb.hadoop.MongoOutputFormat;
import com.mongodb.hadoop.util.MongoConfigUtil; 
 
public class mongo { 
 
    private static final Log log = LogFactory.getLog( mongo.class ); 
 
    public static class TokenizerMapper extends Mapper<Object, BSONObject, Text, IntWritable> { 
 
        private final static IntWritable one = new IntWritable( 1 ); 
        private final Text word = new Text(); 
 
        public void map( Object key, BSONObject value, Context context ) throws IOException, InterruptedException{ 
 
//            System.out.println( "key: " + key ); 
//            System.out.println( "value: " + value ); 
 
            final StringTokenizer itr = new StringTokenizer( value.get( "uuid" ).toString() ); 
            while ( itr.hasMoreTokens() ){ 
                word.set( itr.nextToken() ); 
                context.write( word, one ); 
            } 
        } 
    } 
 
    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> { 
 
        private final IntWritable result = new IntWritable(); 
 
        public void reduce( Text key, Iterable<IntWritable> values, Context context ) 
                throws IOException, InterruptedException{ 
 
            int sum = 0; 
            for ( final IntWritable val : values ){ 
                sum += val.get(); 
            } 
            result.set( sum ); 
            context.write( key, result ); 
        } 
    } 
 
    public static void main( String[] args ) throws Exception{ 
 
        final Configuration conf = new Configuration(); 
        MongoConfigUtil.setInputURI( conf, "mongodb://webuser:axw2mP1i@192.168.1.37:27017/dmp.class_count" ); 
        MongoConfigUtil.setOutputURI( conf, "mongodb://webuser:axw2mP1i@192.168.1.37:27017/dmp.hadoop" ); 
        
        System.out.println( "Conf: " + conf ); 
 
        final Job job = new Job( conf, "mongo" ); 
 
        job.setJarByClass( mongo.class ); 
 
        job.setMapperClass( TokenizerMapper.class ); 
 
        job.setCombinerClass( IntSumReducer.class ); 
        job.setReducerClass( IntSumReducer.class ); 
 
        job.setOutputKeyClass( Text.class ); 
        job.setOutputValueClass( IntWritable.class ); 
 
        job.setInputFormatClass( MongoInputFormat.class ); 
        job.setOutputFormatClass( MongoOutputFormat.class ); 
 
        System.exit( job.waitForCompletion( true ) ? 0 : 1 ); 
    } 
}
