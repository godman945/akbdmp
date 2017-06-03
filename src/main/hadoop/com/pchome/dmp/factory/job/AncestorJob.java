package com.pchome.dmp.factory.job;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.kafka.clients.producer.Producer;
import org.codehaus.jettison.json.JSONObject;
import org.springframework.stereotype.Component;

import com.mongodb.DBObject;

@Component
public abstract class AncestorJob {

	protected Log log = LogFactory.getLog(this.getClass());
//	protected List<DBObject> list = new ArrayList<DBObject>();
	public List<DBObject> list = new ArrayList<DBObject>();

	protected final static String SYMBOL = String.valueOf(new char[]{9, 31});
	protected final static int LIMIT = 0;

	public abstract String getKey(String[] values);
	public abstract String getValue(String[] values);

	public abstract Object getObject(String keys[], Iterable<Text> values);

	public abstract void add(String[] keys, Iterable<Text> values);
	
	public abstract void update();

	public abstract int insert() throws Exception;

	public Set<String> outputCollector = new HashSet<String>();

	public List<String> outputCollectorList = new ArrayList<String>();

	public Map<String,Integer> dailyAddedAmount = new ConcurrentHashMap<String,Integer>();
	
	public static int memCatedCnt = 0;
	public static int memUnCatedCnt = 0;
}

