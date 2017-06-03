package com.pchome.dmp.dao.sql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class KdclStatisticsSourceDAO {

	private static Log log = LogFactory.getLog(KdclStatisticsSourceDAO.class);
	SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

	Connection conn = null;
	String url = "jdbc:mysql://dmpadm.mypchome.com.tw:3306/dmp";	//prd
//	String url = "jdbc:mysql://dmpstg.mypchome.com.tw:3306/dmp";	//stg
	String jdbcDriver = "com.mysql.jdbc.Driver";
	String user = "webuser";
	String password = "7e5nL0H";
	Statement stmt=null;
	String sql="";

	public KdclStatisticsSourceDAO() { }

	public void deleteByBehaviorAndRecordDate(String behavior,String recordDate){

		try{
			sql = "delete from `kdcl_statistics_source` where behavior='" + behavior + "' and record_date='" + recordDate + "'";
//			log.info("deleteByBehaviorAndRecordDate SQL String:" + sql);
			stmt.executeUpdate(sql);
		}catch(Exception e){
			log.error(e.getMessage());
		}
	}

	public void delete(String id_type, String service_type, String behavior, String classify, String recordDate){

		try{
			String sql = "DELETE FROM kdcl_statistics_source WHERE id_type=@@ AND service_type=@@ AND behavior=@@ AND classify=@@ AND record_date=@@";
			sql = sql.replaceFirst("@@", new StringBuffer().append("'").append(id_type).append("'").toString());
			sql = sql.replaceFirst("@@", new StringBuffer().append("'").append(service_type).append("'").toString());
			sql = sql.replaceFirst("@@", new StringBuffer().append("'").append(behavior).append("'").toString());
			sql = sql.replaceFirst("@@", new StringBuffer().append("'").append(classify).append("'").toString());
			sql = sql.replaceFirst("@@", new StringBuffer().append("'").append(recordDate).append("'").toString());
			log.info("sql(del):" + sql);
			stmt.executeUpdate(sql);
		}catch(Exception e){
			log.error(e.getMessage());
		}
	}

	public void insert(String id_type, String service_type, String behavior, String classify, int count, String recordDate){

		try{
			sql = "insert `kdcl_statistics_source` values(null,'" +
					id_type + "','" +
					service_type + "','" +
					behavior + "','" +
					classify + "'," +
					count + ",'" +
					recordDate + "'," +
					"null" + ",'" +
					sdf.format(new Date()) + "')";
//			log.info("insert SQL String:" + sql);
			stmt.executeUpdate(sql);
		}catch(Exception e){
			log.error(e.getMessage());
		}

	}

	public String select(String id_type, String service_type, String behavior, String classify, String recordDate){
		String value = "";
		try {
			String sql = "SELECT `counter` FROM kdcl_statistics_source WHERE id_type=@@ AND service_type=@@ AND behavior=@@ AND classify=@@ AND record_date=@@";
			sql = sql.replaceFirst("@@", new StringBuffer().append("'").append(id_type).append("'").toString());
			sql = sql.replaceFirst("@@", new StringBuffer().append("'").append(service_type).append("'").toString());
			sql = sql.replaceFirst("@@", new StringBuffer().append("'").append(behavior).append("'").toString());
			sql = sql.replaceFirst("@@", new StringBuffer().append("'").append(classify).append("'").toString());
			sql = sql.replaceFirst("@@", new StringBuffer().append("'").append(recordDate).append("'").toString());
			log.info("sql(select):" + sql);
			ResultSet rs = stmt.executeQuery(sql);

			if(rs.last()){
				value = rs.getString("counter");
			}
			return value;

		}catch(Exception e){
			log.error(e.getMessage());
		}
		return value;
	}

	public void dbInit(){
		try{
			conn = DriverManager.getConnection(url, user, password);
			stmt = conn.createStatement();
		}catch(Exception e){
			log.error(e.getMessage());
		}

	}

	public void closeAll( ) {
		try{
			if (stmt != null)
				stmt.close();
			if (conn != null)
				conn.close();
		}catch(Exception e){
			log.error(e.getMessage());
		}
	}



}
