package com.pchome.soft.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
public class MysqlUtil {

	private static MysqlUtil singleton = new MysqlUtil();
	
	private Connection connect = null;
	private Statement statement = null;
	private PreparedStatement preparedStatement = null;
	private ResultSet resultSet = null;
	
	
	Log log = LogFactory.getLog(MysqlUtil.class);

	synchronized static public MysqlUtil getInstance() {
		return singleton;
	}
	
	public void setConnection(String url,String user,String password) throws Exception{
		connect = DriverManager.getConnection(url, user, password);
		statement = connect.createStatement();
	}
	
	
	public void closeConnection() throws Exception{
		if(connect != null){
			connect.close();
		}
		if(preparedStatement != null){
			preparedStatement.close();
		}
		if(resultSet != null){
			resultSet.close();
		}
		if(statement != null){
			statement.close();
		}
	}
	
	
	public ResultSet query(String sql) throws Exception{
		log.info("query : "+sql);
		return statement.executeQuery(sql);

	}
	
	public Connection getConnect() {
		return connect;
	}

	public static void main(String args[]){
		try {
//			String url = "jdbc:mysql://kddbdev.mypchome.com.tw:3306/akb_video";
//			String jdbcDriver = "com.mysql.jdbc.Driver";
//			String user = "keyword";
//			String password =  "K1y0nLine";
//			Connection con = MysqlUtil.getInstance().getConnection(url, user, password);
//			ResultSet resultSet = MysqlUtil.getInstance().query("select * from pfp_code_convert ");
//			while(resultSet.next()){
//				System.out.println(resultSet.getString("convert_seq"));
//			}
//			
//			
//			resultSet = MysqlUtil.getInstance().query("select * from pfp_code_convert_rule ");
//			while(resultSet.next()){
//				System.out.println(resultSet.getString("convert_rule_id"));
//			}
//			
//			
//			
////			System.out.println(con);
////			
////			
//			MysqlUtil.getInstance().closeConnection();
//			
//			System.out.println(con);
//			
//			con.close();
//			System.out.println(con.isClosed());
//			System.out.println("SSS");
//			con.close();
//			
//			System.out.println(con.isClosed());
//			
//			System.out.println(con);
			
			
			
//			System.out.println(MysqlUtil.getInstance().getConnection(url, user, password));
//			MysqlUtil.getInstance().closeConnection();
//			System.out.println(MysqlUtil.getInstance().getConnection(url, user, password));
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}
}

