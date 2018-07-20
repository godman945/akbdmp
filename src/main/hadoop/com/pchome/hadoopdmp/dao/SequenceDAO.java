package com.pchome.hadoopdmp.dao;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class SequenceDAO {

	private static Log log = LogFactory.getLog(SequenceDAO.class);

	Connection conn = null;
	String url = "jdbc:mysql://kddbdev.mypchome.com.tw:3306/akb"; // stg
	String jdbcDriver = "com.mysql.jdbc.Driver";
	String user = "keyword";
	String password =  "K1y0nLine";
	Statement stmt=null;
	String sql="";

	public SequenceDAO() { }
	
	public void updateSequence(int seq ) {
		try {
			String sql = " update sequence  set table_no = "+seq+" WHERE table_name ='pfp_ad_category_new' and table_char ='cateSeq' ";
			stmt.executeUpdate(sql);

		} catch (Exception e) {
			System.out.println(e);
		}
	}
	
	public int querySequence() {
		int table_no=0;
		try {
			String sql = " SELECT * FROM sequence  WHERE 1=1 and table_name ='pfp_ad_category_new' and table_char ='cateSeq' ";
			ResultSet rs = stmt.executeQuery(sql);

			while (rs.next()) {
				table_no = rs.getInt("table_no");
			}
			return table_no;

		} catch (Exception e) {
			System.out.println(e);
		}
		return table_no;
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
