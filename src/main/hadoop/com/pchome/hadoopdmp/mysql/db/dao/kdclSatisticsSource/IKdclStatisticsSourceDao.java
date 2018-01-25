package com.pchome.hadoopdmp.mysql.db.dao.kdclSatisticsSource;


import org.springframework.stereotype.Repository;

import com.pchome.hadoopdmp.data.mysql.pojo.KdclStatisticsSource;
import com.pchome.hadoopdmp.mysql.db.dao.base.IBaseDAO;

@Repository
public interface IKdclStatisticsSourceDao extends IBaseDAO<KdclStatisticsSource, Integer>{
	
public void deleteByBehaviorAndRecordDate(String behavior,String recordDate) throws Exception;
	
	public void delete(String id_type, String service_type, String behavior, String classify, String recordDate) throws Exception;
	
	public void insert(String id_type, String service_type, String behavior, String classify, int count, String recordDate) throws Exception;
	
	public String select(String id_type, String service_type, String behavior, String classify, String recordDate)  throws Exception;
}

