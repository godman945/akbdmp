package com.pchome.hadoopdmp.mysql.db.dao.kdclSatisticsSource;


import org.springframework.stereotype.Repository;

import com.pchome.hadoopdmp.data.mysql.pojo.KdclStatisticsSource;
import com.pchome.hadoopdmp.mysql.db.dao.base.BaseDAO;

@Repository
public class KdclStatisticsSourceDao extends BaseDAO<KdclStatisticsSource, Integer> implements IKdclStatisticsSourceDao{

	@Override
	public void deleteByBehaviorAndRecordDate(String behavior, String recordDate) throws Exception {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void delete(String id_type, String service_type, String behavior, String classify, String recordDate)
			throws Exception {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void insert(String id_type, String service_type, String behavior, String classify, int count,
			String recordDate) throws Exception {
		// TODO Auto-generated method stub
		
	}

	@Override
	public String select(String id_type, String service_type, String behavior, String classify, String recordDate)
			throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

	

	

	
}
