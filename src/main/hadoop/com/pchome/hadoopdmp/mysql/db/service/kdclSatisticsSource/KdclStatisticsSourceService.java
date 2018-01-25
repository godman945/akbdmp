package com.pchome.hadoopdmp.mysql.db.service.kdclSatisticsSource;


import org.springframework.stereotype.Service;

import com.pchome.hadoopdmp.data.mysql.pojo.KdclStatisticsSource;
import com.pchome.hadoopdmp.mysql.db.service.base.BaseService;

@Service
public class KdclStatisticsSourceService extends BaseService<KdclStatisticsSource, Integer> implements IKdclStatisticsSourceService{

	@Override
	public void deleteByBehaviorAndRecordDate(String behavior, String recordDate) throws Exception {
		
	}

	@Override
	public void delete(String id_type, String service_type, String behavior, String classify, String recordDate) throws Exception {
		
	}

	@Override
	public void insert(String id_type, String service_type, String behavior, String classify, int count,String recordDate) throws Exception {
		
	}

	@Override
	public String select(String id_type, String service_type, String behavior, String classify, String recordDate)	throws Exception {
		return null;
	}

	
}
