package com.pchome.hadoopdmp.mysql.db.service.kdclSatisticsSource;


import org.springframework.stereotype.Service;

import com.pchome.hadoopdmp.data.mysql.pojo.KdclStatisticsSource;
import com.pchome.hadoopdmp.mysql.db.service.base.IBaseService;

@Service
public interface IKdclStatisticsSourceService extends IBaseService<KdclStatisticsSource, Integer>{

	public void deleteByBehaviorAndRecordDate(String behavior,String recordDate) throws Exception;
	
	public KdclStatisticsSource findKdclStatisticsSourceByBehaviorAndRecordDate(String behavior,String recordDate,String idType,String classify) throws Exception;
	
	public void delete(String id_type, String service_type, String behavior, String classify, String recordDate) throws Exception;
	
	public void insert(String id_type, String service_type, String behavior, String classify, int count, String recordDate) throws Exception;
	
	public String select(String id_type, String service_type, String behavior, String classify, String recordDate)  throws Exception;
}
