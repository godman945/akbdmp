//package com.pchome.hadoopdmp.mysql.db.service.kdclSatisticsSource;
//
//
//import org.springframework.beans.factory.annotation.Autowired;
//import org.springframework.stereotype.Service;
//
//import com.pchome.hadoopdmp.data.mysql.pojo.KdclStatisticsSource;
//import com.pchome.hadoopdmp.mysql.db.dao.kdclSatisticsSource.IKdclStatisticsSourceDao;
//import com.pchome.hadoopdmp.mysql.db.service.base.BaseService;
//
//@Service
//public class KdclStatisticsSourceService extends BaseService<KdclStatisticsSource, Integer> implements IKdclStatisticsSourceService{
//
//	@Autowired
//	IKdclStatisticsSourceDao kdclStatisticsSourceDao;
//	
//	@Override
//	public void deleteByBehaviorAndRecordDate(String behavior, String recordDate) throws Exception {
//		kdclStatisticsSourceDao.deleteByBehaviorAndRecordDate(behavior,recordDate);
//	}
//
//	@Override
//	public void delete(String id_type, String service_type, String behavior, String classify, String recordDate) throws Exception {
//		
//	}
//
//	@Override
//	public void insert(String id_type, String service_type, String behavior, String classify, int count,String recordDate) throws Exception {
//		
//	}
//
//	@Override
//	public String select(String id_type, String service_type, String behavior, String classify, String recordDate)	throws Exception {
//		return null;
//	}
//
//	@Override
//	public KdclStatisticsSource findKdclStatisticsSourceByBehaviorAndRecordDate(String behavior, String recordDate,String idType,String classify) throws Exception {
//		return kdclStatisticsSourceDao.findKdclStatisticsSourceByBehaviorAndRecordDate(behavior,recordDate,idType,classify);
//	}
//
//	
//}
