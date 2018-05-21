package com.pchome.akbdmp.mysql.db.service.dmp;


import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.pchome.akbdmp.data.mysql.pojo.KdclStatisticsSource;
import com.pchome.akbdmp.mysql.db.dao.dmp.IKdclStatisticsSourceDao;
import com.pchome.akbdmp.mysql.db.service.base.BaseService;

@Service
public class KdclStatisticsSourceService extends BaseService<KdclStatisticsSource, Integer> implements IKdclStatisticsSourceService{
	
	@Autowired
	private IKdclStatisticsSourceDao kdclStatisticsSourceDao;

	@Override
	public void deleteDmpCountByDate(String recordDate) throws Exception {
		kdclStatisticsSourceDao.deleteDmpCountByDate(recordDate);
	}
	
}
