package com.pchome.akbdmp.mysql.db.service.dmp;


import org.springframework.stereotype.Service;

import com.pchome.akbdmp.data.mysql.pojo.KdclStatisticsSource;
import com.pchome.akbdmp.mysql.db.service.base.IBaseService;

@Service
public interface IKdclStatisticsSourceService extends IBaseService<KdclStatisticsSource, Integer>{

	public void deleteDmpCountByDate(String recordDate) throws Exception;

}
