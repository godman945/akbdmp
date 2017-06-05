package com.pchome.akbdmp.mongo.db.service.classcount;

import com.pchome.akbdmp.data.mongo.pojo.ClassCountMongoBean;
import com.pchome.akbdmp.mongo.db.service.base.IBaseService;

public interface IClassCountService extends IBaseService<ClassCountMongoBean>{
	
	public ClassCountMongoBean saveOrUpdate(ClassCountMongoBean classCountMongoBean) throws Exception;
	
	public ClassCountMongoBean findUserId(String userId) throws Exception;
	
}
