package com.pchome.akbdmp.mongo.db.dao.classcount;

import com.pchome.akbdmp.data.mongo.pojo.ClassCountMongoBean;
import com.pchome.akbdmp.mongo.db.dao.base.IBaseDAO;

public interface IClassCountDAO extends IBaseDAO<ClassCountMongoBean> {


	public ClassCountMongoBean saveOrUpdate(ClassCountMongoBean classCountMongoBean) throws Exception;

	public ClassCountMongoBean findUserId(String userId) throws Exception;
}
